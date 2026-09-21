#![forbid(unsafe_code)]

#[cfg(not(test))]
use crabbot_core::types::{Request, Response};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol},
};

#[cfg(not(test))]
use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::{fs, net::TcpStream, sync::Mutex};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
#[cfg(not(test))]
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type StagedEvent = (Value, Option<String>, Option<String>, Option<String>);

struct App {
    client: reqwest::Client,
    #[cfg_attr(test, allow(dead_code))]
    socket: Arc<Mutex<Option<Socket>>>,
    attachments: Arc<Mutex<BTreeMap<String, Attachment>>>,
    inbox: Arc<Mutex<Inbox>>,
    history: Arc<Mutex<HistoryPoll>>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Attachment {
    url: String,
    name: String,
    mime: Option<String>,
    size: Option<usize>,
}

const MEDIA_LIMIT: usize = 4 * 1024 * 1024;
const HISTORY_LIMIT: u64 = 15;
const HISTORY_PAGES: usize = 64;
const INBOX_LIMIT: usize = 4_096;
const EVENT_BYTES_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const INBOX_BYTES_LIMIT: usize = 16 * 1024 * 1024;
const HISTORY_INTERVAL: Duration = Duration::from_secs(60);
const MEDIA_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MEDIA_CACHE_LIMIT: u64 = 64 * 1024 * 1024;
const MEDIA_CACHE_FILES: usize = 256;
const RATE_LIMIT_MAX: Duration = Duration::from_secs(300);
const RATE_LIMIT_POLL_SLICE: Duration = Duration::from_secs(30);
const SOCKET_EVENT_LIMIT: usize = 4_096;
#[derive(Clone, Deserialize, Serialize)]
struct Pending {
    sequence: u64,
    event: Value,
    #[serde(default = "history_pending")]
    history: bool,
    channel: Option<String>,
    timestamp: Option<String>,
    envelope: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Inbox {
    #[serde(default = "inbox_version")]
    version: u8,
    #[serde(default = "first_sequence")]
    next_sequence: u64,
    #[serde(default)]
    channels: BTreeMap<String, String>,
    #[serde(default)]
    scanned: BTreeMap<String, String>,
    #[serde(default)]
    cursors: BTreeMap<String, String>,
    #[serde(default)]
    socket: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    attachments: BTreeMap<String, Attachment>,
    #[serde(default)]
    pending: Vec<Pending>,
}

impl Default for Inbox {
    fn default() -> Self {
        Self {
            version: inbox_version(),
            next_sequence: first_sequence(),
            channels: BTreeMap::new(),
            scanned: BTreeMap::new(),
            cursors: BTreeMap::new(),
            socket: BTreeMap::new(),
            attachments: BTreeMap::new(),
            pending: Vec::new(),
        }
    }
}

#[derive(Default)]
struct HistoryPoll {
    next: Option<Instant>,
    backoff: Duration,
    next_channel: usize,
}

fn inbox_version() -> u8 {
    1
}

fn first_sequence() -> u64 {
    1
}

fn history_pending() -> bool {
    true
}

fn persisted_event(event: &Value) -> Value {
    let mut event = event.clone();

    let Some(text) = event["text"].as_str().filter(|text| !text.is_empty()).map(str::to_owned)
    else {
        return event;
    };

    if let Some(content) = event["content"].as_array_mut() {
        content.retain(|item| !(item["kind"] == "text" && item["text"] == text));
    }

    event
}

fn exposed_event(event: &Value) -> Value {
    let mut event = event.clone();
    let Some(text) = event["text"].as_str().filter(|text| !text.is_empty()).map(str::to_owned)
    else {
        return event;
    };

    let has_text = event["content"].as_array().is_some_and(|content| {
        content.iter().any(|item| item["kind"] == "text" && item["text"] == text)
    });

    if !has_text {
        if let Some(content) = event["content"].as_array_mut() {
            content.insert(0, json!({"kind": "text", "text": text}));
        } else {
            event["content"] = json!([{"kind": "text", "text": text}]);
        }
    }

    event
}

impl Inbox {
    fn pending_event(&self) -> Option<Value> {
        let pending = self.pending.first()?;
        let mut event = exposed_event(&pending.event);
        event["gateway_sequence"] = json!(pending.sequence);
        Some(event)
    }

    fn stage(
        &mut self,
        events: impl IntoIterator<Item = StagedEvent>,
        attachments: &BTreeMap<String, Attachment>,
        channel: &str,
        scanned: Option<&str>,
        cursor: Option<&str>,
    ) -> crabbot_core::Result<()> {
        let events = events.into_iter().collect::<Vec<_>>();
        let staged = self.staged(&events, attachments, channel, scanned, cursor)?;
        save_inbox(&staged)?;
        *self = staged;
        Ok(())
    }

    fn stage_socket(
        &mut self,
        events: impl IntoIterator<Item = StagedEvent>,
        attachments: &BTreeMap<String, Attachment>,
        channel: &str,
    ) -> crabbot_core::Result<()> {
        let events = events.into_iter().collect::<Vec<_>>();
        let staged = self.staged_with_history(&events, attachments, channel, None, None, false)?;
        save_inbox(&staged)?;
        *self = staged;
        Ok(())
    }

    fn staged(
        &self,
        events: &[StagedEvent],
        attachments: &BTreeMap<String, Attachment>,
        channel: &str,
        scanned: Option<&str>,
        cursor: Option<&str>,
    ) -> crabbot_core::Result<Self> {
        self.staged_with_history(events, attachments, channel, scanned, cursor, true)
    }

    fn staged_with_history(
        &self,
        events: &[StagedEvent],
        attachments: &BTreeMap<String, Attachment>,
        channel: &str,
        scanned: Option<&str>,
        cursor: Option<&str>,
        history: bool,
    ) -> crabbot_core::Result<Self> {
        if self.pending.len().saturating_add(events.len()) > INBOX_LIMIT {
            return Err(crabbot_core::Error::Denied("Slack inbox is full.".into()));
        }

        for (event, _, _, _) in events {
            if serde_json::to_vec(&persisted_event(event))?.len() > EVENT_BYTES_LIMIT {
                return Err(crabbot_core::Error::Denied(
                    "Slack normalized event exceeds the size limit.".into(),
                ));
            }
        }

        let mut staged = self.clone();
        staged.attachments = attachments.clone();

        if let Some(timestamp) = scanned {
            staged.stage_scan(channel, timestamp, cursor);
        } else if let Some(cursor) = cursor {
            staged.cursors.insert(channel.to_owned(), cursor.to_owned());
        } else {
            staged.cursors.remove(channel);
        }

        for (event, channel, timestamp, envelope) in events {
            staged.pending.push(Pending {
                sequence: staged.next_sequence,
                event: persisted_event(event),
                history,
                channel: channel.clone(),
                timestamp: timestamp.clone(),
                envelope: envelope.clone(),
            });

            if !history
                && let (Some(channel), Some(timestamp)) = (channel.as_deref(), timestamp.as_deref())
            {
                staged.remember_socket(channel, timestamp);
            }

            staged.next_sequence = staged.next_sequence.saturating_add(1).max(1);
        }

        staged.retain_attachments();
        Ok(staged)
    }

    fn fitting_count(
        &self,
        events: &[StagedEvent],
        attachments: &BTreeMap<String, Attachment>,
        channel: &str,
        scanned: Option<&str>,
        cursor: Option<&str>,
    ) -> crabbot_core::Result<usize> {
        let mut low = 0;
        let mut high = events.len().min(INBOX_LIMIT.saturating_sub(self.pending.len()));

        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let candidate =
                self.staged(&events[..middle], attachments, channel, scanned, cursor)?;

            if serde_json::to_vec(&candidate)?.len() <= INBOX_BYTES_LIMIT {
                low = middle;
            } else {
                high = middle - 1;
            }
        }

        Ok(low)
    }

    fn serialized_size(&self) -> crabbot_core::Result<usize> {
        Ok(serde_json::to_vec(self)?.len())
    }

    fn acknowledge(&mut self, sequence: u64) -> crabbot_core::Result<()> {
        let Some(index) = self.pending.iter().position(|item| item.sequence == sequence) else {
            return (sequence < self.next_sequence).then_some(()).ok_or_else(|| {
                crabbot_core::Error::Denied("Slack acknowledgement is stale.".into())
            });
        };

        let previous = self.clone();
        let pending = self.pending.remove(index);

        if pending.history
            && let (Some(channel), Some(timestamp)) = (pending.channel, pending.timestamp)
            && !self.cursors.contains_key(&channel)
        {
            let current = self.channels.entry(channel).or_default();

            if slack_timestamp_after(&timestamp, current) {
                *current = timestamp;
            }
        }

        self.retain_attachments();

        match save_inbox(self) {
            Ok(()) => Ok(()),

            Err(error) => {
                *self = previous;
                Err(error)
            }
        }
    }

    fn watermark(&self, channel: &str) -> Option<String> {
        [self.channels.get(channel), self.scanned.get(channel)]
            .into_iter()
            .flatten()
            .max_by(|left, right| slack_timestamp_cmp(left, right))
            .cloned()
    }

    fn stage_scan(&mut self, channel: &str, timestamp: &str, cursor: Option<&str>) {
        let current = self.scanned.entry(channel.to_owned()).or_default();

        if slack_timestamp_after(timestamp, current) {
            *current = timestamp.to_owned();
        }

        match cursor {
            Some(cursor) => {
                self.cursors.insert(channel.to_owned(), cursor.to_owned());
            }

            None => {
                self.cursors.remove(channel);
            }
        }
    }

    fn remember_socket(&mut self, channel: &str, timestamp: &str) {
        self.socket.entry(channel.to_owned()).or_default().insert(timestamp.to_owned());

        while self.socket.values().map(BTreeSet::len).sum::<usize>() > SOCKET_EVENT_LIMIT {
            let Some(channel) = self.socket.keys().next().cloned() else { break };

            let Some(timestamp) =
                self.socket.get(&channel).and_then(|timestamps| timestamps.iter().next().cloned())
            else {
                self.socket.remove(&channel);
                continue;
            };

            let remove_channel = self.socket.get_mut(&channel).is_some_and(|timestamps| {
                timestamps.remove(&timestamp);
                timestamps.is_empty()
            });

            if remove_channel {
                self.socket.remove(&channel);
            }
        }
    }

    fn retain_attachments(&mut self) {
        let referenced = self
            .pending
            .iter()
            .flat_map(|pending| pending.event["content"].as_array().into_iter().flatten())
            .filter_map(|content| content["uri"].as_str())
            .filter_map(|uri| uri.strip_prefix("slack://file/"))
            .filter(|id| safe_id(id))
            .collect::<BTreeSet<_>>();

        self.attachments.retain(|id, _| referenced.contains(id.as_str()));
    }
}

fn inbox_path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_HOME").map(|home| PathBuf::from(home).join("slack-inbox.json"))
}

#[cfg_attr(test, allow(dead_code))]
fn load_inbox() -> crabbot_core::Result<Inbox> {
    let Some(path) = inbox_path() else {
        return Ok(Inbox {
            version: inbox_version(),
            next_sequence: first_sequence(),
            ..Inbox::default()
        });
    };

    let Some(bytes) = crabbot_file::load(&path, INBOX_BYTES_LIMIT as u64).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack inbox could not be loaded: {error}."))
    })?
    else {
        return Ok(Inbox {
            version: inbox_version(),
            next_sequence: first_sequence(),
            ..Inbox::default()
        });
    };

    let mut inbox = serde_json::from_slice::<Inbox>(&bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack inbox is invalid: {error}."))
    })?;

    if inbox.version != inbox_version() {
        return Err(crabbot_core::Error::Denied("Slack inbox version is unsupported.".into()));
    }

    inbox.next_sequence = inbox.next_sequence.max(1);
    Ok(inbox)
}

fn save_inbox(inbox: &Inbox) -> crabbot_core::Result<()> {
    let bytes = serde_json::to_vec(inbox)?;

    if bytes.len() > INBOX_BYTES_LIMIT {
        return Err(crabbot_core::Error::Denied(
            "Slack inbox exceeds the startup load limit.".into(),
        ));
    }

    let Some(path) = inbox_path() else {
        return Ok(());
    };

    crabbot_file::save(path, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack inbox could not be stored: {error}."))
    })
}

fn slack_timestamp_after(left: &str, right: &str) -> bool {
    slack_timestamp_cmp(left, right) == Ordering::Greater
}

fn slack_timestamp_cmp(left: &str, right: &str) -> Ordering {
    let left = left.split_once('.').unwrap_or((left, ""));

    let right = right.split_once('.').unwrap_or((right, ""));
    left.0
        .len()
        .cmp(&right.0.len())
        .then_with(|| left.0.cmp(right.0))
        .then_with(|| left.1.cmp(right.1))
}

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Slack client failed: {error}.")))?;

    let inbox = load_inbox()?;
    let app = Arc::new(App {
        client,
        socket: Arc::new(Mutex::new(None)),
        attachments: Arc::new(Mutex::new(inbox.attachments.clone())),
        inbox: Arc::new(Mutex::new(inbox)),
        history: Arc::new(Mutex::new(HistoryPoll::default())),
    });

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "slack".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: Vec::new(),
        },
        move |request| {
            let app = Arc::clone(&app);
            async move { call(&app, request).await }
        },
    )
    .await
}

#[cfg(not(test))]
async fn call(app: &App, request: Request) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let token = std::env::var("CRABBOT_SLACK_BOT_TOKEN").map_err(|_| {
        crabbot_core::Error::Denied("CRABBOT_SLACK_BOT_TOKEN is not configured.".into())
    })?;

    let result = match method.as_str() {
        "poll" => poll(app, &token).await?,

        "ack" => {
            let sequence = params["sequence"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("ack.sequence is required.".into()))?;
            let mut inbox = app.inbox.lock().await;
            inbox.acknowledge(sequence)?;
            let attachments = inbox.attachments.clone();
            drop(inbox);
            *app.attachments.lock().await = attachments;
            json!({"acknowledged": true})
        }

        "send" => send(&app.client, &token, &params).await?,
        "media" => media(app, &token, &params).await?,
        "info" => json!({"mode": "web_api", "socket_mode": true}),
        _ => return Ok(None),
    };

    Ok(Some(Response::ok(id, result)))
}

#[cfg(not(test))]
async fn api(
    client: &reqwest::Client,
    token: &str,
    method: &str,
    body: Value,
) -> crabbot_core::Result<Value> {
    api_at(client, token, method, body, "https://slack.com/api").await
}

async fn api_at(
    client: &reqwest::Client,
    token: &str,
    method: &str,
    body: Value,
    base: &str,
) -> crabbot_core::Result<Value> {
    let response = client
        .post(format!("{base}/{method}"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Slack request failed: {error}.")))?;

    let status = response.status();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    let body = collect_api_body(response.bytes_stream()).await?;
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,

        Err(_error) if status.as_u16() == 429 => {
            let retry_after = retry_after.unwrap_or_default();
            return Err(crabbot_core::Error::Denied(format!(
                "Slack request rate limited; retry_after={retry_after}."
            )));
        }

        Err(error) => {
            return Err(crabbot_core::Error::Denied(format!(
                "Slack response was invalid: {error}."
            )));
        }
    };

    if status.as_u16() == 429 || value["error"] == "ratelimited" {
        let retry_after = value["retry_after"].as_u64().or(retry_after).unwrap_or_default();
        return Err(crabbot_core::Error::Denied(format!(
            "Slack request rate limited; retry_after={retry_after}."
        )));
    }

    if value["ok"] != true {
        return Err(crabbot_core::Error::Denied(
            value["error"].as_str().unwrap_or("Slack rejected the request.").into(),
        ));
    }

    Ok(value)
}

async fn collect_api_body<S, C>(mut stream: S) -> crabbot_core::Result<Vec<u8>>
where
    S: futures_util::Stream<Item = Result<C, reqwest::Error>> + Unpin,
    C: AsRef<[u8]>,
{
    let mut body = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("Slack response failed: {error}."))
        })?;

        if chunk.as_ref().len() > EVENT_BYTES_LIMIT.saturating_sub(body.len()) {
            return Err(crabbot_core::Error::Denied(
                "Slack response exceeded the protocol limit.".into(),
            ));
        }

        body.extend_from_slice(chunk.as_ref());
    }

    Ok(body)
}

async fn pace_history(state: &Arc<Mutex<HistoryPoll>>) -> bool {
    let (wait, rate_limited) = {
        let state = state.lock().await;
        let now = Instant::now();
        let wait = state.next.map_or(Duration::ZERO, |next| next.saturating_duration_since(now));
        (wait, !wait.is_zero() && !state.backoff.is_zero())
    };

    if wait.is_zero() {
        return true;
    }

    if !rate_limited {
        return false;
    }

    tokio::time::sleep(wait.min(RATE_LIMIT_POLL_SLICE)).await;
    false
}

async fn history_succeeded(state: &Arc<Mutex<HistoryPoll>>) {
    let mut state = state.lock().await;
    state.next = Some(Instant::now() + HISTORY_INTERVAL);
    state.backoff = Duration::ZERO;
}

async fn history_rate_limited(state: &Arc<Mutex<HistoryPoll>>, error: &crabbot_core::Error) {
    let mut state = state.lock().await;
    let retry_after = error
        .to_string()
        .split_once("retry_after=")
        .and_then(|(_, value)| value.split_once('.').map(|(value, _)| value))
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs);
    let delay = retry_after.unwrap_or_else(|| {
        let seconds =
            state.backoff.as_secs().max(15).saturating_mul(2).min(RATE_LIMIT_MAX.as_secs());
        Duration::from_secs(seconds)
    });

    state.backoff = delay;
    state.next = Some(Instant::now() + delay.max(Duration::from_secs(1)).min(RATE_LIMIT_MAX));
}

#[cfg(not(test))]
async fn poll(app: &App, token: &str) -> crabbot_core::Result<Value> {
    let channels = std::env::var("CRABBOT_SLACK_CHANNELS").unwrap_or_default();

    if let Ok(app_token) = std::env::var("CRABBOT_SLACK_APP_TOKEN")
        && !app_token.trim().is_empty()
    {
        match socket_poll(app, &app_token, &channels).await {
            Ok(Some(events)) => return Ok(json!({"events": events})),

            Ok(None) => {}

            Err(_error) => {
                *app.socket.lock().await = None;
            }
        }
    }

    poll_at(app, token, &channels, "https://slack.com/api").await
}

async fn poll_at(
    app: &App,
    token: &str,
    channels: &str,
    base: &str,
) -> crabbot_core::Result<Value> {
    if let Some(event) = app.inbox.lock().await.pending_event() {
        return Ok(json!({"events": [event]}));
    }

    let channels = channels
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if channels.is_empty() {
        return Ok(json!({"events": []}));
    }

    if !pace_history(&app.history).await {
        return Ok(json!({"events": []}));
    }

    let mut history_polled = false;
    let start = {
        let mut history = app.history.lock().await;
        let start = history.next_channel % channels.len();
        history.next_channel = (start + 1) % channels.len();
        start
    };

    for offset in 0..channels.len() {
        let channel_index = (start + offset) % channels.len();
        let channel = &channels[channel_index];
        {
            let mut history = app.history.lock().await;
            history.next_channel = (channel_index + 1) % channels.len();
        }

        let (capacity, history_budget, oldest, start_cursor) = {
            let inbox = app.inbox.lock().await;
            (
                INBOX_LIMIT.saturating_sub(inbox.pending.len()),
                INBOX_BYTES_LIMIT.saturating_sub(inbox.serialized_size()?),
                inbox.watermark(channel),
                inbox.cursors.get(channel).cloned(),
            )
        };

        let mut cursor = start_cursor.clone();

        if capacity == 0 || history_budget == 0 {
            continue;
        }

        let mut pages = 0;
        let mut messages = Vec::new();
        let mut message_bytes = 2_usize;
        let mut history_complete = true;
        let socket_events = app.inbox.lock().await.socket.get(channel).cloned().unwrap_or_default();

        loop {
            let remaining = capacity.saturating_sub(messages.len()) as u64;
            let mut body = json!({
                "channel": channel,
                "limit": HISTORY_LIMIT.min(remaining),
                "inclusive": false,
            });

            if cursor.is_none()
                && let Some(oldest) = oldest.as_deref()
            {
                body["oldest"] = json!(oldest);
            }

            if let Some(cursor) = cursor.as_deref() {
                body["cursor"] = json!(cursor);
            }

            let value = match api_at(&app.client, token, "conversations.history", body, base).await
            {
                Ok(value) => {
                    history_polled = true;
                    value
                }

                Err(error) => {
                    if error.to_string().contains("rate limited") {
                        history_rate_limited(&app.history, &error).await;
                        return Ok(json!({"events": []}));
                    }

                    return Err(error);
                }
            };

            let page = value["messages"].as_array().cloned().unwrap_or_default();
            let page_bytes = serde_json::to_vec(&page)?.len();
            messages.extend(page);
            let next_cursor = value["response_metadata"]["next_cursor"]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned);

            if messages.len() >= capacity {
                history_complete = next_cursor.is_none();
                cursor = next_cursor;
                break;
            }

            message_bytes = message_bytes.saturating_add(page_bytes);

            if message_bytes > history_budget {
                history_complete = false;
                cursor = next_cursor;
                break;
            }

            cursor = next_cursor;
            pages += 1;

            if cursor.is_none() {
                break;
            }

            if pages >= HISTORY_PAGES {
                history_complete = false;
                break;
            }
        }

        let mut seen = BTreeSet::new();
        let mut events = Vec::new();
        let mut scanned = None;

        for message in messages {
            let Some(timestamp) = message["ts"].as_str().filter(|value| !value.is_empty()) else {
                continue;
            };

            if socket_events.contains(timestamp) {
                continue;
            }

            if !seen.insert(timestamp.to_owned()) || !message_supported(&message) {
                if scanned.as_deref().is_none_or(|value| slack_timestamp_after(timestamp, value)) {
                    scanned = Some(timestamp.to_owned());
                }

                continue;
            }

            let text = message["text"].as_str().unwrap_or_default();
            let content = remember_files(&message, &app.attachments).await;

            if text.is_empty() && content.is_empty() {
                if scanned.as_deref().is_none_or(|value| slack_timestamp_after(timestamp, value)) {
                    scanned = Some(timestamp.to_owned());
                }

                continue;
            }

            events.push((
                json!({
                    "id": timestamp,
                    "chat": channel,
                    "sender": message["user"].as_str().unwrap_or_default(),
                    "private": channel.starts_with('D'),
                    "text": text,
                    "thread": message["thread_ts"].as_str(),
                    "content": content,
                }),
                Some(channel.to_owned()),
                Some(timestamp.to_owned()),
                None,
            ));
        }

        events.sort_by(|left, right| {
            let left = left.2.as_deref().unwrap_or_default();
            let right = right.2.as_deref().unwrap_or_default();
            slack_timestamp_cmp(left, right)
        });

        let attachments = app.attachments.lock().await.clone();
        let mut inbox = app.inbox.lock().await;
        let fitting = inbox.fitting_count(
            &events,
            &attachments,
            channel,
            scanned.as_deref().filter(|_| history_complete),
            cursor.as_deref(),
        )?;

        if fitting < events.len() {
            continue;
        }

        inbox.stage(events, &attachments, channel, scanned.as_deref(), cursor.as_deref())?;
        drop(inbox);
    }

    if history_polled {
        history_succeeded(&app.history).await;
    }

    Ok(json!({
        "events": app.inbox.lock().await.pending_event().into_iter().collect::<Vec<_>>(),
    }))
}

#[cfg(not(test))]
async fn acknowledge_envelope(stream: &mut Socket, envelope: &str) -> crabbot_core::Result<()> {
    stream.send(Message::Text(json!({"envelope_id": envelope}).to_string().into())).await.map_err(
        |error| {
            crabbot_core::Error::Denied(format!(
                "Slack Socket Mode acknowledgement failed: {error}."
            ))
        },
    )
}

fn channel_allowed(channels: &str, channel: &str) -> bool {
    channels.split(',').map(str::trim).any(|value| value == channel)
}

fn message_supported(value: &Value) -> bool {
    value["hidden"] != true && (value["subtype"].is_null() || value["subtype"] == "file_share")
}

#[cfg(not(test))]
async fn socket_poll(
    app: &App,
    token: &str,
    channels: &str,
) -> crabbot_core::Result<Option<Vec<Value>>> {
    let mut socket = app.socket.lock().await;

    if socket.is_none() {
        let value = api(&app.client, token, "apps.connections.open", json!({})).await?;
        let url = value["url"].as_str().ok_or_else(|| {
            crabbot_core::Error::Denied("Slack Socket Mode did not return a URL.".into())
        })?;

        *socket = Some(
            connect_async(url)
                .await
                .map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Slack Socket Mode connection failed: {error}."
                    ))
                })?
                .0,
        );
    }

    let pending = app.inbox.lock().await.pending.first().cloned();

    if let Some(pending) = pending {
        let stream = socket.as_mut().expect("socket was initialized");

        if let Some(envelope) = pending.envelope.as_deref() {
            acknowledge_envelope(stream, envelope).await?;

            let mut inbox = app.inbox.lock().await;

            if let Some(item) =
                inbox.pending.iter_mut().find(|item| item.sequence == pending.sequence)
            {
                item.envelope = None;

                save_inbox(&inbox)?;
            }
        }

        let mut event = pending.event;
        event["gateway_sequence"] = json!(pending.sequence);
        return Ok(Some(vec![event]));
    }

    let message = {
        let stream = socket.as_mut().expect("socket was initialized");
        tokio::time::timeout(Duration::from_secs(25), stream.next()).await.map_err(|_| {
            crabbot_core::Error::Denied("Slack Socket Mode polling timed out.".into())
        })?
    };

    let message = match message {
        None => {
            *socket = None;
            return Ok(None);
        }

        Some(Ok(message)) => message,

        Some(Err(error)) => {
            *socket = None;
            return Err(crabbot_core::Error::Denied(format!("Slack Socket Mode failed: {error}.")));
        }
    };

    if let Message::Close(_) = &message {
        *socket = None;
        return Ok(None);
    }

    let Message::Text(text) = message else {
        if !matches!(&message, &Message::Ping(_) | &Message::Pong(_)) {
            *socket = None;
            return Ok(None);
        }

        return Ok(Some(Vec::new()));
    };

    let value: Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack Socket Mode event was invalid: {error}."))
    })?;

    if value["type"] == "disconnect" || value["payload"]["type"] == "disconnect" {
        *socket = None;
        return Ok(None);
    }

    let stream = socket.as_mut().expect("socket was initialized");
    let envelope = value["envelope_id"].as_str().map(str::to_owned);

    if let Some(envelope) = envelope.as_deref()
        && (value["payload"]["event"]["type"] != "message"
            || !message_supported(&value["payload"]["event"]))
    {
        acknowledge_envelope(stream, envelope).await?;
    }

    let event = &value["payload"]["event"];

    if event["type"] != "message" || !message_supported(event) {
        return Ok(Some(Vec::new()));
    }

    let Some(chat) = event["channel"].as_str() else {
        if let Some(envelope) = envelope.as_deref() {
            acknowledge_envelope(stream, envelope).await?;
        }

        return Ok(Some(Vec::new()));
    };

    if !channel_allowed(channels, chat) {
        if let Some(envelope) = envelope.as_deref() {
            acknowledge_envelope(stream, envelope).await?;
        }

        return Ok(Some(Vec::new()));
    }

    let Some(timestamp) = event["ts"].as_str().filter(|value| !value.is_empty()) else {
        if let Some(envelope) = envelope.as_deref() {
            acknowledge_envelope(stream, envelope).await?;
        }

        return Ok(Some(Vec::new()));
    };

    let text = event["text"].as_str().unwrap_or_default();
    let content = remember_files(event, &app.attachments).await;

    if text.is_empty() && content.is_empty() {
        if let Some(envelope) = envelope.as_deref() {
            acknowledge_envelope(stream, envelope).await?;
        }

        return Ok(Some(Vec::new()));
    }

    let event = json!({
        "id": timestamp,
        "chat": chat,
        "sender": event["user"].as_str().unwrap_or_default(),
        "private": chat.starts_with('D'),
        "text": text,
        "thread": event["thread_ts"].as_str(),
        "content": content,
    });

    let attachments = app.attachments.lock().await.clone();
    app.inbox.lock().await.stage_socket(
        [(event, Some(chat.to_owned()), Some(timestamp.to_owned()), envelope.clone())],
        &attachments,
        "",
    )?;

    if let Some(envelope) = envelope {
        acknowledge_envelope(stream, &envelope).await?;
        let mut inbox = app.inbox.lock().await;

        if let Some(item) = inbox.pending.last_mut() {
            item.envelope = None;
            save_inbox(&inbox)?;
        }
    }

    Ok(app.inbox.lock().await.pending_event().map(|event| vec![event]))
}

async fn remember_files(
    value: &Value,
    attachments: &Arc<Mutex<BTreeMap<String, Attachment>>>,
) -> Vec<Value> {
    let mut content = Vec::new();

    if let Some(text) = value["text"].as_str().filter(|text| !text.is_empty()) {
        content.push(json!({"kind": "text", "text": text}));
    }

    let Some(files) = value["files"].as_array() else { return content };

    let mut stored = attachments.lock().await;

    for file in files.iter().take(8) {
        let Some(id) = file["id"].as_str().filter(|value| safe_id(value)) else { continue };

        let Some(url) =
            file["url_private_download"].as_str().or_else(|| file["url_private"].as_str())
        else {
            continue;
        };

        if !allowed_url(url) {
            continue;
        }

        let name = file["name"].as_str().unwrap_or("attachment").to_owned();
        let mime = file["mimetype"].as_str().map(str::to_owned);
        let size = file["size"].as_u64().and_then(|value| usize::try_from(value).ok());
        stored.insert(
            id.to_owned(),
            Attachment { url: url.to_owned(), name: name.clone(), mime: mime.clone(), size },
        );
        let uri = format!("slack://file/{id}");
        let kind = if mime.as_deref().is_some_and(|value| value.starts_with("audio/")) {
            "audio"
        } else if mime.as_deref().is_some_and(|value| value.starts_with("image/")) {
            "image"
        } else {
            "file"
        };

        if kind == "image" {
            content.push(json!({"kind": kind, "uri": uri, "alt": name}));
        } else if kind == "audio" {
            content.push(json!({"kind": kind, "uri": uri, "mime": mime}));
        } else {
            content.push(json!({"kind": kind, "uri": uri, "name": name, "mime": mime}));
        }
    }

    content
}

async fn media(app: &App, token: &str, params: &Value) -> crabbot_core::Result<Value> {
    let uri = params["uri"]
        .as_str()
        .and_then(|value| value.strip_prefix("slack://file/"))
        .filter(|value| safe_id(value))
        .ok_or_else(|| crabbot_core::Error::Denied("Slack media URI is invalid.".into()))?;

    let attachment =
        app.attachments.lock().await.get(uri).cloned().ok_or_else(|| {
            crabbot_core::Error::Denied("Slack attachment is unavailable.".into())
        })?;

    if attachment.size.is_some_and(|size| size > MEDIA_LIMIT) {
        return Err(crabbot_core::Error::Denied("Slack attachment is too large.".into()));
    }

    let root = std::env::var_os("CRABBOT_MEDIA")
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME")
                .map(|value| PathBuf::from(value).join("media").into_os_string())
        })
        .map(PathBuf::from)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_MEDIA or CRABBOT_HOME is required.".into())
        })?;
    media_at(app, token, uri, &attachment, &root).await
}

async fn media_at(
    app: &App,
    token: &str,
    uri: &str,
    attachment: &Attachment,
    root: &Path,
) -> crabbot_core::Result<Value> {
    cleanup_media(root);
    fs::create_dir_all(root).await?;
    let name = safe_name(&attachment.name).unwrap_or_else(|| "attachment.bin".into());
    let path = root.join(format!("slack-{uri}-{name}"));

    if cached_media(&path) {
        return Ok(json!({"uri": format!("file://{}", path.display()), "mime": attachment.mime}));
    }

    let response =
        app.client.get(&attachment.url).bearer_auth(token).send().await.map_err(|error| {
            crabbot_core::Error::Denied(format!("Slack media failed: {error}."))
        })?;

    if !response.status().is_success() {
        return Err(crabbot_core::Error::Denied("Slack media download was rejected.".into()));
    }

    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("Slack media response failed: {error}."))
        })?;

        if chunk.len() > MEDIA_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied("Slack attachment is too large.".into()));
        }

        bytes.extend_from_slice(&chunk);
    }

    crabbot_file::save(&path, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack media storage failed: {error}."))
    })?;

    cleanup_media(root);
    Ok(json!({"uri": format!("file://{}", path.display()), "mime": attachment.mime}))
}

fn cached_media(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() <= MEDIA_LIMIT as u64)
}

fn cleanup_media(root: &Path) {
    cleanup_media_with_limits(root, MEDIA_CACHE_LIMIT, MEDIA_CACHE_FILES);
}

fn cleanup_media_with_limits(root: &Path, limit: u64, file_limit: usize) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    let cutoff = SystemTime::now().checked_sub(MEDIA_TTL).unwrap_or(SystemTime::UNIX_EPOCH);
    let mut files = Vec::new();

    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|value| value.is_file()) {
            continue;
        }

        let Ok(metadata) = entry.metadata() else { continue };

        if metadata.modified().is_ok_and(|value| value < cutoff) {
            let _ = std::fs::remove_file(entry.path());
            continue;
        }

        let Ok(modified) = metadata.modified() else { continue };

        files.push((entry.path(), modified, metadata.len()));
    }

    files.sort_by_key(|(_, modified, _)| *modified);
    let mut total = files.iter().map(|(_, _, size)| *size).sum::<u64>();
    let mut count = files.len();

    for (path, _, size) in files {
        if total <= limit && count <= file_limit {
            break;
        }

        if std::fs::remove_file(path).is_ok() {
            total = total.saturating_sub(size);
            count = count.saturating_sub(1);
        }
    }
}

fn allowed_url(value: &str) -> bool {
    reqwest::Url::parse(value).ok().is_some_and(|url| {
        url.scheme() == "https"
            && matches!(url.host_str(), Some("files.slack.com" | "slack-files.com"))
    })
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn safe_name(value: &str) -> Option<String> {
    let value = Path::new(value).file_name()?.to_str()?;
    let mut name = value
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
            {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();

    while name.ends_with([' ', '.']) {
        name.pop();
    }

    if name.is_empty() || name == "." || name == ".." {
        return None;
    }

    let base = name.split('.').next().unwrap_or_default();
    let bytes = base.as_bytes();
    let device = matches!(base.to_ascii_uppercase().as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (bytes.len() == 4
            && (bytes[..3].eq_ignore_ascii_case(b"COM")
                || bytes[..3].eq_ignore_ascii_case(b"LPT"))
            && bytes[3].is_ascii_digit()
            && bytes[3] != b'0');

    if device {
        name.insert(0, '_');
    }

    while name.len() > 128 {
        name.pop();
    }

    (!name.is_empty()).then_some(name)
}

#[cfg(not(test))]
async fn send(
    client: &reqwest::Client,
    token: &str,
    params: &Value,
) -> crabbot_core::Result<Value> {
    send_at(client, token, params, "https://slack.com/api").await
}

async fn send_at(
    client: &reqwest::Client,
    token: &str,
    params: &Value,
    base: &str,
) -> crabbot_core::Result<Value> {
    let channel = params["chat"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Slack send requires a chat.".into()))?;

    let text = params["text"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Slack send requires text.".into()))?;

    api_at(
        client,
        token,
        "chat.postMessage",
        json!({"channel": channel, "text": text, "thread_ts": params["thread"]}),
        base,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the Slack test listener: {error}."),
        }
    }

    #[tokio::test]
    async fn normalizes_text_and_rich_files() {
        let attachments = Arc::new(Mutex::new(BTreeMap::new()));
        let value = json!({
            "text": "hello",
            "files": [
                {"id": "F1", "name": "diagram.png", "mimetype": "image/png", "url_private_download": "https://files.slack.com/files-pri/F1/download"},
                {"id": "F2", "name": "voice.ogg", "mimetype": "audio/ogg", "url_private": "https://slack-files.com/F2"},
                {"id": "F3", "name": "note.txt", "mimetype": "text/plain", "url_private": "https://files.slack.com/files-pri/F3/download"}
            ]
        });

        let content = remember_files(&value, &attachments).await;

        assert_eq!(content[0]["kind"], "text");
        assert_eq!(content[1]["kind"], "image");
        assert_eq!(content[2]["kind"], "audio");
        assert_eq!(content[3]["kind"], "file");
        assert_eq!(attachments.lock().await.len(), 3);
    }

    #[test]
    fn validates_urls_and_names() {
        assert!(allowed_url("https://files.slack.com/files-pri/F1/download"));
        assert!(allowed_url("https://slack-files.com/F2"));
        assert!(!allowed_url("https://example.com/file"));
        assert_eq!(safe_name("../note.txt").as_deref(), Some("note.txt"));
        assert_eq!(safe_name("report:?.txt").as_deref(), Some("report__.txt"));
        assert_eq!(safe_name("CON.txt").as_deref(), Some("_CON.txt"));
        assert_eq!(safe_name("trail. ").as_deref(), Some("trail"));
        assert!(safe_name("/").is_none());
    }

    #[test]
    fn accepts_file_share_messages_only_with_supported_subtypes() {
        assert!(message_supported(&json!({"type": "message"})));
        assert!(message_supported(&json!({"type": "message", "subtype": "file_share"})));
        assert!(!message_supported(&json!({"type": "message", "subtype": "bot_message"})));
        assert!(!message_supported(&json!({"type": "message", "subtype": "message_changed"})));
        assert!(!message_supported(
            &json!({"type": "message", "subtype": "file_share", "hidden": true})
        ));
    }

    #[test]
    fn cleans_expired_media_without_touching_pinned() {
        let root =
            std::env::temp_dir().join(format!("crabbot-slack-cleanup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("pinned")).unwrap();
        let expired = root.join("expired.bin");
        let current = root.join("current.bin");
        let pinned = root.join("pinned").join("image.bin");
        std::fs::write(&expired, b"expired").unwrap();
        std::fs::write(&current, b"current").unwrap();
        std::fs::write(&pinned, b"pinned").unwrap();
        let modified = SystemTime::now().checked_sub(MEDIA_TTL + Duration::from_secs(1)).unwrap();
        std::fs::File::open(&expired)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        cleanup_media(&root);

        assert!(!expired.exists());
        assert!(current.exists());
        assert!(pinned.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn bounds_media_cache_without_removing_pinned_files() {
        let root = std::env::temp_dir().join(format!("crabbot-slack-quota-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("pinned")).unwrap();
        let oldest = root.join("oldest.bin");
        let current = root.join("current.bin");
        let newest = root.join("newest.bin");
        std::fs::write(&oldest, b"old").unwrap();
        std::fs::write(&current, b"now").unwrap();
        std::fs::write(&newest, b"n").unwrap();
        std::fs::write(root.join("pinned/image.bin"), vec![b'x'; 100]).unwrap();
        let now = SystemTime::now();
        std::fs::File::open(&oldest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(now - Duration::from_secs(3)))
            .unwrap();
        std::fs::File::open(&current)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(now - Duration::from_secs(2)))
            .unwrap();
        std::fs::File::open(&newest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(now - Duration::from_secs(1)))
            .unwrap();
        cleanup_media_with_limits(&root, 5, 10);

        assert!(!oldest.exists());
        assert!(current.exists());
        assert!(newest.exists());
        cleanup_media_with_limits(&root, u64::MAX, 1);

        assert!(!current.exists());
        assert!(newest.exists());
        assert!(root.join("pinned/image.bin").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn reuses_completed_media_cache() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = b"complete";
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let root = std::env::temp_dir().join(format!("crabbot-slack-media-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let attachment = Attachment {
            url: format!("http://{address}/media"),
            name: "note.txt".into(),
            mime: Some("text/plain".into()),
            size: None,
        };

        media_at(&app, "token", "F1", &attachment, &root).await.unwrap();
        media_at(&app, "token", "F1", &attachment, &root).await.unwrap();

        assert_eq!(std::fs::read(root.join("slack-F1-note.txt")).unwrap(), b"complete");
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn failed_media_stream_does_not_leave_destination() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = b"short";
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n";
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let root =
            std::env::temp_dir().join(format!("crabbot-slack-media-failed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let attachment = Attachment {
            url: format!("http://{address}/media"),
            name: "note.txt".into(),
            mime: Some("text/plain".into()),
            size: None,
        };

        assert!(media_at(&app, "token", "F1", &attachment, &root).await.is_err());
        assert!(!root.join("slack-F1-note.txt").exists());
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn applies_the_configured_channel_allowlist() {
        assert!(channel_allowed("C1, D1", "C1"));
        assert!(channel_allowed("C1, D1", "D1"));
        assert!(!channel_allowed("C1, D1", "C2"));
        assert!(!channel_allowed("C1", "D1"));
        assert!(!channel_allowed("", "C1"));
        assert!(!channel_allowed("", "D1"));
    }

    #[test]
    fn retains_events_until_host_acknowledgement() {
        let mut inbox = Inbox::default();
        inbox
            .stage(
                [(json!({"id": "1.2", "chat": "C1"}), Some("C1".into()), Some("1.2".into()), None)],
                &BTreeMap::new(),
                "C1",
                None,
                None,
            )
            .unwrap();
        assert_eq!(inbox.pending_event().unwrap()["gateway_sequence"], 1);
        inbox.acknowledge(1).unwrap();
        inbox.acknowledge(1).unwrap();

        assert!(inbox.pending_event().is_none());
        assert_eq!(inbox.channels["C1"], "1.2");
    }

    #[test]
    fn socket_acknowledgement_does_not_advance_history_watermarks() {
        let mut inbox = Inbox::default();
        inbox
            .stage_socket(
                [(json!({"id": "2.0", "chat": "C1"}), Some("C1".into()), Some("2.0".into()), None)],
                &BTreeMap::new(),
                "",
            )
            .unwrap();
        inbox.acknowledge(1).unwrap();

        assert!(!inbox.channels.contains_key("C1"));
        assert!(inbox.socket["C1"].contains("2.0"));
    }

    #[test]
    fn fits_history_events_without_persisting_duplicate_text() {
        let text = "x".repeat(1_500_000);
        let event = json!({
            "id": "1.0",
            "chat": "C1",
            "text": text,
            "content": [{"kind": "text", "text": text}]
        });

        let staged = [(event, Some("C1".into()), Some("1.0".into()), None)];
        let mut inbox = Inbox::default();
        inbox.channels.insert("other".into(), "x".repeat(14_000_000));

        assert_eq!(
            inbox.fitting_count(&staged, &BTreeMap::new(), "C1", None, Some("older")).unwrap(),
            1
        );
        inbox.stage(staged, &BTreeMap::new(), "C1", None, Some("older")).unwrap();

        assert!(inbox.pending[0].event["content"].as_array().unwrap().is_empty());
        assert_eq!(inbox.pending_event().unwrap()["content"][0]["text"], text);
    }

    #[test]
    fn retains_attachment_metadata_until_event_acknowledgement() {
        let attachment = Attachment {
            url: "https://files.slack.com/files-pri/F1/download".into(),
            name: "note.txt".into(),
            mime: Some("text/plain".into()),
            size: None,
        };

        let mut inbox = Inbox::default();
        inbox
            .stage(
                [(json!({"content":[{"kind":"file","uri":"slack://file/F1"}]}), None, None, None)],
                &BTreeMap::from([(String::from("F1"), attachment)]),
                "C1",
                None,
                None,
            )
            .unwrap();
        assert!(inbox.attachments.contains_key("F1"));
        inbox.acknowledge(1).unwrap();

        assert!(inbox.attachments.is_empty());
    }

    #[tokio::test]
    async fn polls_all_channels_and_preserves_direct_message_privacy() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for body in [
                br#"{"ok":true,"messages":[{"ts":"1","user":"U1","text":"group"}]}"#.to_vec(),
                br#"{"ok":true,"messages":[{"ts":"2","user":"U2","text":"direct"}]}"#.to_vec(),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let value = poll_at(&app, "token", "C1,D1", &format!("http://{address}")).await.unwrap();

        assert_eq!(value["events"][0]["chat"], "C1");
        assert_eq!(value["events"][0]["private"], false);
        assert_eq!(app.inbox.lock().await.pending.len(), 2);
        app.inbox.lock().await.acknowledge(1).unwrap();
        let next = app.inbox.lock().await.pending_event().unwrap();

        assert_eq!(next["chat"], "D1");
        assert_eq!(next["private"], true);
        server.await.unwrap();
    }

    #[test]
    fn bounds_event_and_inbox_sizes() {
        let mut inbox = Inbox::default();
        let oversized = "x".repeat(EVENT_BYTES_LIMIT);

        assert!(
            inbox
                .stage(
                    [(json!({"text": oversized}), None, None, None)],
                    &BTreeMap::new(),
                    "C1",
                    None,
                    None,
                )
                .is_err()
        );
        let events = (0..17)
            .map(|_| (json!({"text": "x".repeat(1_000_000)}), None, None, None))
            .collect::<Vec<_>>();
        assert!(inbox.stage(events, &BTreeMap::new(), "C1", None, None).is_err());
        assert!(inbox.pending.is_empty());
    }

    #[tokio::test]
    async fn calls_a_bounded_api_response() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"messages":[]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let value = api_at(
            &reqwest::Client::new(),
            "xoxb-test",
            "conversations.history",
            json!({"channel":"C1"}),
            &format!("http://{address}"),
        )
        .await
        .unwrap();

        assert_eq!(value["ok"], true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_an_oversized_api_response_before_parsing() {
        let body = futures_util::stream::iter(vec![Ok::<_, reqwest::Error>(vec![
            b'x';
            EVENT_BYTES_LIMIT
                + 1
        ])]);

        assert!(collect_api_body(body).await.is_err());
    }

    #[tokio::test]
    async fn sends_a_message_through_the_normalized_contract() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"ts":"1"}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let value = send_at(
            &reqwest::Client::new(),
            "xoxb-test",
            &json!({"chat":"C1","text":"hello"}),
            &format!("http://{address}"),
        )
        .await
        .unwrap();

        assert_eq!(value["ts"], "1");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn polls_history_and_rejects_api_errors() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"messages":[{"ts":"1","user":"U1","text":"hello"}]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let value = poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();

        assert_eq!(value["events"][0]["text"], "hello");
        server.await.unwrap();

        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = br#"{"ok":false,"error":"invalid_auth"}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        assert!(
            api_at(
                &reqwest::Client::new(),
                "token",
                "auth.test",
                json!({}),
                &format!("http://{address}")
            )
            .await
            .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn polls_file_share_messages_and_normalizes_attachments() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = br#"{"ok":true,"messages":[{"ts":"1.0","user":"U1","subtype":"file_share","files":[{"id":"F1","name":"note.txt","mimetype":"text/plain","url_private":"https://files.slack.com/files-pri/F1/download"}]}]}"#;
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let value = poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();

        assert_eq!(value["events"][0]["content"][0]["kind"], "file");
        assert_eq!(value["events"][0]["content"][0]["uri"], "slack://file/F1");
        assert!(app.attachments.lock().await.contains_key("F1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn paginates_history_in_chronological_order() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for body in [
                br#"{"ok":true,"messages":[{"ts":"2.0","user":"U1","text":"new"}],"response_metadata":{"next_cursor":"older"}}"#.to_vec(),
                br#"{"ok":true,"messages":[{"ts":"1.0","user":"U1","text":"old"}]}"#.to_vec(),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let value = match tokio::time::timeout(
            Duration::from_secs(5),
            poll_at(&app, "token", "C1", &format!("http://{address}")),
        )
        .await
        {
            Ok(Ok(value)) => value,

            Ok(Err(error)) => {
                server.abort();
                panic!("Slack history pagination failed: {error}");
            }

            Err(_) => {
                server.abort();
                panic!("Slack history pagination exceeded the poll deadline.");
            }
        };

        assert_eq!(value["events"][0]["id"], "1.0");
        app.inbox.lock().await.acknowledge(1).unwrap();

        assert_eq!(app.inbox.lock().await.pending_event().unwrap()["id"], "2.0");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn skips_history_until_interval_without_sleeping() {
        let state = Arc::new(Mutex::new(HistoryPoll::default()));
        history_succeeded(&state).await;
        let due =
            tokio::time::timeout(Duration::from_millis(100), pace_history(&state)).await.unwrap();
        assert!(!due);
    }

    #[tokio::test]
    async fn defers_history_cursor_when_capacity_is_reached() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let messages = (0..INBOX_LIMIT)
            .map(|id| json!({"ts": id.to_string(), "user": "U1", "text": "event"}))
            .collect::<Vec<_>>();
        let body = serde_json::to_vec(&json!({
            "ok":true,
            "messages":messages,
            "response_metadata":{"next_cursor":"older"}
        }))
        .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();

        assert_eq!(app.inbox.lock().await.pending.len(), INBOX_LIMIT);
        assert_eq!(app.inbox.lock().await.cursors["C1"], "older");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn defers_history_cursor_when_serialized_budget_is_reached() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let text = "x".repeat(1_500_000);
        let body = serde_json::to_vec(&json!({
            "ok": true,
            "messages": [
                {"ts": "2.0", "user": "U1", "text": text},
                {"ts": "1.0", "user": "U1", "text": "x".repeat(1_500_000)},
            ],
            "response_metadata": {"next_cursor": "older"}
        }))
        .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        app.inbox.lock().await.channels.insert("other".into(), "x".repeat(14_000_000));
        tokio::time::timeout(
            Duration::from_secs(5),
            poll_at(&app, "token", "C1", &format!("http://{address}")),
        )
        .await
        .expect("Slack history poll exceeded the test deadline")
        .unwrap();
        {
            let inbox = app.inbox.lock().await;

            assert!(inbox.pending.is_empty());
            assert!(!inbox.cursors.contains_key("C1"));
        }

        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("Slack test server exceeded the test deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn defers_history_when_no_event_fits_serialized_budget() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let body = br#"{
            "ok": true,
            "messages": [{"ts": "1.0", "user": "U1", "text": "hello"}],
            "response_metadata": {"next_cursor": "older"}
        }"#;
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let mut inbox = app.inbox.lock().await;
        let base_size = inbox.serialized_size().unwrap();
        inbox.channels.insert("other".into(), String::new());
        let overhead = inbox.serialized_size().unwrap().saturating_sub(base_size);
        let filler = INBOX_BYTES_LIMIT.saturating_sub(base_size + overhead + 1);
        inbox.channels.insert("other".into(), "x".repeat(filler));

        assert!(inbox.serialized_size().unwrap() < INBOX_BYTES_LIMIT);
        drop(inbox);
        let value = poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();
        let inbox = app.inbox.lock().await;

        assert!(value["events"].as_array().unwrap().is_empty());
        assert!(inbox.pending.is_empty());
        assert!(!inbox.cursors.contains_key("C1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rate_limited_history_returns_without_restarting() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = br#"{"ok":false,"error":"ratelimited","retry_after":1}"#;
            let header = format!(
                "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        let value = poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();

        assert_eq!(value["events"].as_array().unwrap().len(), 0);
        assert!(app.history.lock().await.next.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_invalid_media_and_send_payloads() {
        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
            inbox: Arc::new(Mutex::new(Inbox::default())),
            history: Arc::new(Mutex::new(HistoryPoll::default())),
        };

        assert!(media(&app, "token", &json!({})).await.is_err());
        assert!(
            send_at(&reqwest::Client::new(), "token", &json!({}), "http://unused").await.is_err()
        );

        assert!(
            remember_files(
                &json!({"files":[{"id":"bad id","url_private":"http://evil"}]}),
                &app.attachments
            )
            .await
            .is_empty()
        );
    }
}
