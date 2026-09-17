#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Content, Hello, Protocol, Request, Response},
};
use crabbot_file::{load as load_file, save as save_file};
use futures_util::{SinkExt, Stream, StreamExt};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    net::TcpStream,
    sync::Mutex as AsyncMutex,
    time::{Duration, Instant, timeout},
};

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const TEXT_LIMIT: usize = 256 * 1024;
const MEDIA_LIMIT: usize = 4 * 1024 * 1024;
const ATTACHMENTS: usize = 256;
const ATTACHMENT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

struct GatewayState {
    socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    interval: Duration,
    next_heartbeat: Instant,
    sequence: Option<u64>,
    session_id: Option<String>,
    pending_sequence: Option<u64>,
    interactions: BTreeMap<String, Interaction>,
    attachments: BTreeMap<String, Attachment>,
}

#[derive(Clone)]
struct Interaction {
    token: String,
    expires: Instant,
}

#[derive(Clone)]
struct Attachment {
    url: String,
    name: String,
    mime: Option<String>,
    size: Option<usize>,
    expires: Instant,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Cursor {
    #[serde(default = "cursor_version")]
    version: u8,
    sequence: Option<u64>,
    session_id: Option<String>,
}

fn cursor_version() -> u8 {
    1
}

impl Default for GatewayState {
    fn default() -> Self {
        let (sequence, session_id) = load_cursor();
        Self {
            socket: None,
            interval: Duration::from_secs(45),
            next_heartbeat: Instant::now(),
            sequence,
            session_id,
            pending_sequence: None,
            interactions: Default::default(),
            attachments: Default::default(),
        }
    }
}

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Discord client failed: {error}.")))?;
    let gateway = Arc::new(AsyncMutex::new(GatewayState::default()));

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "discord".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: vec![],
        },
        move |request| {
            let client = client.clone();
            let gateway = Arc::clone(&gateway);
            async move { call_state(&client, &gateway, request).await }
        },
    )
    .await
}

#[cfg(test)]
async fn call(
    client: &reqwest::Client,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    let gateway = AsyncMutex::new(GatewayState::default());
    call_state(client, &gateway, request).await
}

async fn call_state(
    client: &reqwest::Client,
    gateway: &AsyncMutex<GatewayState>,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    let token = credential()?;
    call_with_state(client, gateway, id, method, params, &token, "https://discord.com/api/v10")
        .await
}

fn credential() -> crabbot_core::Result<String> {
    for name in ["CRABBOT_DISCORD_TOKEN"] {
        if let Ok(value) = std::env::var(name)
            && !value.trim().is_empty()
        {
            return Ok(value);
        }
    }
    if let Some(value) = keyring("discord") {
        return Ok(value);
    }
    let Some(path) = std::env::var_os("CRABBOT_CREDENTIALS") else {
        return Err(crabbot_core::Error::Denied("CRABBOT_DISCORD_TOKEN is not configured.".into()));
    };
    crabbot_file::private(&path)?;
    let text = std::fs::read_to_string(path).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials could not be read: {error}."))
    })?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials are invalid: {error}."))
    })?;
    value["CRABBOT_DISCORD_TOKEN"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_DISCORD_TOKEN is not configured.".into())
        })
}

fn keyring(name: &str) -> Option<String> {
    if std::env::var("CRABBOT_KEYRING").ok().as_deref() != Some("1") {
        return None;
    }
    keyring::Entry::new("dev.airscripts.crabbot", name)
        .ok()?
        .get_password()
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
async fn call_with(
    client: &reqwest::Client,
    id: u64,
    method: String,
    params: serde_json::Value,
    token: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let gateway = AsyncMutex::new(GatewayState::default());
    call_with_state(client, &gateway, id, method, params, token, base).await
}

async fn call_with_state(
    client: &reqwest::Client,
    gateway_state: &AsyncMutex<GatewayState>,
    id: u64,
    method: String,
    params: serde_json::Value,
    token: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let Some(operation) = operation(&method, &params)? else {
        return Ok(None);
    };
    let result = match operation {
        Operation::Send { channel, text, content } => {
            send_content(client, &channel, &text, &content, token, base).await?
        }
        Operation::Approval { channel, text, approve, deny } => {
            let body = approval_request(&text, &approve, &deny)?;
            message(
                client,
                reqwest::Method::POST,
                format!("{base}/channels/{channel}/messages"),
                token,
                body,
            )
            .await?
        }
        Operation::Edit { channel, message_id, text } => {
            let mut results = Vec::new();
            for (method, url, body) in edit_requests(base, &channel, &message_id, &text)? {
                results.push(message(client, method, url, token, body).await?);
            }
            let edited = results.remove(0);
            json!({"edited": edited, "sent": results})
        }
        Operation::Poll(timeout) => gateway(gateway_state, token, timeout, base).await?,
        Operation::Ack(sequence) => acknowledge(gateway_state, sequence).await?,
        Operation::Callback { id, text } => {
            callback(client, gateway_state, &id, &text, base).await?
        }
        Operation::Media(uri) => media(client, gateway_state, &uri, token).await?,
    };

    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "Discord response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

async fn read(response: reqwest::Response) -> crabbot_core::Result<String> {
    collect(response.bytes_stream()).await
}

async fn collect<S, C, E>(mut stream: S) -> crabbot_core::Result<String>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("Discord response failed: {error}."))
        })?;
        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied("Discord response was too large.".into()));
        }
        bytes.extend_from_slice(chunk.as_ref());
    }
    String::from_utf8(bytes)
        .map_err(|_| crabbot_core::Error::Denied("Discord response failed.".into()))
}

enum Operation {
    Send { channel: String, text: String, content: Vec<Content> },
    Edit { channel: String, message_id: String, text: String },
    Approval { channel: String, text: String, approve: String, deny: String },
    Callback { id: String, text: String },
    Poll(u64),
    Ack(u64),
    Media(String),
}

fn operation(method: &str, params: &serde_json::Value) -> crabbot_core::Result<Option<Operation>> {
    match method {
        "send" => {
            let channel: String = params["channel"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or_else(|| crabbot_core::Error::Denied("send.channel is invalid.".into()))?
                .into();
            let text: String = params["text"].as_str().unwrap_or_default().into();
            let content = parse_content(&params["content"])?;
            if text.is_empty() && content.is_empty() {
                return Err(crabbot_core::Error::Denied(
                    "send.text or send.content is required.".into(),
                ));
            }
            Ok(Some(Operation::Send { channel, text, content }))
        }
        "edit" => Ok(Some(Operation::Edit {
            channel: params["channel"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or_else(|| crabbot_core::Error::Denied("edit.channel is invalid.".into()))?
                .into(),
            message_id: params["message"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or_else(|| crabbot_core::Error::Denied("edit.message is invalid.".into()))?
                .into(),
            text: params["text"]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= TEXT_LIMIT)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "edit.text is empty or exceeds the Discord limit.".into(),
                    )
                })?
                .into(),
        })),
        "approval" => Ok(Some(Operation::Approval {
            channel: params["channel"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or_else(|| crabbot_core::Error::Denied("approval.channel is invalid.".into()))?
                .into(),
            text: params["text"]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 2_000)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "approval.text is empty or exceeds the Discord limit.".into(),
                    )
                })?
                .into(),
            approve: params["approve"]
                .as_str()
                .filter(|value| custom_id(value))
                .ok_or_else(|| crabbot_core::Error::Denied("approval.approve is invalid.".into()))?
                .into(),
            deny: params["deny"]
                .as_str()
                .filter(|value| custom_id(value))
                .ok_or_else(|| crabbot_core::Error::Denied("approval.deny is invalid.".into()))?
                .into(),
        })),
        "callback" => Ok(Some(Operation::Callback {
            id: params["id"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or_else(|| crabbot_core::Error::Denied("callback.id is invalid.".into()))?
                .into(),
            text: params["text"]
                .as_str()
                .filter(|value| value.len() <= 2_000)
                .unwrap_or_default()
                .into(),
        })),
        "poll" => Ok(Some(Operation::Poll(params["timeout"].as_u64().unwrap_or(25)))),
        "ack" => Ok(Some(Operation::Ack(
            params["sequence"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("ack.sequence is required.".into()))?,
        ))),
        "media" => Ok(Some(Operation::Media(
            params["uri"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("media.uri is required.".into()))?
                .into(),
        ))),
        _ => Ok(None),
    }
}

fn parse_content(value: &serde_json::Value) -> crabbot_core::Result<Vec<Content>> {
    let Some(items) = value.as_array() else {
        return Ok(Vec::new());
    };
    if items.len() > 8 {
        return Err(crabbot_core::Error::Denied("send.content has too many items.".into()));
    }
    items
        .iter()
        .cloned()
        .map(|value| {
            serde_json::from_value(value).map_err(|_| {
                crabbot_core::Error::Denied("send.content contains an invalid item.".into())
            })
        })
        .collect()
}

async fn send_content(
    client: &reqwest::Client,
    channel: &str,
    text: &str,
    content: &[Content],
    token: &str,
    base: &str,
) -> crabbot_core::Result<Value> {
    let mut result = Value::Null;
    for part in chunks(text, 2_000) {
        if part.is_empty() {
            continue;
        }
        result = message(
            client,
            reqwest::Method::POST,
            format!("{base}/channels/{channel}/messages"),
            token,
            json!({"content": part}),
        )
        .await?;
    }
    for item in content {
        match item {
            Content::Text { text } => {
                if !text.is_empty() {
                    for part in chunks(text, 2_000) {
                        result = message(
                            client,
                            reqwest::Method::POST,
                            format!("{base}/channels/{channel}/messages"),
                            token,
                            json!({"content": part}),
                        )
                        .await?;
                    }
                }
            }
            Content::Image { uri, alt } => {
                result = upload(
                    client,
                    channel,
                    uri,
                    "image",
                    alt.as_deref().unwrap_or_default(),
                    token,
                    base,
                )
                .await?;
            }
            Content::File { uri, name, mime } => {
                result = upload(client, channel, uri, name, "", token, base).await?;
                let _ = mime;
            }
            Content::Audio { uri, mime } => {
                result = upload(client, channel, uri, "audio", "", token, base).await?;
                let _ = mime;
            }
        }
    }
    if result.is_null() {
        return Err(crabbot_core::Error::Denied("send.text or send.content is required.".into()));
    }
    Ok(result)
}

async fn upload(
    client: &reqwest::Client,
    channel: &str,
    uri: &str,
    name: &str,
    caption: &str,
    token: &str,
    base: &str,
) -> crabbot_core::Result<Value> {
    let path = local_media(uri)?;
    let bytes = fs::read(&path).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord attachment could not be read: {error}."))
    })?;
    if bytes.len() > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Discord attachment is too large.".into()));
    }
    let filename = safe_name(name).unwrap_or_else(|| "attachment.bin".into());
    let part = reqwest::multipart::Part::bytes(bytes).file_name(filename);
    let payload = json!({"content": caption});
    let response = client
        .post(format!("{base}/channels/{channel}/messages"))
        .header(AUTHORIZATION, format!("Bot {token}"))
        .multipart(
            reqwest::multipart::Form::new()
                .text("payload_json", payload.to_string())
                .part("files[0]", part),
        )
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Discord upload failed: {error}.")))?;
    let status = response.status();
    let body = read(response).await?;
    let body = serde_json::from_str(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord response failed: {error}."))
    })?;
    response_body(status, body)
}

async fn media(
    client: &reqwest::Client,
    gateway: &AsyncMutex<GatewayState>,
    uri: &str,
    token: &str,
) -> crabbot_core::Result<Value> {
    let id = uri
        .strip_prefix("discord://attachment/")
        .filter(|value| snowflake(value))
        .ok_or_else(|| crabbot_core::Error::Denied("Discord media URI is invalid.".into()))?
        .to_owned();
    let attachment = {
        let mut state = gateway.lock().await;
        state.attachments.retain(|_, value| value.expires > Instant::now());
        state.attachments.get(&id).cloned().ok_or_else(|| {
            crabbot_core::Error::Denied("Discord attachment is no longer available.".into())
        })?
    };
    if !discord_media_url(&attachment.url) {
        return Err(crabbot_core::Error::Denied("Discord attachment URL is not allowed.".into()));
    }
    if attachment.size.is_some_and(|size| size > MEDIA_LIMIT) {
        return Err(crabbot_core::Error::Denied("Discord attachment is too large.".into()));
    }
    let response = client
        .get(&attachment.url)
        .header(AUTHORIZATION, format!("Bot {token}"))
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Discord media failed: {error}.")))?;
    let status = response.status();
    let bytes = collect(response.bytes_stream()).await?;
    if !status.is_success() || bytes.len() > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Discord media download failed.".into()));
    }
    let root = media_root()?;
    cleanup(&root);
    fs::create_dir_all(&root).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord media directory failed: {error}."))
    })?;
    let destination = root.join(format!("{id}.bin"));
    crabbot_file::save(&destination, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord media storage failed: {error}."))
    })?;
    Ok(json!({
        "uri": format!("file://{}", destination.display()),
        "name": attachment.name,
        "mime": attachment.mime,
    }))
}

fn local_media(uri: &str) -> crabbot_core::Result<PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("Discord attachment URI is invalid.".into()))?;
    let root = media_root()?;
    let root = fs::canonicalize(root)
        .map_err(|_| crabbot_core::Error::Denied("Discord media root is unavailable.".into()))?;
    let path = fs::canonicalize(path)
        .map_err(|_| crabbot_core::Error::Denied("Discord attachment is unavailable.".into()))?;
    if !path.starts_with(root) {
        return Err(crabbot_core::Error::Denied(
            "Discord attachment leaves the media root.".into(),
        ));
    }
    Ok(path)
}

fn media_root() -> crabbot_core::Result<PathBuf> {
    std::env::var_os("CRABBOT_MEDIA")
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME")
                .map(|value| PathBuf::from(value).join("media").into_os_string())
        })
        .map(PathBuf::from)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_MEDIA or CRABBOT_HOME is required.".into())
        })
}

fn cleanup(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let cutoff = SystemTime::now().checked_sub(ATTACHMENT_TTL).unwrap_or(SystemTime::UNIX_EPOCH);
    for entry in entries.flatten().take(ATTACHMENTS) {
        if entry.file_type().is_ok_and(|value| value.is_file())
            && entry.metadata().and_then(|value| value.modified()).is_ok_and(|value| value < cutoff)
        {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn discord_media_url(value: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(value) else {
        return false;
    };
    if url.scheme() != "https" {
        return false;
    }
    matches!(url.host_str(), Some("cdn.discordapp.com" | "media.discordapp.net"))
}

fn safe_name(value: &str) -> Option<String> {
    let value = Path::new(value).file_name()?.to_str()?;
    if value.is_empty() || value.len() > 255 || value == "." || value == ".." {
        return None;
    }
    Some(value.into())
}

fn snowflake(value: &str) -> bool {
    !value.is_empty() && value.len() <= 20 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn custom_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn approval_request(text: &str, approve: &str, deny: &str) -> crabbot_core::Result<Value> {
    if approve == deny || !custom_id(approve) || !custom_id(deny) {
        return Err(crabbot_core::Error::Denied("Approval callback data is invalid.".into()));
    }
    Ok(json!({
        "content": text,
        "components": [{
            "type": 1,
            "components": [
                {"type": 2, "style": 3, "label": "Approve", "custom_id": approve},
                {"type": 2, "style": 4, "label": "Deny", "custom_id": deny}
            ]
        }]
    }))
}

async fn callback(
    client: &reqwest::Client,
    gateway: &AsyncMutex<GatewayState>,
    id: &str,
    text: &str,
    base: &str,
) -> crabbot_core::Result<Value> {
    let token = {
        let mut gateway = gateway.lock().await;
        gateway.interactions.retain(|_, value| value.expires > Instant::now());
        gateway.interactions.remove(id).map(|interaction| interaction.token).ok_or_else(|| {
            crabbot_core::Error::Denied("Discord interaction is missing or expired.".into())
        })?
    };
    let response = client
        .post(format!("{base}/interactions/{id}/{token}/callback"))
        .json(&json!({
            "type": 7,
            "data": {"content": text, "components": []}
        }))
        .send()
        .await
        .map_err(|_| {
            crabbot_core::Error::Denied("Discord interaction acknowledgement failed.".into())
        })?;
    if !response.status().is_success() {
        return Err(crabbot_core::Error::Denied(
            "Discord interaction acknowledgement was rejected.".into(),
        ));
    }
    Ok(json!({"acknowledged": true}))
}

fn edit_request(
    base: &str,
    channel: &str,
    message_id: &str,
    text: &str,
) -> (reqwest::Method, String, Value) {
    (
        reqwest::Method::PATCH,
        format!("{base}/channels/{channel}/messages/{message_id}"),
        json!({"content": text}),
    )
}

fn edit_requests(
    base: &str,
    channel: &str,
    message_id: &str,
    text: &str,
) -> crabbot_core::Result<Vec<(reqwest::Method, String, Value)>> {
    let mut parts = chunks(text, 2_000).into_iter();
    let first = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("edit.text is required.".into()))?;
    let mut requests = vec![edit_request(base, channel, message_id, &first)];
    requests.extend(parts.map(|part| {
        (
            reqwest::Method::POST,
            format!("{base}/channels/{channel}/messages"),
            json!({"content": part}),
        )
    }));
    Ok(requests)
}

async fn message(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
    token: &str,
    body: Value,
) -> crabbot_core::Result<Value> {
    let response = client
        .request(method, url)
        .header(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bot {token}")).map_err(|error| {
                crabbot_core::Error::Denied(format!("Discord token is invalid: {error}."))
            })?,
        )
        .json(&body)
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Discord request failed: {error}."))
        })?;
    let status = response.status();
    let body = read(response).await?;
    let body: Value = serde_json::from_str(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord response failed: {error}."))
    })?;

    response_body(status, body)
}

async fn gateway(
    gateway_state: &AsyncMutex<GatewayState>,
    token: &str,
    seconds: u64,
    base: &str,
) -> crabbot_core::Result<serde_json::Value> {
    let url = std::env::var("CRABBOT_DISCORD_GATEWAY_URL").unwrap_or_else(|_| gateway_url(base));
    let mut state = gateway_state.lock().await;
    if state.pending_sequence.is_some() {
        return Err(crabbot_core::Error::Denied(
            "Discord Gateway event is awaiting host acknowledgement.".into(),
        ));
    }
    if state.socket.is_none() {
        let (mut socket, _) = connect_async(&url).await.map_err(|error| {
            crabbot_core::Error::Denied(format!("Discord Gateway failed: {error}."))
        })?;
        let hello = timeout(Duration::from_secs(10), socket.next())
            .await
            .map_err(|_| crabbot_core::Error::Denied("Discord Gateway hello timed out.".into()))?
            .ok_or_else(|| {
                crabbot_core::Error::Denied("Discord Gateway closed before hello.".into())
            })?
            .map_err(|error| {
                crabbot_core::Error::Denied(format!("Discord Gateway read failed: {error}."))
            })?;
        state.interval = Duration::from_millis(heartbeat(&hello)?);
        let payload = state.session_id.as_deref().map_or_else(
            || json!({
                "op": 2,
                "d": {
                    "token": token,
                    "intents": intents(),
                    "properties": {"os": "linux", "browser": "crabbot", "device": "crabbot"}
                }
            }),
            |session_id| json!({"op": 6, "d": {"token": token, "session_id": session_id, "seq": state.sequence}}),
        );
        socket.send(Message::Text(payload.to_string().into())).await.map_err(|error| {
            crabbot_core::Error::Denied(format!("Discord Gateway identify failed: {error}."))
        })?;
        state.next_heartbeat = Instant::now() + state.interval;
        state.socket = Some(socket);
    }

    let mut next_heartbeat = state.next_heartbeat;
    let interval = state.interval;
    let mut sequence = state.sequence;
    let committed_sequence = state.sequence;
    let mut session_id = state.session_id.clone();
    let mut pending_sequence = None;
    let mut closed = false;
    let mut interactions = state.interactions.clone();
    interactions.retain(|_, value| value.expires > Instant::now());
    let mut attachments = state.attachments.clone();
    attachments.retain(|_, value| value.expires > Instant::now());
    let socket = state.socket.as_mut().expect("gateway socket is initialized");
    let mut events = Vec::new();
    let deadline = Duration::from_secs(seconds.clamp(1, 30));
    let result = timeout(deadline, async {
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(next_heartbeat) => {
                    socket.send(Message::Text(json!({"op": 1, "d": sequence}).to_string().into())).await
                        .map_err(|error| crabbot_core::Error::Denied(format!("Discord Gateway heartbeat failed: {error}.")))?;
                    next_heartbeat = Instant::now() + interval;
                }
                message = socket.next() => {
                    let Some(message) = message else { closed = true; break; };
                    let message = message.map_err(|error| crabbot_core::Error::Denied(format!("Discord Gateway read failed: {error}.")))?;
                    let Some(value) = message.into_text().ok().and_then(|text| serde_json::from_str::<serde_json::Value>(text.as_ref()).ok()) else { continue; };
                    if value["t"] == "MESSAGE_CREATE" {
                        remember_attachments(&value["d"], &mut attachments);
                    }
                    match prepare_gateway_value(
                        &value,
                        &mut sequence,
                        &mut session_id,
                        committed_sequence,
                        &mut pending_sequence,
                    )? {
                        Action::Event(Some(mut event)) => {
                            if let Some(token) = event
                                .get("callback_token")
                                .and_then(Value::as_str)
                                .filter(|value| !value.is_empty() && value.len() <= 512)
                                .map(str::to_owned)
                                && let Some(id) = event["callback_id"].as_str()
                            {
                                interactions.insert(
                                    id.into(),
                                    Interaction {
                                        token,
                                        expires: Instant::now() + Duration::from_secs(300),
                                    },
                                );
                                if let Some(values) = event.as_object_mut() {
                                    values.remove("callback_token");
                                }
                                while interactions.len() > 128 {
                                    let Some(oldest) = interactions
                                        .iter()
                                        .min_by_key(|(_, value)| value.expires)
                                        .map(|(key, _)| key.clone())
                                    else {
                                        break;
                                    };
                                    interactions.remove(&oldest);
                                }
                            }
                            events.push(event);
                            break;
                        }
                        Action::Event(None) | Action::Ignore => {}
                        Action::Heartbeat(data) => {
                            socket.send(Message::Text(json!({"op": 1, "d": sequence.or(data.as_u64())}).to_string().into())).await
                                .map_err(|error| crabbot_core::Error::Denied(format!("Discord Gateway heartbeat failed: {error}.")))?;
                            next_heartbeat = Instant::now() + interval;
                        }
                        Action::Reconnect => return Err(crabbot_core::Error::Denied("Discord Gateway requested reconnect.".into())),
                        Action::Reject { resumable } => {
                            if !resumable {
                                session_id = None;
                                save_cursor(committed_sequence, None)?;
                            }
                            return Err(crabbot_core::Error::Denied("Discord Gateway rejected the session.".into()));
                        }
                    }
                }
            }
        }
        Ok::<(), crabbot_core::Error>(())
    })
    .await;
    if let Ok(Err(error)) = result {
        state.socket = None;
        return Err(error);
    }
    if events.is_empty() {
        state.sequence = sequence;
    }
    state.pending_sequence = pending_sequence;
    state.interactions = interactions;
    state.attachments = attachments;
    state.session_id = session_id;
    state.next_heartbeat = next_heartbeat;
    if closed {
        state.socket = None;
    }
    if let Err(error) = save_cursor(state.sequence, state.session_id.as_deref()) {
        state.socket = None;
        return Err(error);
    }
    Ok(json!({"events": events}))
}

async fn acknowledge(
    gateway_state: &AsyncMutex<GatewayState>,
    sequence: u64,
) -> crabbot_core::Result<serde_json::Value> {
    let mut state = gateway_state.lock().await;
    if state.pending_sequence != Some(sequence) {
        return Err(crabbot_core::Error::Denied(
            "Discord Gateway acknowledgement is stale.".into(),
        ));
    }
    state.sequence = Some(sequence);
    state.pending_sequence = None;
    save_cursor(state.sequence, state.session_id.as_deref())?;
    Ok(json!({"acknowledged": true}))
}

fn stage_event(
    event: Option<Value>,
    sequence: Option<u64>,
    pending: &mut Option<u64>,
) -> crabbot_core::Result<Option<Value>> {
    let Some(mut event) = event else {
        return Ok(None);
    };
    let Some(sequence) = sequence else {
        return Err(crabbot_core::Error::Denied("Discord Gateway message had no sequence.".into()));
    };
    event["gateway_sequence"] = json!(sequence);
    *pending = Some(sequence);
    Ok(Some(event))
}

fn prepare_gateway_value(
    value: &Value,
    sequence: &mut Option<u64>,
    session_id: &mut Option<String>,
    committed_sequence: Option<u64>,
    pending: &mut Option<u64>,
) -> crabbot_core::Result<Action> {
    if let Some(value) = value["s"].as_u64() {
        *sequence = Some(value);
    }
    if value["t"] == "READY" {
        *session_id = value["d"]["session_id"].as_str().map(str::to_owned);
        save_cursor(committed_sequence, session_id.as_deref())?;
    }
    match action(value) {
        Action::Event(event) => Ok(Action::Event(stage_event(event, *sequence, pending)?)),
        action => Ok(action),
    }
}

fn cursor_path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_HOME").map(|home| PathBuf::from(home).join("discord-gateway.json"))
}

fn load_cursor() -> (Option<u64>, Option<String>) {
    let Some(path) = cursor_path() else {
        return (None, None);
    };
    load_cursor_at(&path)
}

fn load_cursor_at(path: &Path) -> (Option<u64>, Option<String>) {
    let Ok(Some(bytes)) = load_file(path, 4 * 1024) else {
        return (None, None);
    };
    let Ok(value) = serde_json::from_slice::<Cursor>(&bytes) else {
        return (None, None);
    };
    if value.version != 1 {
        return (None, None);
    }
    (value.sequence, value.session_id)
}

fn save_cursor(sequence: Option<u64>, session_id: Option<&str>) -> crabbot_core::Result<()> {
    let Some(path) = cursor_path() else {
        return Ok(());
    };
    save_cursor_at(&path, sequence, session_id)
}

fn save_cursor_at(
    path: &Path,
    sequence: Option<u64>,
    session_id: Option<&str>,
) -> crabbot_core::Result<()> {
    let value = Cursor { version: 1, sequence, session_id: session_id.map(str::to_owned) };
    save_file(path, serde_json::to_vec(&value)?).map_err(|error| {
        crabbot_core::Error::Denied(format!("Discord cursor could not be stored: {error}."))
    })
}

fn heartbeat(value: &Message) -> crabbot_core::Result<u64> {
    let value = value
        .to_text()
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        .ok_or_else(|| crabbot_core::Error::Denied("Discord Gateway hello was invalid.".into()))?;
    if value["op"].as_u64() != Some(10) {
        return Err(crabbot_core::Error::Denied("Discord Gateway hello was invalid.".into()));
    }
    let interval =
        value["d"]["heartbeat_interval"].as_u64().filter(|interval| *interval > 0).ok_or_else(
            || crabbot_core::Error::Denied("Discord Gateway heartbeat was invalid.".into()),
        )?;
    Ok(interval)
}

fn intents() -> u64 {
    std::env::var("CRABBOT_DISCORD_INTENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(37_377)
}

fn gateway_url(base: &str) -> String {
    if base == "https://discord.com/api/v10" {
        return "wss://gateway.discord.gg/?v=10&encoding=json".into();
    }
    if let Some(host) = base.strip_prefix("https://") {
        return format!("wss://{host}");
    }
    if let Some(host) = base.strip_prefix("http://") {
        return format!("ws://{host}");
    }
    base.into()
}

fn chunks(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        current.push(character);
        if current.chars().count() == limit {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() || chunks.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn normalize(value: &serde_json::Value) -> Option<serde_json::Value> {
    if value["author"]["bot"].as_bool() == Some(true) {
        return None;
    }
    let id = value["id"].as_str()?;
    let channel = value["channel_id"].as_str()?;
    let text = value["content"].as_str().unwrap_or_default();
    let mut content = Vec::new();
    if !text.is_empty() {
        content.push(json!({"kind": "text", "text": text}));
    }
    if let Some(attachments) = value["attachments"].as_array() {
        for attachment in attachments {
            let Some(id) = attachment["id"].as_str().filter(|value| snowflake(value)) else {
                continue;
            };
            let uri = format!("discord://attachment/{id}");
            let mime = attachment["content_type"].as_str();
            let voice = attachment["flags"].as_u64().is_some_and(|flags| flags & (1 << 13) != 0);
            let kind = if voice || mime.is_some_and(|value| value.starts_with("audio/")) {
                "audio"
            } else if mime.is_some_and(|value| value.starts_with("image/")) {
                "image"
            } else {
                "file"
            };
            if kind == "image" {
                content.push(
                    json!({"kind": kind, "uri": uri, "alt": attachment["description"].as_str()}),
                );
            } else if kind == "audio" {
                content.push(json!({"kind": kind, "uri": uri, "mime": mime}));
            } else {
                content.push(json!({"kind": kind, "uri": uri, "name": attachment["filename"].as_str().unwrap_or("attachment"), "mime": mime}));
            }
        }
    }
    if content.is_empty() {
        return None;
    }
    let thread = value["thread_id"]
        .as_str()
        .or_else(|| value["thread"]["id"].as_str())
        .or_else(|| matches!(value["channel_type"].as_u64(), Some(10..=12)).then_some(channel))
        .or_else(|| matches!(value["type"].as_u64(), Some(11 | 12)).then(|| channel));
    Some(json!({
        "id": id,
        "chat": channel,
        "kind": if value["guild_id"].is_string() { "guild" } else { "private" },
        "private": !value["guild_id"].is_string(),
        "thread": thread,
        "topic": thread,
        "sender": value["author"]["id"],
        "roles": value["member"]["roles"],
        "text": text,
        "content": content,
    }))
}

fn remember_attachments(value: &Value, attachments: &mut BTreeMap<String, Attachment>) {
    let Some(values) = value["attachments"].as_array() else {
        return;
    };
    let expires = Instant::now() + ATTACHMENT_TTL;
    for value in values {
        let Some(id) = value["id"].as_str().filter(|value| snowflake(value)) else {
            continue;
        };
        let Some(url) = value["url"].as_str().filter(|value| discord_media_url(value)) else {
            continue;
        };
        let name = value["filename"]
            .as_str()
            .and_then(safe_name)
            .unwrap_or_else(|| "attachment.bin".into());
        let mime = value["content_type"].as_str().map(str::to_owned);
        let size = value["size"].as_u64().and_then(|value| usize::try_from(value).ok());
        attachments.insert(id.into(), Attachment { url: url.into(), name, mime, size, expires });
    }
    while attachments.len() > ATTACHMENTS {
        let Some(oldest) =
            attachments.iter().min_by_key(|(_, value)| value.expires).map(|(id, _)| id.clone())
        else {
            break;
        };
        attachments.remove(&oldest);
    }
}

fn normalize_interaction(value: &Value) -> Option<Value> {
    if value["type"].as_u64() != Some(3) {
        return None;
    }
    let id = value["id"].as_str()?;
    let channel = value["channel_id"].as_str()?;
    if !snowflake(id) || !snowflake(channel) {
        return None;
    }
    let data = value["data"]["custom_id"].as_str()?;
    if !custom_id(data) {
        return None;
    }
    let token = value["token"].as_str()?;
    if token.is_empty()
        || token.len() > 512
        || !token.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
    {
        return None;
    }
    let sender = value["member"]["user"]["id"].as_str().or_else(|| value["user"]["id"].as_str())?;
    let roles = value["member"]["roles"].as_array().cloned().unwrap_or_default();
    Some(json!({
        "id": id,
        "kind": "callback",
        "chat": channel,
        "private": value["guild_id"].is_null(),
        "thread": null,
        "topic": null,
        "sender": sender,
        "roles": roles,
        "text": "",
        "content": [],
        "callback_id": id,
        "data": data,
        "callback_token": token,
    }))
}

enum Action {
    Event(Option<Value>),
    Heartbeat(Value),
    Reconnect,
    Reject { resumable: bool },
    Ignore,
}

fn action(value: &Value) -> Action {
    match value["op"].as_u64() {
        Some(0) if value["t"] == "MESSAGE_CREATE" => Action::Event(normalize(&value["d"])),
        Some(0) if value["t"] == "INTERACTION_CREATE" => {
            Action::Event(normalize_interaction(&value["d"]))
        }
        Some(1) => Action::Heartbeat(value["d"].clone()),
        Some(7) => Action::Reconnect,
        Some(9) => Action::Reject { resumable: value["d"].as_bool().unwrap_or(false) },
        _ => Action::Ignore,
    }
}

fn response_body(
    status: reqwest::StatusCode,
    body: serde_json::Value,
) -> crabbot_core::Result<serde_json::Value> {
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Discord rejected the request: {}.",
            body["message"].as_str().unwrap_or("unknown error")
        )));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::{
        Action, BODY_LIMIT, GatewayState, Operation, acknowledge, action, approval_request, call,
        call_with, chunks, collect, custom_id, discord_media_url, edit_request, edit_requests,
        gateway_url, heartbeat, intents, load_cursor_at, local_media, media_root, normalize,
        normalize_interaction, operation, prepare_gateway_value, remember_attachments,
        response_body, safe_name, save_cursor_at, snowflake, stage_event,
    };
    use crabbot_core::types::Request;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::Mutex as AsyncMutex,
        time::{Duration, Instant},
    };
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{WebSocketStream, accept_async};

    #[test]
    fn parses_provider_responses() {
        assert_eq!(response_body(reqwest::StatusCode::OK, json!({"id": "1"})).unwrap()["id"], "1");
        assert!(response_body(reqwest::StatusCode::UNAUTHORIZED, json!({"message":"no"})).is_err());
    }

    #[test]
    fn normalizes_gateway_messages() {
        let message =
            json!({"id":"1","channel_id":"2","content":"hello","author":{"id":"3","bot":false}});
        assert_eq!(normalize(&message).unwrap()["chat"], "2");
        let attachment = json!({"id":"1","channel_id":"2","content":"","attachments":[{"id":"4","url":"https://cdn.discordapp.com/a.png","content_type":"image/png","filename":"a.png"}],"author":{"id":"3","bot":false}});
        assert_eq!(normalize(&attachment).unwrap()["content"][0]["kind"], "image");
        let file = json!({"id":"2","channel_id":"2","guild_id":"9","content":"","thread":{"id":"thread"},"attachments":[{"id":"5","url":"https://cdn.discordapp.com/a.txt","content_type":"text/plain","filename":"a.txt"}],"author":{"id":"3","bot":false}});
        let normalized = normalize(&file).unwrap();
        assert_eq!(normalized["kind"], "guild");
        assert_eq!(normalized["private"], false);
        assert_eq!(normalized["thread"], "thread");
        assert_eq!(normalized["topic"], "thread");
        assert_eq!(normalized["content"][0]["kind"], "file");
        let voice = json!({"id":"6","channel_id":"2","content":"","attachments":[{"id":"7","url":"https://cdn.discordapp.com/a.ogg","content_type":"audio/ogg","filename":"a.ogg"}],"author":{"id":"3","bot":false}});
        assert_eq!(normalize(&voice).unwrap()["content"][0]["kind"], "audio");
        let thread = json!({"id":"3","channel_id":"thread","channel_type":11,"content":"inside","guild_id":"9","author":{"id":"3","bot":false}});
        assert_eq!(normalize(&thread).unwrap()["thread"], "thread");
        assert!(normalize(&json!({"channel_id":"2","content":"hi"})).is_none());
        assert!(
            normalize(&json!({"id":"1","channel_id":"2","content":"hello","author":{"bot":true}}))
                .is_none()
        );
        assert!(normalize(&json!({"id":"1","channel_id":"2","content":""})).is_none());
        assert_eq!(intents(), 37_377);
        assert_eq!(
            gateway_url("https://discord.com/api/v10"),
            "wss://gateway.discord.gg/?v=10&encoding=json"
        );
        assert_eq!(gateway_url("https://example.test"), "wss://example.test");
        assert_eq!(gateway_url("http://127.0.0.1:1"), "ws://127.0.0.1:1");
    }

    #[test]
    fn classifies_gateway_events() {
        assert!(matches!(
            action(
                &json!({"op":0,"t":"MESSAGE_CREATE","d":{"id":"1","channel_id":"2","content":"hi","author":{"id":"3"}}})
            ),
            Action::Event(Some(_))
        ));
        assert!(matches!(
            action(&json!({"op":0,"t":"MESSAGE_CREATE","d":{}})),
            Action::Event(None)
        ));
        assert!(matches!(
            action(
                &json!({"op":0,"t":"INTERACTION_CREATE","d":{"type":3,"id":"1","channel_id":"2","token":"token","data":{"custom_id":"allow"},"user":{"id":"3"}}})
            ),
            Action::Event(Some(_))
        ));
        assert!(matches!(
            action(&json!({"op":1,"d":null})),
            Action::Heartbeat(serde_json::Value::Null)
        ));
        assert!(matches!(action(&json!({"op":7})), Action::Reconnect));
        assert!(matches!(action(&json!({"op":9,"d":true})), Action::Reject { resumable: true }));
        assert!(matches!(action(&json!({"op":9,"d":false})), Action::Reject { resumable: false }));
        assert!(matches!(action(&json!({"op":11})), Action::Ignore));
    }

    #[test]
    fn prepares_channel_operations() {
        assert!(matches!(
            operation("send", &json!({"channel": "1", "text": "hello"})).unwrap(),
            Some(Operation::Send { channel, text, content })
                if channel == "1" && text == "hello" && content.is_empty()
        ));
        assert!(matches!(
            operation("edit", &json!({"channel": "1", "message": "2", "text": "hello"}))
                .unwrap(),
            Some(Operation::Edit { channel, message_id, text })
                if channel == "1" && message_id == "2" && text == "hello"
        ));
        assert!(matches!(
            operation(
                "approval",
                &json!({"channel": "1", "text": "Approve write?", "approve": "allow", "deny": "deny"})
            )
            .unwrap(),
            Some(Operation::Approval { channel, text, approve, deny })
                if channel == "1" && text == "Approve write?" && approve == "allow" && deny == "deny"
        ));
        assert!(matches!(
            operation("callback", &json!({"id": "1", "text": "Denied."})).unwrap(),
            Some(Operation::Callback { id, text }) if id == "1" && text == "Denied."
        ));
        assert!(matches!(operation("poll", &json!({})).unwrap(), Some(Operation::Poll(25))));
        assert!(matches!(
            operation("ack", &json!({"sequence": 7})).unwrap(),
            Some(Operation::Ack(7))
        ));
        assert!(operation("send", &json!({})).is_err());
        assert!(operation("send", &json!({"channel": "1"})).is_err());
        assert!(operation("edit", &json!({"channel": "x", "message": "2", "text": "hi"})).is_err());
        assert!(operation("edit", &json!({"channel": "1", "message": "2", "text": ""})).is_err());
        assert!(
            operation("edit", &json!({"channel": "1", "message": "2", "text": "x".repeat(2_001)}))
                .is_ok()
        );
        assert!(operation("ack", &json!({})).is_err());
        assert!(operation(
            "approval",
            &json!({"channel": "1", "text": "Approve?", "approve": "x".repeat(101), "deny": "deny"})
        )
        .is_err());
        assert!(operation("callback", &json!({"id": "invalid"})).is_err());
        assert!(operation("unknown", &json!({})).unwrap().is_none());
    }

    #[test]
    fn prepares_approval_controls_and_normalizes_interactions() {
        let body = approval_request("Approve write?", "approve-token", "deny-token").unwrap();
        assert_eq!(body["content"], "Approve write?");
        assert_eq!(body["components"][0]["type"], 1);
        assert_eq!(body["components"][0]["components"][0]["label"], "Approve");
        assert_eq!(body["components"][0]["components"][0]["custom_id"], "approve-token");
        assert_eq!(body["components"][0]["components"][1]["label"], "Deny");
        assert_eq!(body["components"][0]["components"][1]["custom_id"], "deny-token");
        assert!(approval_request("Approve?", "same", "same").is_err());

        let event = normalize_interaction(&json!({
            "type": 3,
            "id": "10",
            "channel_id": "20",
            "guild_id": "30",
            "token": "interaction-secret",
            "data": {"custom_id": "approve-token"},
            "member": {"user": {"id": "40"}, "roles": ["50"]}
        }))
        .unwrap();
        assert_eq!(event["kind"], "callback");
        assert_eq!(event["id"], "10");
        assert_eq!(event["chat"], "20");
        assert_eq!(event["sender"], "40");
        assert_eq!(event["roles"], json!(["50"]));
        assert_eq!(event["data"], "approve-token");
        assert_eq!(event["callback_token"], "interaction-secret");
        assert!(normalize_interaction(&json!({"type": 2})).is_none());
        assert!(
            normalize_interaction(&json!({
                "type": 3,
                "id": "10",
                "channel_id": "20",
                "token": "interaction-secret",
                "data": {"custom_id": "invalid custom id"},
                "user": {"id": "40"}
            }))
            .is_none()
        );
    }

    #[test]
    fn prepares_message_edits() {
        let (method, url, body) = edit_request("https://discord.test/api/v10", "1", "2", "hello");
        assert_eq!(method, reqwest::Method::PATCH);
        assert_eq!(url, "https://discord.test/api/v10/channels/1/messages/2");
        assert_eq!(body, json!({"content": "hello"}));

        let requests =
            edit_requests("https://discord.test/api/v10", "1", "2", &"x".repeat(2_001)).unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0, reqwest::Method::PATCH);
        assert_eq!(requests[0].2["content"].as_str().unwrap().len(), 2_000);
        assert_eq!(requests[1].0, reqwest::Method::POST);
        assert_eq!(requests[1].1, "https://discord.test/api/v10/channels/1/messages");
        assert_eq!(requests[1].2["content"], "x");
        assert!(edit_requests("https://discord.test/api/v10", "1", "2", "").is_err());
    }

    #[test]
    fn persists_gateway_cursor() {
        let path = std::env::temp_dir().join(format!(
            "crabbot-discord-cursor-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        assert_eq!(load_cursor_at(&path), (None, None));
        save_cursor_at(&path, Some(7), Some("session")).unwrap();
        assert_eq!(load_cursor_at(&path), (Some(7), Some("session".into())));
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(load_cursor_at(&path), (None, None));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn acknowledges_only_the_pending_gateway_event() {
        let state = AsyncMutex::new(GatewayState {
            socket: None,
            interval: Duration::from_secs(45),
            next_heartbeat: Instant::now(),
            sequence: Some(6),
            session_id: Some("session".into()),
            pending_sequence: Some(7),
            interactions: Default::default(),
            attachments: Default::default(),
        });
        assert_eq!(acknowledge(&state, 7).await.unwrap()["acknowledged"], true);
        assert!(acknowledge(&state, 7).await.is_err());
    }

    #[test]
    fn stages_gateway_sequences_for_host_acknowledgement() {
        let mut pending = None;
        let event = stage_event(Some(json!({"id": "1"})), Some(2), &mut pending).unwrap().unwrap();
        assert_eq!(event["gateway_sequence"], 2);
        assert_eq!(pending, Some(2));
        assert!(stage_event(None, Some(3), &mut pending).unwrap().is_none());
        assert!(stage_event(Some(json!({"id": "1"})), None, &mut pending).is_err());
    }

    #[test]
    fn records_gateway_ready_and_sequences() {
        let mut sequence = None;
        let mut session_id = None;
        let mut pending = None;
        let ready = json!({"op": 0, "t": "READY", "s": 1, "d": {"session_id": "session"}});
        assert!(matches!(
            prepare_gateway_value(&ready, &mut sequence, &mut session_id, None, &mut pending)
                .unwrap(),
            Action::Ignore
        ));
        assert_eq!(sequence, Some(1));
        assert_eq!(session_id.as_deref(), Some("session"));
        let message = json!({
            "op": 0,
            "t": "MESSAGE_CREATE",
            "s": 2,
            "d": {"id": "1", "channel_id": "2", "content": "hi", "author": {"id": "3"}}
        });
        assert!(matches!(
            prepare_gateway_value(&message, &mut sequence, &mut session_id, None, &mut pending)
                .unwrap(),
            Action::Event(Some(_))
        ));
        assert_eq!(pending, Some(2));
    }

    #[test]
    fn validates_gateway_hello() {
        let message =
            Message::Text(json!({"op":10,"d":{"heartbeat_interval":45000}}).to_string().into());
        assert_eq!(heartbeat(&message).unwrap(), 45000);
        let bad = Message::Text(json!({"op":0,"d":{}}).to_string().into());
        assert!(heartbeat(&bad).is_err());
        let zero = Message::Text(json!({"op":10,"d":{"heartbeat_interval":0}}).to_string().into());
        assert!(heartbeat(&zero).is_err());
        let invalid = Message::Text("not json".into());
        assert!(heartbeat(&invalid).is_err());
    }

    #[tokio::test]
    async fn validates_channel_calls() {
        let client = reqwest::Client::new();
        assert!(
            call_with(&client, 1, "unknown".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            call_with(&client, 1, "poll".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .is_err()
        );
        assert!(
            call_with(&client, 1, "send".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .is_err()
        );
        assert!(
            call_with(
                &client,
                1,
                "send".into(),
                json!({"channel":"1"}),
                "secret",
                "http://127.0.0.1:1"
            )
            .await
            .is_err()
        );
        assert!(
            call_with(
                &client,
                1,
                "send".into(),
                json!({"channel":"1","text":"hi"}),
                "bad\nvalue",
                "http://127.0.0.1:1"
            )
            .await
            .is_err()
        );
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "send".into(), params: json!({}) };
        assert!(call(&client, note).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn receives_and_acknowledges_a_gateway_event() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket: WebSocketStream<_> = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":10,"d":{"heartbeat_interval":1000}}).to_string().into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "op":0,
                        "t":"MESSAGE_CREATE",
                        "s":1,
                        "d":{"id":"10","channel_id":"20","content":"hello","author":{"id":"30","bot":false}}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        });
        let state = AsyncMutex::new(GatewayState::default());
        let events =
            super::gateway(&state, "token", 2, &format!("http://{address}")).await.unwrap();
        assert_eq!(events["events"][0]["text"], "hello");
        assert_eq!(events["events"][0]["gateway_sequence"], 1);
        assert_eq!(acknowledge(&state, 1).await.unwrap()["acknowledged"], true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sends_edits_and_acknowledges_approvals() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let body = br#"{"id":"1","ok":true}"#;
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        let client = reqwest::Client::new();
        let base = format!("http://{address}");
        let sent = call_with(
            &client,
            1,
            "send".into(),
            json!({"channel":"1","text":"hello"}),
            "token",
            &base,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sent.result.unwrap()["ok"], true);
        let edited = call_with(
            &client,
            2,
            "edit".into(),
            json!({"channel":"1","message":"2","text":"updated"}),
            "token",
            &base,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(edited.result.unwrap()["edited"]["ok"], true);
        let approval = call_with(
            &client,
            3,
            "approval".into(),
            json!({"channel":"1","text":"Approve?","approve":"yes","deny":"no"}),
            "token",
            &base,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(approval.result.unwrap()["ok"], true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acknowledges_interaction_callbacks_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let state = AsyncMutex::new(GatewayState {
            socket: None,
            interval: Duration::from_secs(45),
            next_heartbeat: Instant::now(),
            sequence: None,
            session_id: None,
            pending_sequence: None,
            interactions: [(
                "10".into(),
                super::Interaction {
                    token: "secret".into(),
                    expires: Instant::now() + Duration::from_secs(60),
                },
            )]
            .into_iter()
            .collect(),
            attachments: Default::default(),
        });
        let value = super::callback(
            &reqwest::Client::new(),
            &state,
            "10",
            "Thanks.",
            &format!("http://{address}"),
        )
        .await
        .unwrap();
        assert_eq!(value["acknowledged"], true);
        assert!(
            super::callback(
                &reqwest::Client::new(),
                &state,
                "10",
                "Again.",
                &format!("http://{address}"),
            )
            .await
            .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounds_channel_bodies() {
        use futures_util::stream;

        assert_eq!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(b"ok".to_vec())])).await.unwrap(),
            "ok"
        );
        assert!(collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])])).await.is_err());
        assert!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]))
                .await
                .is_err()
        );
    }

    #[test]
    fn chunks_long_messages() {
        let parts = chunks(&"x".repeat(9), 4);
        assert_eq!(parts, vec!["xxxx", "xxxx", "x"]);
        assert_eq!(chunks("", 4), vec![String::new()]);
    }

    #[test]
    fn validates_media_metadata_and_identifiers() {
        assert!(discord_media_url("https://cdn.discordapp.com/files/a.png"));
        assert!(discord_media_url("https://media.discordapp.net/files/a.png"));
        assert!(!discord_media_url("http://cdn.discordapp.com/files/a.png"));
        assert!(!discord_media_url("https://example.test/files/a.png"));
        assert_eq!(safe_name("nested/file.txt").as_deref(), Some("file.txt"));
        assert!(safe_name("").is_none());
        assert!(safe_name(&"x".repeat(256)).is_none());
        assert!(snowflake("123456"));
        assert!(!snowflake(""));
        assert!(!snowflake("not-a-snowflake"));
        assert!(custom_id("approve:write"));
        assert!(!custom_id(""));
        assert!(!custom_id("not valid"));

        let mut attachments = std::collections::BTreeMap::new();
        remember_attachments(
            &json!({
                "attachments": [
                    {"id": "1", "url": "https://cdn.discordapp.com/a.txt", "filename": "a.txt", "content_type": "text/plain", "size": 4},
                    {"id": "invalid", "url": "https://cdn.discordapp.com/b.txt"},
                    {"id": "2", "url": "http://example.test/b.txt"}
                ]
            }),
            &mut attachments,
        );
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments["1"].name, "a.txt");

        let content = json!([{"kind":"text","text":"hello"}]);
        assert!(matches!(
            operation("send", &json!({"channel":"1", "content": content})).unwrap(),
            Some(Operation::Send { content, .. }) if content.len() == 1
        ));
        assert!(operation("media", &json!({"uri":"discord://attachment/1"})).is_ok());
        assert!(operation("media", &json!({})).is_err());
        assert!(
            operation("send", &json!({"channel":"1", "content": [{"kind":"unknown"}]})).is_err()
        );
        let too_many =
            serde_json::Value::Array((0..9).map(|_| json!({"kind":"text","text":"x"})).collect());
        assert!(operation("send", &json!({"channel":"1", "content": too_many})).is_err());
        assert!(media_root().is_err());
        assert!(local_media("outside").is_err());
    }

    #[test]
    fn rejects_invalid_interactions_and_cursors() {
        let invalid_token = json!({
            "type": 3,
            "id": "10",
            "channel_id": "20",
            "token": "bad token",
            "data": {"custom_id": "approve"},
            "user": {"id": "40"}
        });
        assert!(normalize_interaction(&invalid_token).is_none());

        let path = std::env::temp_dir().join(format!(
            "crabbot-discord-invalid-cursor-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::write(&path, json!({"version": 2, "sequence": 4}).to_string()).unwrap();
        assert_eq!(load_cursor_at(&path), (None, None));
        let _ = std::fs::remove_file(path);
    }
}
