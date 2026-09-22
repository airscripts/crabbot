#![forbid(unsafe_code)]

#[cfg(not(test))]
use crabbot_core::types::Request;
use crabbot_core::types::Response;
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol},
};

use serde_json::{Value, json};
use std::{
    fs,
    fs::OpenOptions,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    process::Output,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};

#[cfg(not(test))]
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, Command};
#[cfg(not(test))]
use tokio::sync::Mutex;

const OUTPUT_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const MEDIA_LIMIT: usize = 4 * 1024 * 1024;
const MEDIA_CACHE_LIMIT: u64 = 64 * 1024 * 1024;
const MEDIA_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const INBOX_BYTES_LIMIT: usize = 16 * 1024 * 1024;
const QUEUE_LIMIT: usize = 4096;
const RECEIVE_LIMIT: usize = 32;
#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct Pending {
    sequence: u64,
    event: Value,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct Inbox {
    #[serde(default = "inbox_version")]
    version: u8,
    #[serde(default = "first_sequence")]
    next_sequence: u64,
    #[serde(default)]
    pending: Vec<Pending>,
}

impl Default for Inbox {
    fn default() -> Self {
        Self { version: inbox_version(), next_sequence: first_sequence(), pending: Vec::new() }
    }
}

fn inbox_version() -> u8 {
    1
}

fn first_sequence() -> u64 {
    1
}

impl Inbox {
    fn events(&self, id: u64) -> crabbot_core::Result<Vec<Value>> {
        let mut events = Vec::new();

        while events.len() < 32 {
            let Some(pending) = self.pending.get(events.len()) else { break };

            let mut event = pending.event.clone();
            event["gateway_sequence"] = json!(pending.sequence);
            let mut candidate = events.clone();
            candidate.push(event.clone());

            if response_fits(id, &candidate)? {
                events.push(event);
                continue;
            }

            if events.is_empty() {
                return Err(crabbot_core::Error::Denied(
                    "Signal pending event exceeds the JSONL frame limit.".into(),
                ));
            }

            break;
        }

        Ok(events)
    }

    fn stage(&mut self, events: impl IntoIterator<Item = Value>) -> crabbot_core::Result<()> {
        let events = events
            .into_iter()
            .filter(|event| {
                event["id"].as_str().is_none_or(|id| {
                    !self.pending.iter().any(|pending| pending.event["id"].as_str() == Some(id))
                })
            })
            .collect::<Vec<_>>();

        if self.pending.len().saturating_add(events.len()) > QUEUE_LIMIT {
            return Err(crabbot_core::Error::Denied("Signal inbox is full.".into()));
        }

        for event in &events {
            let sequence = self.next_sequence;
            let mut candidate = event.clone();
            candidate["gateway_sequence"] = json!(sequence);

            if !response_fits(u64::MAX, &[candidate])? {
                return Err(crabbot_core::Error::Denied(
                    "Signal pending event exceeds the JSONL frame limit.".into(),
                ));
            }
        }

        let previous = self.clone();

        for event in events {
            self.pending.push(Pending { sequence: self.next_sequence, event });
            self.next_sequence = self.next_sequence.saturating_add(1).max(1);
        }

        match save_inbox(self) {
            Ok(()) => Ok(()),

            Err(error) => {
                *self = previous;
                Err(error)
            }
        }
    }

    fn acknowledge(&mut self, sequence: u64) -> crabbot_core::Result<()> {
        let Some(index) = self.pending.iter().position(|pending| pending.sequence == sequence)
        else {
            return (sequence < self.next_sequence).then_some(()).ok_or_else(|| {
                crabbot_core::Error::Denied("Signal acknowledgement is stale.".into())
            });
        };

        let previous = self.clone();
        self.pending.remove(index);

        match save_inbox(self) {
            Ok(()) => Ok(()),

            Err(error) => {
                *self = previous;
                Err(error)
            }
        }
    }
}

fn response_fits(id: u64, events: &[Value]) -> crabbot_core::Result<bool> {
    let response = Response::ok(id, json!({"events": events}));
    Ok(serde_json::to_vec(&response)?.len().saturating_add(1) <= crabbot_core::jsonl::MAX)
}

fn inbox_path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_HOME").map(|home| PathBuf::from(home).join("signal-inbox.json"))
}

#[cfg_attr(test, allow(dead_code))]
fn receive_spool_path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_HOME").map(|home| PathBuf::from(home).join("signal-receive.jsonl"))
}

#[cfg_attr(test, allow(dead_code))]
fn load_inbox() -> crabbot_core::Result<Inbox> {
    let Some(path) = inbox_path() else {
        return Ok(Inbox::default());
    };

    let Some(bytes) = crabbot_file::load(&path, INBOX_BYTES_LIMIT as u64).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal inbox could not be loaded: {error}."))
    })?
    else {
        return Ok(Inbox::default());
    };

    let mut inbox = serde_json::from_slice::<Inbox>(&bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal inbox is invalid: {error}."))
    })?;

    if inbox.version != inbox_version() {
        return Err(crabbot_core::Error::Denied("Signal inbox version is unsupported.".into()));
    }

    inbox.next_sequence = inbox.next_sequence.max(1);
    Ok(inbox)
}

fn save_inbox(inbox: &Inbox) -> crabbot_core::Result<()> {
    let bytes = serde_json::to_vec(inbox)?;

    if bytes.len() > INBOX_BYTES_LIMIT {
        return Err(crabbot_core::Error::Denied(
            "Signal inbox exceeds the startup load limit.".into(),
        ));
    }

    let Some(path) = inbox_path() else {
        return Ok(());
    };

    crabbot_file::save(path, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal inbox could not be stored: {error}."))
    })
}

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    if inbox_path().is_none() {
        return Err(crabbot_core::Error::Denied(
            "CRABBOT_HOME is required for the Signal inbox.".into(),
        ));
    }

    let inbox = Arc::new(Mutex::new(load_inbox()?));

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "signal".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: Vec::new(),
        },
        move |request| {
            let inbox = Arc::clone(&inbox);
            async move { call(&inbox, request).await }
        },
    )
    .await
}

#[cfg(not(test))]
async fn call(
    inbox: &Arc<Mutex<Inbox>>,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let account = std::env::var("CRABBOT_SIGNAL_ACCOUNT").map_err(|_| {
        crabbot_core::Error::Denied("CRABBOT_SIGNAL_ACCOUNT is not configured.".into())
    })?;

    if account.trim().is_empty() {
        return Err(crabbot_core::Error::Denied(
            "CRABBOT_SIGNAL_ACCOUNT is not configured.".into(),
        ));
    }

    let result = match method.as_str() {
        "poll" => {
            let mut inbox = inbox.lock().await;
            clear_redundant_receive_spool(&inbox)?;
            let mut events = inbox.events(id)?;

            if events.is_empty() {
                let value = poll(&account).await?;
                let polled = value["events"].as_array().cloned().unwrap_or_default();
                inbox.stage(polled)?;
                clear_receive_spool()?;
                events = inbox.events(id)?;
            }

            json!({"events": events})
        }

        "ack" => {
            let sequence = params["sequence"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("ack.sequence is required.".into()))?;
            inbox.lock().await.acknowledge(sequence)?;
            json!({"acknowledged": true})
        }

        "send" => send(&account, &params).await?,
        "media" => media(&account, &params).await?,
        "info" => json!({"account": account}),
        _ => return Ok(None),
    };

    Ok(Some(Response::ok(id, result)))
}

async fn run_with(command: &str, args: &[String]) -> crabbot_core::Result<Value> {
    let output =
        command_output(command, args, OUTPUT_LIMIT, OUTPUT_LIMIT).await.map_err(signal_error)?;

    if output.stdout.len() > OUTPUT_LIMIT || output.stderr.len() > OUTPUT_LIMIT {
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }

    if !output.status.success() {
        return Err(crabbot_core::Error::Denied(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }

    let text = String::from_utf8(output.stdout)
        .map_err(|_| crabbot_core::Error::Denied("signal-cli output was invalid UTF-8.".into()))?;

    Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({"output": text})))
}

async fn command_output(
    command: &str,
    args: &[String],
    stdout_limit: usize,
    stderr_limit: usize,
) -> io::Result<Output> {
    let mut child = Command::new(command)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdout = child.stdout.take().ok_or_else(|| {
        io::Error::new(io::ErrorKind::BrokenPipe, "signal-cli stdout was unavailable.")
    })?;

    let mut stderr = child.stderr.take().ok_or_else(|| {
        io::Error::new(io::ErrorKind::BrokenPipe, "signal-cli stderr was unavailable.")
    })?;

    let result = tokio::time::timeout(
        Duration::from_secs(45),
        capture_output(&mut child, &mut stdout, &mut stderr, stdout_limit, stderr_limit),
    )
    .await;

    match result {
        Ok(Ok(output)) => Ok(output),

        Ok(Err(error)) => {
            terminate_child(&mut child).await;
            Err(error)
        }

        Err(_) => {
            terminate_child(&mut child).await;
            Err(io::Error::new(io::ErrorKind::TimedOut, "signal-cli timed out."))
        }
    }
}

async fn read_output(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    limit: usize,
) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(limit);
    let mut buffer = [0_u8; 8192];

    loop {
        let remaining = limit.saturating_sub(output.len());
        let read_limit = buffer.len().min(remaining.saturating_add(1));
        let count = reader.read(&mut buffer[..read_limit]).await?;

        if count == 0 {
            return Ok(output);
        }

        if count > remaining {
            return Err(output_limit_error());
        }

        output.extend_from_slice(&buffer[..count]);
    }
}

fn output_limit_error() -> io::Error {
    io::Error::new(ErrorKind::FileTooLarge, "signal-cli output exceeded the limit.")
}

async fn capture_output(
    child: &mut Child,
    stdout: &mut (impl AsyncRead + Unpin),
    stderr: &mut (impl AsyncRead + Unpin),
    stdout_limit: usize,
    stderr_limit: usize,
) -> io::Result<Output> {
    let stdout = read_output(stdout, stdout_limit);
    let stderr = read_output(stderr, stderr_limit);
    tokio::pin!(stdout);
    tokio::pin!(stderr);
    let (stdout, stderr) = tokio::select! {
        result = &mut stdout => {
            let stdout = result?;
            let stderr = stderr.await?;
            (stdout, stderr)
        }

        result = &mut stderr => {
            let stderr = result?;
            let stdout = stdout.await?;
            (stdout, stderr)
        }
    };

    let status = child.wait().await?;
    Ok(Output { status, stdout, stderr })
}

async fn stream_output(
    reader: &mut (impl AsyncRead + Unpin),
    mut writer: impl AsyncWrite + Unpin,
    limit: usize,
) -> io::Result<()> {
    let mut written = 0;
    let mut buffer = [0_u8; 8192];

    loop {
        let remaining = limit.saturating_sub(written);
        let read_limit = buffer.len().min(remaining.saturating_add(1));
        let count = reader.read(&mut buffer[..read_limit]).await?;

        if count == 0 {
            writer.flush().await?;
            return Ok(());
        }

        if count > remaining {
            return Err(output_limit_error());
        }

        writer.write_all(&buffer[..count]).await?;
        written += count;
    }
}

async fn receive_output(
    child: &mut Child,
    stdout: impl AsyncRead + Unpin,
    stderr: impl AsyncRead + Unpin,
    spool: impl AsyncWrite + Unpin,
) -> io::Result<Output> {
    let mut stdout = stdout;
    let mut stderr = stderr;
    let stdout = stream_output(&mut stdout, spool, OUTPUT_LIMIT);
    let stderr = read_output(&mut stderr, OUTPUT_LIMIT);
    tokio::pin!(stdout);
    tokio::pin!(stderr);
    let stderr = tokio::select! {
        result = &mut stdout => {
            result?;
            stderr.await?
        }

        result = &mut stderr => {
            let stderr = result?;
            stdout.await?;
            stderr
        }
    };

    let status = child.wait().await?;
    Ok(Output { status, stdout: Vec::new(), stderr })
}

async fn terminate_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn signal_error(error: io::Error) -> crabbot_core::Error {
    match error.kind() {
        io::ErrorKind::TimedOut => crabbot_core::Error::Denied("signal-cli timed out.".into()),

        ErrorKind::FileTooLarge => {
            crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into())
        }

        _ => crabbot_core::Error::Denied(format!("signal-cli failed: {error}.")),
    }
}

#[cfg(not(test))]
async fn poll(account: &str) -> crabbot_core::Result<Value> {
    let command = std::env::var("CRABBOT_SIGNAL_COMMAND").unwrap_or_else(|_| "signal-cli".into());
    let path = receive_spool_path().ok_or_else(|| {
        crabbot_core::Error::Denied("CRABBOT_HOME is required for the Signal receive spool.".into())
    })?;

    let values = if let Some(values) = load_or_clear_receive_spool(&path)? {
        values
    } else {
        receive_to_spool(account, &command, &[], &path).await?;
        load_receive_spool(&path)?.ok_or_else(|| {
            crabbot_core::Error::Denied("Signal receive spool disappeared.".into())
        })?
    };

    let events = values.iter().filter_map(normalize).collect::<Vec<_>>();
    Ok(json!({"events": events}))
}

#[cfg(test)]
async fn poll_with(account: &str, command: &str, prefix: &[String]) -> crabbot_core::Result<Value> {
    let args = receive_args(account, prefix);
    let values = run_jsonl_with(command, &args).await?;
    let events = values.iter().filter_map(normalize).collect::<Vec<_>>();
    Ok(json!({"events": events}))
}

fn receive_args(account: &str, prefix: &[String]) -> Vec<String> {
    let mut args = prefix.to_vec();
    args.extend([
        "-a".into(),
        account.into(),
        "receive".into(),
        "--json".into(),
        "--ignore-attachments".into(),
        "--max-messages".into(),
        RECEIVE_LIMIT.to_string(),
    ]);
    args
}

fn receive_temp_path(path: &Path) -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |value| value.as_nanos());
    path.with_file_name(format!(".signal-receive.jsonl.tmp-{nonce}"))
}

#[cfg_attr(test, allow(dead_code))]
async fn receive_to_spool(
    account: &str,
    command: &str,
    prefix: &[String],
    path: &Path,
) -> crabbot_core::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(crabbot_core::Error::Denied("Signal receive spool path is invalid.".into()));
    };

    fs::create_dir_all(parent).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal receive spool directory failed: {error}."))
    })?;

    if matches!(fs::symlink_metadata(path), Ok(metadata) if metadata.file_type().is_symlink()) {
        return Err(crabbot_core::Error::Denied(
            "Signal receive spool cannot be a symbolic link.".into(),
        ));
    }

    let temporary = receive_temp_path(path);
    let file =
        OpenOptions::new().write(true).create_new(true).open(&temporary).map_err(|error| {
            crabbot_core::Error::Denied(format!(
                "Signal receive spool temporary file could not be opened: {error}."
            ))
        })?;

    crabbot_file::private(&temporary).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        crabbot_core::Error::Denied(format!(
            "Signal receive spool temporary file could not be protected: {error}."
        ))
    })?;

    let mut child = match Command::new(command)
        .args(receive_args(account, prefix))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,

        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(crabbot_core::Error::Denied(format!("signal-cli failed: {error}.")));
        }
    };

    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,

        None => {
            terminate_child(&mut child).await;
            let _ = fs::remove_file(&temporary);
            return Err(crabbot_core::Error::Denied(
                "Signal receive stdout was unavailable.".into(),
            ));
        }
    };

    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,

        None => {
            terminate_child(&mut child).await;
            let _ = fs::remove_file(&temporary);
            return Err(crabbot_core::Error::Denied(
                "Signal receive stderr was unavailable.".into(),
            ));
        }
    };

    let spool = tokio::fs::File::from_std(file);
    let output = match tokio::time::timeout(
        Duration::from_secs(45),
        receive_output(&mut child, stdout, stderr, spool),
    )
    .await
    {
        Ok(Ok(output)) => output,

        Ok(Err(error)) => {
            terminate_child(&mut child).await;
            let _ = fs::remove_file(&temporary);
            return Err(signal_error(error));
        }

        Err(_) => {
            terminate_child(&mut child).await;
            let _ = fs::remove_file(&temporary);
            return Err(crabbot_core::Error::Denied("signal-cli timed out.".into()));
        }
    };

    OpenOptions::new().write(true).open(&temporary).and_then(|file| file.sync_all()).map_err(
        |error| {
            let _ = fs::remove_file(&temporary);
            crabbot_core::Error::Denied(format!(
                "Signal receive spool could not be synced: {error}."
            ))
        },
    )?;

    if output.stderr.len() > OUTPUT_LIMIT {
        let _ = fs::remove_file(&temporary);
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }

    if !output.status.success() {
        let _ = fs::remove_file(&temporary);
        return Err(crabbot_core::Error::Denied(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }

    let metadata = fs::metadata(&temporary).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        crabbot_core::Error::Denied(format!(
            "Signal receive spool temporary file could not be inspected: {error}."
        ))
    })?;

    if metadata.len() > OUTPUT_LIMIT as u64 {
        let _ = fs::remove_file(&temporary);
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }

    if let Err(error) = load_receive_spool(&temporary) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }

    let bytes = match fs::read(&temporary) {
        Ok(bytes) => bytes,

        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(crabbot_core::Error::Denied(format!(
                "Signal receive spool could not be read: {error}."
            )));
        }
    };

    if let Err(error) = crabbot_file::save(path, bytes) {
        let _ = fs::remove_file(&temporary);
        return Err(crabbot_core::Error::Denied(format!(
            "Signal receive spool could not be installed: {error}."
        )));
    }

    let _ = fs::remove_file(&temporary);
    Ok(())
}

#[cfg_attr(test, allow(dead_code))]
fn load_receive_spool(path: &Path) -> crabbot_core::Result<Option<Vec<Value>>> {
    let Some(bytes) = crabbot_file::load(path, OUTPUT_LIMIT as u64).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal receive spool could not be loaded: {error}."))
    })?
    else {
        return Ok(None);
    };

    let text = String::from_utf8(bytes).map_err(|_| {
        crabbot_core::Error::Denied("Signal receive spool was invalid UTF-8.".into())
    })?;

    parse_jsonl(&text).map(Some)
}

#[cfg_attr(test, allow(dead_code))]
fn clear_receive_spool() -> crabbot_core::Result<()> {
    let Some(path) = receive_spool_path() else {
        return Ok(());
    };

    clear_receive_spool_at(&path)
}

fn clear_receive_spool_at(path: &Path) -> crabbot_core::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(crabbot_core::Error::Denied(format!(
            "Signal receive spool could not be removed: {error}."
        ))),
    }
}

fn load_or_clear_receive_spool(path: &Path) -> crabbot_core::Result<Option<Vec<Value>>> {
    match load_receive_spool(path) {
        Ok(values) => Ok(values),

        Err(_) => {
            clear_receive_spool_at(path)?;
            Ok(None)
        }
    }
}

#[cfg_attr(test, allow(dead_code))]
fn clear_redundant_receive_spool(inbox: &Inbox) -> crabbot_core::Result<()> {
    let Some(path) = receive_spool_path() else {
        return Ok(());
    };

    let Some(values) = load_or_clear_receive_spool(&path)? else {
        return Ok(());
    };

    let events = values.iter().filter_map(normalize).collect::<Vec<_>>();

    if events.iter().all(|event| {
        event["id"].as_str().is_some_and(|id| {
            inbox.pending.iter().any(|pending| pending.event["id"].as_str() == Some(id))
        })
    }) {
        clear_receive_spool()?;
    }

    Ok(())
}

#[cfg(test)]
async fn run_jsonl_with(command: &str, args: &[String]) -> crabbot_core::Result<Vec<Value>> {
    let output =
        command_output(command, args, OUTPUT_LIMIT, OUTPUT_LIMIT).await.map_err(signal_error)?;

    if output.stdout.len() > OUTPUT_LIMIT || output.stderr.len() > OUTPUT_LIMIT {
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }

    if !output.status.success() {
        return Err(crabbot_core::Error::Denied(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }

    let text = String::from_utf8(output.stdout)
        .map_err(|_| crabbot_core::Error::Denied("signal-cli output was invalid UTF-8.".into()))?;

    parse_jsonl(&text)
}

fn parse_jsonl(text: &str) -> crabbot_core::Result<Vec<Value>> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).map_err(|_| {
                crabbot_core::Error::Denied("signal-cli JSONL output was invalid.".into())
            })
        })
        .try_fold(Vec::new(), |mut values, value| {
            match value? {
                Value::Array(items) => values.extend(items),
                value => values.push(value),
            }

            Ok(values)
        })
}

fn normalize(value: &Value) -> Option<Value> {
    let envelope = value.get("envelope")?;

    let data = envelope.get("dataMessage")?;
    let source = envelope["sourceNumber"].as_str().or_else(|| envelope["source"].as_str())?;
    let timestamp = envelope["timestamp"].as_i64().unwrap_or_default();
    let group_id = data["groupInfo"]["groupId"].as_str();
    let group = group_id.unwrap_or(source);
    let text = data["message"].as_str().unwrap_or_default();

    if text.is_empty() && data["attachments"].as_array().is_none() {
        return None;
    }

    let content = content(data, group_id.map_or("recipient", |_| "group"), group);
    Some(json!({
        "id": format!("{source}:{group}:{timestamp}"),
        "chat": group,
        "sender": source,
        "private": data["groupInfo"].is_null(),
        "text": text,
        "content": content,
    }))
}

fn content(data: &Value, origin_kind: &str, origin: &str) -> Vec<Value> {
    let mut items = Vec::new();

    if let Some(text) = data["message"].as_str().filter(|text| !text.is_empty()) {
        items.push(json!({"kind": "text", "text": text}));
    }

    for attachment in data["attachments"].as_array().into_iter().flatten().take(8) {
        let id = attachment["id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| attachment["id"].as_i64().map(|value| value.to_string()));

        let Some(id) = id.filter(|id| !id.is_empty() && id.len() <= 128) else { continue };

        let Some(uri) = attachment_uri(&id, origin_kind, origin) else { continue };

        let mime = attachment["contentType"].as_str();
        let name = attachment["filename"].as_str().unwrap_or("attachment");
        let kind = if mime.is_some_and(|value| value.starts_with("audio/")) {
            "audio"
        } else if mime.is_some_and(|value| value.starts_with("image/")) {
            "image"
        } else {
            "file"
        };

        if kind == "image" {
            items.push(json!({"kind": kind, "uri": uri, "alt": name}));
        } else if kind == "audio" {
            items.push(json!({"kind": kind, "uri": uri, "mime": mime}));
        } else {
            items.push(json!({"kind": kind, "uri": uri, "name": name, "mime": mime}));
        }
    }

    items
}

fn attachment_uri(id: &str, origin_kind: &str, origin: &str) -> Option<String> {
    if id.is_empty()
        || id.len() > 128
        || id.chars().any(|character| character.is_control() || matches!(character, '?' | '#'))
        || origin.is_empty()
        || origin.len() > 128
        || origin.chars().any(char::is_control)
    {
        return None;
    }

    Some(format!("signal://attachment/{id}?{origin_kind}={}", URL_SAFE_NO_PAD.encode(origin)))
}

enum AttachmentOrigin {
    Recipient(String),
    Group(String),
}

fn attachment_origin(query: &str) -> crabbot_core::Result<AttachmentOrigin> {
    let (kind, encoded) = query.split_once('=').ok_or_else(|| {
        crabbot_core::Error::Denied("Signal attachment origin is missing.".into())
    })?;

    let origin = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| crabbot_core::Error::Denied("Signal attachment origin is invalid.".into()))?;

    let origin = String::from_utf8(origin)
        .map_err(|_| crabbot_core::Error::Denied("Signal attachment origin is invalid.".into()))?;

    if origin.is_empty() || origin.len() > 128 || origin.chars().any(char::is_control) {
        return Err(crabbot_core::Error::Denied("Signal attachment origin is invalid.".into()));
    }

    match kind {
        "recipient" => Ok(AttachmentOrigin::Recipient(origin)),
        "group" => Ok(AttachmentOrigin::Group(origin)),
        _ => Err(crabbot_core::Error::Denied("Signal attachment origin is invalid.".into())),
    }
}

#[cfg(not(test))]
async fn send(account: &str, params: &Value) -> crabbot_core::Result<Value> {
    let command = std::env::var("CRABBOT_SIGNAL_COMMAND").unwrap_or_else(|_| "signal-cli".into());
    send_with(account, params, &command, &[]).await
}

async fn send_with(
    account: &str,
    params: &Value,
    command: &str,
    prefix: &[String],
) -> crabbot_core::Result<Value> {
    let chat = params["chat"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Signal send requires a chat.".into()))?;

    let text = params["text"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Signal send requires text.".into()))?;

    let mut args = prefix.to_vec();
    args.extend(["-a".into(), account.into(), "send".into(), "-m".into(), text.into()]);

    if let Some(values) = params["content"].as_array() {
        for value in values.iter().take(8) {
            let Some(uri) = value["uri"].as_str() else { continue };

            let path = local_media(uri)?;
            args.extend(["-a".into(), path.display().to_string()]);
        }
    }

    if params["private"] == false {
        args.extend(["--group-id".into(), chat.into()]);
    } else {
        args.push(chat.into());
    }

    run_with(command, &args).await
}

#[cfg(not(test))]
async fn media(account: &str, params: &Value) -> crabbot_core::Result<Value> {
    let uri = params["uri"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("media.uri is required.".into()))?;

    let root = media_root()?;
    let command = std::env::var("CRABBOT_SIGNAL_COMMAND").unwrap_or_else(|_| "signal-cli".into());
    signal_media_at(account, uri, &root, &command, &[]).await
}

async fn media_at(uri: &str, root: &Path) -> crabbot_core::Result<Value> {
    let path = local_media_at(uri, root)?;
    let metadata = tokio::fs::metadata(&path).await?;

    if metadata.len() as usize > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Signal media is too large.".into()));
    }

    Ok(json!({"uri": format!("file://{}", path.display())}))
}

async fn signal_media_at(
    account: &str,
    uri: &str,
    root: &Path,
    command: &str,
    prefix: &[String],
) -> crabbot_core::Result<Value> {
    if uri.starts_with("file://") {
        return media_at(uri, root).await;
    }

    let attachment = uri
        .strip_prefix("signal://attachment/")
        .ok_or_else(|| crabbot_core::Error::Denied("Signal media URI is invalid.".into()))?;

    let (id, query) = attachment.split_once('?').unwrap_or((attachment, ""));

    if id.is_empty()
        || id.len() > 128
        || id.chars().any(|character| character.is_control() || matches!(character, '?' | '#'))
    {
        return Err(crabbot_core::Error::Denied("Signal media URI is invalid.".into()));
    }

    std::fs::create_dir_all(root).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal media directory failed: {error}."))
    })?;

    cleanup_media(root);
    let name =
        format!("signal-{}.bin", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id));

    let destination = root.join(name);

    if let Ok(value) = media_at(&format!("file://{}", destination.display()), root).await {
        return Ok(value);
    }

    let origin = attachment_origin(query)?;
    let bytes = get_attachment(account, id, &origin, command, prefix).await?;

    if bytes.len() > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Signal media is too large.".into()));
    }

    crabbot_file::save(&destination, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Signal media storage failed: {error}."))
    })?;

    cleanup_media(root);
    media_at(&format!("file://{}", destination.display()), root).await
}

fn cleanup_media(root: &Path) {
    cleanup_media_with_limit(root, MEDIA_CACHE_LIMIT);
}

fn cleanup_media_with_limit(root: &Path, limit: u64) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };

    let cutoff = SystemTime::now().checked_sub(MEDIA_TTL).unwrap_or(SystemTime::UNIX_EPOCH);
    let mut files = Vec::new();

    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|value| value.is_file()) {
            continue;
        }

        let Ok(metadata) = entry.metadata() else { continue };

        let Ok(modified) = metadata.modified() else { continue };

        if modified < cutoff {
            let _ = fs::remove_file(entry.path());
            continue;
        }

        files.push((entry.path(), modified, metadata.len()));
    }

    files.sort_by_key(|(_, modified, _)| *modified);
    let mut total = files.iter().map(|(_, _, size)| *size).sum::<u64>();

    for (path, _, size) in files {
        if total <= limit {
            break;
        }

        if fs::remove_file(path).is_ok() {
            total = total.saturating_sub(size);
        }
    }
}

async fn get_attachment(
    account: &str,
    id: &str,
    origin: &AttachmentOrigin,
    command: &str,
    prefix: &[String],
) -> crabbot_core::Result<Vec<u8>> {
    let args = attachment_args(account, id, origin, prefix);
    let encoded_limit = MEDIA_LIMIT.saturating_add(2) / 3 * 4;
    let output =
        command_output(command, &args, encoded_limit, OUTPUT_LIMIT).await.map_err(signal_error)?;

    if output.stdout.len() > encoded_limit || output.stderr.len() > OUTPUT_LIMIT {
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }

    if !output.status.success() {
        return Err(crabbot_core::Error::Denied(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }

    let text = String::from_utf8(output.stdout)
        .map_err(|_| crabbot_core::Error::Denied("signal-cli output was invalid UTF-8.".into()))?;

    let encoded = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| match value {
            Value::String(value) => Some(value),
            Value::Object(value) => value.get("output").and_then(Value::as_str).map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| text.trim().to_owned());

    let encoded =
        encoded.chars().filter(|character| !character.is_whitespace()).collect::<String>();

    STANDARD.decode(encoded).map_err(|_| {
        crabbot_core::Error::Denied("signal-cli attachment was not valid base64.".into())
    })
}

fn attachment_args(
    account: &str,
    id: &str,
    origin: &AttachmentOrigin,
    prefix: &[String],
) -> Vec<String> {
    let mut args = prefix.to_vec();

    args.extend(["-a".into(), account.into(), "getAttachment".into(), "--id".into(), id.into()]);

    match origin {
        AttachmentOrigin::Recipient(value) => args.extend(["--recipient".into(), value.clone()]),
        AttachmentOrigin::Group(value) => args.extend(["--group-id".into(), value.clone()]),
    }

    args
}

fn local_media(uri: &str) -> crabbot_core::Result<std::path::PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .ok_or_else(|| crabbot_core::Error::Denied("Signal media must be a local file.".into()))?;

    let path = Path::new(path);

    if path.components().any(|component| component == std::path::Component::ParentDir) {
        return Err(crabbot_core::Error::Denied("Signal media path is invalid.".into()));
    }

    let root = std::env::var_os("CRABBOT_MEDIA")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME").map(|value| PathBuf::from(value).join("media"))
        })
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_MEDIA or CRABBOT_HOME is required.".into())
        })?;

    local_media_at(uri, &root)
}

#[cfg(not(test))]
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

fn local_media_at(uri: &str, root: &Path) -> crabbot_core::Result<std::path::PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("Signal media must be a local file.".into()))?;

    let root = std::fs::canonicalize(root)
        .map_err(|_| crabbot_core::Error::Denied("Signal media root is unavailable.".into()))?;

    let path = std::fs::canonicalize(path)
        .map_err(|_| crabbot_core::Error::Denied("Signal media file is unavailable.".into()))?;

    if !path.starts_with(&root) {
        return Err(crabbot_core::Error::Denied("Signal media leaves the media root.".into()));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_text_images_audio_and_files() {
        let value = json!({
            "envelope": {
                "sourceNumber": "+1",
                "timestamp": 42,
                "dataMessage": {
                    "message": "hello",
                    "attachments": [
                        {"id": "img", "storedFilename": "/tmp/diagram.png", "contentType": "image/png", "filename": "diagram.png"},
                        {"id": "voice", "contentType": "audio/ogg", "filename": "voice.ogg"},
                        {"id": "note", "contentType": "text/plain", "filename": "note.txt"}
                    ]
                }
            }
        });

        let event = normalize(&value).unwrap();

        assert_eq!(event["id"], "+1:+1:42");
        assert_eq!(event["content"][0]["kind"], "text");
        assert_eq!(event["content"][1]["kind"], "image");
        assert_eq!(event["content"][1]["uri"], "signal://attachment/img?recipient=KzE");
        assert_eq!(event["content"][2]["kind"], "audio");
        assert_eq!(event["content"][3]["kind"], "file");
        let group = json!({
            "envelope": {
                "source": "+2",
                "timestamp": 43,
                "dataMessage": {
                    "groupInfo": {"groupId": "group"},
                    "attachments": [{"id": 7, "contentType": "application/octet-stream"}]
                }
            }
        });

        let group = normalize(&group).unwrap();

        assert_eq!(group["id"], "+2:group:43");
        assert_eq!(group["chat"], "group");
        assert_eq!(group["content"][0]["kind"], "file");
        assert_eq!(group["content"][0]["uri"], "signal://attachment/7?group=Z3JvdXA");
        assert!(normalize(&json!({"envelope":{"dataMessage":{}}})).is_none());
    }

    #[test]
    fn distinguishes_same_timestamp_across_senders() {
        let first = json!({
            "envelope": {
                "sourceNumber": "+1",
                "timestamp": 42,
                "dataMessage": {"message": "one"}
            }
        });

        let second = json!({
            "envelope": {
                "sourceNumber": "+2",
                "timestamp": 42,
                "dataMessage": {"message": "two"}
            }
        });

        assert_ne!(normalize(&first).unwrap()["id"], normalize(&second).unwrap()["id"]);
    }

    #[test]
    fn rejects_unsafe_media_paths() {
        assert!(local_media("file://../outside.txt").is_err());
        assert!(local_media("signal://attachment/1").is_err());
    }

    #[test]
    fn retains_events_until_host_acknowledgement() {
        let mut inbox = Inbox::default();
        inbox.stage([json!({"id": "event", "chat": "chat"})]).unwrap();

        assert_eq!(inbox.events(1).unwrap()[0]["gateway_sequence"], 1);
        inbox.acknowledge(1).unwrap();
        inbox.acknowledge(1).unwrap();

        assert!(inbox.events(1).unwrap().is_empty());
    }

    #[test]
    fn stages_received_events_idempotently() {
        let mut inbox = Inbox::default();
        let event = json!({"id": "event", "chat": "chat"});
        inbox.stage([event.clone()]).unwrap();
        inbox.stage([event]).unwrap();

        assert_eq!(inbox.pending.len(), 1);
    }

    #[test]
    fn includes_a_bounded_receive_limit() {
        assert_eq!(
            receive_args("+1000", &[]),
            vec![
                "-a",
                "+1000",
                "receive",
                "--json",
                "--ignore-attachments",
                "--max-messages",
                "32"
            ]
        );
    }

    #[test]
    fn passes_attachment_origin_to_signal_cli() {
        assert_eq!(
            attachment_args("+1000", "7", &AttachmentOrigin::Recipient("+1".into()), &[]),
            vec!["-a", "+1000", "getAttachment", "--id", "7", "--recipient", "+1"]
        );

        assert_eq!(
            attachment_args("+1000", "7", &AttachmentOrigin::Group("group".into()), &[]),
            vec!["-a", "+1000", "getAttachment", "--id", "7", "--group-id", "group"]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounds_signal_cli_output() {
        let value = run_with("printf", &["%s".into(), "[{\"ok\":true}]".into()]).await.unwrap();

        assert_eq!(value[0]["ok"], true);
        let value = run_with("printf", &["%s".into(), "plain".into()]).await.unwrap();

        assert_eq!(value["output"], "plain");
        assert!(run_with("sh", &["-c".into(), "exit 1".into()]).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_signal_media_into_the_private_root() {
        let root =
            std::env::temp_dir().join(format!("crabbot-signal-fetch-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let value = signal_media_at(
            "+1000",
            "signal://attachment/7?recipient=KzEwMDA",
            &root,
            "sh",
            &["-c".into(), "printf '%s' 'aGVsbG8='".into()],
        )
        .await
        .unwrap();

        let path = value["uri"].as_str().unwrap().strip_prefix("file://").unwrap();

        assert!(std::path::Path::new(path).starts_with(&root));
        assert_eq!(std::fs::read(path).unwrap(), b"hello");
        let cached = signal_media_at(
            "+1000",
            "signal://attachment/7?recipient=KzEwMDA",
            &root,
            "false",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(cached, value);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn expires_and_bounds_media_cache() {
        let root =
            std::env::temp_dir().join(format!("crabbot-signal-cleanup-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let expired = root.join("expired.bin");
        let oldest = root.join("oldest.bin");
        let newest = root.join("newest.bin");
        fs::write(&expired, b"expired").unwrap();
        fs::write(&oldest, b"oldest").unwrap();
        fs::write(&newest, b"newest").unwrap();
        let now = SystemTime::now();
        OpenOptions::new()
            .write(true)
            .open(&expired)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(now - MEDIA_TTL - Duration::from_secs(1)))
            .unwrap();

        OpenOptions::new()
            .write(true)
            .open(&oldest)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(now - Duration::from_secs(2)))
            .unwrap();

        cleanup_media_with_limit(&root, 10);

        assert!(!expired.exists());
        assert!(!oldest.exists());
        assert!(newest.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_an_oversized_pending_event() {
        let mut inbox = Inbox::default();
        inbox.pending.push(Pending {
            sequence: 1,
            event: json!({"text": "x".repeat(crabbot_core::jsonl::MAX)}),
        });

        assert!(inbox.events(1).is_err());
    }

    #[test]
    fn stops_before_an_event_that_would_exceed_the_frame_limit() {
        let inbox = Inbox {
            pending: vec![
                Pending { sequence: 1, event: json!({"text": "small"}) },
                Pending {
                    sequence: 2,
                    event: json!({"text": "x".repeat(crabbot_core::jsonl::MAX)}),
                },
            ],
            ..Default::default()
        };

        assert_eq!(inbox.events(1).unwrap().len(), 1);
    }

    #[test]
    fn rejects_a_full_inbox() {
        let mut inbox = Inbox {
            pending: (0..QUEUE_LIMIT)
                .map(|sequence| Pending {
                    sequence: sequence as u64,
                    event: json!({"id": sequence}),
                })
                .collect(),
            ..Default::default()
        };

        assert!(inbox.stage([json!({"id": "new"})]).is_err());
    }

    #[test]
    fn rejects_an_oversized_staged_event() {
        let mut inbox = Inbox::default();
        let event = json!({"text": "x".repeat(crabbot_core::jsonl::MAX)});

        assert!(inbox.stage([event]).is_err());
    }

    #[test]
    fn rejects_an_acknowledgement_for_a_future_sequence() {
        assert!(Inbox::default().acknowledge(1).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn polls_and_sends_through_a_configured_cli() {
        let poll = poll_with(
            "+1000",
            "sh",
            &["-c".into(), "printf '%s\\n' '{\"envelope\":{\"sourceNumber\":\"+1\",\"timestamp\":7,\"dataMessage\":{\"message\":\"hello\"}}}' '{\"envelope\":{\"sourceNumber\":\"+2\",\"timestamp\":8,\"dataMessage\":{\"message\":\"world\"}}}'".into()],
        )
        .await
        .unwrap();

        assert_eq!(poll["events"][0]["text"], "hello");
        assert_eq!(poll["events"][1]["text"], "world");

        let sent = send_with(
            "+1000",
            &json!({"chat":"+2000","text":"hello"}),
            "sh",
            &["-c".into(), "printf '%s' '{\"sent\":true}'".into()],
        )
        .await
        .unwrap();

        assert_eq!(sent["sent"], true);
        let sent = send_with(
            "+1000",
            &json!({"chat":"group","private":false,"text":"hello"}),
            "sh",
            &["-c".into(), "printf '%s\\n' \"$@\"".into(), "signal-cli".into()],
        )
        .await
        .unwrap();

        assert_eq!(sent["output"], "-a\n+1000\nsend\n-m\nhello\n--group-id\ngroup\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persists_received_output_before_parsing() {
        let root =
            std::env::temp_dir().join(format!("crabbot-signal-spool-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("signal-receive.jsonl");
        receive_to_spool(
            "+1000",
            "sh",
            &[
                "-c".into(),
                "printf '%s\\n' '{\"envelope\":{\"sourceNumber\":\"+1\",\"timestamp\":7,\"dataMessage\":{\"message\":\"hello\"}}}'".into(),
            ],
            &path,
        )
        .await
        .unwrap();

        let values = load_receive_spool(&path).unwrap().unwrap();

        assert_eq!(values.len(), 1);
        assert_eq!(normalize(&values[0]).unwrap()["text"], "hello");
        assert!(fs::read_dir(&root).unwrap().flatten().all(|entry| {
            !entry.file_name().to_string_lossy().starts_with(".signal-receive.jsonl.tmp-")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_receive_preserves_the_previous_spool() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-signal-failed-spool-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("signal-receive.jsonl");
        fs::write(
            &path,
            br#"{"envelope":{"sourceNumber":"+1","timestamp":7,"dataMessage":{"message":"old"}}}"#,
        )
        .unwrap();

        crabbot_file::private(&path).unwrap();

        assert!(receive_to_spool(
            "+1000",
            "sh",
            &["-c".into(), "printf partial; exit 1".into()],
            &path,
        )
        .await
        .is_err());

        let values = load_receive_spool(&path).unwrap().unwrap();

        assert_eq!(normalize(&values[0]).unwrap()["text"], "old");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_receive_output_terminates_and_preserves_the_previous_spool() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-signal-oversized-spool-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("signal-receive.jsonl");
        fs::write(
            &path,
            br#"{"envelope":{"sourceNumber":"+1","timestamp":7,"dataMessage":{"message":"old"}}}"#,
        )
        .unwrap();

        crabbot_file::private(&path).unwrap();

        for stream in ["", " >&2"] {
            let script = format!("head -c {} /dev/zero{}; exec sleep 30", OUTPUT_LIMIT + 1, stream);
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                receive_to_spool("+1000", "sh", &["-c".into(), script], &path),
            )
            .await
            .expect("oversized receive should terminate promptly");

            assert!(result.is_err());
            let values = load_receive_spool(&path).unwrap().unwrap();

            assert_eq!(normalize(&values[0]).unwrap()["text"], "old");
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn removes_invalid_existing_spools() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-signal-invalid-spool-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("signal-receive.jsonl");
        fs::write(&path, b"partial json").unwrap();
        crabbot_file::private(&path).unwrap();

        assert_eq!(load_or_clear_receive_spool(&path).unwrap(), None);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn resolves_media_inside_the_configured_root() {
        let root =
            std::env::temp_dir().join(format!("crabbot-signal-media-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("voice.ogg");
        std::fs::write(&path, b"voice").unwrap();
        let value = media_at(&format!("file://{}", path.display()), &root).await.unwrap();

        assert!(value["uri"].as_str().unwrap().ends_with("voice.ogg"));
        assert!(media_at("file://missing", &root).await.is_err());
        assert!(media_at("signal://attachment/1", &root).await.is_err());

        let outside =
            std::env::temp_dir().join(format!("crabbot-signal-outside-{}", std::process::id()));

        std::fs::write(&outside, b"outside").unwrap();

        assert!(media_at(&format!("file://{}", outside.display()), &root).await.is_err());
        let _ = std::fs::remove_file(outside);
        let _ = std::fs::remove_dir_all(root);
    }
}
