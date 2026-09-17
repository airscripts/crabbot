use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crabbot_core::{
    jsonl,
    types::{Capability, Content, IpcRequest, IpcResponse, Message, Request, Role},
};
use serde_json::{Value, json};
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Duration, timeout},
};

use super::{Cancellation, Stop, state::Store};

const FRAME: usize = jsonl::MAX;
const SUMMARY_LIMIT: usize = 1024;
const DETAIL_LIMIT: usize = 16 * 1024;
const CANCEL_WAIT: Duration = Duration::from_secs(310);
#[cfg(test)]
const CLIENTS: usize = 64;

pub fn token() -> io::Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub struct State {
    pub token: String,
    pub sessions: Arc<Mutex<Store>>,
    pub stop: Arc<Stop>,
    pub slots: Arc<Semaphore>,
    pub cancels: Arc<Mutex<BTreeMap<String, Arc<Cancellation>>>>,
    pub root: PathBuf,
    pub home: PathBuf,
    pub approval_mode: String,
    pub pending: Arc<tokio::sync::Mutex<super::approval::Gate>>,
    pub plugins: super::Plugins,
    pub config: super::Config,
    pub channel: String,
    pub model: String,
}

pub async fn serve(listener: TcpListener, state: Arc<State>) -> io::Result<()> {
    loop {
        tokio::select! {
            _ = state.stop.notified() => return Ok(()),
            result = listener.accept() => {
                let (stream, _) = result?;
                let Ok(slot) = Arc::clone(&state.slots).try_acquire_owned() else {
                    continue;
                };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle(stream, state, slot).await {
                        eprintln!("IPC client closed: {}", super::sentence(error.to_string()));
                    }
                });
            }
        }
    }
}

async fn handle(
    stream: TcpStream,
    state: Arc<State>,
    _slot: OwnedSemaphorePermit,
) -> io::Result<()> {
    let (input, output) = stream.into_split();
    let mut input = BufReader::new(input);
    let mut output = output;

    let request = timeout(Duration::from_secs(10), jsonl::read::<IpcRequest>(&mut input, FRAME))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC authentication timed out."))?
        .map_err(to_io)?;
    let Some(request) = request else {
        return Ok(());
    };
    if !request.valid() || request.token != state.token {
        jsonl::write(&mut output, &IpcResponse::fail(request.id, 401, "Unauthorized."))
            .await
            .map_err(to_io)?;
        return Ok(());
    }

    let response = match request.method.as_str() {
        "plugin.load" => load(&request, &state).await,
        "plugin.unload" => unload(&request, &state).await,
        "plugin.active" => active(&request, &state).await,
        "capability.call" => capability_call(&request, &state).await,
        "approval.list" => approval_list(&request, &state).await,
        "approval.resolve" => approval_resolve(&request, &state).await,
        "session.cancel" => cancel(&request, &state).await,
        _ => dispatch(&request, &state)
            .unwrap_or_else(|error| IpcResponse::fail(request.id, -32000, error.to_string())),
    };
    jsonl::write(&mut output, &response).await.map_err(to_io)
}

async fn approval_list(request: &IpcRequest, state: &State) -> IpcResponse {
    let items = match state.pending.lock().await.list() {
        Ok(items) => items,
        Err(error) => return IpcResponse::fail(request.id, -32000, error.to_string()),
    };
    let result = json!({"items": items});
    if serde_json::to_vec(&result).map_or(true, |value| value.len() >= FRAME) {
        return IpcResponse::fail(request.id, -32000, "Approval list exceeds the IPC frame limit.");
    }
    IpcResponse::ok(request.id, result)
}

async fn approval_resolve(request: &IpcRequest, state: &State) -> IpcResponse {
    let Some(id) = request.params["id"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "approval.resolve.id is required.");
    };
    let Some(approved) = request.params["approved"].as_bool() else {
        return IpcResponse::fail(request.id, -32602, "approval.resolve.approved is required.");
    };
    match state.pending.lock().await.resolve_local(id, approved) {
        Some(approved) => {
            IpcResponse::ok(request.id, json!({"resolved": true, "approved": approved}))
        }
        None => IpcResponse::fail(
            request.id,
            -32004,
            "Approval is no longer pending or its ID is invalid.",
        ),
    }
}

async fn capability_call(request: &IpcRequest, state: &State) -> IpcResponse {
    let Some(service) = request.params["service"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "capability.call.service is required.");
    };
    let Some(method) = request.params["method"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "capability.call.method is required.");
    };
    let Some(params) = request.params.get("params") else {
        return IpcResponse::fail(request.id, -32602, "capability.call.params is required.");
    };
    if serde_json::to_vec(params).map_or(true, |value| value.len() > DETAIL_LIMIT) {
        return IpcResponse::fail(request.id, -32602, "Capability request exceeds its size limit.");
    }
    let (capability, allowed) = match service {
        "timer" => (Capability::Timer, ["add", "list", "remove"].as_slice()),
        "memory" => (Capability::Memory, ["audit", "forget", "list", "remember"].as_slice()),
        _ => {
            return IpcResponse::fail(
                request.id,
                -32602,
                "Capability is not available to the TUI.",
            );
        }
    };
    if !allowed.contains(&method) {
        return IpcResponse::fail(
            request.id,
            -32602,
            "Capability method is not available to the TUI.",
        );
    }
    let Some((_, plugin)) = state.plugins.find(capability).await else {
        return IpcResponse::fail(
            request.id,
            -32000,
            "The requested capability plugin is not loaded.",
        );
    };
    let result = match plugin.call(Request::call(request.id, method, params.clone())).await {
        Ok(response) => {
            if let Some(error) = response.error {
                return IpcResponse::fail(request.id, error.code, error.message);
            }
            let Some(result) = response.result else {
                return IpcResponse::fail(request.id, -32000, "Capability returned no result.");
            };
            result
        }
        Err(error) => return IpcResponse::fail(request.id, -32000, error.to_string()),
    };
    if serde_json::to_vec(&result).map_or(true, |value| value.len().saturating_add(1024) > FRAME) {
        return IpcResponse::fail(
            request.id,
            -32000,
            "Capability response exceeds the IPC frame limit.",
        );
    }
    IpcResponse::ok(request.id, result)
}

async fn cancel(request: &IpcRequest, state: &State) -> IpcResponse {
    let Some(id) = request.params["id"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "session.cancel.id is required.");
    };
    let token = {
        let mut sessions = match state.sessions.lock() {
            Ok(sessions) => sessions,
            Err(error) => return IpcResponse::fail(request.id, -32000, lock(error).to_string()),
        };
        let active = sessions.sessions.get(id).is_some_and(|session| session.inflight.is_some());
        if let Err(error) = sessions.cancel(id) {
            return IpcResponse::fail(request.id, -32000, error.to_string());
        }
        if active {
            let mut cancels = match state.cancels.lock() {
                Ok(cancels) => cancels,
                Err(error) => {
                    return IpcResponse::fail(request.id, -32000, lock(error).to_string());
                }
            };
            Some(Arc::clone(
                cancels.entry(id.into()).or_insert_with(|| Arc::new(Cancellation::new())),
            ))
        } else {
            None
        }
    };
    if let Some(token) = token {
        token.request();
        if timeout(CANCEL_WAIT, token.wait()).await.is_err() {
            return IpcResponse::fail(
                request.id,
                -32000,
                "Cancellation was requested but the active turn did not acknowledge it.",
            );
        }
        match state.cancels.lock() {
            Ok(mut cancels) => {
                if cancels.get(id).is_some_and(|current| Arc::ptr_eq(current, &token)) {
                    cancels.remove(id);
                }
            }
            Err(error) => return IpcResponse::fail(request.id, -32000, lock(error).to_string()),
        }
    } else if let Ok(mut cancels) = state.cancels.lock() {
        cancels.remove(id);
    }
    IpcResponse::ok(request.id, json!({"id": id, "status": "cancelled"}))
}

async fn load(request: &IpcRequest, state: &State) -> IpcResponse {
    let Some(id) = request.params["id"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "plugin.load.id is required.");
    };
    let capability = if id == state.channel {
        if !super::ready(id) {
            return IpcResponse::fail(request.id, -32000, "Channel credentials are not ready.");
        }
        Some(Capability::Channel)
    } else if id == state.model {
        if !super::ready(id) {
            return IpcResponse::fail(request.id, -32000, "Model credentials are not ready.");
        }
        Some(Capability::Model)
    } else {
        None
    };
    match super::load_plugin(&state.home, id, capability, false, &state.config, &state.plugins)
        .await
    {
        Ok(hello) => IpcResponse::ok(
            request.id,
            json!({"id": hello.id, "version": hello.version, "loaded": true}),
        ),
        Err(error) => IpcResponse::fail(request.id, -32000, error.to_string()),
    }
}

async fn unload(request: &IpcRequest, state: &State) -> IpcResponse {
    let Some(id) = request.params["id"].as_str() else {
        return IpcResponse::fail(request.id, -32602, "plugin.unload.id is required.");
    };
    match super::unload_plugin(id, &state.plugins).await {
        Ok(unloaded) => IpcResponse::ok(request.id, json!({"id": id, "unloaded": unloaded})),
        Err(error) => IpcResponse::fail(request.id, -32000, error.to_string()),
    }
}

async fn active(request: &IpcRequest, state: &State) -> IpcResponse {
    let items = state.plugins.all().await.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
    IpcResponse::ok(request.id, json!({"items": items}))
}

fn dispatch(request: &IpcRequest, state: &State) -> io::Result<IpcResponse> {
    let result = match request.method.as_str() {
        "status" => {
            json!({
                "running": true,
                "sessions": state.sessions.lock().map_err(lock)?.sessions.len(),
                "plugins": plugins(&state.home),
                "approval": state.approval_mode.as_str(),
            })
        }
        "plugin.list" => json!({"items": plugins(&state.home)}),
        "session.list" => {
            let sessions = state.sessions.lock().map_err(lock)?;
            json!({"items": sessions.sessions.values().map(summary).collect::<Vec<_>>()})
        }
        "delivery.list" => {
            let sessions = state.sessions.lock().map_err(lock)?;
            json!({"items": sessions.outbox.iter().map(delivery).collect::<Vec<_>>()})
        }
        "delivery.retry" => {
            if request.params["yes"] != Value::Bool(true) {
                return Ok(IpcResponse::fail(
                    request.id,
                    -32602,
                    "Delivery retry requires confirmation.",
                ));
            }
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("delivery.retry.id is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.retry_delivery(id)?;
            json!({"id": id, "status": "pending"})
        }
        "delivery.drop" => {
            if request.params["yes"] != Value::Bool(true) {
                return Ok(IpcResponse::fail(
                    request.id,
                    -32602,
                    "Delivery drop requires confirmation.",
                ));
            }
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("delivery.drop.id is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.drop_delivery(id)?;
            json!({"id": id, "status": "dropped"})
        }
        "session.get" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.get.id is required."))?;
            let sessions = state.sessions.lock().map_err(lock)?;
            let session =
                sessions.sessions.get(id).ok_or_else(|| not_found("Session was not found."))?;
            detail(session)
        }
        "session.ensure" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.ensure.id is required."))?;
            let model = request.params["model"].as_str().unwrap_or("gpt-4o-mini");
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.ensure(id, model)?;
            json!({"id": id})
        }
        "session.new" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.new.id is required."))?;
            let model = request.params["model"].as_str().unwrap_or("gpt-4o-mini");
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.create(id, model)?;
            json!({"id": id})
        }
        "session.append" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.append.id is required."))?;
            let message: Message = serde_json::from_value(request.params["message"].clone())
                .map_err(|error| invalid(&format!("session.append.message is invalid: {error}")))?;
            if message.session != id
                || !matches!(&message.role, Role::User | Role::Assistant)
                || message.content.is_empty()
                || message.content.iter().any(|content| {
                    !matches!(content, Content::Text { text } if text.len() <= DETAIL_LIMIT)
                })
            {
                return Err(invalid("session.append.message is outside the supported bounds."));
            }
            let mut sessions = state.sessions.lock().map_err(lock)?;
            let session =
                sessions.sessions.get(id).ok_or_else(|| not_found("Session was not found."))?;
            if session.inflight.is_some() || session.status == "working" {
                return Err(invalid("Session is already working."));
            }
            sessions.append(id, message)?;
            json!({"id": id, "saved": true})
        }
        "session.clear" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.clear.id is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            let session =
                sessions.sessions.get(id).ok_or_else(|| not_found("Session was not found."))?;
            if session.inflight.is_some() || session.status == "working" {
                return Err(invalid("A working session cannot be cleared."));
            }
            sessions.clear_history(id)?;
            json!({"id": id, "cleared": true})
        }
        "session.fork" => {
            let source = request.params["source"]
                .as_str()
                .ok_or_else(|| invalid("session.fork.source is required."))?;
            let target = request.params["target"]
                .as_str()
                .ok_or_else(|| invalid("session.fork.target is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.fork(source, target)?;
            json!({"id": target})
        }
        "session.delete" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.delete.id is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.remove(id)?;
            let worktree = match super::state::remove_worktree(&state.root, id) {
                Ok(()) => json!({"status": "removed"}),
                Err(error) => json!({
                    "status": "pending",
                    "error": super::sentence(error.to_string()),
                }),
            };
            state.cancels.lock().map_err(lock)?.remove(id);
            json!({"id": id, "worktree": worktree})
        }
        "session.model" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.model.id is required."))?;
            let model = request.params["model"]
                .as_str()
                .ok_or_else(|| invalid("session.model.model is required."))?;
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.set_model(id, model)?;
            json!({"id": id, "model": model})
        }
        "session.workspace" => {
            let id = request.params["id"]
                .as_str()
                .ok_or_else(|| invalid("session.workspace.id is required."))?;
            let workspace = match &request.params["workspace"] {
                Value::Null => None,
                Value::String(path) if !path.trim().is_empty() && path.len() <= 4096 => {
                    let path = Path::new(path)
                        .canonicalize()
                        .map_err(|_| invalid("Workspace directory is unavailable."))?;
                    if !path.is_dir() {
                        return Err(invalid("Workspace must be a directory."));
                    }
                    Some(path.to_string_lossy().into_owned())
                }
                _ => return Err(invalid("session.workspace.workspace is invalid.")),
            };
            let mut sessions = state.sessions.lock().map_err(lock)?;
            sessions.set_workspace(id, workspace.as_deref())?;
            json!({"id": id, "workspace": workspace})
        }
        "shutdown" => {
            state.stop.signal();
            json!({"ok": true})
        }
        _ => return Ok(IpcResponse::fail(request.id, -32601, "Method not found.")),
    };

    Ok(IpcResponse::ok(request.id, result))
}

pub(crate) fn summary(session: &super::state::Session) -> Value {
    let short =
        |value: Option<&String>| value.map(|value| super::clip(value.clone(), SUMMARY_LIMIT));
    json!({
        "id": super::clip(session.id.clone(), SUMMARY_LIMIT),
        "model": super::clip(session.model.clone(), SUMMARY_LIMIT),
        "channel": short(session.channel.as_ref()),
        "chat": short(session.chat.as_ref()),
        "thread": short(session.thread.as_ref()),
        "private": session.private,
        "status": super::clip(session.status.clone(), SUMMARY_LIMIT),
        "queued": session.queued.len(),
        "inflight": session.inflight.is_some(),
        "created": session.created,
        "updated": session.updated,
    })
}

pub(crate) fn delivery(item: &super::state::Delivery) -> Value {
    json!({
        "id": super::clip(item.id.clone(), SUMMARY_LIMIT),
        "channel": super::clip(item.channel.clone(), SUMMARY_LIMIT),
        "chat": super::clip(item.chat.clone(), SUMMARY_LIMIT),
        "thread": item.thread.as_ref().map(|value| super::clip(value.clone(), SUMMARY_LIMIT)),
        "message_id": item.message_id.as_ref().map(|value| super::clip(value.clone(), SUMMARY_LIMIT)),
        "status": format!("{:?}", item.status).to_lowercase(),
        "attempts": item.attempts,
        "error": item.last_error.as_ref().map(|value| super::clip(value.clone(), SUMMARY_LIMIT)),
        "created": item.created,
        "updated": item.updated,
    })
}

pub(crate) fn detail(session: &super::state::Session) -> Value {
    let mut messages = Vec::new();
    let mut truncated = false;
    for item in session.messages.iter().rev() {
        let item = message_value(item);
        messages.push(item);
        if serde_json::to_vec(&messages)
            .map_or(true, |bytes| bytes.len().saturating_add(DETAIL_LIMIT) > FRAME)
        {
            messages.pop();
            truncated = true;
            break;
        }
    }
    messages.reverse();
    let mut value = summary(session);
    value["messages"] = Value::Array(messages);
    value["workspace"] =
        session.workspace.as_ref().map_or(Value::Null, |path| Value::String(path.clone()));
    value["truncated"] = Value::Bool(truncated);
    value
}

fn message_value(message: &Message) -> Value {
    let role = match message.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    };
    let content = message
        .content
        .iter()
        .map(|item| match item {
            Content::Text { text } => json!({
                "kind": "text",
                "text": super::clip(text.clone(), DETAIL_LIMIT),
            }),
            Content::Image { uri, alt } => json!({
                "kind": "image",
                "uri": super::clip(uri.clone(), DETAIL_LIMIT),
                "alt": alt.as_ref().map(|value| super::clip(value.clone(), DETAIL_LIMIT)),
            }),
            Content::File { uri, name, mime } => json!({
                "kind": "file",
                "uri": super::clip(uri.clone(), DETAIL_LIMIT),
                "name": super::clip(name.clone(), DETAIL_LIMIT),
                "mime": mime.as_ref().map(|value| super::clip(value.clone(), DETAIL_LIMIT)),
            }),
            Content::Audio { uri, mime } => json!({
                "kind": "audio",
                "uri": super::clip(uri.clone(), DETAIL_LIMIT),
                "mime": mime.as_ref().map(|value| super::clip(value.clone(), DETAIL_LIMIT)),
            }),
        })
        .collect::<Vec<_>>();
    json!({
        "id": super::clip(message.id.clone(), SUMMARY_LIMIT),
        "session": super::clip(message.session.clone(), SUMMARY_LIMIT),
        "role": role,
        "sender": message.sender.as_ref().map(|value| super::clip(value.clone(), SUMMARY_LIMIT)),
        "content": content,
    })
}

pub async fn call(
    home: impl AsRef<std::path::Path>,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    let home = home.as_ref();
    let token = std::fs::read_to_string(home.join("ipc.token"))?.trim().to_owned();
    let port = std::fs::read_to_string(home.join("ipc.port"))?
        .trim()
        .parse::<u16>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let (input, output) = stream.into_split();
    let mut input = BufReader::new(input);
    let mut output = output;
    jsonl::write(&mut output, &IpcRequest::call(1, token, method, params)).await.map_err(to_io)?;
    let response: IpcResponse =
        jsonl::read(&mut input, FRAME).await.map_err(to_io)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "IPC closed before response.")
        })?;
    if !valid(&response) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "IPC response was invalid."));
    }
    match (response.result, response.error) {
        (Some(value), _) => Ok(value),
        (_, Some(error)) => Err(io::Error::new(io::ErrorKind::PermissionDenied, error.message)),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "IPC response was empty.")),
    }
}

fn to_io(error: crabbot_core::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn plugins(home: &std::path::Path) -> Vec<Value> {
    let mut items = std::fs::read_dir(home.join("plugins"))
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            let id = entry.file_name().into_string().ok()?;
            if id.is_empty() || id.len() > SUMMARY_LIMIT || !id.bytes().all(valid_plugin_byte) {
                return None;
            }
            let binary = if cfg!(windows) {
                format!("crabbot-plugin-{id}.exe")
            } else {
                format!("crabbot-plugin-{id}")
            };
            let health = entry.path().join("bin").join(binary).is_file();
            Some(json!({
                "id": id,
                "status": "installed",
                "health": if health { "ready" } else { "missing" },
            }))
        })
        .take(256)
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    items
}

fn valid_plugin_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
}

fn valid(response: &IpcResponse) -> bool {
    response.id == 1 && response.valid()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn not_found(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, message)
}

fn lock<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("Session lock is poisoned.")
}

#[cfg(test)]
mod tests {
    use super::super::{Stop, state::Store};
    use super::{
        FRAME, State, active, approval_list, approval_resolve, cancel as cancel_request,
        capability_call, dispatch, load, plugins, unload,
    };
    use crabbot_core::{
        jsonl,
        types::{Content, IpcRequest, IpcResponse, Message, Role},
    };
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    use tokio::{
        io::BufReader,
        net::{TcpListener, TcpStream},
        sync::Semaphore,
        time::Duration,
    };

    fn state() -> State {
        state_at("")
    }

    fn state_at(label: &str) -> State {
        let path = PathBuf::from(format!(
            "/tmp/crabbot-ipc-test-{}-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test"),
            label
        ));
        let _ = std::fs::remove_file(&path);
        State {
            token: "secret".into(),
            sessions: Arc::new(Mutex::new(Store::load(path).unwrap())),
            stop: Arc::new(Stop::new()),
            slots: Arc::new(Semaphore::new(super::CLIENTS)),
            cancels: Arc::new(Mutex::new(BTreeMap::new())),
            root: PathBuf::from("/tmp"),
            home: PathBuf::from(format!("/tmp/crabbot-ipc-home-{}-{}", std::process::id(), label)),
            approval_mode: "off".into(),
            pending: Arc::new(tokio::sync::Mutex::new(crate::approval::Gate::new().unwrap())),
            plugins: crate::Plugins::default(),
            config: crate::Config::default(),
            channel: "telegram".into(),
            model: "codex".into(),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn proxies_only_supported_capability_calls() {
        use std::os::unix::fs::PermissionsExt;

        let state = state_at("capability-call");
        let binary = PathBuf::from(format!("/tmp/crabbot-ipc-capability-{}", std::process::id()));
        let script = r#"while IFS= read -r line; do id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); case "$line" in *'"method":"hello"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"protocol":{"major":0,"minor":1},"id":"timer","version":"0.1.0","capabilities":["timer"]}}' ;; *'"method":"list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"items":[{"id":7,"text":"reminder"}]}}' ;; *'"method":"shutdown"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        std::fs::write(&binary, script).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let process = crabbot_core::plugin::Process::start(&binary).await.unwrap();
        state.plugins.insert(crate::Live::new(process)).await;

        let response = capability_call(
            &IpcRequest::call(
                1,
                "secret",
                "capability.call",
                json!({"service": "timer", "method": "list", "params": {}}),
            ),
            &state,
        )
        .await;
        assert_eq!(response.result.unwrap()["items"][0]["id"], 7);

        let denied = capability_call(
            &IpcRequest::call(
                2,
                "secret",
                "capability.call",
                json!({"service": "timer", "method": "due", "params": {}}),
            ),
            &state,
        )
        .await;
        assert_eq!(denied.error.unwrap().code, -32602);

        let bad_service = capability_call(
            &IpcRequest::call(
                3,
                "secret",
                "capability.call",
                json!({"service": "tools", "method": "execute", "params": {}}),
            ),
            &state,
        )
        .await;
        assert_eq!(bad_service.error.unwrap().code, -32602);

        if let Some(plugin) = state.plugins.remove("timer").await {
            plugin.stop().await.unwrap();
        }
        let _ = std::fs::remove_file(binary);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn loads_a_plugin_into_the_running_registry() {
        use std::os::unix::fs::PermissionsExt;

        let state = state_at("plugin-load");
        let home = state.home.clone();
        let plugin = state.home.join("plugins/tools");
        let binary = plugin.join("bin/crabbot-plugin-tools");
        let _ = std::fs::remove_dir_all(&home);
        std::fs::create_dir_all(binary.parent().unwrap()).unwrap();
        std::fs::write(
            plugin.join("crabbot-plugin.toml"),
            "id = 'tools'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['tool']\n",
        )
        .unwrap();
        std::fs::write(
            &binary,
            "#!/bin/sh\nwhile IFS= read -r line; do case \"$line\" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"tools\",\"version\":\"0.1.0\",\"capabilities\":[\"tool\"]}}' ;; *shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done\n",
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let entry = crate::Entry {
            source: "test".into(),
            revision: "local".into(),
            pinned: false,
            default: false,
            hash: crate::digest(&plugin.join("crabbot-plugin.toml"), &binary).unwrap(),
            version: "0.1.0".into(),
            protocol: crabbot_core::types::Protocol::CURRENT,
            capabilities: vec!["tool".into()],
            permissions: Vec::new(),
            secrets: Vec::new(),
            linked: false,
            commands: Vec::new(),
        };
        crate::save_lock_at(
            &state.home,
            &crate::Lock { plugins: BTreeMap::from([("tools".into(), entry)]) },
        )
        .unwrap();

        let request = IpcRequest::call(1, "secret", "plugin.load", json!({"id": "tools"}));
        let response = load(&request, &state).await;

        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(response.result.unwrap()["loaded"], true);
        let loaded = state.plugins.get("tools").await.unwrap();
        assert_eq!(loaded.hello.id, "tools");
        let request = IpcRequest::call(2, "secret", "plugin.active", json!({}));
        let response = active(&request, &state).await;
        assert_eq!(response.result.unwrap()["items"], json!(["tools"]));
        let request = IpcRequest::call(2, "secret", "plugin.unload", json!({"id": "tools"}));
        let response = unload(&request, &state).await;
        assert_eq!(response.result.unwrap()["unloaded"], true);
        assert!(state.plugins.get("tools").await.is_none());
        let request = IpcRequest::call(3, "secret", "plugin.active", json!({}));
        let response = active(&request, &state).await;
        assert_eq!(response.result.unwrap()["items"], json!([]));
        let _ = std::fs::remove_dir_all(home);
    }

    #[tokio::test]
    async fn dispatches_authenticated_session_calls() {
        let state = state();
        let status = dispatch(&IpcRequest::call(0, "secret", "status", json!({})), &state).unwrap();
        let status = status.result.unwrap();
        assert_eq!(status["sessions"], 0);
        assert!(status["plugins"].is_array());
        assert_eq!(status["approval"], "off");
        let new =
            dispatch(&IpcRequest::call(1, "secret", "session.new", json!({"id": "one"})), &state)
                .unwrap();
        assert_eq!(new.result.unwrap()["id"], "one");
        let ensured = dispatch(
            &IpcRequest::call(
                21,
                "secret",
                "session.ensure",
                json!({"id": "terminal", "model": "test-model"}),
            ),
            &state,
        )
        .unwrap();
        assert_eq!(ensured.result.unwrap()["id"], "terminal");
        let ensured = dispatch(
            &IpcRequest::call(
                22,
                "secret",
                "session.ensure",
                json!({"id": "terminal", "model": "ignored-model"}),
            ),
            &state,
        )
        .unwrap();
        assert_eq!(ensured.result.unwrap()["id"], "terminal");
        let message = Message {
            id: "terminal-user-1".into(),
            session: "terminal".into(),
            role: Role::User,
            sender: Some("tui".into()),
            content: vec![Content::Text { text: "hello".into() }],
        };
        state.sessions.lock().unwrap().set_status("terminal", "cancelled").unwrap();
        let appended = dispatch(
            &IpcRequest::call(
                23,
                "secret",
                "session.append",
                json!({"id": "terminal", "message": message}),
            ),
            &state,
        )
        .unwrap();
        assert_eq!(appended.result.unwrap()["saved"], true);
        let terminal = dispatch(
            &IpcRequest::call(24, "secret", "session.get", json!({"id": "terminal"})),
            &state,
        )
        .unwrap()
        .result
        .unwrap();
        assert_eq!(terminal["messages"][0]["content"][0]["text"], "hello");
        assert_eq!(terminal["status"], "idle");
        let invalid_message = Message {
            id: "terminal-tool-1".into(),
            session: "terminal".into(),
            role: Role::Tool,
            sender: None,
            content: vec![Content::Text { text: "untrusted".into() }],
        };
        assert!(
            dispatch(
                &IpcRequest::call(
                    25,
                    "secret",
                    "session.append",
                    json!({"id": "terminal", "message": invalid_message}),
                ),
                &state,
            )
            .is_err()
        );
        state.sessions.lock().unwrap().set_status("terminal", "working").unwrap();
        assert!(
            dispatch(
                &IpcRequest::call(26, "secret", "session.clear", json!({"id": "terminal"}),),
                &state,
            )
            .is_err()
        );
        state.sessions.lock().unwrap().set_status("terminal", "idle").unwrap();
        let cleared = dispatch(
            &IpcRequest::call(27, "secret", "session.clear", json!({"id": "terminal"})),
            &state,
        )
        .unwrap();
        assert_eq!(cleared.result.unwrap()["cleared"], true);
        let terminal = dispatch(
            &IpcRequest::call(28, "secret", "session.get", json!({"id": "terminal"})),
            &state,
        )
        .unwrap()
        .result
        .unwrap();
        assert!(terminal["messages"].as_array().unwrap().is_empty());
        let list =
            dispatch(&IpcRequest::call(2, "secret", "session.list", json!({})), &state).unwrap();
        assert_eq!(list.result.unwrap()["items"][0]["id"], "one");
        let get =
            dispatch(&IpcRequest::call(3, "secret", "session.get", json!({"id": "one"})), &state)
                .unwrap();
        assert_eq!(get.result.unwrap()["model"], "gpt-4o-mini");
        let model = dispatch(
            &IpcRequest::call(
                4,
                "secret",
                "session.model",
                json!({"id": "one", "model": "test-model"}),
            ),
            &state,
        )
        .unwrap();
        assert_eq!(model.result.unwrap()["model"], "test-model");
        let fork = dispatch(
            &IpcRequest::call(
                5,
                "secret",
                "session.fork",
                json!({"source": "one", "target": "copy"}),
            ),
            &state,
        )
        .unwrap();
        assert_eq!(fork.result.unwrap()["id"], "copy");
        state.sessions.lock().unwrap().set_status("one", "working").unwrap();
        assert!(
            dispatch(
                &IpcRequest::call(6, "secret", "session.delete", json!({"id": "one"})),
                &state,
            )
            .is_err()
        );
        state.sessions.lock().unwrap().set_status("one", "idle").unwrap();
        let cancelled = cancel_request(
            &IpcRequest::call(6, "secret", "session.cancel", json!({"id": "copy"})),
            &state,
        )
        .await;
        assert_eq!(cancelled.result.unwrap()["status"], "cancelled");
        let deleted = dispatch(
            &IpcRequest::call(6, "secret", "session.delete", json!({"id": "copy"})),
            &state,
        )
        .unwrap();
        assert_eq!(deleted.result.unwrap()["id"], "copy");
        assert_eq!(
            dispatch(&IpcRequest::call(7, "secret", "unknown", json!({})), &state)
                .unwrap()
                .error
                .unwrap()
                .code,
            -32601
        );
        assert!(
            dispatch(&IpcRequest::call(8, "secret", "session.get", json!({})), &state).is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(9, "secret", "session.get", json!({"id": "missing"})),
                &state
            )
            .is_err()
        );
        assert!(
            dispatch(&IpcRequest::call(10, "secret", "session.new", json!({})), &state).is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(11, "secret", "session.new", json!({"id": "bad_id"})),
                &state
            )
            .is_err()
        );
        assert!(
            dispatch(&IpcRequest::call(12, "secret", "session.new", json!({"id": "one"})), &state)
                .is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(
                    13,
                    "secret",
                    "session.fork",
                    json!({"source": "missing", "target": "new"})
                ),
                &state
            )
            .is_err()
        );
        assert!(
            cancel_request(
                &IpcRequest::call(14, "secret", "session.cancel", json!({"id": "missing"})),
                &state,
            )
            .await
            .error
            .is_some()
        );
        assert!(
            dispatch(
                &IpcRequest::call(16, "secret", "session.fork", json!({"target": "new"})),
                &state,
            )
            .is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(17, "secret", "session.fork", json!({"source": "one"})),
                &state,
            )
            .is_err()
        );
        assert!(
            cancel_request(&IpcRequest::call(18, "secret", "session.cancel", json!({})), &state,)
                .await
                .error
                .is_some()
        );
        assert!(
            dispatch(&IpcRequest::call(19, "secret", "session.model", json!({})), &state,).is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(20, "secret", "session.model", json!({"id": "one"})),
                &state,
            )
            .is_err()
        );
        assert!(
            dispatch(
                &IpcRequest::call(
                    21,
                    "secret",
                    "session.model",
                    json!({"id": "one", "model": ""}),
                ),
                &state,
            )
            .is_err()
        );
        let shutdown =
            dispatch(&IpcRequest::call(15, "secret", "shutdown", json!({})), &state).unwrap();
        assert_eq!(shutdown.result.unwrap()["ok"], true);
        let _ = std::fs::remove_file(format!("/tmp/crabbot-ipc-test-{}.json", std::process::id()));
    }

    #[test]
    fn lists_safe_plugins_only() {
        let root = PathBuf::from(format!("/tmp/crabbot-ipc-plugins-{}", std::process::id()));
        let plugin_root = root.join("plugins");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&plugin_root).unwrap();
        std::fs::create_dir(plugin_root.join("codex")).unwrap();
        std::fs::create_dir(plugin_root.join("telegram-1")).unwrap();
        std::fs::write(plugin_root.join("file"), "not a plugin").unwrap();
        std::fs::create_dir(plugin_root.join("Bad")).unwrap();
        assert_eq!(
            plugins(&root),
            vec![
                json!({"id": "codex", "status": "installed", "health": "missing"}),
                json!({"id": "telegram-1", "status": "installed", "health": "missing"}),
            ]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dispatches_delivery_controls_with_confirmation() {
        let state = state_at("delivery");
        let mut store = state.sessions.lock().unwrap();
        store.create("one", "model").unwrap();
        store
            .reply(
                "one",
                Message {
                    id: "reply".into(),
                    session: "one".into(),
                    role: Role::Assistant,
                    sender: None,
                    content: vec![Content::Text { text: "hello".into() }],
                },
                "delivery",
                "telegram",
                "7",
                None,
                "hello",
            )
            .unwrap();
        store.uncertain("delivery", "timeout").unwrap();
        drop(store);

        let list =
            dispatch(&IpcRequest::call(1, "secret", "delivery.list", json!({})), &state).unwrap();
        assert_eq!(list.result.unwrap()["items"][0]["status"], "uncertain");
        assert!(
            dispatch(
                &IpcRequest::call(2, "secret", "delivery.retry", json!({"id": "delivery"})),
                &state
            )
            .unwrap()
            .error
            .is_some()
        );
        dispatch(
            &IpcRequest::call(
                3,
                "secret",
                "delivery.retry",
                json!({"id": "delivery", "yes": true}),
            ),
            &state,
        )
        .unwrap();
        dispatch(
            &IpcRequest::call(4, "secret", "delivery.drop", json!({"id": "delivery", "yes": true})),
            &state,
        )
        .unwrap();
        assert!(state.sessions.lock().unwrap().outbox.is_empty());
    }

    #[test]
    fn stores_only_canonical_session_workspaces() {
        let state = state_at("workspace");
        let root = PathBuf::from(format!("/tmp/crabbot-ipc-workspace-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        dispatch(&IpcRequest::call(1, "secret", "session.new", json!({"id": "one"})), &state)
            .unwrap();

        let selected = dispatch(
            &IpcRequest::call(
                2,
                "secret",
                "session.workspace",
                json!({"id": "one", "workspace": root.display().to_string()}),
            ),
            &state,
        )
        .unwrap()
        .result
        .unwrap();
        assert_eq!(
            selected["workspace"],
            std::fs::canonicalize(&root).unwrap().display().to_string()
        );

        let session =
            dispatch(&IpcRequest::call(3, "secret", "session.get", json!({"id": "one"})), &state)
                .unwrap()
                .result
                .unwrap();
        assert_eq!(session["workspace"], selected["workspace"]);

        assert!(
            dispatch(
                &IpcRequest::call(
                    4,
                    "secret",
                    "session.workspace",
                    json!({"id": "one", "workspace": root.join("missing").display().to_string()}),
                ),
                &state,
            )
            .is_err()
        );

        let reset = dispatch(
            &IpcRequest::call(
                5,
                "secret",
                "session.workspace",
                json!({"id": "one", "workspace": null}),
            ),
            &state,
        )
        .unwrap()
        .result
        .unwrap();
        assert!(reset["workspace"].is_null());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cancellation_waits_for_turn_acknowledgement() {
        let state = Arc::new(state());
        state.sessions.lock().unwrap().create("active", "model").unwrap();
        state
            .sessions
            .lock()
            .unwrap()
            .begin(
                "active",
                Message {
                    id: "message".into(),
                    session: "active".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: "cancel me".into() }],
                },
            )
            .unwrap();

        let request = IpcRequest::call(1, "secret", "session.cancel", json!({"id": "active"}));
        let cancel_state = Arc::clone(&state);
        let cancel_request =
            tokio::spawn(async move { cancel_request(&request, &cancel_state).await });
        let token = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(token) = state.cancels.lock().unwrap().get("active").cloned() {
                    break token;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), token.cancelled()).await.unwrap();
        assert!(!cancel_request.is_finished());
        token.acknowledge();
        let response = cancel_request.await.unwrap();
        assert_eq!(response.result.unwrap()["status"], "cancelled");
        assert!(state.cancels.lock().unwrap().get("active").is_none());
    }

    #[tokio::test]
    async fn lists_and_resolves_approvals_over_authenticated_ipc() {
        let state = state_at("approval-controls");
        let challenge = state
            .pending
            .lock()
            .await
            .issue(crate::approval::Target {
                channel: "telegram".into(),
                chat: "7".into(),
                thread: None,
                session: "telegram-7".into(),
                tool: "write".into(),
                args: json!({"path": "note.txt", "text": "approved"}),
            })
            .unwrap();
        let request = IpcRequest::call(1, "secret", "approval.list", json!({}));
        let listed = approval_list(&request, &state).await;
        let listed = listed.result.unwrap();
        let id = listed["items"][0]["id"].as_str().unwrap();
        assert_eq!(id.len(), 24);

        let request =
            IpcRequest::call(2, "secret", "approval.resolve", json!({"id": id, "approved": true}));
        let resolved = approval_resolve(&request, &state).await;
        assert_eq!(resolved.result.unwrap()["approved"], true);
        assert!(challenge.answer.await.unwrap());
    }

    #[tokio::test]
    async fn validates_approval_resolution_requests() {
        let state = state_at("approval-invalid");
        let missing_id = approval_resolve(
            &IpcRequest::call(1, "secret", "approval.resolve", json!({"approved": true})),
            &state,
        )
        .await;
        assert!(missing_id.error.unwrap().message.contains(".id is required"));
        let missing_action = approval_resolve(
            &IpcRequest::call(1, "secret", "approval.resolve", json!({"id": "bad"})),
            &state,
        )
        .await;
        assert!(missing_action.error.unwrap().message.contains(".approved is required"));
        let expired = approval_resolve(
            &IpcRequest::call(
                1,
                "secret",
                "approval.resolve",
                json!({"id": "0123456789abcdef01234567", "approved": true}),
            ),
            &state,
        )
        .await;
        assert_eq!(expired.error.unwrap().code, -32004);
    }

    #[test]
    fn bounds_session_list_payloads() {
        let state = state_at("-large");
        {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.create("large", "model").unwrap();
            for index in 0..100 {
                sessions.sessions.get_mut("large").unwrap().messages.push(Message {
                    id: index.to_string(),
                    session: "large".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: "x".repeat(256 * 1024) }],
                });
            }
        }
        let response =
            dispatch(&IpcRequest::call(1, "secret", "session.list", json!({})), &state).unwrap();
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(encoded.len().saturating_add(1) <= FRAME);
        assert!(response.result.unwrap()["items"][0]["messages"].is_null());
        let detail =
            dispatch(&IpcRequest::call(2, "secret", "session.get", json!({"id": "large"})), &state)
                .unwrap();
        assert!(serde_json::to_vec(&detail).unwrap().len().saturating_add(1) <= FRAME);
        let _ = std::fs::remove_file(format!(
            "/tmp/crabbot-ipc-test-{}-large.json",
            std::process::id()
        ));
    }

    #[test]
    fn reports_pending_worktree_cleanup() {
        let root = std::env::temp_dir().join(format!("crabbot-ipc-cleanup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join(".crabbot/worktrees/copy")).unwrap();
        let state = State {
            token: "secret".into(),
            sessions: Arc::new(Mutex::new(Store::load(root.join("sessions.json")).unwrap())),
            stop: Arc::new(Stop::new()),
            slots: Arc::new(Semaphore::new(super::CLIENTS)),
            cancels: Arc::new(Mutex::new(BTreeMap::new())),
            root: root.clone(),
            home: root.clone(),
            approval_mode: "off".into(),
            pending: Arc::new(tokio::sync::Mutex::new(crate::approval::Gate::new().unwrap())),
            plugins: crate::Plugins::default(),
            config: crate::Config::default(),
            channel: "telegram".into(),
            model: "codex".into(),
        };
        state.sessions.lock().unwrap().create("copy", "model").unwrap();
        let response = dispatch(
            &IpcRequest::call(1, "secret", "session.delete", json!({"id": "copy"})),
            &state,
        )
        .unwrap();
        let result = response.result.unwrap();
        assert_eq!(result["worktree"]["status"], "pending");
        assert!(result["worktree"]["error"].as_str().is_some());
        assert!(!state.sessions.lock().unwrap().sessions.contains_key("copy"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejects_bad_credentials_and_missing_ipc_files() {
        let root = std::env::temp_dir().join(format!("crabbot-ipc-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        assert!(super::call(&root, "status", json!({})).await.is_err());
        assert_eq!(
            super::to_io(crabbot_core::Error::Denied("no".into())).kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(super::invalid("bad").kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(super::not_found("missing").kind(), std::io::ErrorKind::NotFound);
        assert_eq!(super::token().unwrap().len(), 64);
        assert!(super::valid(&IpcResponse::ok(1, json!({}))));
        assert!(!super::valid(&IpcResponse::ok(2, json!({}))));
    }

    #[tokio::test]
    async fn serves_authenticated_and_rejects_unknown_clients() {
        let root = std::env::temp_dir().join(format!("crabbot-ipc-serve-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let listener = match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener failed: {error}"),
        };
        let port = listener.local_addr().unwrap().port();
        std::fs::write(root.join("ipc.token"), "secret").unwrap();
        std::fs::write(root.join("ipc.port"), port.to_string()).unwrap();
        let state = Arc::new(State {
            token: "secret".into(),
            sessions: Arc::new(Mutex::new(Store::load(root.join("sessions.json")).unwrap())),
            stop: Arc::new(Stop::new()),
            slots: Arc::new(Semaphore::new(super::CLIENTS)),
            cancels: Arc::new(Mutex::new(BTreeMap::new())),
            root: root.clone(),
            home: root.clone(),
            approval_mode: "off".into(),
            pending: Arc::new(tokio::sync::Mutex::new(crate::approval::Gate::new().unwrap())),
            plugins: crate::Plugins::default(),
            config: crate::Config::default(),
            channel: "telegram".into(),
            model: "codex".into(),
        });
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move { super::serve(listener, task_state).await });

        let status = super::call(&root, "status", json!({})).await.unwrap();
        assert_eq!(status["running"], true);

        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (read, mut write) = stream.into_split();
        jsonl::write(&mut write, &IpcRequest::call(2, "wrong", "status", json!({}))).await.unwrap();
        let mut read = BufReader::new(read);
        let response: IpcResponse = jsonl::read(&mut read, FRAME).await.unwrap().unwrap();
        assert_eq!(response.error.unwrap().code, 401);

        state.stop.signal();
        task.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
