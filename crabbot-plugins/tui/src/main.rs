#![forbid(unsafe_code)]

use crabbot_core::{
    jsonl,
    plugin::{Process, serve_with},
    types::{
        Capability, CommandSpec, Content, Hello, IpcRequest, IpcResponse, Message, ModelRequest,
        Protocol, Request, Response, Role,
    },
};
use serde_json::Value;
use std::future::Future;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    serve_with(hello(), call).await
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "tui".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Client],
        commands: vec![CommandSpec {
            name: "tui".into(),
            description: "Start the terminal interface.".into(),
            interactive: true,
        }],
    }
}

async fn call(request: Request) -> crabbot_core::Result<Option<Response>> {
    let Request::Call { id, method, params, .. } = request else {
        return Ok(None);
    };
    if method != "command" || params["name"] != "tui" {
        return Ok(None);
    }
    run().await?;
    Ok(Some(Response::ok(
        id,
        serde_json::json!({
            "status": "closed",
        }),
    )))
}

async fn run() -> crabbot_core::Result<()> {
    let input = terminal("r")?;
    let output = terminal("w")?;
    let plugin = std::env::var("CRABBOT_MODEL_PLUGIN").unwrap_or_else(|_| "codex".into());
    let model = std::env::var("CRABBOT_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
    let home = std::env::var("CRABBOT_HOME").unwrap_or_else(|_| ".config/crabbot".into());
    run_at(input, output, home, plugin, model).await
}

async fn run_at(
    input: std::fs::File,
    output: std::fs::File,
    home: String,
    plugin: String,
    model: String,
) -> crabbot_core::Result<()> {
    run_with(input, output, home, plugin, model, remote).await
}

async fn run_with<C, F>(
    input: std::fs::File,
    output: std::fs::File,
    home: String,
    plugin: String,
    mut model: String,
    host: C,
) -> crabbot_core::Result<()>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let input = BufReader::new(tokio::fs::File::from_std(input));
    let mut output = tokio::fs::File::from_std(output);
    let mut session = String::from("tui");
    let (selected_model, mut messages, mut workspace) =
        ensure_session(&home, &session, &model, host).await?;
    model = selected_model;
    output.write_all(b"Crabbot terminal. Type /help for commands.\n> ").await?;
    output.flush().await?;

    let name = if cfg!(windows) {
        format!("crabbot-plugin-{plugin}.exe")
    } else {
        format!("crabbot-plugin-{plugin}")
    };
    let path = std::path::PathBuf::from(&home).join("plugins").join(&plugin).join("bin").join(name);
    let mut process = Process::start(path).await?;
    let mut sequence = 0_u64;
    let result = async {
        let mut lines = input.lines();
        while let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line == "/quit" || line == "/exit" {
                break;
            }
            if line == "/help" {
                output
                    .write_all(
                        b"Commands: /help, /status, /approval, /approvals, /approve <id>, /deny <id>, /sessions, /deliveries, /retry <id>, /drop <id>, /model <name>, /session <id>, /new <id>, /plugins, /workspace [path|reset], /timer <list|add|remove>, /memory <list|remember|forget>, /clear, /quit.\n> ",
                    )
                    .await?;
                output.flush().await?;
                continue;
            }
            if line == "/timer" || line.starts_with("/timer ") {
                let command = line.strip_prefix("/timer").unwrap_or_default().trim();
                let mut parts = command.splitn(3, ' ');
                let action = parts.next().unwrap_or_default();
                let result = match action {
                    "list" if parts.next().is_none() => {
                        capability(home.clone(), "timer", "list", serde_json::json!({}), host)
                            .await
                            .map(|value| format_timers(&value))
                    }
                    "add" => match (parts.next(), parts.next()) {
                        (Some(delay), Some(text)) => match delay.parse::<u64>() {
                            Ok(delay) if (1..=31_536_000).contains(&delay) && !text.trim().is_empty() => {
                                let id = timer_id();
                                capability(
                                    home.clone(),
                                    "timer",
                                    "add",
                                    serde_json::json!({"id": id, "delay": delay, "text": text.trim()}),
                                    host,
                                )
                                .await
                                .map(|_| format!("Timer {id} was scheduled.\n> "))
                            }
                            _ => Ok("Timer delay must be between 1 and 31536000 seconds, with a reminder text.\n> ".into()),
                        },
                        _ => Ok("Usage: /timer add <seconds> <text>.\n> ".into()),
                    },
                    "remove" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
                        Some(id) if parts.next().is_none() => capability(
                            home.clone(),
                            "timer",
                            "remove",
                            serde_json::json!({"id": id}),
                            host,
                        )
                        .await
                        .map(|value| {
                            if value["deleted"] == true {
                                format!("Timer {id} was removed.\n> ")
                            } else {
                                format!("Timer {id} was not found.\n> ")
                            }
                        }),
                        _ => Ok("Usage: /timer remove <id>.\n> ".into()),
                    },
                    _ => Ok("Usage: /timer <list|add|remove>.\n> ".into()),
                };
                let text = result.unwrap_or_else(|error| {
                    format!("Timer action failed: {}.\n> ", sentence(error.to_string()))
                });
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/memory" || line.starts_with("/memory ") {
                let command = line.strip_prefix("/memory").unwrap_or_default().trim();
                let result = if command == "list" {
                    capability(
                        home.clone(),
                        "memory",
                        "list",
                        serde_json::json!({"scope": session}),
                        host,
                    )
                    .await
                    .map(|value| format_memories(&value))
                } else if let Some(value) = command.strip_prefix("remember ") {
                    match value.split_once('=') {
                        Some((key, value)) if !key.trim().is_empty() && !value.trim().is_empty() => {
                            capability(
                                home.clone(),
                                "memory",
                                "remember",
                                serde_json::json!({
                                    "key": key.trim(),
                                    "value": value.trim(),
                                    "scope": session,
                                    "mode": "suggest",
                                    "approved": true
                                }),
                                host,
                            )
                            .await
                            .map(|_| "Memory saved for this session.\n> ".into())
                        }
                        _ => Ok("Usage: /memory remember <key>=<value>.\n> ".into()),
                    }
                } else if let Some(key) = command.strip_prefix("forget ") {
                    if key.trim().is_empty() {
                        Ok("Usage: /memory forget <key>.\n> ".into())
                    } else {
                        capability(
                            home.clone(),
                            "memory",
                            "forget",
                            serde_json::json!({"key": key.trim(), "scope": session}),
                            host,
                        )
                        .await
                        .map(|value| {
                            if value["deleted"] == true {
                                "Memory removed.\n> ".into()
                            } else {
                                "Memory was not found.\n> ".into()
                            }
                        })
                    }
                } else {
                    Ok("Usage: /memory <list|remember|forget>.\n> ".into())
                };
                let text = result.unwrap_or_else(|error| {
                    format!("Memory action failed: {}.\n> ", sentence(error.to_string()))
                });
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/plugins" {
                if let Ok(value) = host(home.clone(), "plugin.list".into(), serde_json::json!({})).await {
                    output.write_all(format_plugins(&value).as_bytes()).await?;
                    output.flush().await?;
                    continue;
                }
                let path = std::path::PathBuf::from(&home).join("plugins");
                let mut names = std::fs::read_dir(path)
                    .ok()
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        entry
                            .file_type()
                            .ok()
                            .filter(std::fs::FileType::is_dir)
                            .and_then(|_| entry.file_name().into_string().ok())
                    })
                    .collect::<Vec<_>>();
                names.sort();
                let text = if names.is_empty() {
                    "Installed plugins: none.\n> ".to_owned()
                } else {
                    format!("Installed plugins: {}.\n> ", names.join(", "))
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/status" {
                let text = match host(home.clone(), "status".into(), serde_json::json!({})).await {
                    Ok(value) => format_status(&value),
                    Err(error) => format!("Daemon status unavailable: {}.\n> ", sentence(error.to_string())),
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/approval" {
                let text = match host(home.clone(), "status".into(), serde_json::json!({})).await {
                    Ok(value) => format_approval(&value),
                    Err(error) => format!(
                        "Approval status unavailable: {}.\n> ",
                        sentence(error.to_string())
                    ),
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/approvals" {
                let text = match host(home.clone(), "approval.list".into(), serde_json::json!({})).await {
                    Ok(value) => format_approvals(&value),
                    Err(error) => format!(
                        "Pending approvals unavailable: {}.\n> ",
                        sentence(error.to_string())
                    ),
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/approve" || line == "/deny" {
                output.write_all(b"Usage: /approve <id> or /deny <id>.\n> ").await?;
                output.flush().await?;
                continue;
            }
            if let Some(id) = line.strip_prefix("/approve ") {
                let text = approval_action(&home, id, true, host).await;
                output.write_all(format!("{text}\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if let Some(id) = line.strip_prefix("/deny ") {
                let text = approval_action(&home, id, false, host).await;
                output.write_all(format!("{text}\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/sessions" {
                let text = match host(home.clone(), "session.list".into(), serde_json::json!({})).await {
                    Ok(value) => format_sessions(&value),
                    Err(error) => format!("Session list unavailable: {}.\n> ", sentence(error.to_string())),
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/deliveries" {
                let text = match host(home.clone(), "delivery.list".into(), serde_json::json!({})).await {
                    Ok(value) => format_deliveries(&value),
                    Err(error) => format!(
                        "Delivery list unavailable: {}.\n> ",
                        sentence(error.to_string())
                    ),
                };
                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if let Some(id) = line.strip_prefix("/retry ") {
                let text = delivery_action(&home, "delivery.retry", id, host).await;
                output.write_all(format!("{text}\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if let Some(id) = line.strip_prefix("/drop ") {
                let text = delivery_action(&home, "delivery.drop", id, host).await;
                output.write_all(format!("{text}\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }
            if line == "/workspace" {
                let default_workspace = std::env::var("CRABBOT_ROOT").ok();
                let active = workspace
                    .as_deref()
                    .or(default_workspace.as_deref())
                    .unwrap_or("not configured");
                output
                    .write_all(format!("Workspace: {active}.\n> ").as_bytes())
                    .await?;
                output.flush().await?;
                continue;
            }
            if let Some(value) = line.strip_prefix("/workspace ") {
                let value = value.trim();
                if value.is_empty() {
                    output.write_all(b"Workspace path cannot be empty.\n> ").await?;
                } else {
                    let selected = (value != "reset").then_some(value);
                    match host(
                        home.clone(),
                        "session.workspace".into(),
                        serde_json::json!({"id": session, "workspace": selected}),
                    )
                    .await
                    {
                        Ok(result) => {
                            workspace = result["workspace"].as_str().map(str::to_owned);
                            let default_workspace = std::env::var("CRABBOT_ROOT").ok();
                            let active = workspace
                                .as_deref()
                                .or(default_workspace.as_deref())
                                .unwrap_or("not configured");
                            output
                                .write_all(format!("Workspace: {active}.\n> ").as_bytes())
                                .await?;
                        }
                        Err(error) => {
                            output
                                .write_all(
                                    format!("Workspace was not changed: {}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }
                output.flush().await?;
                continue;
            }
            if line == "/clear" {
                host(home.clone(), "session.clear".into(), serde_json::json!({"id": session}))
                    .await?;
                messages.clear();
                output.write_all(b"Conversation cleared.\n> ").await?;
                output.flush().await?;
                continue;
            }
            if line == "/model" {
                output.write_all(b"Model cannot be empty.\n> ").await?;
                output.flush().await?;
                continue;
            }
            if let Some(value) = line.strip_prefix("/model ") {
                let value = value.trim();
                if value.is_empty() {
                    output.write_all(b"Model cannot be empty.\n> ").await?;
                } else {
                    host(
                        home.clone(),
                        "session.model".into(),
                        serde_json::json!({"id": session, "model": value}),
                    )
                    .await?;
                    model = value.to_owned();
                    output.write_all(format!("Using model {model}.\n> ").as_bytes()).await?;
                }
                output.flush().await?;
                continue;
            }
            if let Some(value) = line.strip_prefix("/session ") {
                let value = value.trim();
                if !valid_session(value) {
                    output.write_all(b"Session ID is invalid.\n> ").await?;
                } else {
                    match read_session(&home, value, host).await {
                        Ok((selected_model, history, selected_workspace)) => {
                            session = value.to_owned();
                            model = selected_model;
                            messages = history;
                            workspace = selected_workspace;
                            output
                                .write_all(format!("Using session {session}.\n> ").as_bytes())
                                .await?;
                        }
                        Err(error) => {
                            output
                                .write_all(
                                    format!("Session unavailable: {}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }
                output.flush().await?;
                continue;
            }
            if let Some(value) = line.strip_prefix("/new ") {
                let value = value.trim();
                if !valid_session(value) {
                    output.write_all(b"Session ID is invalid.\n> ").await?;
                } else {
                    match host(
                        home.clone(),
                        "session.new".into(),
                        serde_json::json!({"id": value, "model": model}),
                    )
                    .await
                    {
                        Ok(_) => {
                            let (selected_model, history, selected_workspace) =
                                read_session(&home, value, host).await?;
                            session = value.to_owned();
                            model = selected_model;
                            messages = history;
                            workspace = selected_workspace;
                            output
                                .write_all(format!("Created session {session}.\n> ").as_bytes())
                                .await?;
                        }
                        Err(error) => {
                            output
                                .write_all(
                                    format!("Session creation failed: {}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }
                output.flush().await?;
                continue;
            }
            if line.is_empty() {
                output.write_all(b"> ").await?;
                output.flush().await?;
                continue;
            }
            sequence = sequence.saturating_add(1);
            let user = Message {
                id: message_id("user", sequence),
                session: session.clone(),
                role: Role::User,
                sender: Some("tui".into()),
                content: vec![Content::Text { text: line.into() }],
            };
            if let Err(error) = host(
                home.clone(),
                "session.append".into(),
                serde_json::json!({"id": session, "message": user}),
            )
            .await
            {
                output
                    .write_all(
                        format!("Turn was not saved: {}.\n> ", sentence(error.to_string())).as_bytes(),
                    )
                    .await?;
                output.flush().await?;
                continue;
            }
            messages.push(user);
            let (sender, mut events) = mpsc::channel(32);
            let request = Request::call(
                sequence,
                "generate",
                serde_json::to_value(ModelRequest {
                    model: model.clone(),
                    workspace: workspace.clone().or_else(|| std::env::var("CRABBOT_ROOT").ok()),
                    messages: messages.clone(),
                    stream: true,
                    tools: Vec::new(),
                })?,
            );
            let call = process.call_stream_async(request, move |note| {
                let sender = sender.clone();
                async move {
                    sender.send(note).await.map_err(|_| {
                        crabbot_core::Error::Protocol("Terminal output is unavailable.".into())
                    })
                }
            });
            tokio::pin!(call);
            let mut streamed = String::new();
            let response = loop {
                tokio::select! {
                    result = &mut call => break result?,
                    event = events.recv() => {
                        if let Some(text) = event.and_then(stream_text) {
                            streamed.push_str(&text);
                            output.write_all(text.as_bytes()).await?;
                            output.flush().await?;
                        }
                    }
                }
            };
            while let Ok(event) = events.try_recv() {
                if let Some(text) = stream_text(event) {
                    streamed.push_str(&text);
                    output.write_all(text.as_bytes()).await?;
                }
            }
            if let Some(error) = response.error {
                return Err(crabbot_core::Error::Denied(error.message));
            }
            let value = response.result.ok_or_else(|| {
                crabbot_core::Error::Denied("The intelligence plugin returned no result.".into())
            })?;
            let reply: crabbot_core::types::ModelReply = serde_json::from_value(value)?;
            let assistant = Message {
                id: message_id("assistant", sequence),
                session: session.clone(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: reply.text.clone() }],
            };
            let saved = host(
                home.clone(),
                "session.append".into(),
                serde_json::json!({"id": session, "message": assistant}),
            )
            .await;
            messages.push(assistant);
            if streamed.is_empty() {
                output.write_all(reply.text.as_bytes()).await?;
            } else if let Some(rest) = reply.text.strip_prefix(&streamed) {
                output.write_all(rest.as_bytes()).await?;
            } else if !reply.text.is_empty() {
                output.write_all(b"\n").await?;
                output.write_all(reply.text.as_bytes()).await?;
            }
            if let Err(error) = saved {
                output
                    .write_all(
                        format!("\nReply was not saved: {}.", sentence(error.to_string())).as_bytes(),
                    )
                    .await?;
            }
            output.write_all(b"\n> ").await?;
            output.flush().await?;
        }
        Ok::<(), crabbot_core::Error>(())
    }
    .await;
    let stopped = process.stop().await;
    result?;
    stopped
}

async fn ensure_session<C, F>(
    home: &str,
    id: &str,
    model: &str,
    host: C,
) -> crabbot_core::Result<(String, Vec<Message>, Option<String>)>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    host(home.into(), "session.ensure".into(), serde_json::json!({"id": id, "model": model}))
        .await?;
    read_session(home, id, host).await
}

async fn read_session<C, F>(
    home: &str,
    id: &str,
    host: C,
) -> crabbot_core::Result<(String, Vec<Message>, Option<String>)>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let value = host(home.into(), "session.get".into(), serde_json::json!({"id": id})).await?;
    if value["status"] == "working" || value["inflight"] == true {
        return Err(crabbot_core::Error::Denied("Session is already working.".into()));
    }
    let model = value["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("Session has no model configured.".into()))?
        .to_owned();
    let messages = serde_json::from_value(value["messages"].clone())?;
    let workspace = value["workspace"].as_str().map(str::to_owned);
    Ok((model, messages, workspace))
}

fn valid_session(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

async fn remote(home: String, method: String, params: Value) -> crabbot_core::Result<Value> {
    control(&home, &method, params).await
}

async fn capability<C, F>(
    home: String,
    service: &str,
    method: &str,
    params: Value,
    host: C,
) -> crabbot_core::Result<Value>
where
    C: Fn(String, String, Value) -> F,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    host(
        home,
        "capability.call".into(),
        serde_json::json!({"service": service, "method": method, "params": params}),
    )
    .await
}

fn format_timers(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Timers: none.\n> ".into();
    };
    let items = items
        .iter()
        .take(100)
        .filter_map(|item| {
            let id = item["id"].as_u64()?;
            let due = item["due"].as_u64()?;
            let text = item["text"].as_str().unwrap_or_default();
            Some(format!("{id} (due {due}): {}", short(text)))
        })
        .collect::<Vec<_>>();
    if items.is_empty() {
        "Timers: none.\n> ".into()
    } else {
        format!("Timers: {}.\n> ", items.join("; "))
    }
}

fn format_memories(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Memories: none.\n> ".into();
    };
    let items = items
        .iter()
        .take(100)
        .filter_map(|item| {
            let key = item["key"].as_str()?;
            let value = item["value"].as_str().unwrap_or_default();
            Some(format!("{} = {}", short(key), short(value)))
        })
        .collect::<Vec<_>>();
    if items.is_empty() {
        "Memories: none.\n> ".into()
    } else {
        format!("Memories: {}.\n> ", items.join("; "))
    }
}

fn short(value: &str) -> String {
    let mut chars = value.chars();
    let mut short = chars.by_ref().take(120).collect::<String>();
    if chars.next().is_some() {
        short.push('…');
    }
    short
}

fn timer_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos().min(u64::MAX as u128) as u64)
}

fn message_id(role: &str, sequence: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("tui-{role}-{now}-{sequence}")
}

fn stream_text(request: Request) -> Option<String> {
    let Request::Note { method, params, .. } = request else {
        return None;
    };
    if method != "event" || params["event"]["kind"] != "text" {
        return None;
    }
    params["event"]["text"].as_str().map(str::to_owned)
}

fn terminal(mode: &str) -> crabbot_core::Result<std::fs::File> {
    let path =
        if cfg!(windows) { if mode == "r" { "CONIN$" } else { "CONOUT$" } } else { "/dev/tty" };
    let mut options = std::fs::OpenOptions::new();
    if mode == "r" {
        options.read(true);
    } else {
        options.write(true);
    }
    options.open(path).map_err(crabbot_core::Error::from)
}

async fn control(home: &str, method: &str, params: Value) -> crabbot_core::Result<Value> {
    let token = tokio::fs::read_to_string(std::path::Path::new(home).join("ipc.token")).await?;
    let port = tokio::fs::read_to_string(std::path::Path::new(home).join("ipc.port"))
        .await?
        .trim()
        .parse::<u16>()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("The daemon port is invalid: {error}."))
        })?;
    let exchange = async {
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let (input, output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut output = output;
        jsonl::write(&mut output, &IpcRequest::call(1, token.trim(), method, params))
            .await
            .map_err(|error| crabbot_core::Error::Denied(error.to_string()))?;
        let response: IpcResponse =
            jsonl::read(&mut input, jsonl::MAX).await?.ok_or_else(|| {
                crabbot_core::Error::Denied("The daemon closed the IPC connection.".into())
            })?;
        if !response.valid() || response.id != 1 {
            return Err(crabbot_core::Error::Denied(
                "The daemon returned an invalid response.".into(),
            ));
        }
        match (response.result, response.error) {
            (Some(value), _) => Ok(value),
            (_, Some(error)) => Err(crabbot_core::Error::Denied(error.message)),
            _ => Err(crabbot_core::Error::Denied("The daemon returned an empty response.".into())),
        }
    };
    timeout(Duration::from_secs(5), exchange)
        .await
        .map_err(|_| crabbot_core::Error::Denied("The daemon did not respond in time.".into()))?
}

async fn delivery_action<C, F>(home: &str, method: &str, id: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let id = id.trim();
    if id.is_empty()
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return "Delivery ID is invalid.".into();
    }
    match host(home.into(), method.into(), serde_json::json!({"id": id, "yes": true})).await {
        Ok(value) => format!("Delivery {}.", value["status"].as_str().unwrap_or("updated")),
        Err(error) => format!("Delivery update unavailable: {}.", sentence(error.to_string())),
    }
}

fn format_status(value: &Value) -> String {
    let running = value["running"].as_bool().unwrap_or(false);
    let sessions = value["sessions"].as_u64().unwrap_or(0);
    format!("Daemon: {}. Sessions: {sessions}.\n> ", if running { "running" } else { "stopped" })
}

fn format_approval(value: &Value) -> String {
    let mode = value["approval"].as_str().unwrap_or("off");
    format!("Approvals: {mode}.\n> ")
}

fn format_approvals(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Pending approvals: none.\n> ".into();
    };
    if items.is_empty() {
        return "Pending approvals: none.\n> ".into();
    }
    let rows = items
        .iter()
        .filter_map(|item| {
            let id = item["id"].as_str()?;
            let target = &item["target"];
            let tool = target["tool"].as_str().unwrap_or("tool");
            let session = target["session"].as_str().unwrap_or("unknown session");
            let args = preview(&target["args"].to_string(), 512);
            Some(format!("{id}: {tool} for {session} — {args}"))
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return "Pending approvals: none.\n> ".into();
    }
    format!("Pending approvals:\n{}\nUse /approve <id> or /deny <id>.\n> ", rows.join("\n"))
}

fn preview(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let output = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() { format!("{output}…") } else { output }
}

async fn approval_action<C, F>(home: &str, id: &str, approved: bool, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let id = id.trim();
    if id.len() != 24 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return "Approval ID is invalid.".into();
    }
    match host(
        home.into(),
        "approval.resolve".into(),
        serde_json::json!({"id": id, "approved": approved}),
    )
    .await
    {
        Ok(value) if value["resolved"] == true => {
            if approved { "Approval accepted." } else { "Approval denied." }.into()
        }
        Ok(_) => "Approval is no longer pending.".into(),
        Err(error) => format!("Approval action failed: {}.", sentence(error.to_string())),
    }
}

fn format_sessions(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Sessions: none.\n> ".into();
    };
    if items.is_empty() {
        return "Sessions: none.\n> ".into();
    }
    let names = items
        .iter()
        .filter_map(|item| {
            let id = item["id"].as_str()?;
            let status = item["status"].as_str().unwrap_or("unknown");
            Some(format!("{id} ({status})"))
        })
        .collect::<Vec<_>>();
    if names.is_empty() {
        "Sessions: none.\n> ".into()
    } else {
        format!("Sessions: {}.\n> ", names.join(", "))
    }
}

fn format_plugins(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Installed plugins: none.\n> ".into();
    };
    let names = items
        .iter()
        .filter_map(|item| {
            let id = item["id"].as_str()?;
            Some(
                item["health"]
                    .as_str()
                    .map_or_else(|| id.to_owned(), |health| format!("{id} ({health})")),
            )
        })
        .collect::<Vec<String>>();
    if names.is_empty() {
        "Installed plugins: none.\n> ".into()
    } else {
        format!("Installed plugins: {}.\n> ", names.join(", "))
    }
}

fn format_deliveries(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Deliveries: none.\n> ".into();
    };
    let names = items
        .iter()
        .filter_map(|item| {
            let id = item["id"].as_str()?;
            let status = item["status"].as_str().unwrap_or("unknown");
            Some(format!("{id} ({status})"))
        })
        .collect::<Vec<_>>();
    if names.is_empty() {
        "Deliveries: none.\n> ".into()
    } else {
        format!("Deliveries: {}.\n> ", names.join(", "))
    }
}

fn sentence(value: String) -> String {
    let value = value.trim().trim_end_matches('.');
    if value.is_empty() {
        return "Unknown error".into();
    }
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Unknown error".into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn describes_client_capability() {
        let hello = super::hello();
        assert_eq!(hello.id, "tui");
        assert_eq!(hello.capabilities, vec![crabbot_core::types::Capability::Client]);
        assert_eq!(hello.commands[0].name, "tui");
    }

    #[test]
    fn formats_status() {
        let text = super::format_status(&serde_json::json!({"running": true, "sessions": 2}));
        assert_eq!(text, "Daemon: running. Sessions: 2.\n> ");

        let text = super::format_status(&serde_json::json!({"running": false}));
        assert_eq!(text, "Daemon: stopped. Sessions: 0.\n> ");
    }

    #[test]
    fn formats_approval() {
        assert_eq!(
            super::format_approval(&serde_json::json!({"approval": "auto"})),
            "Approvals: auto.\n> "
        );
    }

    #[test]
    fn formats_pending_approvals() {
        let value = json!({
            "items": [{
                "id": "0123456789abcdef01234567",
                "target": {
                    "session": "telegram-7",
                    "tool": "write",
                    "args": {"path": "note.txt"}
                }
            }]
        });
        assert_eq!(
            super::format_approvals(&value),
            "Pending approvals:\n0123456789abcdef01234567: write for telegram-7 — {\"path\":\"note.txt\"}\nUse /approve <id> or /deny <id>.\n> "
        );
        assert_eq!(super::format_approvals(&json!({"items": []})), "Pending approvals: none.\n> ");
        assert_eq!(super::preview(&"x".repeat(600), 512).chars().count(), 513);
    }

    #[test]
    fn formats_sessions() {
        let text = super::format_sessions(&serde_json::json!({
            "items": [{"id": "one", "status": "idle"}]
        }));
        assert_eq!(text, "Sessions: one (idle).\n> ");

        assert_eq!(
            super::format_sessions(&serde_json::json!({"items": [{}]})),
            "Sessions: none.\n> "
        );
        assert_eq!(super::format_sessions(&serde_json::json!({})), "Sessions: none.\n> ");
    }

    #[test]
    fn formats_plugins() {
        assert_eq!(
            super::format_plugins(&serde_json::json!({
                "items": [{"id": "codex", "health": "ready"}]
            })),
            "Installed plugins: codex (ready).\n> "
        );
    }

    #[test]
    fn formats_deliveries() {
        assert_eq!(
            super::format_deliveries(
                &serde_json::json!({"items": [{"id": "one", "status": "pending"}]})
            ),
            "Deliveries: one (pending).\n> "
        );
        assert_eq!(super::format_deliveries(&serde_json::json!({})), "Deliveries: none.\n> ");
    }

    #[test]
    fn formats_timer_and_memory_records() {
        assert_eq!(
            super::format_timers(&json!({"items": [{"id": 7, "due": 42, "text": "call back"}]})),
            "Timers: 7 (due 42): call back.\n> "
        );
        assert_eq!(
            super::format_memories(&json!({"items": [{"key": "drink", "value": "tea"}]})),
            "Memories: drink = tea.\n> "
        );
    }

    #[tokio::test]
    async fn validates_delivery_actions() {
        assert_eq!(
            super::delivery_action("/tmp/missing", "delivery.retry", "bad id", mock_control).await,
            "Delivery ID is invalid."
        );
        assert_eq!(
            super::approval_action("/tmp", "bad", true, mock_control).await,
            "Approval ID is invalid."
        );
    }

    #[test]
    fn sentence_capitalizes_errors() {
        assert_eq!(super::sentence("connection failed.".into()), "Connection failed");
    }

    async fn mock_control(
        _home: String,
        method: String,
        params: serde_json::Value,
    ) -> crabbot_core::Result<serde_json::Value> {
        match method.as_str() {
            "session.get" => Ok(json!({
                "id": params["id"],
                "model": "base",
                "workspace": null,
                "status": "idle",
                "inflight": false,
                "messages": []
            })),
            "capability.call" => match (params["service"].as_str(), params["method"].as_str()) {
                (Some("timer"), Some("list")) => Ok(json!({
                    "items": [{"id": 7, "due": 42, "text": "call back"}]
                })),
                (Some("timer"), Some("add")) => Ok(json!({"id": params["params"]["id"]})),
                (Some("timer"), Some("remove")) => Ok(json!({"deleted": true})),
                (Some("memory"), Some("list")) => Ok(json!({
                    "items": [{"key": "drink", "value": "tea"}]
                })),
                (Some("memory"), Some("remember")) => Ok(json!({"ok": true})),
                (Some("memory"), Some("forget")) => Ok(json!({"deleted": true})),
                _ => Err(crabbot_core::Error::Denied("Method not found.".into())),
            },
            "approval.list" => Ok(json!({
                "items": [{
                    "id": "0123456789abcdef01234567",
                    "target": {
                        "session": "telegram-7",
                        "tool": "write",
                        "args": {"path": "note.txt"}
                    }
                }]
            })),
            "approval.resolve" => Ok(json!({"resolved": true, "approved": params["approved"]})),
            "session.workspace" => Ok(json!({
                "id": params["id"],
                "workspace": params["workspace"]
            })),
            "session.ensure" | "session.new" | "session.append" | "session.clear"
            | "session.model" => Ok(json!({"id": params["id"]})),
            _ => Err(crabbot_core::Error::Denied("Method not found.".into())),
        }
    }

    #[test]
    fn extracts_text_stream_events() {
        let event = crabbot_core::types::Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: serde_json::json!({"event": {"kind": "text", "text": "part"}}),
        };
        assert_eq!(super::stream_text(event), Some("part".into()));
        assert!(
            super::stream_text(crabbot_core::types::Request::call(
                1,
                "event",
                serde_json::json!({})
            ))
            .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_commands_and_model_turn() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let root = std::env::temp_dir().join(format!("crabbot-tui-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let binary = root.join("plugins/codex/bin/crabbot-plugin-codex");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(
            &binary,
            r#"#!/bin/sh
while IFS= read -r line; do case "$line" in *generate*) printf '%s\n' '{"jsonrpc":"2.0","method":"event","params":{"event":{"kind":"text","text":"Reply"}}}'; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"text":"Reply","stop":"stop","events":[]}}' ;; *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        let input_path = root.join("input");
        let output_path = root.join("output");
        fs::write(
            &input_path,
            format!(
                "/help\n/status\n/approval\n/approvals\n/approve 0123456789abcdef01234567\n/deny 0123456789abcdef01234567\n/sessions\n/deliveries\n/retry bad id\n/drop bad id\n/plugins\n/workspace\n/workspace {}\n/workspace reset\n/timer list\n/timer add 30 break\n/timer remove 7\n/memory list\n/memory remember drink=tea\n/memory forget drink\n/model \n/model test\n/session bad!\n/new two\n/session one\n/clear\n\nhello\n/quit\n",
                root.display()
            ),
        )
        .unwrap();
        let input = fs::File::open(&input_path).unwrap();
        let output = fs::File::create(&output_path).unwrap();
        let run = super::run_with(
            input,
            output,
            root.display().to_string(),
            "codex".into(),
            "base".into(),
            mock_control,
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), run).await.unwrap().unwrap();
        let text = fs::read_to_string(&output_path).unwrap();
        assert_eq!(text.matches("Reply").count(), 1);
        assert!(text.contains("Model cannot be empty."));
        assert!(text.contains("Daemon status unavailable"));
        assert!(text.contains("Pending approvals:"));
        assert!(text.contains("Approval accepted."));
        assert!(text.contains("Approval denied."));
        assert!(text.contains("Installed plugins: codex"));
        assert!(text.contains("Created session two."));
        assert!(text.contains("Using session one."));
        assert!(text.contains(&format!("Workspace: {}.", root.display())));
        assert!(text.contains("Timers: 7 (due 42): call back."));
        assert!(text.contains("was scheduled."));
        assert!(text.contains("Timer 7 was removed."));
        assert!(text.contains("Memories: drink = tea."));
        assert!(text.contains("Memory saved for this session."));
        assert!(text.contains("Memory removed."));
        let _ = fs::remove_dir_all(root);
    }
}
