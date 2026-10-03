#![forbid(unsafe_code)]

mod data;
mod date;
#[cfg(test)]
mod offline;
mod ui;

#[cfg(test)]
use crabbot_core::plugin::Process;
use crabbot_core::{
    jsonl,
    plugin::serve_with,
    types::{
        Capability, CommandSpec, Content, Hello, IpcRequest, IpcResponse, Message, Protocol,
        Request, Response, Role,
    },
};

#[cfg(test)]
use crabbot_core::types::ToolSpec;
use serde_json::Value;
#[cfg(test)]
use std::sync::Arc;
use std::{future::Future, path::PathBuf, pin::Pin};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, duplex,
};

use tokio::net::TcpStream;
#[cfg(test)]
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

const DEFAULT_MODEL: &str = "unset";
const DEFAULT_NAME: &str = "Crabbot";
const DEFAULT_SESSION_ID: &str = "default";
const SESSION_LIST_PAGE_SIZE: usize = 5;
const PLUGIN_LIST_PAGE_SIZE: usize = 5;
const STREAMING_TURN_TIMEOUT: Duration = Duration::from_secs(310);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(305);
#[cfg(test)]
const SAVED_REPLY_LIMIT: usize = 16 * 1024;
// 365 days (one year), expressed in seconds.
const MAX_TIMER_DELAY_SECONDS: u64 = 31_536_000;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.first().is_some_and(|value| value == "--crabbot-cli") {
        return match run(&args[1..]).await {
            Ok(output) => {
                if let Some(output) = output {
                    println!("{output}");
                }

                std::process::ExitCode::SUCCESS
            }

            Err(error) => {
                eprintln!("Error: {}", cli_error_message(&error));
                std::process::ExitCode::FAILURE
            }
        };
    }

    match serve_with(hello(), call).await {
        Ok(()) => std::process::ExitCode::SUCCESS,

        Err(error) => {
            eprintln!("Error: {}", cli_error_message(&error));
            std::process::ExitCode::FAILURE
        }
    }
}

fn cli_error_message(error: &crabbot_core::Error) -> String {
    match error {
        crabbot_core::Error::Denied(message) => message.clone(),
        _ => error.to_string(),
    }
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "tui".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Client],
        commands: vec![CommandSpec {
            name: "tui".into(),
            description: "Open the TUI; the Crabbot daemon must be running.".into(),
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

    let args = params["args"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    match run(&args).await? {
        Some(output) => Ok(Some(Response::ok(id, Value::String(output)))),
        None => Ok(Some(Response::ok(id, serde_json::json!({"status": "closed"})))),
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct TuiOptions {
    once: Option<String>,
    session: Option<String>,
    model: Option<String>,
    plugin: Option<String>,
}

struct EngineConfig {
    home: String,
    plugin: String,
    model: String,
    model_override: Option<String>,
    session: String,
}

#[cfg(test)]
struct ToolsProcess {
    process: Process,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EngineEvent {
    GenerationStarted,
    CompactionStarted,
    CompactionFinished,
    AssistantText(String),
    SystemText(String),
    SystemNotice(String),
    AssistantFinished,
    GenerationFinished { interrupted: bool },
}

type StreamingHost = fn(
    String,
    String,
    Value,
    mpsc::UnboundedSender<EngineEvent>,
) -> Pin<Box<dyn Future<Output = crabbot_core::Result<Value>> + Send>>;

#[derive(Default)]
struct EngineEvents {
    session: Option<mpsc::UnboundedSender<ui::SessionView>>,
    engine: Option<mpsc::UnboundedSender<EngineEvent>>,
    streaming_host: Option<StreamingHost>,
}

struct SessionReservation {
    release: Option<Pin<Box<dyn Future<Output = crabbot_core::Result<Value>> + Send>>>,
    owner: String,
}

impl SessionReservation {
    fn new<C, F>(home: String, session: String, owner: String, host: C) -> Self
    where
        C: Fn(String, String, Value) -> F + Copy + Send + 'static,
        F: Future<Output = crabbot_core::Result<Value>> + Send + 'static,
    {
        let release_owner = owner.clone();
        let release = async move {
            host(
                home,
                "session.release".into(),
                serde_json::json!({"id": session, "owner": release_owner}),
            )
            .await
        };

        Self { release: Some(Box::pin(release)), owner }
    }

    async fn release(&mut self) -> crabbot_core::Result<Value> {
        self.release
            .take()
            .ok_or_else(|| crabbot_core::Error::Denied("Session reservation was released.".into()))?
            .await
    }
}

impl Drop for SessionReservation {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            tokio::spawn(release);
        }
    }
}

fn parse_options(args: &[String]) -> crabbot_core::Result<TuiOptions> {
    let mut options = TuiOptions::default();

    let mut index = 0;

    while index < args.len() {
        let name = args[index].as_str();
        index += 1;

        let value = match name {
            "--once" | "--session" | "--model" | "--plugin" => {
                let value = args.get(index).ok_or_else(|| {
                    crabbot_core::Error::Denied(format!("{name} requires a value."))
                })?;
                index += 1;
                value.clone()
            }

            _ => {
                return Err(crabbot_core::Error::Denied(format!(
                    "Unknown TUI option {name}. Use `crab tui --help` for usage."
                )));
            }
        };

        match name {
            "--once" => options.once = Some(value),
            "--session" => options.session = Some(value),
            "--model" => options.model = Some(value),
            "--plugin" => options.plugin = Some(value),
            _ => unreachable!(),
        }
    }

    if options.once.as_ref().is_some_and(|prompt| prompt.trim().is_empty()) {
        return Err(crabbot_core::Error::Denied("--once requires a non-empty prompt.".into()));
    }

    if options.session.as_deref().is_some_and(|id| !valid_session(id)) {
        return Err(crabbot_core::Error::Denied("Session ID is invalid.".into()));
    }

    Ok(options)
}

async fn run(args: &[String]) -> crabbot_core::Result<Option<String>> {
    let options = parse_options(args)?;
    let model_override = options.model.clone();
    let plugin = options
        .plugin
        .or_else(|| std::env::var("CRABBOT_MODEL_PLUGIN").ok())
        .unwrap_or_else(|| "codex".into());

    let model = options
        .model
        .or_else(|| std::env::var("CRABBOT_MODEL").ok())
        .unwrap_or_else(|| DEFAULT_MODEL.into());

    let session = selected_session_id(options.session);
    let home = std::env::var("CRABBOT_HOME").unwrap_or_else(|_| ".config/crabbot".into());

    require_daemon(&home).await?;

    if let Some(prompt) = options.once {
        return run_once(home, plugin, model, model_override, session, prompt, daemon_control)
            .await
            .map(Some);
    }

    let input = terminal("r")?;
    let output = terminal("w")?;
    run_at(input, output, home, plugin, model, model_override, session).await?;

    Ok(None)
}

fn selected_session_id(session: Option<String>) -> String {
    tui_session_id(&session.unwrap_or_else(|| DEFAULT_SESSION_ID.into()))
}

fn tui_session_id(name: &str) -> String {
    format!("tui-{name}")
}

fn tui_session_name(id: &str) -> Option<&str> {
    let name = id.strip_prefix("tui-")?;
    valid_session(name).then_some(name)
}

async fn run_at(
    _input: std::fs::File,
    output: std::fs::File,
    home: String,
    plugin: String,
    model: String,
    model_override: Option<String>,
    session: String,
) -> crabbot_core::Result<()> {
    let name = std::env::var("CRABBOT_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());
    ui::run(
        output,
        home,
        ui::ModelOptions { plugin, model, model_override },
        session,
        name,
        daemon_control,
    )
    .await
}

async fn run_once<C, F>(
    home: String,
    plugin: String,
    model: String,
    model_override: Option<String>,
    session: String,
    prompt: String,
    host: C,
) -> crabbot_core::Result<String>
where
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = crabbot_core::Result<Value>> + Send + 'static,
{
    let (mut input, engine_input) = duplex(16 * 1024);

    let (mut output, engine_output) = duplex(64 * 1024);
    let (interrupt_tx, interrupt_rx) = mpsc::unbounded_channel();
    drop(interrupt_tx);

    let engine = tokio::spawn(run_with(
        engine_input,
        engine_output,
        EngineConfig { home, plugin, model, model_override, session },
        host,
        EngineEvents::default(),
        interrupt_rx,
    ));

    input.write_all(serde_json::to_string(&prompt)?.as_bytes()).await?;
    input.write_all(b"\n/quit\n").await?;
    drop(input);

    let mut transcript = String::new();
    output.read_to_string(&mut transcript).await?;
    engine.await.map_err(|error| crabbot_core::Error::Denied(error.to_string()))??;

    let bot_name = std::env::var("CRABBOT_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());
    let greeting = format!("{bot_name} terminal. Type /help for commands.\n> ");
    let transcript = transcript.strip_prefix(&greeting).unwrap_or(&transcript);

    Ok(transcript.trim_end_matches("\n> ").trim().to_owned())
}

async fn run_with<R, W, C, F>(
    input: R,
    mut output: W,
    config: EngineConfig,
    host: C,
    events: EngineEvents,
    mut interrupt_rx: mpsc::UnboundedReceiver<()>,
) -> crabbot_core::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = crabbot_core::Result<Value>> + Send + 'static,
{
    let EngineConfig { home, plugin, mut model, model_override, mut session } = config;
    let EngineEvents { session: session_events, engine: engine_events, streaming_host } = events;
    let input = BufReader::new(input);
    let initial_session = ensure_session(&home, &session, &model, host).await?;

    model = if let Some(model_override) = model_override {
        host(
            home.clone(),
            "session.model".into(),
            serde_json::json!({"id": session, "model": model_override}),
        )
        .await?;
        model_override
    } else {
        initial_session.model.clone()
    };

    let mut messages = initial_session.messages;
    let mut workspace = initial_session.workspace;

    let inventory = host(home.clone(), "plugin.list".into(), serde_json::json!({}))
        .await
        .unwrap_or(Value::Null);

    let model_plugin_available = inventory["items"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["id"] == plugin
                && item["health"] == "ready"
                && item["capabilities"].as_array().is_some_and(|capabilities| {
                    capabilities.iter().any(|capability| capability == "model")
                })
        })
    });

    let bot_name = std::env::var("CRABBOT_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());

    if engine_events.is_some() {
        output.write_all(b"> ").await?;
    } else {
        output
            .write_all(format!("{bot_name} terminal. Type /help for commands.\n> ").as_bytes())
            .await?;
    }

    output.flush().await?;

    if !model_plugin_available && engine_events.is_none() {
        output
            .write_all(
                format!("No intelligence plugin is installed at {plugin}. Install one to send prompts; session commands remain available.\n> ")
                .as_bytes(),
            )
            .await?;
        output.flush().await?;
    }

    let mut sequence = 0_u64;
    let result = async {
        let mut lines = input.lines();

        while let Some(line) = lines.next_line().await? {
            let line = decode_terminal_input(&line);
            let line = line.trim();

            if line == "/quit" || line == "/exit" {
                break;
            }

            if line == "/help" {
                let daemon = host(home.clone(), "status".into(), serde_json::json!({})).await.is_ok();
                let help = command_help(&home, model_plugin_available, daemon);
                output.write_all(help.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/timer" || line.starts_with("/timer ") {
                let command = line.strip_prefix("/timer").unwrap_or_default().trim();
                let text = timer_command(home.clone(), command, host).await;

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/memory" || line.starts_with("/memory ") {
                let command = line.strip_prefix("/memory").unwrap_or_default().trim();
                let text = memory_command(home.clone(), session.clone(), command, host).await;

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/plugins" || line.starts_with("/plugins ") {
                let text = match parse_plugin_list_page(line) {
                    Ok(page) => {
                        let value = match host(
                            home.clone(),
                            "plugin.list".into(),
                            serde_json::json!({}),
                        )
                        .await
                        {
                            Ok(value) => value,
                            Err(_) => local_plugin_list(&home),
                        };

                        format!("{}\n> ", format_plugins(&value, page))
                    }

                    Err(()) => "Usage: /plugins [page].\n> ".into(),
                };

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/status" {
                let text = match host(home.clone(), "status".into(), serde_json::json!({})).await {
                    Ok(value) => format_status(&value),

                    Err(error) if daemon_unavailable(&error) => {
                        "Background runtime: stopped. Start it with `crab service start`.\n> ".into()
                    }

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

            if line == "/session list" || line.starts_with("/session list ") {
                let text = match parse_session_list_page(line) {
                    Ok(page) => {
                        match host(home.clone(), "session.list".into(), serde_json::json!({})).await {
                            Ok(value) => {
                                format_sessions(&value, &session, model_plugin_available, page)
                            }

                            Err(error) => format!("{}.\n> ", sentence(error.to_string())),
                        }
                    }

                    Err(()) => "Usage: /session list [page].\n> ".into(),
                };

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/session help" {
                output.write_all(session_help().as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if let Some(value) = line.strip_prefix("/session rename ") {
                let target = value.trim();

                if !valid_session(target) {
                    output.write_all(b"Session ID is invalid. Use lowercase letters, numbers, and hyphens.\n> ").await?;
                } else if tui_session_name(&session) == Some(target) {
                    output.write_all(b"Session ID is unchanged.\n> ").await?;
                } else {
                    match host(
                        home.clone(),
                        "session.rename".into(),
                        serde_json::json!({"id": session, "target": tui_session_id(target)}),
                    )
                    .await
                    {
                        Ok(_) => {
                            let old_name = tui_session_name(&session).unwrap_or(&session);
                            output.write_all(format!("Renamed session {old_name} to {target}.\n> ").as_bytes()).await?;
                            session = tui_session_id(target);
                            let view = read_session(&home, &session, host).await?;
                            model = view.model.clone();
                            messages = view.messages.clone();
                            workspace = view.workspace.clone();
                            send_session_view(&session_events, view);
                        }

                        Err(error) => {
                            output
                                .write_all(
                                    format!("{}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }

                output.flush().await?;
                continue;
            }

            let session_action = [
                ("/session archive ", "archive"),
                ("/session unarchive ", "unarchive"),
                ("/session delete ", "delete"),
            ]
            .into_iter()
            .find_map(|(prefix, action)| line.strip_prefix(prefix).map(|value| (action, value)));

            if let Some((action, value)) = session_action {
                let allow_confirmation = action == "delete";
                let usage = match action {
                    "archive" => "Usage: /session archive <id>...|--all.\n> ",
                    "unarchive" => "Usage: /session unarchive <id>...|--all.\n> ",
                    _ => "Usage: /session delete <id>...|--all [-y|--yes] [--deep].\n> ",
                };

                match parse_session_targets(value, allow_confirmation) {
                    Ok(targets) if action == "delete" && !targets.confirmed => {
                        output.write_all(b"Deleting sessions permanently removes their history. Re-run with -y to confirm.\n> ").await?;
                    }

                    Ok(targets) => {
                        let result = apply_session_action(
                            &home,
                            action,
                            targets,
                            &session,
                            host,
                        )
                        .await;
                        output.write_all(format!("{result}\n> ").as_bytes()).await?;
                    }

                    Err(()) => output.write_all(usage.as_bytes()).await?,
                }

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
                let active = workspace_root(&home, workspace.as_deref());
                output
                    .write_all(format!("Current workspace: {}\n> ", active.display()).as_bytes())
                    .await?;

                output.flush().await?;
                continue;
            }

            if let Some(value) = line.strip_prefix("/workspace ") {
                let value = value.trim();

                if value.is_empty() {
                    output.write_all(b"Workspace path cannot be empty.\n> ").await?;
                } else {
                    let selected = match resolve_workspace(value) {
                        Ok(selected) => selected,

                        Err(error) => {
                            output
                                .write_all(format!("Workspace was not changed: {error}.\n> ").as_bytes())
                                .await?;
                            output.flush().await?;
                            continue;
                        }
                    };

                    match host(
                        home.clone(),
                        "session.workspace".into(),
                        serde_json::json!({"id": session, "workspace": selected}),
                    )
                    .await
                    {
                        Ok(result) => {
                            workspace = result["workspace"].as_str().map(str::to_owned);
                            let active = workspace_root(&home, workspace.as_deref());
                            let message = if value == "reset" {
                                format!(
                                    "Workspace reset to the configured default: {}\n> ",
                                    active.display()
                                )
                            } else {
                                format!("Workspace changed to: {}\n> ", active.display())
                            };

                            output.write_all(message.as_bytes()).await?;
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

            if line == "/compact" {
                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::CompactionStarted);
                }

                let result = if !model_plugin_available {
                    Err(crabbot_core::Error::Denied(
                        "The selected model plugin is unavailable.".into(),
                    ))
                } else {
                    compact_command(&home, &session, &plugin, host).await
                };

                match result {
                    Ok((Some((removed, retained)), _, view)) => {
                        messages = view.messages.clone();
                        send_session_view(&session_events, view);
                        output
                            .write_all(
                                format!(
                                    "Compacted {removed} older messages into a summary; kept {retained} recent messages.\n> "
                                )
                                .as_bytes(),
                            )
                            .await?;
                    }

                    Ok((None, Some(reason), _)) if reason == "already_compacted" => {
                        output.write_all(b"This conversation was just compacted; skipping another compaction.\n> ").await?;
                    }

                    Ok((None, _, _)) => {
                        output.write_all(b"There are not enough earlier turns to compact.\n> ").await?;
                    }

                    Err(error) => {
                        output
                            .write_all(format!("Compaction failed: {}.\n> ", sentence(error.to_string())).as_bytes())
                            .await?;
                    }
                }

                output.flush().await?;

                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::CompactionFinished);
                }

                continue;
            }

            if line == "/model help" {
                output.write_all(model_help().as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/model" || line == "/model show" {
                output.write_all(format!("Current model: {model}.\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/model list" {
                let text = if !model_plugin_available {
                    "The selected model plugin is unavailable.".into()
                } else {
                    sequence = sequence.saturating_add(1);

                    match host(
                        home.clone(),
                        "model.list".into(),
                        serde_json::json!({"plugin": plugin}),
                    )
                    .await {
                        Ok(value) => value["text"].as_str().unwrap_or("The model plugin returned no model list.").to_owned(),
                        Err(error) => format!("Model list failed: {}", sentence(error.to_string())),
                    }
                };

                output.write_all(format!("{text}\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/model set" {
                output.write_all(b"Usage: /model set <id>. Use /model list to see Codex models.\n> ").await?;
                output.flush().await?;
                continue;
            }

            if let Some(value) = line
                .strip_prefix("/model set ")
                .or_else(|| line.strip_prefix("/model "))
            {
                let value = value.trim();

                if !valid_model(value) {
                    output.write_all(b"Model ID is invalid. Use letters, numbers, and . _ : / -.\n> ").await?;
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

            if line == "/session switch" || line == "/session create" {
                output.write_all(b"Use /session help to see session commands.\n> ").await?;
                output.flush().await?;
                continue;
            }

            if let Some(value) = line
                .strip_prefix("/session switch ")
                .or_else(|| {
                    line.strip_prefix("/session ")
                        .filter(|value| value.split_whitespace().count() == 1)
                })
            {
                let value = value.trim();

                if !valid_session(value) {
                    output.write_all(b"Session ID is invalid.\n> ").await?;
                } else {
                    let target = tui_session_id(value);

                    match read_session(&home, &target, host).await {
                        Ok(view) => {
                            session = target;
                            model = view.model.clone();
                            messages = view.messages.clone();
                            workspace = view.workspace.clone();
                            output
                                .write_all(format!("Using session {}.\n> ", tui_session_name(&session).unwrap_or(&session)).as_bytes())
                                .await?;
                            send_session_view(&session_events, view);
                        }

                        Err(error) => {
                            output
                                .write_all(
                                    format!("{}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }

                output.flush().await?;
                continue;
            }

            if line == "/session" || line == "/session switch" {
                output.write_all(b"Use /session help to see session commands.\n> ").await?;
                output.flush().await?;
                continue;
            }

            if let Some(value) = line
                .strip_prefix("/session create ")
                .or_else(|| line.strip_prefix("/new "))
            {
                let value = value.trim();

                if !valid_session(value) {
                    output.write_all(b"Session ID is invalid.\n> ").await?;
                } else {
                    match host(
                        home.clone(),
                        "session.new".into(),
                        serde_json::json!({"id": tui_session_id(value), "model": model}),
                    )
                    .await
                    {
                        Ok(_) => {
                            let session_id = tui_session_id(value);
                            let view = read_session(&home, &session_id, host).await?;
                            session = session_id;
                            model = view.model.clone();
                            messages = view.messages.clone();
                            workspace = view.workspace.clone();

                            output
                                .write_all(format!("Created session {}.\n> ", tui_session_name(&session).unwrap_or(&session)).as_bytes())
                                .await?;

                            send_session_view(&session_events, view);
                        }

                        Err(error) => {
                            output
                                .write_all(
                                    format!("{}.\n> ", sentence(error.to_string()))
                                        .as_bytes(),
                                )
                                .await?;
                        }
                    }
                }

                output.flush().await?;
                continue;
            }

            if line == "/new" {
                output.write_all(b"Use /session help to see session commands.\n> ").await?;
                output.flush().await?;
                continue;
            }

            if line.starts_with('/') {
                output
                    .write_all(format!("Unknown TUI command: {}. Use /help to see available commands.\n> ", line.split_whitespace().next().unwrap_or(line)).as_bytes())
                    .await?;

                output.flush().await?;
                continue;
            }

            if line.is_empty() {
                output.write_all(b"> ").await?;
                output.flush().await?;
                continue;
            }

            let terminal_command = line.strip_prefix('!');

            if terminal_command.is_some_and(|command| command.trim().is_empty()) {
                output.write_all(b"Usage: !<shell command>\n> ").await?;
                output.flush().await?;
                continue;
            }

            if model == "unset" && terminal_command.is_none() {
                output
                    .write_all(b"No model is selected. Use /model list, then /model set <id>.\n> ")
                    .await?;

                output.flush().await?;
                continue;
            }

            let reservation = host(
                home.clone(),
                "session.reserve".into(),
                serde_json::json!({"id": session}),
            )
            .await;

            let owner = match reservation {
                Ok(value) => value["owner"].as_str().map(str::to_owned),

                Err(error) => {
                    let message = if error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("already working")
                    {
                        "Crabbot is already working in this session. Wait for the current turn to finish."
                            .to_owned()
                    } else {
                        sentence(error.to_string())
                    };

                    if let Some(events) = &engine_events {
                        let _ = events.send(EngineEvent::SystemNotice(message));
                        output.write_all(b"> ").await?;
                    } else {
                        output.write_all(format!("{message}\n> ").as_bytes()).await?;
                    }

                    output.flush().await?;
                    continue;
                }
            };

            let Some(owner) = owner else {
                return Err(crabbot_core::Error::Denied(
                    "Session reservation returned no owner token.".into(),
                ));
            };

            let mut reservation =
                SessionReservation::new(home.clone(), session.clone(), owner, host);

            let current_session = match read_session_reserved(&home, &session, host).await {
                Ok(view) => view,

                Err(error) => {
                    let _ = reservation.release().await;
                    return Err(error);
                }
            };

            messages = current_session.messages;

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
                "session.append_reserved".into(),
                serde_json::json!({"id": session, "owner": reservation.owner, "message": user}),
            )
            .await
            {
                let _ = reservation.release().await;

                let message = format!("Turn was not saved: {}.", sentence(error.to_string()));

                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::SystemNotice(message));
                    output.write_all(b"> ").await?;
                } else {
                    output.write_all(format!("{message}\n> ").as_bytes()).await?;
                }

                output.flush().await?;
                continue;
            }

            messages.push(user);

            if let Some(events) = &engine_events {
                let _ = events.send(EngineEvent::GenerationStarted);
            }

            let (method, params) = if terminal_command.is_some() {
                (
                    "session.command",
                    serde_json::json!({"id": session, "owner": reservation.owner}),
                )
            } else {
                ("session.answer", serde_json::json!({"id": session, "plugin": plugin}))
            };

            let streaming_turn = streaming_host.is_some() && engine_events.is_some();
            let response: Pin<
                Box<dyn Future<Output = crabbot_core::Result<Value>> + Send>,
            > = match (streaming_host, engine_events.as_ref()) {
                (Some(streaming_host), Some(events)) => {
                    streaming_host(home.clone(), method.into(), params, events.clone())
                }

                _ => Box::pin(host(home.clone(), method.into(), params)),
            };

            tokio::pin!(response);
            let (response, interrupted) = loop {
                tokio::select! {
                    biased;
                    response = &mut response => break (response, false),

                    incoming = lines.next_line() => {
                        let Some(command) = incoming? else { break (Err(crabbot_core::Error::Denied("Terminal input closed during generation".into())), true) };

                        let command = decode_terminal_input(&command);
                        let command = command.trim();

                        if command == "/quit" || command == "/exit" {
                            let _ = host(home.clone(), "session.cancel".into(), serde_json::json!({"id": session})).await;
                            break (Err(crabbot_core::Error::Denied("Generation interrupted".into())), true);
                        }

                        if command == "/approvals" || command.starts_with("/approve") || command.starts_with("/deny") {
                            let message = resolve_turn_approval(
                                &home,
                                &session,
                                &reservation.owner,
                                command,
                                &mut sequence,
                                host,
                            )
                            .await?;

                            let reported = engine_events
                                .as_ref()
                                .is_some_and(|events| {
                                    events
                                        .send(EngineEvent::SystemNotice(
                                            message.trim_end().to_owned(),
                                        ))
                                        .is_ok()
                                });

                            if !reported {
                                output.write_all(message.as_bytes()).await?;
                                output.flush().await?;
                            }

                            continue;
                        }

                        output.write_all(b"A turn is still running. Use /approvals, /approve <id>, /deny <id>, or Esc to cancel.\n").await?;
                        output.flush().await?;
                    }

                    _ = interrupt_rx.recv(), if !interrupt_rx.is_closed() => {
                        let _ = host(
                            home.clone(),
                            "session.cancel".into(),
                            serde_json::json!({"id": session}),
                        ).await;
                        break (Err(crabbot_core::Error::Denied("Generation interrupted".into())), true);
                    }
                }
            };

            let _ = reservation.release().await;

            match response {
                Ok(value) => {
                    let reply = value["text"].as_str().unwrap_or_default();

                    if !reply.is_empty() {
                        output.write_all(reply.as_bytes()).await?;
                        output.write_all(b"\n").await?;
                    }
                }

                Err(error) => {
                    if interrupted {
                        output.write_all(b"Generation interrupted.\n").await?;
                    } else {
                        output
                            .write_all(format!("Request failed: {}.\n", sentence(error.to_string())).as_bytes())
                            .await?;
                    }
                }
            }

            if streaming_turn
                && let Some(events) = &engine_events
            {
                let _ = events.send(EngineEvent::AssistantFinished);
            }

            if let Some(events) = &engine_events {
                let _ = events.send(EngineEvent::GenerationFinished { interrupted });
            }

            output.write_all(b"> ").await?;
            output.flush().await?;
            continue;

        }

        Ok::<(), crabbot_core::Error>(())
    }
    .await;

    result?;
    Ok(())
}

fn workspace_root(home: &str, session_workspace: Option<&str>) -> PathBuf {
    session_workspace
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("CRABBOT_ROOT").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(home).join("workspace"))
}

fn resolve_workspace(value: &str) -> Result<Option<String>, &'static str> {
    let current = std::env::var_os("CRABBOT_TUI_LAUNCH_DIR").map(PathBuf::from).map_or_else(
        || std::env::current_dir().map_err(|_| "the current directory is unavailable"),
        Ok,
    )?;

    resolve_workspace_at(value, &current)
}

fn resolve_workspace_at(
    value: &str,
    current: &std::path::Path,
) -> Result<Option<String>, &'static str> {
    if value == "reset" {
        return Ok(None);
    }

    let path = PathBuf::from(value);
    let path = if path.is_absolute() { path } else { current.join(path) };

    let path = path.canonicalize().map_err(|_| "the directory is unavailable")?;

    if !path.is_dir() {
        return Err("the selected path is not a directory");
    }

    Ok(Some(path.to_string_lossy().into_owned()))
}

async fn ensure_session<C, F>(
    home: &str,
    id: &str,
    model: &str,
    host: C,
) -> crabbot_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    host(home.into(), "session.ensure".into(), serde_json::json!({"id": id, "model": model}))
        .await?;
    read_session_with_reservation(home, id, host, true).await
}

async fn read_session<C, F>(home: &str, id: &str, host: C) -> crabbot_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    read_session_with_reservation(home, id, host, false).await
}

async fn read_session_reserved<C, F>(
    home: &str,
    id: &str,
    host: C,
) -> crabbot_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    read_session_with_reservation(home, id, host, true).await
}

async fn read_session_with_reservation<C, F>(
    home: &str,
    id: &str,
    host: C,
    reserved: bool,
) -> crabbot_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let value = host(home.into(), "session.get".into(), serde_json::json!({"id": id})).await?;

    if !reserved && (value["status"] == "working" || value["inflight"] == true) {
        return Err(crabbot_core::Error::Denied("Session is already working.".into()));
    }

    let model = value["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("Session has no model configured.".into()))?
        .to_owned();

    let messages = serde_json::from_value(value["messages"].clone())?;
    let workspace = value["workspace"].as_str().map(str::to_owned);
    let context_usage = serde_json::from_value(value["context_usage"].clone()).ok().flatten();
    let working = value["status"] == "working" || value["inflight"] == true;
    Ok(ui::SessionView { id: id.to_owned(), model, workspace, context_usage, messages, working })
}

fn send_session_view(
    session_events: &Option<mpsc::UnboundedSender<ui::SessionView>>,
    view: ui::SessionView,
) {
    if let Some(session_events) = session_events {
        let _ = session_events.send(view);
    }
}

fn valid_session(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Default)]
struct SessionTargets {
    ids: Vec<String>,
    all: bool,
    confirmed: bool,
    deep: bool,
}

fn parse_session_targets(value: &str, allow_confirmation: bool) -> Result<SessionTargets, ()> {
    let mut targets = SessionTargets::default();

    for part in value.split_whitespace() {
        match part {
            "--all" if !targets.all => targets.all = true,

            "-y" | "--yes" if allow_confirmation && !targets.confirmed => {
                targets.confirmed = true;
            }

            "-y" | "--yes" => return Err(()),

            "--deep" if allow_confirmation && !targets.deep => targets.deep = true,

            "--deep" => return Err(()),

            _ if valid_session(part) && !targets.ids.iter().any(|id| id == part) => {
                targets.ids.push(part.to_owned());
            }

            _ => return Err(()),
        }
    }

    if (targets.all && !targets.ids.is_empty()) || (!targets.all && targets.ids.is_empty()) {
        return Err(());
    }

    Ok(targets)
}

async fn apply_session_action<C, F>(
    home: &str,
    action: &str,
    targets: SessionTargets,
    active: &str,
    host: C,
) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let mut ids = targets.ids.into_iter().map(|id| tui_session_id(&id)).collect::<Vec<_>>();

    if targets.all {
        let sessions =
            match host(home.to_owned(), "session.list".into(), serde_json::json!({})).await {
                Ok(value) => value,
                Err(error) => return sentence(error.to_string()),
            };

        let archived = action == "unarchive";

        ids = sessions["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| item["id"].as_str().is_some_and(|id| tui_session_name(id).is_some()))
            .filter(|item| {
                action == "delete" || item["archived"].as_bool().unwrap_or(false) == archived
            })
            .filter_map(|item| item["id"].as_str().map(str::to_owned))
            .filter(|id| valid_session(id))
            .collect();
    }

    if ids.is_empty() {
        return format!("No sessions to {action}.");
    }

    let mut completed = Vec::new();
    let mut already = Vec::new();
    let mut failures = Vec::new();
    let mut active_skipped = false;

    for id in ids {
        if id == active && matches!(action, "archive" | "delete") {
            active_skipped = true;
            continue;
        }

        let method = match action {
            "archive" => "session.archive",
            "unarchive" => "session.unarchive",
            _ => "session.delete",
        };

        let params = serde_json::json!({
            "id": id,
            "deep": action == "delete" && targets.deep,
        });

        match host(home.to_owned(), method.into(), params).await {
            Ok(result) if action == "delete" || result["changed"] != false => {
                let name = tui_session_name(&id).unwrap_or(&id).to_owned();

                if action == "delete"
                    && targets.deep
                    && let Err(error) = data::delete_fallback_session(home, &id)
                {
                    failures.push(format!(
                        "Could not remove local fallback for {name}: {}",
                        sentence(error.to_string())
                    ));
                }

                completed.push(name);
            }

            Ok(_) => already.push(tui_session_name(&id).unwrap_or(&id).to_owned()),
            Err(error) => failures.push(sentence(error.to_string())),
        }
    }

    if completed.len() + already.len() + failures.len() == 1 && !active_skipped {
        if let Some(id) = completed.first() {
            return if action == "delete" {
                format!("Deleted session {id}.")
            } else {
                format_session_archive(id, action == "archive", true)
            };
        }

        if let Some(id) = already.first() {
            return format_session_archive(id, action == "archive", false);
        }

        return failures.into_iter().next().unwrap_or_else(|| "Session action failed.".into());
    }

    let verb = match action {
        "archive" => "Archived",
        "unarchive" => "Unarchived",
        _ => "Deleted",
    };

    let no_op_verb = match action {
        "archive" => "archived",
        "unarchive" => "unarchived",
        _ => "deleted",
    };

    let mut summary = if completed.is_empty() {
        format!("No sessions were {no_op_verb}")
    } else {
        format!("{verb} {} sessions: {}", completed.len(), completed.join(", "))
    };

    if !already.is_empty() {
        summary.push_str(&format!(". Already in that state: {}", already.join(", ")));
    }

    if active_skipped {
        summary.push_str(&format!(
            ". Kept active session {}",
            tui_session_name(active).unwrap_or(active)
        ));
    }

    if !failures.is_empty() {
        summary.push_str(&format!(". Failed: {}", failures.join("; ")));
    }

    summary.push('.');
    summary
}

fn valid_model(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

#[cfg(test)]
fn invalid_model_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();

    ["invalid model", "unknown model", "model not found"]
        .iter()
        .any(|phrase| message.contains(phrase))
}

fn plugin_binary_path(home: &str, id: &str) -> std::path::PathBuf {
    std::path::Path::new(home)
        .join("plugins")
        .join(id)
        .join("bin")
        .join(crabbot_core::plugin::binary_name(id))
}

#[cfg(test)]
fn read_only_tools() -> Vec<ToolSpec> {
    [
        (
            "read",
            "Read a text file inside the active workspace.",
            serde_json::json!({"path": {"type": "string"}}),
            vec!["path"],
        ),
        (
            "list",
            "List entries in a workspace directory.",
            serde_json::json!({"path": {"type": "string"}}),
            Vec::new(),
        ),
        (
            "search",
            "Search workspace text for a phrase.",
            serde_json::json!({
                "path": {"type": "string"},
                "text": {"type": "string"}
            }),
            vec!["text"],
        ),
        (
            "fetch",
            "Fetch bounded text from a public HTTPS URL.",
            serde_json::json!({"url": {"type": "string"}}),
            vec!["url"],
        ),
    ]
    .into_iter()
    .map(|(name, description, properties, required)| ToolSpec {
        name: name.into(),
        description: Some(description.into()),
        schema: serde_json::json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        }),
    })
    .collect()
}

#[cfg(test)]
async fn serve_read_only_tool(
    tools: Arc<Mutex<Option<ToolsProcess>>>,
    request: Request,
    workspace: Option<String>,
) -> crabbot_core::Result<Response> {
    let Request::Call { id, method, params, .. } = request else {
        return Err(crabbot_core::Error::Protocol("Tool requests require an ID.".into()));
    };

    if method != "host/tool" {
        return Ok(Response::fail(id, -32601, "Method not found."));
    }

    let Some(name) = params["name"]
        .as_str()
        .filter(|name| matches!(*name, "read" | "list" | "search" | "fetch"))
    else {
        return Ok(Response::fail(id, -32000, "Only read-only tools are available in the TUI."));
    };

    let mut arguments = params.get("args").cloned().unwrap_or_else(|| serde_json::json!({}));
    let Some(arguments) = arguments.as_object_mut() else {
        return Ok(Response::fail(id, -32602, "Tool arguments must be an object."));
    };

    if let Some(workspace) = workspace {
        arguments.insert("workspace".into(), Value::String(workspace));
    }

    let mut tools = tools.lock().await;
    let Some(tools) = tools.as_mut() else {
        return Ok(Response::fail(id, -32000, "The tools plugin is unavailable."));
    };

    let response =
        tools.process.call(Request::call(id, name, Value::Object(arguments.clone()))).await?;

    if let Some(error) = response.error {
        return Ok(Response::fail(id, error.code, error.message));
    }

    let result = response.result.unwrap_or(Value::Null);
    let text = match name {
        "read" => result["text"].as_str().unwrap_or_default().to_owned(),
        "list" => result["items"]
            .as_array()
            .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("\n"))
            .unwrap_or_default(),
        "search" => serde_json::to_string_pretty(&result["hits"]).unwrap_or_else(|_| "[]".into()),

        "fetch" => {
            let status = result["status"].as_u64().unwrap_or_default();
            let content_type = result["content_type"].as_str().unwrap_or("unknown");
            let text = result["text"].as_str().unwrap_or_default();
            let truncated = result["truncated"].as_bool().unwrap_or(false);
            let suffix = if truncated { "\n[response truncated at the text limit]" } else { "" };

            format!("HTTP {status} ({content_type})\n{text}{suffix}")
        }

        _ => unreachable!(),
    };

    Ok(Response::ok(id, serde_json::json!({"text": text})))
}

fn has_capability(home: &str, capability: &str) -> bool {
    let plugins = std::path::Path::new(home).join("plugins");

    std::fs::read_dir(plugins).ok().into_iter().flatten().flatten().any(|entry| {
        let Ok(file_type) = entry.file_type() else {
            return false;
        };

        if !file_type.is_dir() {
            return false;
        }

        let id = entry.file_name().to_string_lossy().into_owned();
        let directory = entry.path();

        if !plugin_binary_path(home, &id).is_file() {
            return false;
        }

        std::fs::read_to_string(directory.join("crabbot-plugin.toml"))
            .ok()
            .and_then(|manifest| toml::from_str::<Value>(&manifest).ok())
            .and_then(|value| value["capabilities"].as_array().cloned())
            .is_some_and(|capabilities| {
                capabilities.iter().any(|value| value.as_str() == Some(capability))
            })
    })
}

fn command_help(home: &str, has_model: bool, daemon: bool) -> String {
    let commands = [
        ("/help", "Show commands available in this session."),
        ("/status", "Show whether the background runtime is running."),
        ("/plugins [page]", "Browse installed plugins."),
        ("/session help", "Show session commands."),
        ("/new <id>", "Create and switch to a session."),
        ("/workspace [path|reset]", "Show or change this session's filesystem root."),
        ("/clear", "Clear this session's conversation."),
        ("/statusline [reset]", "Choose which details appear in the bottom statusline."),
        ("/animation [on|off]", "Show or configure typewriter animation."),
        ("/quit, /exit", "Leave the TUI."),
    ];

    let mut command_rows = commands.to_vec();

    if daemon {
        command_rows.push(("!<command>", "Run a shell command directly in this session."));
    }

    let mut sections = vec![format!("Commands:\n{}", format_help_rows(&command_rows))];
    let mut conditional = Vec::new();

    if has_model {
        conditional.push(("/model <help|list|show|set>", "Manage the selected model."));
        conditional.push(("/compact", "Summarize older turns and keep recent conversation."));
    }

    if daemon && has_capability(home, "tool") {
        conditional.extend([
            ("/approval", "Show approval policy."),
            ("/approvals", "List pending tool approvals."),
            ("/approve <id>", "Approve a pending tool action."),
            ("/deny <id>", "Deny a pending tool action."),
        ]);
    }

    if daemon && has_capability(home, "channel") {
        conditional.extend([
            ("/deliveries", "List pending channel deliveries."),
            ("/retry <id>", "Retry a delivery."),
            ("/drop <id>", "Drop a delivery."),
        ]);
    }

    if daemon && has_capability(home, "timer") {
        conditional.push(("/timer <list|add|remove>", "Manage timers."));
    }

    if daemon && has_capability(home, "memory") {
        conditional.push(("/memory <list|remember|forget>", "Manage memories."));
    }

    if !conditional.is_empty() {
        sections.push(format!(
            "Conditional commands (shown only when usable):\n{}",
            format_help_rows(&conditional)
        ));
    }

    sections.push(
        "Plugin commands run in the CLI: crab <command>.\nPage Up/Down scroll the conversation; the mouse wheel scrolls the pane under the pointer. Hold Shift while dragging to select and copy text.\nCtrl+O adds a message line.".into(),
    );

    format!("{}\n> ", sections.join("\n\n"))
}

fn format_help_rows(rows: &[(&str, &str)]) -> String {
    let command_width = rows.iter().map(|(command, _)| command.len()).max().unwrap_or_default();

    rows.iter()
        .map(|(command, description)| format!("  {command:<command_width$}  {description}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn session_help() -> String {
    let commands = [
        ("/session help", "Show these commands."),
        ("/session list [page]", "List sessions, 10 per page; active first, then recent."),
        ("/session create <id>", "Create and switch to a session."),
        ("/session switch <id>", "Switch to a saved session."),
        ("/session rename <new-id>", "Rename the active session."),
        ("/session archive <id>...|--all", "Archive saved sessions."),
        ("/session unarchive <id>...|--all", "Restore archived sessions."),
        (
            "/session delete <id>...|--all [-y] [--deep]",
            "Permanently delete sessions; --deep also removes local and shared session data.",
        ),
        ("/new <id>", "Create and switch to a session (shortcut)."),
    ];

    format!("Session commands:\n{}\n> ", format_help_rows(&commands))
}

fn model_help() -> &'static str {
    "Model commands:\n  /model help          Show these commands.\n  /model list          List Codex models (when Codex is selected).\n  /model show          Show the selected model.\n  /model set <id>      Select a model.\n  /model <id>          Select a model (shortcut).\n> "
}

async fn compact_command<C, F>(
    home: &str,
    session: &str,
    plugin: &str,
    host: C,
) -> crabbot_core::Result<(Option<(usize, usize)>, Option<String>, ui::SessionView)>
where
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = crabbot_core::Result<Value>> + Send + 'static,
{
    let reservation =
        host(home.to_owned(), "session.reserve".into(), serde_json::json!({"id": session})).await?;

    let owner = reservation["owner"].as_str().map(str::to_owned).ok_or_else(|| {
        crabbot_core::Error::Denied("Session reservation returned no owner token.".into())
    })?;

    let mut reservation =
        SessionReservation::new(home.to_owned(), session.to_owned(), owner.clone(), host);

    let response = host(
        home.to_owned(),
        "session.compact".into(),
        serde_json::json!({"id": session, "plugin": plugin, "owner": owner}),
    )
    .await;

    let released = reservation.release().await;

    let value = match response {
        Ok(value) => {
            released?;

            value
        }

        Err(error) => {
            let _ = released;

            return Err(error);
        }
    };

    let view = read_session(home, session, host).await?;
    let compacted = value["compacted"].as_bool().unwrap_or(false).then(|| {
        (
            value["removed"].as_u64().unwrap_or_default() as usize,
            value["retained"].as_u64().unwrap_or_default() as usize,
        )
    });

    let reason = value["reason"].as_str().map(str::to_owned);

    Ok((compacted, reason, view))
}

#[cfg(test)]
async fn local_aware(home: String, method: String, params: Value) -> crabbot_core::Result<Value> {
    if method == "session.answer" {
        return Ok(serde_json::json!({"text": "Reply"}));
    }

    if method == "session.compact" {
        return Ok(serde_json::json!({"compacted": false}));
    }

    if method == "model.list" {
        return Ok(
            serde_json::json!({"text": "Codex models (1):\n  gpt-test - Test model (default)"}),
        );
    }

    if method == "plugin.list" {
        return Ok(serde_json::json!({"items": [{
            "id": "codex", "health": "ready", "capabilities": ["model"]
        }]}));
    }

    match control(&home, &method, params.clone()).await {
        Ok(value) => Ok(value),

        Err(error) if method.starts_with("session.") && error.is_unavailable() => {
            offline::control(&home, &method, params).await
        }

        Err(error) => Err(error.into_core()),
    }
}

async fn require_daemon(home: &str) -> crabbot_core::Result<()> {
    control(home, "status", serde_json::json!({})).await.map(|_| ()).map_err(|error| match error {
        ControlError::Unavailable(_) => crabbot_core::Error::Denied(
            concat!(
                "The Crabbot daemon is not running.\n",
                "Start it with `crab service start` or run `crabbot-daemon`."
            )
            .into(),
        ),
        ControlError::Failed(error) => error,
    })
}

fn decode_terminal_input(line: &str) -> String {
    serde_json::from_str::<String>(line).unwrap_or_else(|_| line.to_owned())
}

async fn resolve_turn_approval<C, F>(
    home: &str,
    session: &str,
    owner: &str,
    command: &str,
    sequence: &mut u64,
    host: C,
) -> crabbot_core::Result<String>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    if command == "/approvals" {
        let value = host(home.to_owned(), "approval.list".into(), serde_json::json!({})).await?;
        return Ok(format_approvals(&value));
    }

    let (approved, supplied_id) = if command == "/approve" {
        (true, "")
    } else if command == "/deny" {
        (false, "")
    } else if let Some(id) = command.strip_prefix("/approve ") {
        (true, id.trim())
    } else if let Some(id) = command.strip_prefix("/deny ") {
        (false, id.trim())
    } else {
        return Ok("Usage: /approve <id> or /deny <id>.\n".into());
    };

    let list = host(home.to_owned(), "approval.list".into(), serde_json::json!({})).await?;
    let Some(items) = list["items"].as_array() else {
        return Ok("Pending approvals: none.\n".into());
    };

    let candidates = items
        .iter()
        .filter(|item| {
            if supplied_id.is_empty() {
                item["target"]["session"] == session
            } else {
                item["id"].as_str() == Some(supplied_id)
            }
        })
        .collect::<Vec<_>>();

    let Some(item) = (candidates.len() == 1).then(|| candidates[0]) else {
        return Ok(if candidates.is_empty() {
            "No matching pending approval. Use /approvals to see the current requests.\n".into()
        } else {
            "More than one approval is pending for this session; use /approve <id> or /deny <id>.\n"
                .into()
        });
    };

    let id = item["id"].as_str().unwrap_or_default();
    let result = host(
        home.to_owned(),
        "approval.resolve".into(),
        serde_json::json!({"id": id, "approved": approved}),
    )
    .await;
    let reply = match result {
        Ok(_) if approved => "Approval accepted.".to_owned(),
        Ok(_) => "Approval denied.".to_owned(),
        Err(error) => sentence(error.to_string()),
    };

    *sequence = sequence.saturating_add(1);
    let user = Message {
        id: message_id("user", *sequence),
        session: session.into(),
        role: Role::User,
        sender: Some("tui".into()),
        content: vec![Content::Text { text: command.into() }],
    };

    host(
        home.to_owned(),
        "session.append_reserved".into(),
        serde_json::json!({"id": session, "owner": owner, "message": user}),
    )
    .await?;

    *sequence = sequence.saturating_add(1);
    let assistant = Message {
        id: message_id("system-assistant", *sequence),
        session: session.into(),
        role: Role::Assistant,
        sender: None,
        content: vec![Content::Text { text: reply.clone() }],
    };

    host(
        home.to_owned(),
        "session.append_reserved".into(),
        serde_json::json!({"id": session, "owner": owner, "message": assistant}),
    )
    .await?;

    Ok(format!("{reply}\n"))
}

async fn daemon_control(
    home: String,
    method: String,
    params: Value,
) -> crabbot_core::Result<Value> {
    control(&home, &method, params).await.map_err(ControlError::into_core)
}

enum ControlError {
    Unavailable(crabbot_core::Error),
    Failed(crabbot_core::Error),
}

impl From<crabbot_core::Error> for ControlError {
    fn from(error: crabbot_core::Error) -> Self {
        Self::Failed(error)
    }
}

impl ControlError {
    #[cfg(test)]
    fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }

    fn into_core(self) -> crabbot_core::Error {
        match self {
            Self::Unavailable(error) | Self::Failed(error) => error,
        }
    }
}

fn unavailable_before_delivery(error: std::io::Error) -> ControlError {
    ControlError::Unavailable(error.into())
}

fn daemon_unavailable(error: &crabbot_core::Error) -> bool {
    match error {
        crabbot_core::Error::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::BrokenPipe
        ),
        crabbot_core::Error::Denied(message) => message == "The daemon did not respond in time.",
        _ => false,
    }
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

async fn timer_command<C, F>(home: String, command: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let mut parts = command.splitn(3, ' ');
    let action = parts.next().unwrap_or_default();
    let result = match action {
        "list" if parts.next().is_none() => {
            capability(home, "timer", "list", serde_json::json!({}), host)
                .await
                .map(|value| format_timers(&value))
        }

        "add" => match (parts.next(), parts.next()) {
            (Some(delay), Some(text)) => match delay.parse::<u64>() {
                Ok(delay)
                    if (1..=MAX_TIMER_DELAY_SECONDS).contains(&delay)
                        && !text.trim().is_empty() =>
                {
                    let id = timer_id();
                    capability(
                        home,
                        "timer",
                        "add",
                        serde_json::json!({"id": id, "delay": delay, "text": text.trim()}),
                        host,
                    )
                    .await
                    .map(|_| format!("Timer {id} was scheduled.\n> "))
                }

                _ => Ok(format!(
                    "Timer delay must be between 1 and {MAX_TIMER_DELAY_SECONDS} seconds, with a reminder text.\n> "
                )),
            },

            _ => Ok("Usage: /timer add <seconds> <text>.\n> ".into()),
        },

        "remove" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
            Some(id) if parts.next().is_none() => {
                capability(home, "timer", "remove", serde_json::json!({"id": id}), host).await.map(
                    |value| {
                        if value["deleted"] == true {
                            format!("Timer {id} was removed.\n> ")
                        } else {
                            format!("Timer {id} was not found.\n> ")
                        }
                    },
                )
            }

            _ => Ok("Usage: /timer remove <id>.\n> ".into()),
        },

        _ => Ok("Usage: /timer <list|add|remove>.\n> ".into()),
    };

    result.unwrap_or_else(|error| {
        format!("Timer action failed: {}.\n> ", sentence(error.to_string()))
    })
}

async fn memory_command<C, F>(home: String, session: String, command: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    let result = if command == "list" {
        capability(home, "memory", "list", serde_json::json!({"scope": session}), host)
            .await
            .map(|value| format_memories(&value))
    } else if let Some(value) = command.strip_prefix("remember ") {
        match value.split_once('=') {
            Some((key, value)) if !key.trim().is_empty() && !value.trim().is_empty() => capability(
                home,
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
            .map(|_| "Memory saved for this session.\n> ".into()),

            _ => Ok("Usage: /memory remember <key>=<value>.\n> ".into()),
        }
    } else if let Some(key) = command.strip_prefix("forget ") {
        if key.trim().is_empty() {
            Ok("Usage: /memory forget <key>.\n> ".into())
        } else {
            capability(
                home,
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

    result.unwrap_or_else(|error| {
        format!("Memory action failed: {}.\n> ", sentence(error.to_string()))
    })
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

#[cfg(test)]
fn saveable_reply(text: &str) -> (String, bool) {
    if text.len() <= SAVED_REPLY_LIMIT {
        return (text.to_owned(), false);
    }

    let mut end = SAVED_REPLY_LIMIT;

    while !text.is_char_boundary(end) {
        end -= 1;
    }

    (text[..end].to_owned(), true)
}

#[cfg(test)]
fn stream_text(request: Request) -> Option<String> {
    let Request::Note { method, params, .. } = request else {
        return None;
    };

    if method != "event" || params["event"]["kind"] != "text" {
        return None;
    }

    params["event"]["text"].as_str().map(str::to_owned)
}

fn final_reply_suffix<'a>(streamed: &str, final_text: &'a str) -> &'a str {
    if streamed.is_empty() {
        final_text
    } else {
        final_text.strip_prefix(streamed).unwrap_or_default()
    }
}

fn terminal(mode: &str) -> crabbot_core::Result<std::fs::File> {
    let path = match (cfg!(windows), mode) {
        (true, "r") => "CONIN$",
        (true, _) => "CONOUT$",
        (false, _) => "/dev/tty",
    };

    let mut options = std::fs::OpenOptions::new();

    if mode == "r" {
        options.read(true);
    } else {
        options.write(true);
    }

    options.open(path).map_err(crabbot_core::Error::from)
}

async fn control(
    home: &str,
    method: &str,
    params: Value,
) -> std::result::Result<Value, ControlError> {
    control_with_stream(home, method, params, None).await
}

async fn control_with_stream(
    home: &str,
    method: &str,
    params: Value,
    events: Option<mpsc::UnboundedSender<EngineEvent>>,
) -> std::result::Result<Value, ControlError> {
    let token = tokio::fs::read_to_string(std::path::Path::new(home).join("ipc.token"))
        .await
        .map_err(unavailable_before_delivery)?;
    let port = tokio::fs::read_to_string(std::path::Path::new(home).join("ipc.port"))
        .await
        .map_err(unavailable_before_delivery)?
        .trim()
        .parse::<u16>()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("The daemon port is invalid: {error}."))
        })?;

    let streaming_turn = matches!(method, "session.answer" | "session.command");

    let exchange = async {
        let stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|error| ControlError::Unavailable(error.into()))?;

        let (input, output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut output = output;
        jsonl::write(&mut output, &IpcRequest::call(1, token.trim(), method, params))
            .await
            .map_err(|error| {
                ControlError::Failed(crabbot_core::Error::Denied(error.to_string()))
            })?;

        let system_output = method == "session.command";
        let mut streamed = String::new();

        loop {
            let response: IpcResponse = jsonl::read(&mut input, jsonl::MAX)
                .await
                .map_err(ControlError::Failed)?
                .ok_or_else(|| {
                    ControlError::Failed(crabbot_core::Error::Denied(
                        "The daemon closed the IPC connection.".into(),
                    ))
                })?;

            if !response.valid() || response.id != 1 {
                return Err(ControlError::Failed(crabbot_core::Error::Denied(
                    "The daemon returned an invalid response.".into(),
                )));
            }

            if let Some(error) = response.error {
                return Err(ControlError::Failed(crabbot_core::Error::Denied(error.message)));
            }

            let Some(value) = response.result else {
                return Err(ControlError::Failed(crabbot_core::Error::Denied(
                    "The daemon returned an empty response.".into(),
                )));
            };

            if streaming_turn {
                if value["event"] == "approval" {
                    let id = value["id"].as_str().unwrap_or("unknown");
                    let text = value["text"].as_str().unwrap_or("a tool action");
                    let tool = value["tool"].as_str().unwrap_or("tool");
                    let arguments = value["arguments"].as_str().unwrap_or("{}");
                    let command = value["command"].as_str();

                    if let Some(events) = &events {
                        let _ = events.send(EngineEvent::SystemNotice(format_approval_notice(
                            id, tool, arguments, command, text,
                        )));
                    }

                    continue;
                }

                if value["event"] == "system" {
                    if let (Some(events), Some(text)) = (&events, value["text"].as_str()) {
                        let _ = events.send(EngineEvent::SystemText(text.to_owned()));
                    }

                    continue;
                }

                if value["event"] == "text" {
                    if let Some(text) = value["text"].as_str() {
                        streamed.push_str(text);

                        if !text.is_empty()
                            && let Some(events) = &events
                        {
                            let event = if system_output {
                                EngineEvent::SystemText(text.to_owned())
                            } else {
                                EngineEvent::AssistantText(text.to_owned())
                            };

                            let _ = events.send(event);
                        }
                    }

                    continue;
                }

                if value["done"] == true {
                    let final_text = value["text"].as_str().unwrap_or(&streamed);

                    if let Some(events) = &events {
                        let suffix = final_reply_suffix(&streamed, final_text);

                        if !suffix.is_empty() {
                            let event = if system_output {
                                EngineEvent::SystemText(suffix.to_owned())
                            } else {
                                EngineEvent::AssistantText(suffix.to_owned())
                            };

                            let _ = events.send(event);
                        }

                        return Ok(serde_json::json!({"text": ""}));
                    }

                    return Ok(serde_json::json!({"text": final_text}));
                }

                continue;
            }

            return Ok(value);
        }
    };

    let wait = if streaming_turn {
        STREAMING_TURN_TIMEOUT
    } else if method == "session.compact" {
        COMPACTION_TIMEOUT
    } else {
        CONTROL_TIMEOUT
    };

    timeout(wait, exchange).await.map_err(|_| {
        ControlError::Failed(crabbot_core::Error::Denied(
            "The daemon did not respond in time.".into(),
        ))
    })?
}

fn daemon_streaming_control(
    home: String,
    method: String,
    params: Value,
    events: mpsc::UnboundedSender<EngineEvent>,
) -> Pin<Box<dyn Future<Output = crabbot_core::Result<Value>> + Send>> {
    Box::pin(async move {
        control_with_stream(&home, &method, params, Some(events))
            .await
            .map_err(ControlError::into_core)
    })
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
        Err(error) => format!("{}.", sentence(error.to_string())),
    }
}

fn format_approval_notice(
    id: &str,
    tool: &str,
    arguments: &str,
    command: Option<&str>,
    details: &str,
) -> String {
    let mut message = String::from("Approval needed.");
    message.push_str(&format!("\nTool: {tool}"));

    if tool == "shell" {
        message.push_str("\nCommand: ");
        message.push_str(command.unwrap_or("(command unavailable)"));
    } else {
        message.push_str(&format!("\nArguments: {arguments}"));
        message.push_str("\nAction: ");

        let action = details
            .trim()
            .strip_prefix("Crabbot requests approval to ")
            .unwrap_or(details.trim())
            .trim_end_matches('.');

        message.push_str(action);
    }

    message.push_str(&format!("\nApprove: /approve {id}\nDeny: /deny {id}"));
    message
}

fn format_status(value: &Value) -> String {
    let running = value["running"].as_bool().unwrap_or(false);
    let sessions = value["sessions"].as_u64().unwrap_or(0);
    format!(
        "Background runtime: {}. Sessions: {sessions}.\n> ",
        if running { "running" } else { "stopped" }
    )
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
        Err(error) => format!("{}.", sentence(error.to_string())),
    }
}

fn parse_session_list_page(command: &str) -> Result<usize, ()> {
    let mut parts = command.split_whitespace();

    if parts.next() != Some("/session") || parts.next() != Some("list") {
        return Err(());
    }

    let page = match parts.next() {
        Some(value) => value.parse::<usize>().ok().filter(|page| *page > 0).ok_or(())?,
        None => 1,
    };

    if parts.next().is_some() {
        return Err(());
    }

    Ok(page)
}

fn parse_plugin_list_page(command: &str) -> Result<usize, ()> {
    let mut parts = command.split_whitespace();

    if parts.next() != Some("/plugins") {
        return Err(());
    }

    let page = match parts.next() {
        Some(value) => value.parse::<usize>().ok().filter(|page| *page > 0).ok_or(())?,
        None => 1,
    };

    if parts.next().is_some() {
        return Err(());
    }

    Ok(page)
}

fn format_sessions(
    value: &Value,
    active_session: &str,
    model_available: bool,
    page: usize,
) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Sessions: none.\n> ".into();
    };

    let mut items = items.iter().filter(|item| item["id"].as_str().is_some()).collect::<Vec<_>>();

    items.retain(|item| item["id"].as_str().is_some_and(|id| tui_session_name(id).is_some()));

    if items.is_empty() {
        return "Sessions: none.\n> ".into();
    }

    items.sort_by(|left, right| {
        SessionState::from_item(left, active_session)
            .priority()
            .cmp(&SessionState::from_item(right, active_session).priority())
            .then_with(|| {
                right["updated"]
                    .as_u64()
                    .unwrap_or_default()
                    .cmp(&left["updated"].as_u64().unwrap_or_default())
            })
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });

    let total = items.len();
    let pages = total.div_ceil(SESSION_LIST_PAGE_SIZE);

    if page > pages {
        return format!("Page {page} is out of range; sessions have {pages} pages.\n> ");
    }

    let start = (page - 1) * SESSION_LIST_PAGE_SIZE;
    let end = (start + SESSION_LIST_PAGE_SIZE).min(total);
    let sessions = items[start..end]
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let id = item["id"].as_str()?;
            let state = SessionState::from_item(item, active_session);
            let status = state.label();

            let model = if model_available {
                item["model"].as_str().filter(|model| !model.is_empty()).unwrap_or("unset")
            } else {
                "unset"
            };

            let mut details = vec![format!("model: {model}")];

            if let Some(created) = item["created"].as_u64().and_then(date::format_date) {
                details.push(format!("created: {created}"));
            }

            if let Some(updated) = item["updated"].as_u64().and_then(date::format_date) {
                details.push(format!("updated: {updated}"));
            }

            if let Some(messages) = item["messages"].as_u64() {
                details.push(format!("messages: {messages}"));
            }

            Some((index, format!("{} | {status}", tui_session_name(id).unwrap_or(id)), details))
        })
        .collect::<Vec<_>>();

    let session_count = sessions.len();
    let sessions = sessions
        .into_iter()
        .map(|(index, label, details)| {
            format_tree_entry(&label, &details, index + 1 == session_count)
        })
        .collect::<Vec<_>>();

    if sessions.is_empty() {
        "Sessions: none.\n> ".into()
    } else {
        format!(
            "Sessions | {}–{} of {total} | page {page}/{pages}:\n{}\n> ",
            start + 1,
            end,
            sessions.join("\n\n")
        )
    }
}

struct SessionState<'a> {
    status: &'a str,
    working: bool,
    active: bool,
    archived: bool,
}

impl<'a> SessionState<'a> {
    fn from_item(item: &'a Value, active_session: &str) -> Self {
        Self {
            status: item["status"].as_str().unwrap_or("unknown"),
            working: item["status"] == "working" || item["inflight"] == true,
            active: item["id"] == active_session,
            archived: item["archived"] == true,
        }
    }

    fn label(&self) -> &'a str {
        match (self.archived, self.working, self.active) {
            (true, _, _) => "archived",
            (false, true, _) => "working",
            (false, false, true) => "active",
            (false, false, false) => self.status,
        }
    }

    fn priority(&self) -> u8 {
        match (self.working, self.active, self.archived) {
            (true, _, _) => 0,
            (false, true, _) => 1,
            (false, false, true) => 3,
            (false, false, false) => 2,
        }
    }
}

fn format_session_archive(id: &str, archived: bool, changed: bool) -> String {
    if !changed {
        let state = if archived { "archived" } else { "unarchived" };

        return format!("Session {id} is already {state}.");
    }

    let action = if archived { "Archived" } else { "Unarchived" };

    format!("{action} session {id}.")
}

fn format_plugins(value: &Value, page: usize) -> String {
    let Some(items) = value["items"].as_array() else {
        return "No plugins installed.".into();
    };

    if items.is_empty() {
        return "No plugins installed.".into();
    }

    let mut items = items.iter().collect::<Vec<_>>();
    items.sort_by_key(|item| item["id"].as_str().unwrap_or("unknown"));

    let total = items.len();
    let pages = total.div_ceil(PLUGIN_LIST_PAGE_SIZE);

    if page == 0 || page > pages {
        return format!("Page {page} is out of range; plugins have {pages} pages.");
    }

    let start = (page - 1) * PLUGIN_LIST_PAGE_SIZE;
    let end = (start + PLUGIN_LIST_PAGE_SIZE).min(total);
    let mut output = format!("Plugins | {}–{} of {total} | page {page}/{pages}:", start + 1, end);

    for (index, item) in items[start..end].iter().enumerate() {
        let id = item["id"].as_str().unwrap_or("unknown");
        let version = item["version"].as_str().unwrap_or("unknown");
        let health = item["health"].as_str().unwrap_or("unknown");

        let protocol = format!(
            "{}.{}",
            item["protocol"]["major"].as_u64().unwrap_or_default(),
            item["protocol"]["minor"].as_u64().unwrap_or_default()
        );

        let capabilities = format_string_list(&item["capabilities"]);
        let permissions = format_string_list(&item["permissions"]);
        let commands = item["commands"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|command| command["name"].as_str())
            .collect::<Vec<_>>();

        let commands = if commands.is_empty() { "none".into() } else { commands.join(", ") };

        let separator = if index == 0 { "\n" } else { "\n\n" };

        let metadata = [
            format!("health: {health}"),
            format!("capabilities: {capabilities}"),
            format!("protocol: {protocol}"),
            format!("commands: {commands}"),
            format!("permissions: {permissions}"),
        ];

        output.push_str(&format!(
            "{separator}{}",
            format_tree_entry(&format!("{id} | v{version}"), &metadata, index + 1 == end - start)
        ));
    }

    output
}

fn format_tree_entry(label: &str, metadata: &[String], is_last: bool) -> String {
    let root_branch = if is_last { "└─" } else { "├─" };

    let child_prefix = if is_last { "   " } else { "│  " };

    let mut lines = vec![format!("{root_branch} {label}")];

    lines.extend(metadata.iter().enumerate().map(|(index, detail)| {
        let branch = if index + 1 == metadata.len() { "└─" } else { "├─" };

        format!("{child_prefix}{branch} {detail}")
    }));

    lines.join("\n")
}

fn format_string_list(value: &Value) -> String {
    let values =
        value.as_array().into_iter().flatten().filter_map(Value::as_str).collect::<Vec<_>>();

    if values.is_empty() { "none".into() } else { values.join(", ") }
}

fn local_plugin_list(home: &str) -> Value {
    let root = std::path::PathBuf::from(home).join("plugins");
    let mut items = std::fs::read_dir(root)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;

            if !file_type.is_dir() {
                return None;
            }

            let id = entry.file_name().into_string().ok()?;

            if id.is_empty()
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            {
                return None;
            }

            let manifest_path = entry.path().join("crabbot-plugin.toml");
            let manifest = crabbot_file::load(&manifest_path, 64 * 1024)
                .ok()
                .flatten()
                .and_then(|bytes| toml::from_str::<Value>(&String::from_utf8_lossy(&bytes)).ok())
                .unwrap_or(Value::Null);

            let health = if plugin_binary_path(home, &id).is_file() { "ready" } else { "missing" };

            Some(serde_json::json!({
                "id": id,
                "version": manifest["version"],
                "protocol": manifest["protocol"],
                "capabilities": manifest["capabilities"],
                "commands": manifest["commands"],
                "permissions": manifest["permissions"],
                "health": health,
            }))
        })
        .collect::<Vec<_>>();

    items.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));

    serde_json::json!({"items": items})
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
    let mut value = value.trim();

    while let Some(message) = value.strip_prefix("Denied: ") {
        value = message.trim();
    }

    let value = value.trim_end_matches('.');
    let value = value.replace("Session was not found", "Session not found");

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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn persists_message_timestamps_as_utc_epoch_milliseconds() {
        let before =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();

        let id = super::message_id("user", 4);
        let after =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();

        let timestamp = id.split('-').nth(2).unwrap().parse::<u128>().unwrap();

        assert!(timestamp >= before && timestamp <= after);
        assert!(id.ends_with("-4"));
    }

    #[test]
    fn describes_client_capability() {
        let hello = super::hello();

        assert_eq!(hello.id, "tui");
        assert_eq!(hello.capabilities, vec![crabbot_core::types::Capability::Client]);
        assert_eq!(hello.commands[0].name, "tui");
        assert!(hello.commands[0].description.contains("daemon must be running"));
    }

    #[test]
    fn bounds_saved_replies_without_splitting_utf8() {
        let text = format!("{}🦀tail", "a".repeat(super::SAVED_REPLY_LIMIT - 1));
        let (saved, truncated) = super::saveable_reply(&text);

        assert!(truncated);
        assert_eq!(saved.len(), super::SAVED_REPLY_LIMIT - 1);
        assert!(saved.is_char_boundary(saved.len()));
        assert_eq!(super::saveable_reply("short"), ("short".into(), false));
    }

    #[test]
    fn parses_one_shot_and_session_options() {
        let args = [
            "--once".into(),
            "hello there".into(),
            "--session".into(),
            "work-1".into(),
            "--model".into(),
            "small".into(),
            "--plugin".into(),
            "ollama".into(),
        ];

        let options = super::parse_options(&args).unwrap();

        assert_eq!(options.once.as_deref(), Some("hello there"));
        assert_eq!(options.session.as_deref(), Some("work-1"));
        assert_eq!(options.model.as_deref(), Some("small"));
        assert_eq!(options.plugin.as_deref(), Some("ollama"));
    }

    #[test]
    fn defaults_to_the_default_session_and_keeps_explicit_selection() {
        assert_eq!(super::selected_session_id(None), "tui-default");
        assert_eq!(super::selected_session_id(Some("work".into())), "tui-work");
    }

    #[test]
    fn does_not_assume_a_provider_model_by_default() {
        assert_eq!(super::DEFAULT_MODEL, "unset");
    }

    #[test]
    fn suggests_model_selection_only_for_model_errors() {
        assert!(super::invalid_model_error("Invalid model: gpt-missing"));
        assert!(super::invalid_model_error("model not found"));
        assert!(!super::invalid_model_error(
            "runtimeWorkspaceRoots requires experimentalApi capability"
        ));
    }

    #[test]
    fn parses_multiple_and_all_session_targets_with_confirmation() {
        let targets = super::parse_session_targets("one two-three", false).unwrap();

        assert_eq!(targets.ids, ["one", "two-three"]);
        assert!(!targets.all);
        assert!(!targets.confirmed);

        let targets = super::parse_session_targets("--all -y", true).unwrap();

        assert!(targets.all);
        assert!(targets.confirmed);
        let targets = super::parse_session_targets("one --deep --yes", true).unwrap();

        assert!(targets.deep);
        assert!(targets.confirmed);
        assert!(super::parse_session_targets("--all one", true).is_err());
        assert!(super::parse_session_targets("one one", true).is_err());
        assert!(super::parse_session_targets("-y", true).is_err());
        assert!(super::parse_session_targets("-y", false).is_err());
        assert!(super::parse_session_targets("one --deep", false).is_err());
    }

    #[tokio::test]
    async fn bulk_session_actions_preserve_the_active_session() {
        let targets = super::parse_session_targets("--all", false).unwrap();
        let result =
            super::apply_session_action("/tmp", "archive", targets, "tui-other", mock_control)
                .await;

        assert_eq!(result, "Archived session saved.");

        let targets = super::parse_session_targets("--all -y", true).unwrap();
        let result =
            super::apply_session_action("/tmp", "delete", targets, "tui-saved", mock_control).await;

        assert_eq!(result, "No sessions were deleted. Kept active session saved.");
    }

    #[tokio::test]
    async fn deep_session_delete_removes_matching_local_fallback() {
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-deep-delete-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let home = root.to_string_lossy().into_owned();

        super::offline::control(&home, "session.new", json!({"id": "tui-first", "model": "test"}))
            .await
            .unwrap();

        super::offline::control(&home, "session.new", json!({"id": "tui-second", "model": "test"}))
            .await
            .unwrap();

        let targets = super::parse_session_targets("first --deep -y", true).unwrap();
        let result =
            super::apply_session_action(&home, "delete", targets, "tui-active", mock_deep_delete)
                .await;

        assert_eq!(result, "Deleted session first.");
        assert!(
            super::offline::control(&home, "session.get", json!({"id": "tui-first"}))
                .await
                .is_err()
        );

        assert!(
            super::offline::control(&home, "session.get", json!({"id": "tui-second"}))
                .await
                .is_ok()
        );

        let targets = super::parse_session_targets("second -y", true).unwrap();
        let result =
            super::apply_session_action(&home, "delete", targets, "tui-active", mock_control).await;

        assert_eq!(result, "Deleted session second.");
        assert!(
            super::offline::control(&home, "session.get", json!({"id": "tui-second"}))
                .await
                .is_ok()
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_invalid_one_shot_and_session_options() {
        assert!(super::parse_options(&["--once".into()]).is_err());
        assert!(super::parse_options(&["--once".into(), "  ".into()]).is_err());
        assert!(super::parse_options(&["--session".into(), "Bad_ID".into()]).is_err());
        assert!(super::parse_options(&["--bogus".into()]).is_err());
    }

    #[test]
    fn validates_model_ids_and_only_lists_available_commands() {
        assert!(super::valid_model("provider/model-2.1"));
        assert!(!super::valid_model("two models"));
        assert!(!super::valid_model(""));

        let basic = super::command_help("missing-home", false, false);

        assert!(basic.contains("/statusline"));
        assert!(basic.contains("/session help"));
        assert!(!basic.contains("/compact"));
        assert!(!basic.contains("/model [id]"));
        assert!(!basic.contains("/deliveries"));
        assert!(basic.contains("Plugin commands run in the CLI: crab <command>."));
        assert!(basic.contains("Page Up/Down scroll the conversation"));
        assert!(basic.contains("Ctrl+O adds a message line."));
        assert!(!basic.contains("Shift+Enter"));

        let description_columns = basic
            .lines()
            .filter_map(|line| {
                if line.contains("Show or change this session's filesystem root.")
                    || line.contains("Choose which details appear in the bottom statusline.")
                {
                    line.find("Show").or_else(|| line.find("Choose"))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(description_columns.len(), 2);
        assert_eq!(description_columns[0], description_columns[1]);

        let model_commands = super::command_help("missing-home", true, false);

        assert!(model_commands.contains("/model <help|list|show|set>"));
        assert!(model_commands.contains("/compact"));
        assert!(model_commands.contains("/plugins [page]"));
        assert!(model_commands.contains("Conditional commands (shown only when usable):"));
        assert!(model_commands.contains("\n\nPlugin commands run in the CLI:"));
        assert!(!model_commands.contains("`crab"));
        assert!(super::model_help().contains("/model list"));

        let sessions = super::session_help();

        assert!(sessions.contains("/session archive <id>...|--all"));
        assert!(sessions.contains("/session unarchive <id>...|--all"));
        assert!(sessions.contains("/session rename <new-id>"));
        assert!(sessions.contains("/session delete <id>...|--all [-y] [--deep]"));
        assert!(sessions.contains("--deep also removes local and shared session data."));
        assert!(sessions.contains("/new <id>"));
        assert!(!sessions.contains("/sessions"));

        let aligned_columns = [
            "List sessions, 10 per page; active first, then recent.",
            "Permanently delete sessions; --deep also removes local and shared session data.",
        ]
        .map(|description| sessions.lines().find_map(|line| line.find(description)).unwrap());

        assert_eq!(aligned_columns[0], aligned_columns[1]);
    }

    #[test]
    fn conditional_command_descriptions_share_one_indented_column() {
        let rows = super::format_help_rows(&[
            ("/model <help|list|show|set>", "Manage the selected model."),
            ("/approval", "Show approval policy."),
            ("/approvals", "List pending tool approvals."),
        ]);

        let descriptions =
            ["Manage the selected model.", "Show approval policy.", "List pending tool approvals."];

        let description_columns = rows
            .lines()
            .zip(descriptions)
            .map(|(line, description)| line.find(description).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(description_columns[0], description_columns[1]);
        assert_eq!(description_columns[1], description_columns[2]);
    }

    #[test]
    fn detects_only_installed_plugin_capabilities() {
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-capability-{}", std::process::id()));

        let plugin = root.join("plugins/timer");
        let binary = crabbot_core::plugin::binary_name("timer");

        std::fs::create_dir_all(plugin.join("bin")).unwrap();
        std::fs::write(plugin.join("crabbot-plugin.toml"), "capabilities = ['timer']\n").unwrap();
        std::fs::write(plugin.join("bin").join(binary), "test").unwrap();

        assert!(super::has_capability(root.to_str().unwrap(), "timer"));
        assert!(!super::has_capability(root.to_str().unwrap(), "memory"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn falls_back_only_when_failure_precedes_request_delivery() {
        let connection_refused = super::unavailable_before_delivery(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));

        assert!(connection_refused.is_unavailable());

        let missing_daemon_files = super::unavailable_before_delivery(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));

        assert!(missing_daemon_files.is_unavailable());

        let timeout = super::ControlError::Failed(crabbot_core::Error::Denied(
            "The daemon did not respond in time.".into(),
        ));

        assert!(!timeout.is_unavailable());

        let connection_reset = super::ControlError::Failed(crabbot_core::Error::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        ));

        assert!(!connection_reset.is_unavailable());

        let protocol_error = super::ControlError::Failed(crabbot_core::Error::Denied(
            "The daemon returned an invalid response.".into(),
        ));

        assert!(!protocol_error.is_unavailable());
    }

    #[test]
    fn formats_status() {
        let text = super::format_status(&serde_json::json!({"running": true, "sessions": 2}));

        assert_eq!(text, "Background runtime: running. Sessions: 2.\n> ");

        let text = super::format_status(&serde_json::json!({"running": false}));

        assert_eq!(text, "Background runtime: stopped. Sessions: 0.\n> ");
    }

    #[test]
    fn formats_approval() {
        assert_eq!(
            super::format_approval(&serde_json::json!({"approval": "auto"})),
            "Approvals: auto.\n> "
        );
    }

    #[test]
    fn formats_live_approval_with_exact_shell_command_and_separate_actions() {
        assert_eq!(
            super::format_approval_notice(
                "approval-1",
                "shell",
                "{\"command\":\"rm foo\"}",
                Some("rm foo"),
                "Crabbot requests approval to run this shell command: rm foo.",
            ),
            "Approval needed.\nTool: shell\nCommand: rm foo\nApprove: /approve approval-1\nDeny: /deny approval-1"
        );

        assert_eq!(
            super::format_approval_notice(
                "approval-3",
                "shell",
                "{\"command\":\"rm foo.\"}",
                Some("rm foo."),
                "Crabbot requests approval to run this shell command: rm foo..",
            ),
            "Approval needed.\nTool: shell\nCommand: rm foo.\nApprove: /approve approval-3\nDeny: /deny approval-3"
        );

        assert_eq!(
            super::format_approval_notice(
                "approval-2",
                "write",
                "{\"path\":\"foo\"}",
                None,
                "Crabbot requests approval to write to foo..",
            ),
            "Approval needed.\nTool: write\nArguments: {\"path\":\"foo\"}\nAction: write to foo\nApprove: /approve approval-2\nDeny: /deny approval-2"
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
        // The fixtures use 2025-01-02 and 2025-01-03 UTC timestamps in Unix seconds.
        let text = super::format_sessions(
            &serde_json::json!({
                "items": [{
                    "id": "tui-one",
                    "model": "provider/model",
                    "status": "idle",
                    "created": 1735776000,
                    "updated": 1735862400,
                    "messages": 3
                }]
            }),
            "tui-one",
            true,
            1,
        );

        assert_eq!(
            text,
            "Sessions | 1–1 of 1 | page 1/1:\n└─ one | active\n   ├─ model: provider/model\n   ├─ created: 2025-01-02\n   ├─ updated: 2025-01-03\n   └─ messages: 3\n> "
        );

        let multiple = super::format_sessions(
            &serde_json::json!({
                "items": [
                    {"id": "tui-one", "status": "idle"},
                    {"id": "tui-two", "status": "idle"}
                ]
            }),
            "tui-one",
            false,
            1,
        );

        assert!(multiple.contains("page 1/1:\n├─ one | active"));
        assert!(multiple.contains("│  └─ model: unset\n\n└─ two | idle"));

        assert_eq!(
            super::format_sessions(&serde_json::json!({"items": [{}]}), "", false, 1),
            "Sessions: none.\n> "
        );

        assert_eq!(
            super::format_sessions(
                &serde_json::json!({
                    "items": [{"id": "tui-old", "status": "idle", "archived": true}]
                }),
                "",
                false,
                1
            ),
            "Sessions | 1–1 of 1 | page 1/1:\n└─ old | archived\n   └─ model: unset\n> "
        );

        assert_eq!(
            super::format_sessions(&serde_json::json!({}), "", false, 1),
            "Sessions: none.\n> "
        );
    }

    #[test]
    fn session_state_keeps_display_and_sort_precedence_explicit() {
        let archived_working = serde_json::json!({
            "id": "archived",
            "status": "working",
            "archived": true
        });

        let active_archived = serde_json::json!({
            "id": "active",
            "status": "idle",
            "archived": true
        });

        let archived_working = super::SessionState::from_item(&archived_working, "");
        let active_archived = super::SessionState::from_item(&active_archived, "active");

        assert_eq!(archived_working.label(), "archived");
        assert_eq!(archived_working.priority(), 0);
        assert_eq!(active_archived.label(), "archived");
        assert_eq!(active_archived.priority(), 1);
    }

    #[test]
    fn paginates_sessions_with_working_and_active_sessions_first() {
        let value = serde_json::json!({
            "items": [
                {"id": "tui-active", "status": "idle", "updated": 1},
                {"id": "tui-working", "status": "working", "updated": 2},
                {"id": "tui-idle-old", "status": "idle", "updated": 3},
                {"id": "tui-idle-new", "status": "idle", "updated": 30},
                {"id": "tui-archived", "status": "idle", "archived": true, "updated": 100},
                {"id": "tui-idle-04", "status": "idle", "updated": 4},
                {"id": "tui-idle-05", "status": "idle", "updated": 5},
                {"id": "tui-idle-06", "status": "idle", "updated": 6},
                {"id": "tui-idle-07", "status": "idle", "updated": 7},
                {"id": "tui-idle-08", "status": "idle", "updated": 8},
                {"id": "tui-idle-09", "status": "idle", "updated": 9},
            ]
        });

        let first_page = super::format_sessions(&value, "tui-active", false, 1);
        let working_position = first_page.find("working | working").unwrap();
        let active_position = first_page.find("active | active").unwrap();
        let recent_position = first_page.find("idle-new | idle").unwrap();

        assert!(working_position < active_position);
        assert!(active_position < recent_position);
        assert!(first_page.contains("idle-new | idle"));
        assert!(first_page.contains("Sessions | 1–5 of 11 | page 1/3:"));
        assert!(!first_page.contains("archived | archived"));

        let second_page = super::format_sessions(&value, "tui-active", false, 2);

        assert!(second_page.contains("Sessions | 6–10 of 11 | page 2/3:"));
        assert!(second_page.contains("idle-old | idle"));
        assert!(!second_page.contains("archived | archived"));

        let third_page = super::format_sessions(&value, "tui-active", false, 3);

        assert!(third_page.contains("Sessions | 11–11 of 11 | page 3/3:"));
        assert!(third_page.contains("archived | archived"));
    }

    #[test]
    fn parses_session_list_page_numbers_and_rejects_invalid_input() {
        assert_eq!(super::parse_session_list_page("/session list"), Ok(1));
        assert_eq!(super::parse_session_list_page("/session list 3"), Ok(3));
        assert!(super::parse_session_list_page("/session list 0").is_err());
        assert!(super::parse_session_list_page("/session list next").is_err());
        assert!(super::parse_session_list_page("/session list 2 extra").is_err());
    }

    #[test]
    fn parses_plugin_list_pages_and_rejects_invalid_input() {
        assert_eq!(super::parse_plugin_list_page("/plugins"), Ok(1));
        assert_eq!(super::parse_plugin_list_page("/plugins 3"), Ok(3));
        assert!(super::parse_plugin_list_page("/plugins 0").is_err());
        assert!(super::parse_plugin_list_page("/plugins next").is_err());
        assert!(super::parse_plugin_list_page("/plugins 2 extra").is_err());
    }

    #[test]
    fn decodes_json_encoded_live_turn_commands() {
        assert_eq!(
            super::decode_terminal_input(r#""/approve 0123456789abcdef01234567""#),
            "/approve 0123456789abcdef01234567"
        );

        assert_eq!(super::decode_terminal_input("/approvals"), "/approvals");
    }

    #[test]
    fn reports_session_archive_transitions_and_noops_clearly() {
        assert_eq!(super::format_session_archive("one", true, true), "Archived session one.");
        assert_eq!(
            super::format_session_archive("one", true, false),
            "Session one is already archived."
        );

        assert_eq!(super::format_session_archive("one", false, true), "Unarchived session one.");
        assert_eq!(
            super::format_session_archive("one", false, false),
            "Session one is already unarchived."
        );
    }

    #[test]
    fn session_action_errors_are_not_prefixed_with_redundant_operation_text() {
        let message = super::sentence("Session was not found.".into());

        assert_eq!(message, "Session not found");
        assert!(!message.contains("update failed"));
    }

    #[test]
    fn formats_plugins() {
        assert_eq!(
            super::format_plugins(
                &serde_json::json!({
                    "items": [{
                        "id": "codex",
                        "version": "1.2.3",
                        "health": "ready",
                        "protocol": {"major": 0, "minor": 1},
                        "capabilities": ["model", "vision"],
                        "commands": [{"name": "codex"}],
                        "permissions": ["network", "process"]
                    }]
                }),
                1
            ),
            "Plugins | 1–1 of 1 | page 1/1:\n└─ codex | v1.2.3\n   ├─ health: ready\n   ├─ capabilities: model, vision\n   ├─ protocol: 0.1\n   ├─ commands: codex\n   └─ permissions: network, process"
        );

        let items = (0..9)
            .map(|index| {
                serde_json::json!({
                    "id": format!("plugin-{index:02}"),
                    "version": "1.0.0",
                    "health": "ready"
                })
            })
            .collect::<Vec<_>>();

        let inventory = serde_json::json!({"items": items});
        let first_page = super::format_plugins(&inventory, 1);
        let second_page = super::format_plugins(&inventory, 2);

        assert!(first_page.contains("Plugins | 1–5 of 9 | page 1/2:"));
        assert!(first_page.contains("├─ plugin-00"));
        assert!(first_page.contains("page 1/2:\n├─ plugin-00"));
        assert!(first_page.contains("│  └─ permissions: none\n\n├─ plugin-01"));
        assert!(!first_page.contains("plugin-05"));
        assert!(second_page.contains("Plugins | 6–9 of 9 | page 2/2:"));
        assert!(second_page.contains("└─ plugin-08"));
        assert!(!second_page.contains("plugin-00"));
        assert!(super::format_plugins(&inventory, 3).contains("plugins have 2 pages"));
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

    #[tokio::test]
    async fn resolves_pending_approvals_during_a_daemon_turn_and_saves_the_interaction() {
        let mut sequence = 0;
        let response = super::resolve_turn_approval(
            "/tmp/crabbot",
            "telegram-7",
            "reservation",
            "/approve",
            &mut sequence,
            mock_control,
        )
        .await
        .unwrap();

        assert_eq!(response, "Approval accepted.\n");
        assert_eq!(sequence, 2);
    }

    #[tokio::test]
    async fn resolves_an_id_specific_approval_from_live_turn_input() {
        let mut sequence = 0;
        let command = super::decode_terminal_input(r#""/approve 0123456789abcdef01234567""#);
        let response = super::resolve_turn_approval(
            "/tmp/crabbot",
            "telegram-7",
            "reservation",
            &command,
            &mut sequence,
            mock_control,
        )
        .await
        .unwrap();

        assert_eq!(response, "Approval accepted.\n");
        assert_eq!(sequence, 2);
    }

    #[test]
    fn sentence_capitalizes_errors() {
        assert_eq!(super::sentence("connection failed.".into()), "Connection failed");

        assert_eq!(super::sentence("Denied: Session was not found.".into()), "Session not found");

        assert_eq!(
            super::sentence("Denied: Denied: Codex request failed.".into()),
            "Codex request failed"
        );

        assert_eq!(
            super::sentence("Denied: Session already exists.".into()),
            "Session already exists"
        );
    }

    async fn mock_control(
        _home: String,
        method: String,
        params: serde_json::Value,
    ) -> crabbot_core::Result<serde_json::Value> {
        if [
            "session.answer",
            "session.ensure",
            "session.new",
            "session.append",
            "session.append_reserved",
            "session.reserve",
            "session.renew",
            "session.release",
            "session.compact",
            "session.clear",
            "session.model",
            "session.rename",
            "session.delete",
        ]
        .contains(&method.as_str())
        {
            if method == "session.answer" {
                return Ok(json!({"text": "Reply"}));
            }

            if method == "session.reserve" {
                return Ok(json!({"id": params["id"], "owner": "mock-reservation-owner"}));
            }

            if method == "session.compact" {
                return Ok(json!({"compacted": false}));
            }

            return Ok(json!({"id": params["id"]}));
        }

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

            "plugin.list" => Ok(json!({
                "items": [{"id": "codex", "health": "ready", "capabilities": ["model"]}]
            })),

            "model.list" => Ok(json!({
                "text": "Codex models (1):\n  gpt-test - Test model (default)"
            })),

            "approval.resolve" => Ok(json!({"resolved": true, "approved": params["approved"]})),
            // The fixture uses 2025-01-02 and 2025-01-03 UTC timestamps in Unix seconds.
            "session.list" => Ok(json!({
                "items": [{
                    "id": "tui-saved",
                    "model": "provider/model",
                    "status": "idle",
                    "created": 1735776000,
                    "updated": 1735862400,
                    "messages": 2
                }]
            })),
            "session.workspace" => Ok(json!({
                "id": params["id"],
                "workspace": params["workspace"]
            })),

            "session.archive" | "session.unarchive" => {
                if method == "session.unarchive" && params["id"] == "tui-missing" {
                    Err(crabbot_core::Error::Denied("Session was not found.".into()))
                } else {
                    Ok(json!({"id": params["id"], "changed": true}))
                }
            }

            _ => Err(crabbot_core::Error::Denied("Method not found.".into())),
        }
    }

    async fn mock_deep_delete(
        _home: String,
        method: String,
        params: serde_json::Value,
    ) -> crabbot_core::Result<serde_json::Value> {
        assert_eq!(method, "session.delete");
        assert_eq!(params["deep"], true);

        Ok(json!({"id": params["id"], "worktree": {"status": "removed"}}))
    }

    async fn delayed_answer_control(
        home: String,
        method: String,
        params: serde_json::Value,
    ) -> crabbot_core::Result<serde_json::Value> {
        if method == "session.answer" {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            return Ok(json!({"text": "late reply"}));
        }

        super::local_aware(home, method, params).await
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

    #[test]
    fn final_reply_does_not_repeat_or_replace_text_already_streamed() {
        assert_eq!(super::final_reply_suffix("Hello", "Hello world"), " world");
        assert_eq!(super::final_reply_suffix("Hello world", "Hello world"), "");
        assert_eq!(super::final_reply_suffix("Hello", "A duplicated final reply"), "");
        assert_eq!(super::final_reply_suffix("", "Non-streamed reply"), "Non-streamed reply");
    }

    #[test]
    fn tui_advertises_only_read_only_workspace_tools() {
        let names = super::read_only_tools().into_iter().map(|tool| tool.name).collect::<Vec<_>>();

        assert_eq!(names, ["read", "list", "search", "fetch"]);
    }

    #[test]
    fn workspace_defaults_to_the_home_workspace_and_honors_session_selection() {
        assert_eq!(
            super::workspace_root("/tmp/crabbot-home", None),
            super::PathBuf::from("/tmp/crabbot-home/workspace")
        );

        assert_eq!(
            super::workspace_root("/tmp/crabbot-home", Some("/work/project")),
            super::PathBuf::from("/work/project")
        );
    }

    #[test]
    fn relative_workspace_paths_resolve_against_the_tui_launch_directory() {
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-workspace-{}", std::process::id()));
        let project = root.join("project");
        std::fs::create_dir_all(&project).unwrap();

        assert_eq!(
            super::resolve_workspace_at(".", &project).unwrap(),
            Some(project.canonicalize().unwrap().to_string_lossy().into_owned())
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tui_refuses_mutating_tool_calls_even_without_a_tool_process() {
        let response = super::serve_read_only_tool(
            std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            crabbot_core::types::Request::call(
                1,
                "host/tool",
                serde_json::json!({"name": "write", "args": {"path": "file", "text": "x"}}),
            ),
            None,
        )
        .await
        .unwrap();

        assert!(response.error.unwrap().message.contains("Only read-only tools"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tui_routes_read_only_tool_calls_to_the_installed_tools_plugin() {
        let script = concat!(
            "while IFS= read -r line; do\n",
            "id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9][0-9]*\\).*/\\1/p')\n",
            "case \"$line\" in\n",
            "*'\"method\":\"hello\"'*) printf '%s\\n' ",
            "'{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"tools\",\"version\":\"0.1.0\",\"capabilities\":[\"tool\"]}}' ;;\n",
            "*'\"method\":\"list\"'*) printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"items\":[\"Cargo.toml\",\"src\"]}}\\n' \"$id\" ;;\n",
            "*'\"method\":\"fetch\"'*) printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"status\":200,\"content_type\":\"text/plain\",\"text\":\"Fetched text\",\"truncated\":true}}\\n' \"$id\" ;;\n",
            "*'\"method\":\"shutdown\"'*) printf '{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"ok\":true}}\\n' \"$id\"; exit 0 ;;\n",
            "esac\n",
            "done\n",
        );

        let process =
            crabbot_core::plugin::Process::start_with("sh", ["-c", script]).await.unwrap();

        let tools =
            std::sync::Arc::new(tokio::sync::Mutex::new(Some(super::ToolsProcess { process })));

        let response = super::serve_read_only_tool(
            std::sync::Arc::clone(&tools),
            crabbot_core::types::Request::call(
                7,
                "host/tool",
                json!({"name": "list", "args": {"path": "."}}),
            ),
            Some("/workspace/project".into()),
        )
        .await
        .unwrap();

        assert_eq!(response.result.unwrap()["text"], "Cargo.toml\nsrc");

        let response = super::serve_read_only_tool(
            std::sync::Arc::clone(&tools),
            crabbot_core::types::Request::call(
                8,
                "host/tool",
                json!({"name": "fetch", "args": {"url": "https://example.com"}}),
            ),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            response.result.unwrap()["text"],
            "HTTP 200 (text/plain)\nFetched text\n[response truncated at the text limit]"
        );

        if let Some(tools) = tools.lock().await.take() {
            tools.process.stop().await.unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn model_turn_uses_workspace_tools_without_repeating_the_final_reply() {
        use std::{fs, os::unix::fs::PermissionsExt, time::SystemTime};
        use tokio::io::duplex;

        let nonce = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("crabbot-tui-tools-{nonce}"));
        let codex = root.join("plugins/codex/bin/crabbot-plugin-codex");
        let tools = root.join("plugins/tools/bin/crabbot-plugin-tools");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::create_dir_all(tools.parent().unwrap()).unwrap();

        let codex_script = r#"#!/bin/sh
while IFS= read -r line; do
    request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$line" in
        *'"method":"hello"'*)
            printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}'
            ;;
        *'"method":"generate"'*)
            printf '%s\n' '{"jsonrpc":"2.0","id":71,"method":"host/tool","params":{"name":"list","args":{"path":"."}}}'
            IFS= read -r tool_reply
            case "$tool_reply" in *Cargo.toml*src*) ;; *) exit 42 ;; esac
            printf '%s\n' '{"jsonrpc":"2.0","method":"event","params":{"event":{"kind":"text","text":"Directory entries: Cargo.toml, src."}}}'
            printf '{"jsonrpc":"2.0","id":%s,"result":{"text":"Cargo.toml, src.","stop":"stop","events":[]}}\n' "$request_id"
            ;;
        *'"method":"shutdown"'*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$request_id"
            exit 0
            ;;
    esac
done
"#;

        let tools_script = r#"#!/bin/sh
while IFS= read -r line; do
    request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$line" in
        *'"method":"hello"'*)
            printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"tools","version":"0.1.0","capabilities":["tool"]}}'
            ;;
        *'"method":"list"'*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"items":["Cargo.toml","src"]}}\n' "$request_id"
            ;;
        *'"method":"shutdown"'*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$request_id"
            exit 0
            ;;
    esac
done
"#;
        fs::write(&codex, codex_script).unwrap();
        fs::write(&tools, tools_script).unwrap();
        fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&tools, fs::Permissions::from_mode(0o700)).unwrap();

        let (mut input_tx, input_rx) = duplex(1024);
        input_tx.write_all(b"List the current directory\n/quit\n").await.unwrap();
        drop(input_tx);
        let (output_tx, mut output_rx) = duplex(16 * 1024);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = super::run_with(
            input_rx,
            output_tx,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "codex".into(),
                model: "gpt-test".into(),
                model_override: None,
                session: "default".into(),
            },
            super::local_aware,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        tokio::time::timeout(std::time::Duration::from_secs(10), engine).await.unwrap().unwrap();
        let mut output = String::new();
        output_rx.read_to_string(&mut output).await.unwrap();

        assert_eq!(output.matches("Reply").count(), 1);

        let _ = fs::remove_dir_all(root);
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
                "/help\n/status\n/approval\n/approvals\n/approve 0123456789abcdef01234567\n/deny 0123456789abcdef01234567\n/session list\n/session help\n/deliveries\n/retry bad id\n/drop bad id\n/plugins\n/workspace\n/workspace {}\n/workspace reset\n/timer list\n/timer add 30 break\n/timer remove 7\n/memory list\n/memory remember drink=tea\n/memory forget drink\n/model \n/model test\n/session bad!\n/new two\n/session create test-one\n/session one\n/session rename renamed\n/session archive two\n/session unarchive two\n/session unarchive missing\n/session delete two -y\n/session delete renamed -y\n/session archive renamed\n/compact\n/clear\n\nhello\n/quit\n",
                root.display()
            ),
        )
        .unwrap();

        let input = tokio::fs::File::from_std(fs::File::open(&input_path).unwrap());
        let output = tokio::fs::File::from_std(fs::File::create(&output_path).unwrap());
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);
        let run = super::run_with(
            input,
            output,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "codex".into(),
                model: "base".into(),
                model_override: None,
                session: "default".into(),
            },
            mock_control,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        tokio::time::timeout(std::time::Duration::from_secs(10), run).await.unwrap().unwrap();
        let text = fs::read_to_string(&output_path).unwrap();

        assert_eq!(text.matches("Reply").count(), 1);
        assert!(text.contains("Current model: base."));
        assert!(text.contains("Daemon status unavailable"));
        assert!(text.contains("Pending approvals:"));
        assert!(text.contains("Approval accepted."));
        assert!(text.contains("Approval denied."));
        assert!(text.contains("There are not enough earlier turns to compact."));
        assert!(text.contains(
            "Plugins | 1–1 of 1 | page 1/1:\n└─ codex | vunknown\n   ├─ health: ready\n   ├─ capabilities: model\n   ├─ protocol: 0.0\n   ├─ commands: none\n   └─ permissions: none"
        ));

        assert!(text.contains("Created session two."));
        assert!(text.contains("Created session test-one."));
        assert!(text.contains(concat!(
            "Sessions | 1–1 of 1 | page 1/1:\n└─ saved | idle\n",
            "   ├─ model: provider/model\n   ├─ created: 2025-01-02\n",
            "   ├─ updated: 2025-01-03\n   └─ messages: 2"
        )));

        assert!(text.contains("Using session one."));
        assert!(text.contains("Renamed session one to renamed."));
        assert!(text.contains("Archived session two."));
        assert!(text.contains("Unarchived session two."));
        assert!(text.contains("Session not found\n> "));
        assert!(!text.contains("Session update failed:"));
        assert!(text.contains("Deleted session two."));
        assert!(text.contains("Kept active session renamed"));
        assert!(text.contains("Kept active session renamed"));
        assert!(text.contains(&format!("Workspace changed to: {}", root.display())));
        assert!(text.contains("Timers: 7 (due 42): call back."));
        assert!(text.contains("was scheduled."));
        assert!(text.contains("Timer 7 was removed."));
        assert!(text.contains("Memories: drink = tea."));
        assert!(text.contains("Memory saved for this session."));
        assert!(text.contains("Memory removed."));

        let one_shot = super::run_once(
            root.display().to_string(),
            "codex".into(),
            "base".into(),
            None,
            "once".into(),
            "hello once".into(),
            mock_control,
        )
        .await
        .unwrap();

        assert_eq!(one_shot, "Reply");

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keeps_the_session_engine_alive_after_a_model_error() {
        use std::{fs, os::unix::fs::PermissionsExt};
        use tokio::io::duplex;

        let root =
            std::env::temp_dir().join(format!("crabbot-tui-model-error-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);

        let binary = root.join("plugins/codex/bin/crabbot-plugin-codex");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(
            &binary,
            r#"#!/bin/sh
count=0
while IFS= read -r line; do
    request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$line" in
        *'"method":"generate"'*)
            count=$((count + 1))
            if [ "$count" -eq 1 ]; then
                printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"Denied: Codex rejected thread/start: Invalid model"}}\n' "$request_id"
            else
                printf '{"jsonrpc":"2.0","id":%s,"result":{"text":"Recovered reply","stop":"stop","events":[]}}\n' "$request_id"
            fi
            ;;
        *'"method":"hello"'*)
            printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}'
            ;;
        *shutdown*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$request_id"
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();

        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        crate::local_aware(
            root.display().to_string(),
            "session.ensure".into(),
            serde_json::json!({"id": "default", "model": "gpt-test"}),
        )
        .await
        .unwrap();

        let (mut input_tx, input_rx) = duplex(1024);
        input_tx.write_all(b"first\nsecond\n/quit\n").await.unwrap();
        drop(input_tx);
        let (output_tx, mut output_rx) = duplex(16 * 1024);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = super::run_with(
            input_rx,
            output_tx,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "codex".into(),
                model: "gpt-test".into(),
                model_override: None,
                session: "default".into(),
            },
            super::local_aware,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        tokio::time::timeout(std::time::Duration::from_secs(10), engine).await.unwrap().unwrap();

        let mut output = String::new();
        output_rx.read_to_string(&mut output).await.unwrap();

        assert_eq!(output.matches("Reply").count(), 2);
        assert!(!output.contains("The session engine is no longer running"));

        let session =
            super::read_session(&root.display().to_string(), "default", super::local_aware)
                .await
                .unwrap();

        assert_eq!(session.messages.len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn model_subcommands_list_show_and_select_models() {
        use std::{fs, os::unix::fs::PermissionsExt};
        use tokio::io::duplex;

        let root =
            std::env::temp_dir().join(format!("crabbot-tui-model-list-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);

        let binary = root.join("plugins/codex/bin/crabbot-plugin-codex");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(
            &binary,
            r#"#!/bin/sh
while IFS= read -r line; do
    request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$line" in
        *models*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"text":"Codex models (1):\\n  gpt-test - Test model (default)"}}\n' "$request_id"
            ;;
        *hello*)
            printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}'
            ;;
        *shutdown*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$request_id"
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();

        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        crate::local_aware(
            root.display().to_string(),
            "session.ensure".into(),
            serde_json::json!({"id": "default", "model": "unset"}),
        )
        .await
        .unwrap();

        let (mut input_tx, input_rx) = duplex(1024);
        input_tx
            .write_all(b"/model help\n/model show\n/model list\n/model set gpt-test\n/quit\n")
            .await
            .unwrap();

        drop(input_tx);
        let (output_tx, mut output_rx) = duplex(16 * 1024);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = super::run_with(
            input_rx,
            output_tx,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "codex".into(),
                model: "unset".into(),
                model_override: None,
                session: "default".into(),
            },
            super::local_aware,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        tokio::time::timeout(std::time::Duration::from_secs(10), engine).await.unwrap().unwrap();

        let mut output = String::new();
        output_rx.read_to_string(&mut output).await.unwrap();

        assert!(output.contains("Model commands:"));
        assert!(output.contains("Current model: unset."));
        assert!(output.contains("gpt-test - Test model (default)"));
        assert!(output.contains("Using model gpt-test."));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escape_interrupts_generation_and_restarts_the_plugin() {
        use std::{fs, os::unix::fs::PermissionsExt};
        use tokio::io::duplex;

        let root =
            std::env::temp_dir().join(format!("crabbot-tui-interrupt-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        let binary = root.join("plugins/codex/bin/crabbot-plugin-codex");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(
            &binary,
            r#"#!/bin/sh
while IFS= read -r line; do case "$line" in *generate*) printf '%s\n' '{"jsonrpc":"2.0","method":"event","params":{"event":{"kind":"text","text":"Partial"}}}'; sleep 30; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"text":"Partial answer","stop":"stop","events":[]}}' ;; *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#,
        )
        .unwrap();

        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        let (mut input_tx, input_rx) = duplex(4096);
        input_tx.write_all(b"hello\n").await.unwrap();
        let (output_tx, mut output_rx) = duplex(16 * 1024);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        let (engine_event_tx, mut engine_event_rx) = tokio::sync::mpsc::unbounded_channel();

        let engine = tokio::spawn(super::run_with(
            input_rx,
            output_tx,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "codex".into(),
                model: "base".into(),
                model_override: None,
                session: "default".into(),
            },
            delayed_answer_control,
            super::EngineEvents { engine: Some(engine_event_tx), ..super::EngineEvents::default() },
            interrupt_rx,
        ));

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(3), engine_event_rx.recv())
                .await
                .unwrap(),
            Some(super::EngineEvent::GenerationStarted)
        );

        let mut output = Vec::new();
        let mut buffer = [0_u8; 256];

        interrupt_tx.send(()).unwrap();

        while !String::from_utf8_lossy(&output).contains("Generation interrupted.") {
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                output_rx.read(&mut buffer),
            )
            .await
            .unwrap()
            .unwrap();

            output.extend_from_slice(&buffer[..count]);
        }

        assert!(!String::from_utf8_lossy(&output).contains("late reply"));
        assert_eq!(
            engine_event_rx.recv().await,
            Some(super::EngineEvent::GenerationFinished { interrupted: true })
        );

        input_tx.write_all(b"/quit\n").await.unwrap();
        drop(input_tx);
        tokio::time::timeout(std::time::Duration::from_secs(3), engine)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let saved = super::read_session(&root.display().to_string(), "default", super::local_aware)
            .await
            .unwrap();

        assert_eq!(saved.messages.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_without_daemon_or_intelligence_plugin() {
        let root = std::env::temp_dir().join(format!("crabbot-tui-offline-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let input = tokio::io::duplex(4096);
        let (mut input_writer, input_reader) = input;
        let (output_writer, mut output_reader) = tokio::io::duplex(16 * 1024);

        input_writer.write_all(b"/session list\n/quit\n").await.unwrap();
        drop(input_writer);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let run = super::run_with(
            input_reader,
            output_writer,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "missing-model".into(),
                model: "small".into(),
                model_override: None,
                session: "tui-offline".into(),
            },
            super::local_aware,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        let output = async {
            let mut value = String::new();
            output_reader.read_to_string(&mut value).await.unwrap();
            value
        };

        let (result, text) = tokio::join!(run, output);
        result.unwrap();

        assert!(text.contains("No intelligence plugin is installed"));
        assert!(text.contains("└─ offline | active\n   ├─ model: unset\n   ├─ created:"));
        assert!(super::data::plugin_file(&root, "tui", "sessions.json").is_file());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn interactive_engine_omits_the_transcript_startup_banner() {
        let root = std::env::temp_dir().join(format!("crabbot-tui-banner-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let (mut input_writer, input_reader) = tokio::io::duplex(1024);
        input_writer.write_all(b"/quit\n").await.unwrap();
        drop(input_writer);
        let (output_writer, mut output_reader) = tokio::io::duplex(1024);
        let (engine_event_tx, _engine_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = super::run_with(
            input_reader,
            output_writer,
            super::EngineConfig {
                home: root.display().to_string(),
                plugin: "missing-model".into(),
                model: "unset".into(),
                model_override: None,
                session: "default".into(),
            },
            super::local_aware,
            super::EngineEvents { engine: Some(engine_event_tx), ..super::EngineEvents::default() },
            interrupt_rx,
        );

        let output = async {
            let mut output = String::new();
            output_reader.read_to_string(&mut output).await.unwrap();
            output
        };

        let (result, output) = tokio::join!(engine, output);
        result.unwrap();

        assert!(!output.contains("terminal. Type /help"));
        assert!(!output.contains("No intelligence plugin is installed"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn requires_the_daemon_for_the_session_backend() {
        let root = std::env::temp_dir().join(format!("crabbot-tui-backend-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let home = root.display().to_string();
        let error = super::require_daemon(&home).await.unwrap_err();

        assert!(error.to_string().contains("daemon is not running"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn formats_denied_cli_errors_without_debug_wrappers() {
        let error = crabbot_core::Error::Denied(
            "The Crabbot daemon is not running.\nStart it with `crab service start`.".into(),
        );

        assert_eq!(
            super::cli_error_message(&error),
            "The Crabbot daemon is not running.\nStart it with `crab service start`."
        );
    }

    #[tokio::test]
    async fn applies_an_explicit_model_override_to_an_existing_session() {
        let root = std::env::temp_dir().join(format!("crabbot-tui-model-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let home = root.display().to_string();

        super::local_aware(
            home.clone(),
            "session.ensure".into(),
            serde_json::json!({"id": "saved", "model": "saved-model"}),
        )
        .await
        .unwrap();

        let (mut input_writer, input_reader) = tokio::io::duplex(4096);
        let (output_writer, mut output_reader) = tokio::io::duplex(16 * 1024);
        input_writer.write_all(b"/quit\n").await.unwrap();
        drop(input_writer);
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = super::run_with(
            input_reader,
            output_writer,
            super::EngineConfig {
                home: home.clone(),
                plugin: "missing-model".into(),
                model: "fallback-model".into(),
                model_override: Some("alternate-model".into()),
                session: "saved".into(),
            },
            super::local_aware,
            super::EngineEvents::default(),
            interrupt_rx,
        );

        let output = async {
            let mut output = String::new();
            output_reader.read_to_string(&mut output).await.unwrap();
            output
        };

        let (result, _) = tokio::join!(engine, output);
        result.unwrap();

        let saved = super::read_session(&home, "saved", super::local_aware).await.unwrap();

        assert_eq!(saved.model, "alternate-model");
        let _ = std::fs::remove_dir_all(root);
    }
}
