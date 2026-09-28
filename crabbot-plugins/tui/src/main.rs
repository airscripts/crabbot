#![forbid(unsafe_code)]

mod data;
mod date;
mod offline;
mod ui;

use crabbot_core::{
    jsonl,
    plugin::{Process, serve_with},
    types::{
        Capability, CommandSpec, Content, Hello, IpcRequest, IpcResponse, Message, ModelRequest,
        Protocol, Request, Response, Role,
    },
};

use serde_json::Value;
use std::{future::Future, pin::Pin};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, duplex,
};

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

const DEFAULT_MODEL: &str = "gpt-6-luna";
const DEFAULT_NAME: &str = "Crabbot";
const DEFAULT_SESSION_ID: &str = "default";
const SESSION_LIST_PAGE_SIZE: usize = 10;
const SAVED_REPLY_LIMIT: usize = 16 * 1024;
// 365 days (one year), expressed in seconds.
const MAX_TIMER_DELAY_SECONDS: u64 = 31_536_000;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.first().is_some_and(|value| value == "--crabbot-cli") {
        if let Some(output) = run(&args[1..]).await? {
            println!("{output}");
        }

        return Ok(());
    }

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
            description: "Open the TUI; local sessions do not require the background runtime."
                .into(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineEvent {
    GenerationStarted,
    GenerationFinished { interrupted: bool },
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

    if let Some(prompt) = options.once {
        return run_once(home, plugin, model, model_override, session, prompt).await.map(Some);
    }

    let input = terminal("r")?;
    let output = terminal("w")?;
    run_at(input, output, home, plugin, model, model_override, session).await?;

    Ok(None)
}

fn selected_session_id(session: Option<String>) -> String {
    session.unwrap_or_else(|| DEFAULT_SESSION_ID.into())
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
    let backend = select_session_backend(&home, &session, &model).await?;

    ui::run(
        output,
        home,
        ui::ModelOptions { plugin, model, model_override },
        session,
        name,
        move |home, method, params| {
            control_with_backend(backend, home, method, params, daemon_control)
        },
    )
    .await
}

async fn run_once(
    home: String,
    plugin: String,
    model: String,
    model_override: Option<String>,
    session: String,
    prompt: String,
) -> crabbot_core::Result<String> {
    let path = plugin_binary_path(&home, &plugin);

    if !path.is_file() {
        return Err(crabbot_core::Error::Denied(format!(
            "Intelligence plugin {plugin} is not installed. Install a model plugin to use --once."
        )));
    }

    let backend = select_session_backend(&home, &session, &model).await?;

    let (mut input, engine_input) = duplex(16 * 1024);
    let (mut output, engine_output) = duplex(64 * 1024);
    let (interrupt_tx, interrupt_rx) = mpsc::unbounded_channel();
    drop(interrupt_tx);

    let engine = tokio::spawn(run_with(
        engine_input,
        engine_output,
        EngineConfig { home, plugin, model, model_override, session },
        move |home, method, params| {
            control_with_backend(backend, home, method, params, daemon_control)
        },
        None,
        None,
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
    session_events: Option<mpsc::UnboundedSender<ui::SessionView>>,
    engine_events: Option<mpsc::UnboundedSender<EngineEvent>>,
    mut interrupt_rx: mpsc::UnboundedReceiver<()>,
) -> crabbot_core::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: Fn(String, String, Value) -> F + Copy + Send + 'static,
    F: Future<Output = crabbot_core::Result<Value>> + Send + 'static,
{
    let EngineConfig { home, plugin, mut model, model_override, mut session } = config;
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

    let bot_name = std::env::var("CRABBOT_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());
    output
        .write_all(format!("{bot_name} terminal. Type /help for commands.\n> ").as_bytes())
        .await?;

    output.flush().await?;

    let path = plugin_binary_path(&home, &plugin);
    let mut process = if path.is_file() { Some(Process::start(path).await?) } else { None };

    if process.is_none() {
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
            let line = serde_json::from_str::<String>(&line).unwrap_or(line);
            let line = line.trim();

            if line == "/quit" || line == "/exit" {
                break;
            }

            if line == "/help" {
                let daemon = host(home.clone(), "status".into(), serde_json::json!({})).await.is_ok();
                let help = command_help(&home, process.is_some(), daemon);
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

            if line == "/plugins" {
                let value = match host(home.clone(), "plugin.list".into(), serde_json::json!({})).await {
                    Ok(value) => value,
                    Err(_) => local_plugin_list(&home),
                };

                let text = format!("{}\n> ", format_plugins(&value));

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
                                format_sessions(&value, &session, process.is_some(), page)
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
                } else if target == session {
                    output.write_all(b"Session ID is unchanged.\n> ").await?;
                } else {
                    match host(
                        home.clone(),
                        "session.rename".into(),
                        serde_json::json!({"id": session, "target": target}),
                    )
                    .await
                    {
                        Ok(_) => {
                            output.write_all(format!("Renamed session {session} to {target}.\n> ").as_bytes()).await?;
                            session = target.to_owned();
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
                    _ => "Usage: /session delete <id>...|--all [-y|--yes].\n> ",
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
                let default_workspace = std::env::var("CRABBOT_ROOT").ok();
                let active = workspace
                    .as_deref()
                    .or(default_workspace.as_deref())
                    .unwrap_or("not configured");
                output.write_all(format!("Current workspace: {active}\n> ").as_bytes()).await?;
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
                            let message = if value == "reset" {
                                format!("Workspace reset to the configured default: {active}\n> ")
                            } else {
                                format!("Workspace changed to: {active}\n> ")
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

            if line == "/model" {
                output.write_all(format!("Current model: {model}.\n> ").as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if let Some(value) = line.strip_prefix("/model ") {
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
                    match read_session(&home, value, host).await {
                        Ok(view) => {
                            session = value.to_owned();
                            model = view.model.clone();
                            messages = view.messages.clone();
                            workspace = view.workspace.clone();
                            output
                                .write_all(format!("Using session {session}.\n> ").as_bytes())
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
                        serde_json::json!({"id": value, "model": model}),
                    )
                    .await
                    {
                        Ok(_) => {
                            let view = read_session(&home, value, host).await?;
                            session = value.to_owned();
                            model = view.model.clone();
                            messages = view.messages.clone();
                            workspace = view.workspace.clone();
                            output
                                .write_all(format!("Created session {session}.\n> ").as_bytes())
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

            let Some(active_process) = process.as_mut() else {
                output
                    .write_all(
                        b"No intelligence plugin is installed. Install a model plugin to send messages; session and help commands remain available.\n> ",
                    )
                    .await?;

                output.flush().await?;
                continue;
            };

            let reservation = host(
                home.clone(),
                "session.reserve".into(),
                serde_json::json!({"id": session}),
            )
            .await;

            let owner = match reservation {
                Ok(value) => value["owner"].as_str().map(str::to_owned),

                Err(error) => {
                    output
                        .write_all(format!("{}\n> ", sentence(error.to_string())).as_bytes())
                        .await?;

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
            let mut streamed = String::new();
            let response = {
                let call = active_process.call_stream_async(request, move |note| {
                    let sender = sender.clone();

                    async move {
                        sender.send(note).await.map_err(|_| {
                            crabbot_core::Error::Protocol("Terminal output is unavailable.".into())
                        })
                    }
                });

                tokio::pin!(call);

                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::GenerationStarted);
                }

                let mut reservation_refresh = tokio::time::interval(Duration::from_secs(30));
                reservation_refresh.tick().await;

                loop {
                    tokio::select! {
                        biased;

                        _ = interrupt_rx.recv(), if !interrupt_rx.is_closed() => break None,

                        _ = reservation_refresh.tick() => {
                            if host(
                                home.clone(),
                                "session.renew".into(),
                                serde_json::json!({"id": session, "owner": reservation.owner}),
                            )
                            .await
                            .is_err()
                            {
                                break None;
                            }
                        }

                        result = &mut call => break Some(result),

                        event = events.recv() => {
                            if let Some(text) = event.and_then(stream_text) {
                                streamed.push_str(&text);
                                output.write_all(text.as_bytes()).await?;
                                output.flush().await?;
                            }
                        }
                    }
                }
            };

            let Some(response) = response else {
                while let Ok(event) = events.try_recv() {
                    if let Some(text) = stream_text(event) {
                        streamed.push_str(&text);
                        output.write_all(text.as_bytes()).await?;
                    }
                }

                let mut truncated = false;

                if !streamed.is_empty() {
                    let (saved_text, was_truncated) = saveable_reply(&streamed);
                    truncated = was_truncated;

                    let assistant = Message {
                        id: message_id("assistant", sequence),
                        session: session.clone(),
                        role: Role::Assistant,
                        sender: None,
                        content: vec![Content::Text { text: saved_text }],
                    };

                    host(
                        home.clone(),
                        "session.append_reserved".into(),
                        serde_json::json!({"id": session, "owner": reservation.owner, "message": assistant}),
                    )
                    .await?;
                    messages.push(assistant);
                }

                reservation.release().await?;

                if let Some(process) = process.as_mut() {
                    process.restart().await?;
                }

                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::GenerationFinished { interrupted: true });
                }

                output.write_all(b"\nGeneration interrupted.").await?;

                if truncated {
                    output.write_all(b" Only the first 16 KiB was saved.").await?;
                }

                output.write_all(b"\n> ").await?;
                output.flush().await?;
                continue;
            };

            let response = match response {
                Ok(response) => response,

                Err(error) => {
                    let _ = reservation.release().await;
                    return Err(error);
                }
            };

            while let Ok(event) = events.try_recv() {
                if let Some(text) = stream_text(event) {
                    streamed.push_str(&text);
                    output.write_all(text.as_bytes()).await?;
                }
            }

            if let Some(error) = response.error {
                let _ = reservation.release().await;
                return Err(crabbot_core::Error::Denied(error.message));
            }

            let Some(value) = response.result else {
                let _ = reservation.release().await;
                return Err(crabbot_core::Error::Denied(
                    "The intelligence plugin returned no result.".into(),
                ));
            };

            let reply: crabbot_core::types::ModelReply = serde_json::from_value(value)?;
            let (saved_text, truncated) = saveable_reply(&reply.text);
            let assistant = Message {
                id: message_id("assistant", sequence),
                session: session.clone(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: saved_text }],
            };

            let saved = host(
                home.clone(),
                "session.append_reserved".into(),
                serde_json::json!({"id": session, "owner": reservation.owner, "message": assistant}),
            )
            .await;
            messages.push(assistant);

            reservation.release().await?;

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

            if truncated {
                output.write_all(b"\nOnly the first 16 KiB of the reply was saved.").await?;
            }

            output.write_all(b"\n> ").await?;
            output.flush().await?;

            if let Some(events) = &engine_events {
                let _ = events.send(EngineEvent::GenerationFinished { interrupted: false });
            }
        }

        Ok::<(), crabbot_core::Error>(())
    }
    .await;

    let stopped = match process {
        Some(process) => process.stop().await,
        None => Ok(()),
    };

    result?;
    stopped
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
    read_session(home, id, host).await
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
    Ok(ui::SessionView { id: id.to_owned(), model, workspace, messages })
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
    let mut ids = targets.ids;

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

        match host(home.to_owned(), method.into(), serde_json::json!({"id": id})).await {
            Ok(result) if action == "delete" || result["changed"] != false => {
                completed.push(id);
            }

            Ok(_) => already.push(id),
            Err(error) => failures.push(format!("{id}: {}", sentence(error.to_string()))),
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
        summary.push_str(&format!(". Kept active session {active}"));
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

fn plugin_binary_path(home: &str, id: &str) -> std::path::PathBuf {
    std::path::Path::new(home)
        .join("plugins")
        .join(id)
        .join("bin")
        .join(crabbot_core::plugin::binary_name(id))
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
    let mut help = String::from(
        "Commands:\n  /help                      Show commands available in this session.\n  /status                    Show whether the background runtime is running.\n  /plugins                   List installed plugins.\n  /session help              Show session commands.\n  /new <id>                  Create and switch to a session.\n  /workspace [path|reset]    Show or change this session's filesystem root.\n  /clear                     Clear this session's conversation.\n  /statusline [format|reset] Show, configure, or reset the bottom statusline.\n  /animation [on|off]        Show or configure typewriter animation.\n  /quit, /exit               Leave the TUI.\n",
    );
    let mut conditional = Vec::new();

    if has_model {
        conditional.push("  /model [id]              Show or change the model ID.");
    }

    if daemon && has_capability(home, "tool") {
        conditional.extend([
            "  /approval                Show approval policy.",
            "  /approvals               List pending tool approvals.",
            "  /approve <id>            Approve a pending tool action.",
            "  /deny <id>               Deny a pending tool action.",
        ]);
    }

    if daemon && has_capability(home, "channel") {
        conditional.extend([
            "  /deliveries              List pending channel deliveries.",
            "  /retry <id>              Retry a delivery.",
            "  /drop <id>               Drop a delivery.",
        ]);
    }

    if daemon && has_capability(home, "timer") {
        conditional.push("  /timer <list|add|remove> Manage timers.");
    }

    if daemon && has_capability(home, "memory") {
        conditional.push("  /memory <list|remember|forget> Manage memories.");
    }

    if !conditional.is_empty() {
        help.push_str("\nConditional Commands (shown only when usable):\n");
        help.push_str(&conditional.join("\n"));
        help.push('\n');
    }

    help.push_str(
        "\nPlugin commands are CLI commands; run them as `crab <command>`.\nPage Up/Down scroll the conversation by a page; the mouse wheel scrolls the pane under the pointer.\nUse Ctrl+O for a new message line.\n> ",
    );
    help
}

fn session_help() -> &'static str {
    "Session commands:\n  /session help                       Show these commands.\n  /session list [page]                List sessions, 10 per page; active first, then recent.\n  /session create <id>                Create and switch to a session.\n  /session switch <id>                Switch to a saved session.\n  /session rename <new-id>            Rename the active session.\n  /session archive <id>...|--all      Archive saved sessions.\n  /session unarchive <id>...|--all    Restore archived sessions.\n  /session delete <id>...|--all [-y]  Permanently delete sessions.\n  /new <id>                           Create and switch to a session (shortcut).\n> "
}

#[cfg(test)]
async fn local_aware(home: String, method: String, params: Value) -> crabbot_core::Result<Value> {
    match control(&home, &method, params.clone()).await {
        Ok(value) => Ok(value),

        Err(error) if method.starts_with("session.") && error.is_unavailable() => {
            offline::control(&home, &method, params).await
        }

        Err(error) => Err(error.into_core()),
    }
}

#[derive(Clone, Copy)]
enum SessionBackend {
    Daemon,
    Offline,
}

async fn select_session_backend(
    home: &str,
    session: &str,
    model: &str,
) -> crabbot_core::Result<SessionBackend> {
    let params = serde_json::json!({"id": session, "model": model});

    match control(home, "session.ensure", params.clone()).await {
        Ok(_) => Ok(SessionBackend::Daemon),

        Err(error) if error.is_unavailable() => {
            offline::control(home, "session.ensure", params).await?;
            Ok(SessionBackend::Offline)
        }

        Err(error) => Err(error.into_core()),
    }
}

async fn control_with_backend<C, F>(
    backend: SessionBackend,
    home: String,
    method: String,
    params: Value,
    daemon: C,
) -> crabbot_core::Result<Value>
where
    C: Fn(String, String, Value) -> F,
    F: Future<Output = crabbot_core::Result<Value>>,
{
    if method.starts_with("session.")
        && let SessionBackend::Offline = backend
    {
        return offline::control(&home, &method, params).await;
    }

    daemon(home, method, params).await
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

        match (response.result, response.error) {
            (Some(value), _) => Ok(value),

            (_, Some(error)) => {
                Err(ControlError::Failed(crabbot_core::Error::Denied(error.message)))
            }

            _ => Err(ControlError::Failed(crabbot_core::Error::Denied(
                "The daemon returned an empty response.".into(),
            ))),
        }
    };

    timeout(Duration::from_secs(5), exchange).await.map_err(|_| {
        ControlError::Failed(crabbot_core::Error::Denied(
            "The daemon did not respond in time.".into(),
        ))
    })?
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
        .filter_map(|item| {
            let id = item["id"].as_str()?;
            let state = SessionState::from_item(item, active_session);
            let status = state.label();

            let model = if model_available {
                item["model"].as_str().filter(|model| !model.is_empty()).unwrap_or("unset")
            } else {
                "unset"
            };

            let marker = state.marker();

            let mut details = vec![format!("model: {model}")];

            if let Some(created) = item["created"].as_u64().and_then(date::format_date) {
                details.push(format!("created: {created}"));
            }

            if let Some(updated) = item["updated"].as_u64().and_then(date::format_date) {
                details.push(format!("updated: {updated}"));
            }

            if let Some(messages) = item["messages"].as_u64() {
                details.push(format!("{messages} messages"));
            }

            Some(format!("{marker} {id} · {status}\n  {}", details.join(" · ")))
        })
        .collect::<Vec<_>>();

    if sessions.is_empty() {
        "Sessions: none.\n> ".into()
    } else {
        format!(
            "Sessions ({}-{} of {total}, page {page}/{pages}):\n{}\n> ",
            start + 1,
            end,
            sessions.join("\n")
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

    fn marker(&self) -> &'static str {
        match self.label() {
            "active" | "working" => "*",
            "archived" => "-",
            _ => "o",
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

fn format_plugins(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "No plugins installed.".into();
    };

    if items.is_empty() {
        return "No plugins installed.".into();
    }

    let mut output = format!("Installed plugins ({}):", items.len());

    for item in items {
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

        output.push_str(&format!(
            "\n- {id} {version}\n  health: {health}\n  protocol: {protocol}\n  capabilities: {capabilities}\n  commands: {commands}\n  permissions: {permissions}"
        ));
    }

    output
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
    let value = value.trim();
    let value = value.strip_prefix("Denied: ").unwrap_or(value);
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
    fn describes_client_capability() {
        let hello = super::hello();

        assert_eq!(hello.id, "tui");
        assert_eq!(hello.capabilities, vec![crabbot_core::types::Capability::Client]);
        assert_eq!(hello.commands[0].name, "tui");
        assert!(hello.commands[0].description.contains("do not require the background runtime"));
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
        assert_eq!(super::selected_session_id(None), "default");
        assert_eq!(super::selected_session_id(Some("work".into())), "work");
    }

    #[test]
    fn defaults_generation_to_the_documented_model() {
        assert_eq!(super::DEFAULT_MODEL, "gpt-6-luna");
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
        assert!(super::parse_session_targets("--all one", true).is_err());
        assert!(super::parse_session_targets("one one", true).is_err());
        assert!(super::parse_session_targets("-y", true).is_err());
        assert!(super::parse_session_targets("-y", false).is_err());
    }

    #[tokio::test]
    async fn bulk_session_actions_preserve_the_active_session() {
        let targets = super::parse_session_targets("--all", false).unwrap();
        let result =
            super::apply_session_action("/tmp", "archive", targets, "other", mock_control).await;

        assert_eq!(result, "Archived session saved.");

        let targets = super::parse_session_targets("--all -y", true).unwrap();
        let result =
            super::apply_session_action("/tmp", "delete", targets, "saved", mock_control).await;

        assert_eq!(result, "No sessions were deleted. Kept active session saved.");
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
        assert!(!basic.contains("/model [id]"));
        assert!(!basic.contains("/deliveries"));
        assert!(basic.contains("Plugin commands are CLI commands"));
        assert!(basic.contains("Page Up/Down scroll the conversation by a page"));
        assert!(basic.contains("Use Ctrl+O for a new message line."));
        assert!(!basic.contains("Shift+Enter"));

        let description_columns = basic
            .lines()
            .filter_map(|line| {
                if line.contains("Show or change this session's filesystem root.")
                    || line.contains("Show, configure, or reset the bottom statusline.")
                {
                    line.find("Show")
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(description_columns.len(), 2);
        assert_eq!(description_columns[0], description_columns[1]);

        let sessions = super::session_help();

        assert!(sessions.contains("/session archive <id>...|--all"));
        assert!(sessions.contains("/session unarchive <id>...|--all"));
        assert!(sessions.contains("/session rename <new-id>"));
        assert!(sessions.contains("/session delete <id>...|--all [-y]"));
        assert!(sessions.contains("/new <id>"));
        assert!(!sessions.contains("/sessions"));
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
                    "id": "one",
                    "model": "provider/model",
                    "status": "idle",
                    "created": 1735776000,
                    "updated": 1735862400,
                    "messages": 3
                }]
            }),
            "one",
            true,
            1,
        );

        assert_eq!(
            text,
            "Sessions (1-1 of 1, page 1/1):\n* one · active\n  model: provider/model · created: 2025-01-02 · updated: 2025-01-03 · 3 messages\n> "
        );

        assert_eq!(
            super::format_sessions(&serde_json::json!({"items": [{}]}), "", false, 1),
            "Sessions: none.\n> "
        );

        assert_eq!(
            super::format_sessions(
                &serde_json::json!({
                    "items": [{"id": "old", "status": "idle", "archived": true}]
                }),
                "",
                false,
                1
            ),
            "Sessions (1-1 of 1, page 1/1):\n- old · archived\n  model: unset\n> "
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
                {"id": "active", "status": "idle", "updated": 1},
                {"id": "working", "status": "working", "updated": 2},
                {"id": "idle-old", "status": "idle", "updated": 3},
                {"id": "idle-new", "status": "idle", "updated": 30},
                {"id": "archived", "status": "idle", "archived": true, "updated": 100},
                {"id": "idle-04", "status": "idle", "updated": 4},
                {"id": "idle-05", "status": "idle", "updated": 5},
                {"id": "idle-06", "status": "idle", "updated": 6},
                {"id": "idle-07", "status": "idle", "updated": 7},
                {"id": "idle-08", "status": "idle", "updated": 8},
                {"id": "idle-09", "status": "idle", "updated": 9},
            ]
        });

        let first_page = super::format_sessions(&value, "active", false, 1);
        let working_position = first_page.find("working · working").unwrap();
        let active_position = first_page.find("active · active").unwrap();
        let recent_position = first_page.find("idle-new · idle").unwrap();
        let older_position = first_page.find("idle-old · idle").unwrap();

        assert!(working_position < active_position);
        assert!(active_position < recent_position);
        assert!(recent_position < older_position);
        assert!(first_page.contains("Sessions (1-10 of 11, page 1/2):"));
        assert!(!first_page.contains("archived · archived"));

        let second_page = super::format_sessions(&value, "active", false, 2);

        assert!(second_page.contains("Sessions (11-11 of 11, page 2/2):"));
        assert!(second_page.contains("archived · archived"));
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
            super::format_plugins(&serde_json::json!({
                "items": [{
                    "id": "codex",
                    "version": "1.2.3",
                    "health": "ready",
                    "protocol": {"major": 0, "minor": 1},
                    "capabilities": ["model", "vision"],
                    "commands": [{"name": "codex"}],
                    "permissions": ["network", "process"]
                }]
            })),
            "Installed plugins (1):\n- codex 1.2.3\n  health: ready\n  protocol: 0.1\n  capabilities: model, vision\n  commands: codex\n  permissions: network, process"
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

        assert_eq!(super::sentence("Denied: Session was not found.".into()), "Session not found");

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
            "session.ensure",
            "session.new",
            "session.append",
            "session.append_reserved",
            "session.reserve",
            "session.renew",
            "session.release",
            "session.clear",
            "session.model",
            "session.rename",
            "session.delete",
        ]
        .contains(&method.as_str())
        {
            if method == "session.reserve" {
                return Ok(json!({"id": params["id"], "owner": "mock-reservation-owner"}));
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

            "approval.resolve" => Ok(json!({"resolved": true, "approved": params["approved"]})),
            // The fixture uses 2025-01-02 and 2025-01-03 UTC timestamps in Unix seconds.
            "session.list" => Ok(json!({
                "items": [{
                    "id": "saved",
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
                if method == "session.unarchive" && params["id"] == "missing" {
                    Err(crabbot_core::Error::Denied("Session was not found.".into()))
                } else {
                    Ok(json!({"id": params["id"], "changed": true}))
                }
            }

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
                "/help\n/status\n/approval\n/approvals\n/approve 0123456789abcdef01234567\n/deny 0123456789abcdef01234567\n/session list\n/session help\n/deliveries\n/retry bad id\n/drop bad id\n/plugins\n/workspace\n/workspace {}\n/workspace reset\n/timer list\n/timer add 30 break\n/timer remove 7\n/memory list\n/memory remember drink=tea\n/memory forget drink\n/model \n/model test\n/session bad!\n/new two\n/session create test-one\n/session one\n/session rename renamed\n/session archive two\n/session unarchive two\n/session unarchive missing\n/session delete two -y\n/session delete renamed -y\n/session archive renamed\n/clear\n\nhello\n/quit\n",
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
            None,
            None,
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
        assert!(text.contains("Installed plugins (1):\n- codex unknown\n  health: ready"));
        assert!(text.contains("Created session two."));
        assert!(text.contains("Created session test-one."));
        assert!(text.contains("Sessions (1-1 of 1, page 1/1):\no saved · idle\n  model: provider/model · created: 2025-01-02 · updated: 2025-01-03 · 2 messages"));
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
        )
        .await
        .unwrap();

        assert_eq!(one_shot, "Reply");

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
            super::local_aware,
            None,
            Some(engine_event_tx),
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

        while !String::from_utf8_lossy(&output).contains("Partial") {
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                output_rx.read(&mut buffer),
            )
            .await
            .unwrap()
            .unwrap();
            output.extend_from_slice(&buffer[..count]);
        }

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

        assert!(String::from_utf8_lossy(&output).contains("Partial"));
        assert!(!String::from_utf8_lossy(&output).contains("Partial answer"));
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

        assert_eq!(saved.messages.len(), 2);
        assert_eq!(saved.messages[1].content[0].render(), "Partial");

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
                session: "offline".into(),
            },
            super::local_aware,
            None,
            None,
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
        assert!(text.contains("* offline · active\n  model: unset · created:"));
        assert!(super::data::plugin_file(&root, "tui", "sessions.json").is_file());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn keeps_the_offline_session_backend_when_daemon_becomes_available() {
        let root = std::env::temp_dir().join(format!("crabbot-tui-backend-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let home = root.display().to_string();
        let backend = super::select_session_backend(&home, "offline", "small").await.unwrap();

        async fn available_daemon(
            _home: String,
            _method: String,
            _params: serde_json::Value,
        ) -> crabbot_core::Result<serde_json::Value> {
            Ok(serde_json::json!({"items": [{"id": "daemon"}]}))
        }

        let sessions = super::control_with_backend(
            backend,
            home,
            "session.list".into(),
            serde_json::json!({}),
            available_daemon,
        )
        .await
        .unwrap();

        assert_eq!(sessions["items"][0]["id"], "offline");

        let _ = std::fs::remove_dir_all(root);
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
            None,
            None,
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
