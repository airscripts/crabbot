#![forbid(unsafe_code)]

mod data;
mod date;
#[cfg(test)]
mod offline;
mod ui;

#[cfg(test)]
use krabb_core::plugin::Process;
use krabb_core::{
    jsonl,
    plugin::serve_with,
    types::{
        Capability, CommandSpec, Content, Hello, IpcRequest, IpcResponse, Message, Protocol,
        Request, Response, Role,
    },
};

#[cfg(test)]
use krabb_core::types::ToolSpec;
use serde_json::Value;
#[cfg(test)]
use std::sync::Arc;
use std::{
    future::Future,
    io::{BufRead, Read},
    path::PathBuf,
    pin::Pin,
};

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, duplex};

use tokio::net::TcpStream;
#[cfg(test)]
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

const DEFAULT_MODEL: &str = "unset";
const DEFAULT_NAME: &str = "Krabb";
const DEFAULT_SESSION_ID: &str = "default";
const SESSION_LIST_PAGE_SIZE: usize = 5;
const PLUGIN_LIST_PAGE_SIZE: usize = 5;
const SCHEDULE_HISTORY_PAGE_SIZE: usize = 5;
const STREAMING_TURN_TIMEOUT: Duration = Duration::from_secs(310);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(305);
#[cfg(test)]
const SAVED_REPLY_LIMIT: usize = 16 * 1024;
// 365 days (one year), expressed in seconds.
const MAX_SCHEDULE_DELAY_SECONDS: u64 = 31_536_000;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.first().is_some_and(|value| value == "--krabb-cli") {
        let command_args = &args[1..];
        let json = command_args.iter().any(|value| value == "--json");
        let debug = command_args.iter().any(|value| value == "--debug");
        let verbose = command_args.iter().any(|value| value == "--verbose");
        let verbosity = if debug {
            krabb_log::Verbosity::Debug
        } else if verbose {
            krabb_log::Verbosity::Verbose
        } else {
            krabb_log::Verbosity::Quiet
        };

        krabb_log::initialize(krabb_log::Mode::Command { verbosity, json });

        return match run_cli(&args[1..]).await {
            Ok(()) => std::process::ExitCode::SUCCESS,

            Err(error) => {
                eprintln!("{}", cli_error_message(&error));
                std::process::ExitCode::FAILURE
            }
        };
    }

    krabb_log::run_plugin("tui", serve_with(hello(), call)).await
}

fn cli_error_message(error: &krabb_core::Error) -> String {
    match error {
        krabb_core::Error::Denied(message) => message.clone(),
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
            description: "Open the TUI; the Krabb daemon must be running.".into(),
            interactive: true,
        }],
    }
}

async fn call(request: Request) -> krabb_core::Result<Option<Response>> {
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
    RequestFailed(String),
    SystemNotice(String),
    AssistantFinished,
    GenerationFinished { interrupted: bool },
}

type StreamingHost = fn(
    String,
    String,
    Value,
    mpsc::UnboundedSender<EngineEvent>,
) -> Pin<Box<dyn Future<Output = krabb_core::Result<Value>> + Send>>;

#[derive(Default)]
struct EngineEvents {
    session: Option<mpsc::UnboundedSender<ui::SessionView>>,
    engine: Option<mpsc::UnboundedSender<EngineEvent>>,
    streaming_host: Option<StreamingHost>,
    stop_after_generation: bool,
}

struct SessionReservation {
    release: Option<Pin<Box<dyn Future<Output = krabb_core::Result<Value>> + Send>>>,
    owner: String,
}

impl SessionReservation {
    fn new<C, F>(home: String, session: String, owner: String, host: C) -> Self
    where
        C: Fn(String, String, Value) -> F + Copy + Send + 'static,
        F: Future<Output = krabb_core::Result<Value>> + Send + 'static,
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

    async fn release(&mut self) -> krabb_core::Result<Value> {
        self.release
            .take()
            .ok_or_else(|| krabb_core::Error::Denied("Session reservation was released.".into()))?
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

fn parse_options(args: &[String]) -> krabb_core::Result<TuiOptions> {
    let mut options = TuiOptions::default();

    let mut index = 0;

    while index < args.len() {
        let name = args[index].as_str();
        index += 1;

        let value = match name {
            "--once" | "--session" | "--model" | "--plugin" => {
                let value = args.get(index).ok_or_else(|| {
                    krabb_core::Error::Denied(format!("{name} requires a value."))
                })?;
                index += 1;
                value.clone()
            }

            _ => {
                return Err(krabb_core::Error::Denied(format!(
                    "Unknown TUI option {name}. Use `krabb tui --help` for usage."
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
        return Err(krabb_core::Error::Denied("--once requires a non-empty prompt.".into()));
    }

    if options.session.as_deref().is_some_and(|id| !valid_session(id)) {
        return Err(krabb_core::Error::Denied("Session ID is invalid.".into()));
    }

    Ok(options)
}

async fn run(args: &[String]) -> krabb_core::Result<Option<String>> {
    let mut options = parse_options(args)?;

    if let Some(prompt) = options.once.take() {
        return run_one_shot(options, prompt).await.map(Some);
    }

    run_interactive(options).await?;

    Ok(None)
}

async fn run_cli(args: &[String]) -> krabb_core::Result<()> {
    let mut options = parse_options(args)?;

    if let Some(prompt) = options.once.take() {
        let output = run_one_shot(options, prompt).await?;
        println!("{output}");

        return Ok(());
    }

    run_interactive(options).await
}

async fn run_one_shot(options: TuiOptions, prompt: String) -> krabb_core::Result<String> {
    let model_override = options.model.clone();
    let plugin = options
        .plugin
        .or_else(|| std::env::var("KRABB_MODEL_PLUGIN").ok())
        .unwrap_or_else(|| "codex".into());

    let model = options
        .model
        .or_else(|| std::env::var("KRABB_MODEL").ok())
        .unwrap_or_else(|| DEFAULT_MODEL.into());

    let home = std::env::var("KRABB_HOME").unwrap_or_else(|_| ".config/krabb".into());

    require_daemon(&home).await?;

    let temporary_session = options.session.is_none();
    let session = match options.session {
        Some(session) => selected_session_id(Some(session)),
        None => temporary_run_namespace()?,
    };

    let result = run_once(
        home.clone(),
        plugin,
        model,
        model_override,
        session.clone(),
        prompt,
        daemon_control,
    )
    .await;

    if temporary_session {
        let cleanup = daemon_control(
            home,
            "session.delete".into(),
            serde_json::json!({"id": session, "deep": true}),
        )
        .await;

        if result.is_ok() {
            cleanup?;
        }
    }

    result
}

async fn run_interactive(options: TuiOptions) -> krabb_core::Result<()> {
    let model_override = options.model.clone();
    let plugin = options
        .plugin
        .or_else(|| std::env::var("KRABB_MODEL_PLUGIN").ok())
        .unwrap_or_else(|| "codex".into());

    let model = options
        .model
        .or_else(|| std::env::var("KRABB_MODEL").ok())
        .unwrap_or_else(|| DEFAULT_MODEL.into());

    let home = std::env::var("KRABB_HOME").unwrap_or_else(|_| ".config/krabb".into());

    require_daemon(&home).await?;

    let session = resolve_session_id(&home, options.session, daemon_control).await?;

    let input = terminal("r")?;
    let output = terminal("w")?;
    run_at(input, output, home, plugin, model, model_override, session).await
}

fn selected_session_id(session: Option<String>) -> String {
    tui_session_id(&session.unwrap_or_else(|| DEFAULT_SESSION_ID.into()))
}

fn temporary_run_namespace() -> krabb_core::Result<String> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| {
        krabb_core::Error::Denied(format!("Could not create a temporary run namespace: {error}."))
    })?;

    let suffix = random.iter().map(|byte| format!("{byte:02x}")).collect::<String>();

    Ok(format!("tui-once-{suffix}"))
}

async fn resolve_session_id<C, F>(
    home: &str,
    session: Option<String>,
    host: C,
) -> krabb_core::Result<String>
where
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = krabb_core::Result<Value>> + Send + 'static,
{
    if session.is_some() {
        return Ok(selected_session_id(session));
    }

    let response = host(home.to_owned(), "session.list".into(), Value::Null).await?;
    let state_path = data::plugin_file(home, "tui", "last-session.json");
    let last_session = data::load_last_session(&state_path);

    Ok(resume_session_id(&response, last_session.as_deref()))
}

fn resume_session_id(sessions: &Value, last_session: Option<&str>) -> String {
    let Some(items) = sessions["items"].as_array() else {
        return selected_session_id(None);
    };

    let available = |item: &&Value| {
        item["id"]
            .as_str()
            .is_some_and(|id| tui_session_name(id).is_some() && item["archived"] != true)
    };

    if let Some(last_session) = last_session
        && items.iter().filter(available).any(|item| item["id"] == last_session)
    {
        return last_session.to_owned();
    }

    items
        .iter()
        .filter(available)
        .max_by_key(|item| item["updated"].as_u64().unwrap_or_default())
        .and_then(|item| item["id"].as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| selected_session_id(None))
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
) -> krabb_core::Result<()> {
    let name = std::env::var("KRABB_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());
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
) -> krabb_core::Result<String>
where
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = krabb_core::Result<Value>> + Send + 'static,
{
    let (mut input, engine_input) = duplex(16 * 1024);

    let (_output, engine_output) = duplex(64 * 1024);
    let (engine_events, mut event_rx) = mpsc::unbounded_channel();
    let (interrupt_tx, interrupt_rx) = mpsc::unbounded_channel();
    drop(interrupt_tx);

    let engine = tokio::spawn(run_with(
        engine_input,
        engine_output,
        EngineConfig { home, plugin, model, model_override, session },
        host,
        EngineEvents {
            engine: Some(engine_events),
            stop_after_generation: true,
            ..EngineEvents::default()
        },
        interrupt_rx,
    ));

    input.write_all(serde_json::to_string(&prompt)?.as_bytes()).await?;
    input.write_all(b"\n").await?;

    engine.await.map_err(|error| krabb_core::Error::Denied(error.to_string()))??;

    let mut reply = String::new();
    let mut failed = false;

    while let Ok(event) = event_rx.try_recv() {
        match event {
            EngineEvent::AssistantText(text) => reply.push_str(&text),

            EngineEvent::RequestFailed(_) | EngineEvent::SystemNotice(_) => failed = true,

            _ => {}
        }
    }

    if failed {
        return Err(krabb_core::Error::Denied("The one-shot request did not complete.".into()));
    }

    Ok(reply)
}

async fn run_with<R, W, C, F>(
    input: R,
    mut output: W,
    config: EngineConfig,
    host: C,
    events: EngineEvents,
    mut interrupt_rx: mpsc::UnboundedReceiver<()>,
) -> krabb_core::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = krabb_core::Result<Value>> + Send + 'static,
{
    let EngineConfig { home, plugin, mut model, model_override, mut session } = config;
    let EngineEvents {
        session: session_events,
        engine: engine_events,
        streaming_host,
        stop_after_generation,
    } = events;
    let input = BufReader::new(input);
    let initial_session = ensure_session(&home, &session, &model, host).await?;
    let last_session_path = data::plugin_file(&home, "tui", "last-session.json");
    data::save_last_session(&last_session_path, &session)?;

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

    let bot_name = std::env::var("KRABB_NAME").unwrap_or_else(|_| DEFAULT_NAME.into());

    if engine_events.is_some() {
        output.write_all(b"> ").await?;
    } else {
        output
            .write_all(format!("{bot_name} terminal. Type /help for commands.\n> ").as_bytes())
            .await?;
    }

    output.flush().await?;

    if !model_plugin_available {
        let message = format!(
            "No intelligence plugin is installed at {plugin}. Install one to send prompts; session commands remain available."
        );

        if let Some(events) = &engine_events {
            let _ = events.send(EngineEvent::SystemNotice(message));
        } else {
            output.write_all(format!("{message}\n> ").as_bytes()).await?;
        }

        output.flush().await?;

        if stop_after_generation {
            return Ok(());
        }
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

            if line == "/schedule" || line.starts_with("/schedule ") {
                let command = line.strip_prefix("/schedule").unwrap_or_default().trim();
                let text = schedule_command(
                    home.clone(),
                    command,
                    session.clone(),
                    plugin.clone(),
                    model.clone(),
                    workspace.clone(),
                    host,
                )
                .await;

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

            if line == "/store" || line.starts_with("/store ") {
                let command = line.strip_prefix("/store").unwrap_or_default().trim();
                let text = store_command(home.clone(), session.clone(), command, host).await;

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/data" || line.starts_with("/data ") {
                let command = line.strip_prefix("/data").unwrap_or_default().trim();
                let text = data_command(home.clone(), session.clone(), command, host).await;

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/plugin" || line.starts_with("/plugin ") {
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

                    Err(()) => "Usage: /plugin [page].\n> ".into(),
                };

                output.write_all(text.as_bytes()).await?;
                output.flush().await?;
                continue;
            }

            if line == "/status" {
                let text = match host(home.clone(), "status".into(), serde_json::json!({})).await {
                    Ok(value) => format_status(&value),

                    Err(error) if daemon_unavailable(&error) => {
                        "Background runtime: stopped. Start it with `krabb service start`.\n> ".into()
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
                            data::save_last_session(&last_session_path, &session)?;
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
                    Err(krabb_core::Error::Denied(
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
                            data::save_last_session(&last_session_path, &session)?;
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
                            data::save_last_session(&last_session_path, &session)?;
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
                let message = "No model is selected. Use /model list, then /model set <id>.";

                if let Some(events) = &engine_events {
                    let _ = events.send(EngineEvent::SystemNotice(message.into()));
                } else {
                    output.write_all(format!("{message}\n> ").as_bytes()).await?;
                }

                output.flush().await?;

                if stop_after_generation {
                    break;
                }

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
                        "Krabb is already working in this session. Wait for the current turn to finish."
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
                return Err(krabb_core::Error::Denied(
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

                if stop_after_generation {
                    break;
                }

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
                Box<dyn Future<Output = krabb_core::Result<Value>> + Send>,
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
                        let Some(command) = incoming? else { break (Err(krabb_core::Error::Denied("Terminal input closed during generation".into())), true) };

                        let command = decode_terminal_input(&command);
                        let command = command.trim();

                        if command == "/quit" || command == "/exit" {
                            let _ = host(home.clone(), "session.cancel".into(), serde_json::json!({"id": session})).await;
                            break (Err(krabb_core::Error::Denied("Generation interrupted".into())), true);
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
                        break (Err(krabb_core::Error::Denied("Generation interrupted".into())), true);
                    }
                }
            };

            let _ = reservation.release().await;

            let request_succeeded = response.is_ok();

            match response {
                Ok(value) => {
                    let reply = value["text"].as_str().unwrap_or_default();

                    if !reply.is_empty() && !streaming_turn {
                        if let Some(events) = &engine_events {
                            let _ = events.send(EngineEvent::AssistantText(reply.to_owned()));
                        } else {
                            output.write_all(reply.as_bytes()).await?;
                            output.write_all(b"\n").await?;
                        }
                    }
                }

                Err(error) => {
                    if interrupted {
                        output.write_all(b"Generation interrupted.\n").await?;
                    } else if let Some(events) = &engine_events {
                        let message = format!(
                            "Request failed: {}.",
                            sentence(error.to_string())
                        );

                        let _ = events.send(EngineEvent::RequestFailed(message));
                    } else {
                        output
                            .write_all(format!("Request failed: {}.\n", sentence(error.to_string())).as_bytes())
                            .await?;
                    }
                }
            }

            if request_succeeded
                && streaming_turn
                && let Some(events) = &engine_events
            {
                let _ = events.send(EngineEvent::AssistantFinished);
            }

            if let Some(events) = &engine_events {
                let _ = events.send(EngineEvent::GenerationFinished { interrupted });
            }

            output.write_all(b"> ").await?;
            output.flush().await?;

            if stop_after_generation {
                break;
            }

            continue;

        }

        Ok::<(), krabb_core::Error>(())
    }
    .await;

    result?;
    Ok(())
}

fn workspace_root(home: &str, session_workspace: Option<&str>) -> PathBuf {
    session_workspace
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("KRABB_ROOT").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(home).join("workspace"))
}

fn resolve_workspace(value: &str) -> Result<Option<String>, &'static str> {
    let current = std::env::var_os("KRABB_TUI_LAUNCH_DIR").map(PathBuf::from).map_or_else(
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
) -> krabb_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    host(home.into(), "session.ensure".into(), serde_json::json!({"id": id, "model": model}))
        .await?;
    read_session_with_reservation(home, id, host, true).await
}

async fn read_session<C, F>(home: &str, id: &str, host: C) -> krabb_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    read_session_with_reservation(home, id, host, false).await
}

async fn read_session_reserved<C, F>(
    home: &str,
    id: &str,
    host: C,
) -> krabb_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    read_session_with_reservation(home, id, host, true).await
}

async fn read_session_with_reservation<C, F>(
    home: &str,
    id: &str,
    host: C,
    reserved: bool,
) -> krabb_core::Result<ui::SessionView>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    let value = host(home.into(), "session.get".into(), serde_json::json!({"id": id})).await?;

    if !reserved && (value["status"] == "working" || value["inflight"] == true) {
        return Err(krabb_core::Error::Denied("Session is already working.".into()));
    }

    let model = value["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| krabb_core::Error::Denied("Session has no model configured.".into()))?
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
    F: Future<Output = krabb_core::Result<Value>>,
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
        .join(krabb_core::plugin::binary_name(id))
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
) -> krabb_core::Result<Response> {
    let Request::Call { id, method, params, .. } = request else {
        return Err(krabb_core::Error::Protocol("Tool requests require an ID.".into()));
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

        std::fs::read_to_string(directory.join("krabb-plugin.toml"))
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
        ("/history list|clear|help", "Show or clear saved TUI input history."),
        ("/status", "Show whether the background runtime is running."),
        ("/plugin [page]", "Browse installed plugins."),
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

    if daemon && has_capability(home, "scheduler") {
        conditional.push(("/schedule", "Manage schedules."));
    }

    if daemon && has_capability(home, "memory") {
        conditional.push(("/memory", "Manage memories."));
    }

    if daemon && has_capability(home, "store") {
        conditional.push(("/data", "Manage session-scoped SQLite values and structured data."));
    }

    if !conditional.is_empty() {
        sections.push(format!(
            "Conditional commands (shown only when usable):\n{}",
            format_help_rows(&conditional)
        ));
    }

    sections.push(
        "Plugin commands run in the CLI: krabb <command>.\nPage Up/Down scroll the conversation; the mouse wheel scrolls the pane under the pointer. Hold Shift while dragging to select and copy text.\nCtrl+O adds a message line.".into(),
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

    format!("Manage sessions.\n\nCommands:\n{}\n> ", format_help_rows(&commands))
}

fn model_help() -> String {
    format!(
        "Manage the selected model.\n\nCommands:\n{}\n> ",
        format_help_rows(&[
            ("/model help", "Show this help."),
            ("/model list", "List Codex models (when Codex is selected)."),
            ("/model show", "Show the selected model."),
            ("/model set <id>", "Select a model."),
            ("/model <id>", "Select a model (shortcut)."),
        ])
    )
}

async fn compact_command<C, F>(
    home: &str,
    session: &str,
    plugin: &str,
    host: C,
) -> krabb_core::Result<(Option<(usize, usize)>, Option<String>, ui::SessionView)>
where
    C: Fn(String, String, Value) -> F + Copy + Send + Sync + 'static,
    F: Future<Output = krabb_core::Result<Value>> + Send + 'static,
{
    let reservation =
        host(home.to_owned(), "session.reserve".into(), serde_json::json!({"id": session})).await?;

    let owner = reservation["owner"].as_str().map(str::to_owned).ok_or_else(|| {
        krabb_core::Error::Denied("Session reservation returned no owner token.".into())
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
async fn local_aware(home: String, method: String, params: Value) -> krabb_core::Result<Value> {
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

async fn require_daemon(home: &str) -> krabb_core::Result<()> {
    control(home, "status", serde_json::json!({})).await.map(|_| ()).map_err(|error| match error {
        ControlError::Unavailable(_) => krabb_core::Error::Denied(
            concat!(
                "The Krabb daemon is not running.\n",
                "Start it with `krabb service start` or run `krabb-daemon`."
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
) -> krabb_core::Result<String>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
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

async fn daemon_control(home: String, method: String, params: Value) -> krabb_core::Result<Value> {
    control(&home, &method, params).await.map_err(ControlError::into_core)
}

enum ControlError {
    Unavailable(krabb_core::Error),
    Failed(krabb_core::Error),
}

impl From<krabb_core::Error> for ControlError {
    fn from(error: krabb_core::Error) -> Self {
        Self::Failed(error)
    }
}

impl ControlError {
    #[cfg(test)]
    fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }

    fn into_core(self) -> krabb_core::Error {
        match self {
            Self::Unavailable(error) | Self::Failed(error) => error,
        }
    }
}

fn unavailable_before_delivery(error: std::io::Error) -> ControlError {
    ControlError::Unavailable(error.into())
}

fn daemon_unavailable(error: &krabb_core::Error) -> bool {
    match error {
        krabb_core::Error::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::BrokenPipe
        ),
        krabb_core::Error::Denied(message) => message == "The daemon did not respond in time.",
        _ => false,
    }
}

async fn capability<C, F>(
    home: String,
    service: &str,
    method: &str,
    params: Value,
    host: C,
) -> krabb_core::Result<Value>
where
    C: Fn(String, String, Value) -> F,
    F: Future<Output = krabb_core::Result<Value>>,
{
    host(
        home,
        "capability.call".into(),
        serde_json::json!({"service": service, "method": method, "params": params}),
    )
    .await
}

async fn schedule_command<C, F>(
    home: String,
    command: &str,
    session: String,
    provider: String,
    model: String,
    workspace: Option<String>,
    host: C,
) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    if command.is_empty() || command == "help" {
        return schedule_help().into();
    }

    let mut parts = command.splitn(3, ' ');
    let action = parts.next().unwrap_or_default();
    let remaining = command.strip_prefix(action).unwrap_or_default().trim();
    let result = match action {
        "list" => {
            let requested = parts.next();

            if parts.next().is_some() {
                return "Usage: /schedule list [PAGE].\n> ".into();
            }

            let page = match requested.map(str::parse::<u64>).transpose() {
                Ok(Some(page)) if page > 0 => page,
                Ok(None) => 1,
                _ => return "Usage: /schedule list [PAGE].\n> ".into(),
            };

            capability(home, "scheduler", "list", serde_json::json!({"page": page}), host)
                .await
                .map(|value| format_schedules(&value))
        }

        "add" => match parse_schedule_add(remaining, &home) {
            Ok((mut params, instruction)) => {
                let id = schedule_id();
                params["id"] = Value::from(id);
                params["text"] = Value::String(instruction.clone());
                params["activate"] = Value::Bool(true);
                params["context"] = serde_json::json!({
                    "provider": provider.clone(),
                    "model": model.clone(),
                    "workspace": workspace.clone(),
                    "tools": configured_tui_tools(&home),
                    "shell": configured_tui_shell(&home),
                    "origin": {"channel": "tui", "session": session}
                });

                capability(home.clone(), "scheduler", "add", params, host)
                    .await
                    .map(|value| {
                        let next_run = value["due"]
                            .as_u64()
                            .map(|due| {
                                krabb_date::format_datetime_in_timezone(
                                    due,
                                    value["zone"].as_str().unwrap_or("UTC"),
                                )
                            })
                            .unwrap_or_else(|| "unknown time".into());
                        let name = value["name"].as_str().unwrap_or("Scheduled task");
                        let tools_access = if value["context"]["tools"] == true {
                            "enabled"
                        } else {
                            "disabled"
                        };

                        let shell_access = if value["context"]["tools"] == true
                            && value["context"]["shell"] == true
                        {
                            "enabled"
                        } else {
                            "disabled"
                        };

                        let frequency = if let Some(repeat) =
                            value["repeat"].as_u64().filter(|repeat| *repeat > 0)
                        {
                            format!("\nFrequency: every {}", krabb_date::format_duration(repeat))
                        } else if let Some(cron) = value["cron"].as_str() {
                            format!(
                                "\nFrequency: {cron} ({})",
                                value["zone"].as_str().unwrap_or("UTC")
                            )
                        } else {
                            String::new()
                        };

                        format!(
                            "Schedule {name} ({id}) is active.\n\nInstruction: {instruction}\nProvider: {provider}; model: {model}.\nWorkspace: {}\nTools: {tools_access}\nShell: {shell_access}\nNext run: {next_run}{frequency}\n\nCheck it with /schedule list; pause it with /schedule pause {id}.\n> ",
                            workspace.as_deref().unwrap_or("configured workspace")
                        )
                    })
            }

            Err(error) => Ok(format!("{error}\n> ")),
        },

        "history" => {
            let mut arguments = remaining.split_whitespace();
            let selector = arguments.next();
            let page_value = arguments.next();
            let page = match page_value.map(str::parse::<usize>).transpose() {
                Ok(Some(page)) if page > 0 => page,
                Ok(None) => 1,
                _ => return "Usage: /schedule history ID|NAME [PAGE].\n> ".into(),
            };

            if selector.is_none() || arguments.next().is_some() {
                Ok("Usage: /schedule history ID|NAME [PAGE].\n> ".into())
            } else {
                let selector = selector.unwrap_or_default();

                match schedule_history_selector(&home, selector, host).await {
                    Ok((id, name)) => read_schedule_history(&home, id, page)
                        .map(|(records, total, path)| {
                            if page > total.div_ceil(SCHEDULE_HISTORY_PAGE_SIZE).max(1) {
                                return format!(
                                    "Schedule history page {page} is out of range. There are {} page(s).\n> ",
                                    total.div_ceil(SCHEDULE_HISTORY_PAGE_SIZE).max(1)
                                );
                            }

                            format_schedule_history(&records, total, id, &name, page, &path)
                        })
                        .map_err(krabb_core::Error::Denied),
                    Err(error) => Ok(format!("{error}\n> ")),
                }
            }
        }

        "remove" => match parts.next() {
            Some(selector) if parts.next().is_none() => {
                match schedule_selector(&home, selector, host).await {
                    Ok((id, name)) => {
                        capability(home, "scheduler", "remove", serde_json::json!({"id": id}), host)
                            .await
                            .map(|value| {
                                if value["cancel_requested"] == true {
                                    format!("Cancellation requested for schedule {name} ({id}). Its current action will be interrupted; a side effect may already have happened.\n> ")
                                } else if value["deleted"] == true {
                                    format!("Schedule {name} ({id}) was removed.\n> ")
                                } else {
                                    format!("Schedule {name} ({id}) was not found.\n> ")
                                }
                            })
                    }

                    Err(error) => Ok(format!("{error}\n> ")),
                }
            }

            _ => Ok("Usage: /schedule remove <id-or-name>.\n> ".into()),
        },

        "approve" | "pause" | "resume" | "run" | "retry" => {
            let selector = parts.next();

            if selector.is_none() || parts.next().is_some() {
                Ok(format!("Usage: /schedule {action} ID|NAME.\n> "))
            } else {
                let selector = selector.unwrap_or_default();

                match schedule_selector(&home, selector, host).await {
                    Ok((id, name)) => capability(home, "scheduler", action, serde_json::json!({"id": id}), host)
                        .await
                        .map(|value| match action {
                            "run" => format!("Schedule {name} ({id}) queued to run now. Check its result with /schedule list.\n> "),
                            "retry" => format!("Schedule {name} ({id}) retry queued.\n> "),

                            _ => {
                                let status = value["status"].as_str().unwrap_or(action);
                                format!("Schedule {name} ({id}) is {status}.\n> ")
                            }
                        }),

                    Err(error) => Ok(format!("{error}\n> ")),
                }
            }
        }

        _ => Ok("Unknown schedule command. Run /schedule help for usage.\n> ".into()),
    };

    result.unwrap_or_else(|error| {
        format!("Scheduler action failed: {}.\n> ", sentence(error.to_string()))
    })
}

#[cfg(test)]
async fn schedule_selector_id<C, F>(home: &str, selector: &str, host: C) -> Result<u64, String>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    schedule_selector(home, selector, host).await.map(|(id, _)| id)
}

async fn schedule_selector<C, F>(
    home: &str,
    selector: &str,
    host: C,
) -> Result<(u64, String), String>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    let id = selector.parse::<u64>().ok();

    let mut page = 1;

    loop {
        let value = capability(
            home.to_owned(),
            "scheduler",
            "list",
            serde_json::json!({"page": page}),
            host,
        )
        .await
        .map_err(|error| error.to_string())?;
        let items =
            value["items"].as_array().ok_or("The scheduler returned an invalid task list.")?;

        if let Some(item) = items
            .iter()
            .find(|item| item["name"].as_str() == Some(selector) || item["id"].as_u64() == id)
        {
            let id = item["id"].as_u64().ok_or("The schedule has an invalid ID.")?;
            let name = item["name"].as_str().unwrap_or("Scheduled task").to_owned();

            return Ok((id, name));
        }

        if page >= value["pages"].as_u64().unwrap_or(1) {
            return Err(format!("No schedule named or numbered {selector} was found."));
        }

        page += 1;
    }
}

async fn schedule_history_selector<C, F>(
    home: &str,
    selector: &str,
    host: C,
) -> Result<(u64, String), String>
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    if let Ok(id) = selector.parse::<u64>() {
        return Ok((id, selector.to_owned()));
    }

    let live_error = match schedule_selector(home, selector, host).await {
        Ok(schedule) => return Ok(schedule),
        Err(error) => error,
    };

    let directory = PathBuf::from(home).join("logs/schedules");
    let prefix = format!("{selector}-");
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Err(live_error),
        Err(error) => return Err(format!("Could not list schedule history: {error}.")),
    };

    for entry in entries.filter_map(Result::ok) {
        let Some(filename) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };

        if let Some(id) = filename
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.strip_suffix(".jsonl"))
            .and_then(|value| value.parse::<u64>().ok())
        {
            return Ok((id, selector.to_owned()));
        }
    }

    Err(live_error)
}

fn read_schedule_history(
    home: &str,
    id: u64,
    page: usize,
) -> Result<(Vec<Value>, usize, PathBuf), String> {
    let directory = PathBuf::from(home).join("logs/schedules");
    let suffix = format!("-{id}.jsonl");
    let path = match std::fs::read_dir(&directory) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(&suffix))
            })
            .unwrap_or_else(|| directory.join(format!("{id}.jsonl"))),

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            directory.join(format!("{id}.jsonl"))
        }

        Err(error) => return Err(format!("Could not list schedule history: {error}.")),
    };

    let retained_limit = page
        .checked_mul(SCHEDULE_HISTORY_PAGE_SIZE)
        .ok_or_else(|| "Schedule history page is too large.".to_owned())?;
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), 0, path));
        }

        Err(error) => return Err(format!("Could not open schedule history: {error}.")),
    };

    let mut reader = std::io::BufReader::new(file);
    let mut line = Vec::new();
    let mut records = std::collections::VecDeque::new();
    let mut line_number = 0;
    let mut total = 0_usize;

    loop {
        line.clear();

        let read = (&mut reader)
            .take(64 * 1024 + 1)
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("Could not read schedule history: {error}."))?;

        if read == 0 {
            break;
        }

        line_number += 1;

        if read > 64 * 1024 {
            return Err(format!("Schedule history line {line_number} exceeds the 64 KiB limit."));
        }

        if line.last() == Some(&b'\n') {
            line.pop();
        }

        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }

        let text = std::str::from_utf8(&line).map_err(|error| {
            format!("Schedule history line {line_number} is not UTF-8: {error}.")
        })?;
        let record: Value = serde_json::from_str(text).map_err(|error| {
            format!("Schedule history line {line_number} is invalid JSON: {error}.")
        })?;

        if !record.is_object() {
            return Err(format!("Schedule history line {line_number} is not a JSON object."));
        }

        total += 1;

        if records.len() == retained_limit {
            records.pop_front();
        }

        records.push_back(record);
    }

    let skip = (page - 1).saturating_mul(SCHEDULE_HISTORY_PAGE_SIZE);
    let count = total.saturating_sub(skip).min(SCHEDULE_HISTORY_PAGE_SIZE);
    let start = records.len().saturating_sub(skip.saturating_add(count));
    let page_records = records.into_iter().skip(start).take(count).collect();

    Ok((page_records, total, path))
}

fn format_schedule_history(
    records: &[Value],
    total: usize,
    id: u64,
    name: &str,
    page: usize,
    path: &std::path::Path,
) -> String {
    let pages = total.div_ceil(SCHEDULE_HISTORY_PAGE_SIZE).max(1);
    let start = if total == 0 { 0 } else { (page - 1) * SCHEDULE_HISTORY_PAGE_SIZE + 1 };

    let end = (start + records.len()).saturating_sub(1).min(total);
    let name = records
        .last()
        .and_then(|record| record["schedule"]["name"].as_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(name);
    let mut output =
        format!("Schedule history: {name} | {start}–{end} of {total} | page {page}/{pages}:\n");

    if records.is_empty() {
        output.push_str("No execution history has been recorded.\n");
    } else {
        output.push('\n');
        let displayed = records.iter().rev().collect::<Vec<_>>();

        for (index, record) in displayed.iter().enumerate() {
            let run = &record["run"];
            let run_id =
                run["id"].as_u64().map_or_else(|| "Run".to_owned(), |id| format!("Run {id}"));
            let outcome = run["status"]
                .as_str()
                .or_else(|| record["status"].as_str())
                .map(schedule_outcome_label)
                .unwrap_or("outcome unknown");
            let mut details = Vec::new();

            if let Some(timestamp) = record["timestamp"].as_str() {
                let recorded = timestamp
                    .parse::<jiff::Timestamp>()
                    .ok()
                    .and_then(|timestamp| u64::try_from(timestamp.as_second()).ok())
                    .map(krabb_date::format_datetime)
                    .unwrap_or_else(|| schedule_history_text(timestamp));
                details.push(format!("Recorded: {recorded}"));
            }

            let timezone = record["schedule"]["timezone"].as_str().unwrap_or("UTC");

            for (field, label) in [
                ("scheduled_for", "Scheduled for"),
                ("next_run_at", "Next run"),
                ("retry_at", "Retry at"),
            ] {
                if let Some(timestamp) = run[field].as_u64() {
                    details.push(format!(
                        "{label}: {}",
                        krabb_date::format_datetime_in_timezone(timestamp, timezone)
                    ));
                }
            }

            if let Some(attempt) = run["attempt"].as_u64() {
                details.push(format!(
                    "Attempt: {attempt} (retries configured: {})",
                    run["retry_limit"].as_u64().unwrap_or_default()
                ));
            }

            if run["manual"] == true {
                details.push("Trigger: manual run".into());
            } else if run["retry"] == true {
                details.push("Trigger: manual retry".into());
            }

            if let Some(policy) = run["missed_policy"].as_str() {
                details.push(format!("Missed-run policy: {}", schedule_history_text(policy)));
            }

            if let Some(provider) = record["agent"]["provider"].as_str() {
                let model = record["agent"]["model"].as_str().map(schedule_history_text);
                details.push(format!(
                    "Agent: {}{}",
                    schedule_history_text(provider),
                    model.map_or_else(String::new, |model| format!(" ({model})"))
                ));
            }

            if let Some(workspace) = record["agent"]["workspace"].as_str() {
                details.push(format!("Workspace: {}", schedule_history_text(workspace)));
            }

            let result = schedule_history_text(record["result"].as_str().unwrap_or_default());
            details.push(if result.is_empty() {
                "Result: (no result text)".into()
            } else {
                format!("Result: {result}")
            });

            output.push_str(&format_tree_entry(
                &format!("{run_id} | {outcome}"),
                &details,
                index + 1 == displayed.len(),
            ));

            if index + 1 < displayed.len() {
                output.push_str("\n\n");
            }
        }
    }

    output.push_str(&format!(
        "\nRaw JSONL: {}\nUse `krabb schedule history {id} --raw` to print the raw log.\n> ",
        schedule_history_text(&path.display().to_string())
    ));
    output
}

fn schedule_outcome_label(outcome: &str) -> &'static str {
    match outcome {
        "completed" => "completed successfully",
        "retrying" => "failed; retry queued",
        "continuing" => "failed; next occurrence remains scheduled",
        "cancelled" => "cancelled",
        "skipped" => "skipped",
        _ => "failed; task paused",
    }
}

fn schedule_history_text(value: &str) -> String {
    value
        .chars()
        .filter_map(|character| {
            if character.is_control() {
                character.is_whitespace().then_some(' ')
            } else if character == '—' {
                Some('-')
            } else {
                Some(character)
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn schedule_help() -> &'static str {
    r#"Schedule autonomous agent work.

Commands:
  /schedule help                           Show this help
  /schedule list [PAGE]                    List schedules and latest outcomes
  /schedule history ID|NAME [PAGE]         Show schedule run history
  /schedule add TIMING [options] TEXT      Add a one-time or recurring task
  /schedule run ID|NAME                    Run a task now
  /schedule retry ID|NAME                  Retry a paused task
  /schedule pause ID|NAME                  Pause a task
  /schedule resume ID|NAME                 Resume a paused task
  /schedule remove ID|NAME                 Cancel future runs

Timing:
  --after DURATION                         Run after a delay, such as 5m or "1 month"
  --at DATE-TIME                           Run at "YYYY-MM-DD HH:MM" locally or RFC 3339
  --every DURATION                         Repeat at a fixed interval
  --cron 'FIELDS' --timezone AREA/CITY     Repeat on a cron schedule

Options:
  --timezone AREA/CITY                     Timezone for local --at and cron
  --name NAME                              Selectable name; no spaces
  --missed skip|coalesce|replay            Handle runs missed while offline
  --retries 0..5                           Retry reported failures with backoff
  --on-failure pause|continue              Recurring task policy (default: pause)

Notes:
  Durations accept s, m, h, d, w, mo, and y units, or written units. Months are
  30 days and years are 365 days. Quote --at values containing spaces. Local
  --at uses --timezone, the configured scheduler timezone, or the machine's timezone.
  Fixed intervals start after one interval unless --after or --at sets the first
  run. Replay runs at most five missed occurrences.
  You can also describe a task
  in natural language. Tasks capture the current provider, model, workspace,
  tools, and shell settings. Schedule names contain no spaces and can replace IDs
  in commands. Requests activate immediately without a separate approval step.
  Uncertain side effects pause for review.
> "#
}

fn parse_schedule_add(input: &str, home: &str) -> Result<(Value, String), String> {
    let mut tokens = schedule_tokens(input)?;

    if tokens.is_empty() {
        return Err(
            "Usage: /schedule add --after DURATION INSTRUCTION. Run /schedule help for details."
                .into(),
        );
    }

    if !tokens[0].starts_with("--") && !tokens.iter().any(|token| token.starts_with("--")) {
        let delay = parse_schedule_seconds(&tokens[0], "--after")?;

        if tokens.len() < 2 {
            return Err(
                "Schedule delay must be between 5 seconds and 1 year, with an instruction.".into(),
            );
        }

        tokens.splice(0..1, ["--after".into(), delay.to_string()]);
    }

    let mut after = None;
    let mut every = None;
    let mut at_input = None;
    let mut cron = None;
    let mut timezone = None;
    let mut missed = None;
    let mut retries = None;
    let mut on_failure = None;
    let mut schedule_name = None;
    let mut instruction_words = Vec::new();
    let mut index = 0;

    while index < tokens.len() {
        let name = tokens[index].as_str();
        index += 1;

        if name == "--" {
            instruction_words.extend_from_slice(&tokens[index..]);
            break;
        }

        if !name.starts_with("--") {
            instruction_words.push(name.to_owned());
            continue;
        }

        let value = tokens.get(index).ok_or_else(|| format!("{name} requires a value."))?.clone();
        index += 1;

        match name {
            "--after" => after = Some(parse_schedule_seconds(&value, name)?),
            "--every" => every = Some(parse_schedule_seconds(&value, name)?),

            "--at" => {
                at_input = Some(value);
            }

            "--cron" => cron = Some(value),
            "--timezone" => timezone = Some(value),

            "--missed" if matches!(value.as_str(), "skip" | "coalesce" | "replay") => {
                missed = Some(value)
            }

            "--missed" => return Err("--missed must be skip, coalesce, or replay.".into()),

            "--retries" => {
                let count = value
                    .parse::<u8>()
                    .map_err(|_| "--retries must be a number from 0 to 5.".to_owned())?;

                if count > 5 {
                    return Err("--retries must be a number from 0 to 5.".into());
                }

                retries = Some(count);
            }

            "--on-failure" if matches!(value.as_str(), "pause" | "continue") => {
                on_failure = Some(value)
            }

            "--on-failure" => return Err("--on-failure must be pause or continue.".into()),
            "--name" => schedule_name = Some(value),
            _ => return Err(format!("Unknown schedule option {name}. Run /schedule help.")),
        }
    }

    let instruction = instruction_words.join(" ").trim().to_owned();

    if instruction.is_empty() {
        return Err("A schedule instruction is required.".into());
    }

    if every.is_some() && cron.is_some() {
        return Err("Choose either --every or --cron for a recurring schedule.".into());
    }

    let selected_times = [
        after.is_some() || (every.is_some() && at_input.is_none()),
        at_input.is_some(),
        cron.is_some(),
    ]
    .into_iter()
    .filter(|selected| *selected)
    .count();

    if selected_times != 1 {
        return Err(
            "Choose one schedule time: --after, --at, or --cron; --every controls repetition."
                .into(),
        );
    }

    if on_failure.as_deref() == Some("continue") && every.is_none() && cron.is_none() {
        return Err("--on-failure continue requires a recurring schedule.".into());
    }

    let configured_timezone =
        std::fs::read_to_string(std::path::Path::new(home).join("config.toml"))
            .ok()
            .and_then(|source| toml::from_str::<toml::Value>(&source).ok())
            .and_then(|config| {
                config.get("scheduler")?.get("timezone")?.as_str().map(str::to_owned)
            });

    let timezone = timezone.or(configured_timezone);

    let at = at_input
        .as_deref()
        .map(|value| krabb_date::parse_datetime(value, timezone.as_deref()))
        .transpose()?;

    if at.is_some_and(|seconds| seconds < schedule_now().saturating_add(5)) {
        return Err("--at must be at least five seconds in the future.".into());
    }

    if cron.is_some() && timezone.is_none() {
        return Err("Cron schedules need --timezone or scheduler.timezone in config.toml.".into());
    }

    let delay = after.or_else(|| every.filter(|_| at.is_none()));
    let mut params = serde_json::json!({
        "delay": delay.unwrap_or_default(),
        "zone": timezone.unwrap_or_else(|| "UTC".into())
    });

    if let Some(every) = every {
        params["repeat"] = Value::from(every);
    }

    if let Some(at) = at {
        params["at"] = Value::from(at);
    }

    if let Some(cron) = cron {
        params["cron"] = Value::String(cron);
    }

    if let Some(missed) = missed {
        params["missed"] = Value::String(missed);
    }

    if let Some(retries) = retries {
        params["retries"] = Value::from(retries);
    }

    if let Some(on_failure) = on_failure {
        params["on_failure"] = Value::String(on_failure);
    }

    if let Some(name) = schedule_name {
        if name.chars().count() > 80 || !valid_schedule_name(name.trim()) {
            return Err("Schedule names must start with a letter or number and contain only letters, numbers, hyphens, underscores, or dots (up to 80 characters).".into());
        }

        params["name"] = Value::String(name.trim().to_owned());
    }

    Ok((params, instruction))
}

fn valid_schedule_name(name: &str) -> bool {
    let mut characters = name.chars();

    characters.next().is_some_and(|character| character.is_ascii_alphanumeric())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn parse_schedule_seconds(value: &str, option: &str) -> Result<u64, String> {
    let seconds = match value.parse::<u64>() {
        Ok(seconds) => seconds,
        Err(_) => krabb_date::parse_duration(value)
            .map_err(|_| format!("{option} needs a duration such as 5m or \"1 month\"."))?,
    };

    if !(5..=MAX_SCHEDULE_DELAY_SECONDS).contains(&seconds) {
        return Err(format!(
            "{option} must be between 5 and {MAX_SCHEDULE_DELAY_SECONDS} seconds."
        ));
    }

    Ok(seconds)
}

fn schedule_tokens(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;

    let mut characters = input.chars().peekable();

    while let Some(character) = characters.next() {
        if escaped {
            token.push(character);
            escaped = false;
            started = true;
            continue;
        }

        if character == '\\' {
            escaped = true;
            started = true;
            continue;
        }

        if let Some(active_quote) = quote {
            if character == active_quote
                && characters.peek().is_none_or(|next| next.is_whitespace())
            {
                quote = None;
            } else {
                token.push(character);
            }

            started = true;
            continue;
        }

        match character {
            '\'' | '"' if token.is_empty() => {
                quote = Some(character);
                started = true;
            }

            character if character.is_whitespace() => {
                if started {
                    tokens.push(std::mem::take(&mut token));
                    started = false;
                }
            }

            _ => {
                token.push(character);
                started = true;
            }
        }
    }

    if escaped {
        token.push('\\');
    }

    if quote.is_some() {
        return Err("Schedule arguments have an unclosed quote.".into());
    }

    if started {
        tokens.push(token);
    }

    Ok(tokens)
}

fn schedule_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn configured_tui_tools(home: &str) -> bool {
    has_capability(home, "tool")
        && std::fs::read_to_string(std::path::Path::new(home).join("config.toml"))
            .ok()
            .and_then(|text| toml::from_str::<Value>(&text).ok())
            .and_then(|config| config["clients"]["tui"]["tools"].as_bool())
            .unwrap_or(false)
}

fn configured_tui_shell(home: &str) -> bool {
    std::fs::read_to_string(std::path::Path::new(home).join("config.toml"))
        .ok()
        .and_then(|text| toml::from_str::<Value>(&text).ok())
        .and_then(|config| {
            config["clients"]["tui"]["shell"].as_bool().or_else(|| config["shell"].as_bool())
        })
        .unwrap_or(false)
}

async fn store_command<C, F>(home: String, session: String, command: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    if command.is_empty() || command == "help" {
        return store_help().into();
    }

    let result = if let Some(value) = command.strip_prefix("put ") {
        match value.split_once('=') {
            Some((key, value)) if !key.trim().is_empty() => store_value(
                home,
                session,
                "put",
                key.trim(),
                Some(Value::String(value.to_owned())),
                host,
            )
            .await
            .map(|_| "Stored session value.\n> ".into()),

            _ => Ok("Usage: /store put KEY=VALUE. Run /store help for details.\n> ".into()),
        }
    } else if let Some(key) = command.strip_prefix("get ") {
        store_value(home, session, "get", key.trim(), None, host).await.map(|value| {
            if value["found"] != true {
                return "No value is stored for that key.\n> ".into();
            }

            let rendered =
                serde_json::to_string_pretty(&value["value"]).unwrap_or_else(|_| "null".into());
            let mut shown = rendered.chars().take(4_096).collect::<String>();

            if rendered.chars().count() > 4_096 {
                shown.push_str("\n[value truncated]");
            }

            format!("{shown}\n> ")
        })
    } else if let Some(key) = command.strip_prefix("delete ") {
        store_value(home, session, "delete", key.trim(), None, host).await.map(|value| {
            if value["deleted"] == true {
                "Deleted session value.\n> ".into()
            } else {
                "No value is stored for that key.\n> ".into()
            }
        })
    } else {
        Ok("Unknown store command. Run /store help for usage.\n> ".into())
    };

    result.unwrap_or_else(|error| {
        format!("Store action failed: {}.\n> ", sentence(error.to_string()))
    })
}

async fn store_value<C, F>(
    home: String,
    session: String,
    method: &str,
    key: &str,
    value: Option<Value>,
    host: C,
) -> krabb_core::Result<Value>
where
    C: Fn(String, String, Value) -> F,
    F: Future<Output = krabb_core::Result<Value>>,
{
    if key.is_empty() {
        return Err(krabb_core::Error::Denied("A store key is required".into()));
    }

    if key.len() > 2_048 {
        return Err(krabb_core::Error::Denied("The store key exceeds the size limit".into()));
    }

    let mut params = serde_json::json!({"key": format!("tui:{session}:{key}")});

    if let Some(value) = value {
        params["value"] = value;
    }

    capability(home, "sqlite", method, params, host).await
}

fn store_help() -> &'static str {
    r#"`/store` is an alias for `/data value`.

Commands:
  /data value help                  Show this help
  /data value put KEY=VALUE         Save a text value
  /data value get KEY               Show a saved value
  /data value delete KEY            Delete a saved value

Keys are scoped to the active TUI session. Stored values persist in the local
SQLite database. Use `/data` for both simple values and structured records.
> "#
}

async fn data_command<C, F>(home: String, session: String, command: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    let args = command.split_whitespace().collect::<Vec<_>>();

    if args.first() == Some(&"value") {
        let result = store_command(home, session, &args[1..].join(" "), host).await;

        return result.replace("/store", "/data value");
    }

    let parse = |value: &str| {
        serde_json::from_str::<Value>(value)
            .map_err(|_| krabb_core::Error::Denied("Data arguments must contain valid JSON".into()))
    };

    let mut params = serde_json::json!({"scope": format!("tui:{session}")});

    let operation = match args.as_slice() {
        [] | ["help"] | ["--help"] | ["-h"] => return data_help().into(),
        ["database", "list"] => "list_databases",
        ["database", "create", _] => "create_database",
        ["database", "drop", _] => "drop_database",
        ["table", "list", _] => "list_tables",
        ["table", "create", _, _, ..] => "create_table",
        ["table", "describe", _, _] => "describe_table",
        ["column", "add", _, _, ..] => "add_column",
        ["column", "remove", _, _, _] => "drop_column",
        ["table", "drop", _, _] => "drop_table",
        ["insert", _, _, ..] => "insert",
        ["query", _, _] | ["query", _, _, ..] => "query",
        ["update", _, _, _, ..] => "update",
        ["delete", _, _, _] => "delete",
        _ => return "Unknown data command. Run /data help for usage.\n> ".into(),
    };

    params["operation"] = Value::String(operation.into());

    match args.as_slice() {
        ["database", "list"] => {}
        ["database", _, database] => params["database"] = Value::String((*database).into()),
        ["table", "list", database] => params["database"] = Value::String((*database).into()),

        ["table", "create", database, table, ..] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());

            let columns = if args.len() == 4 {
                Ok(Value::Array(Vec::new()))
            } else {
                parse(&args[4..].join(" "))
            };

            match columns {
                Ok(value) => params["columns"] = value,

                Err(error) => {
                    return format!("Data action failed: {}.\n> ", sentence(error.to_string()));
                }
            }
        }

        ["column", "add", database, table, ..] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());

            let column = match args.as_slice() {
                ["column", "add", _, _, name, kind] => {
                    Ok(serde_json::json!({"name": name, "type": column_type(kind)}))
                }

                ["column", "add", _, _, name, kind, "--required"] => Ok(serde_json::json!({
                    "name": name,
                    "type": column_type(kind),
                    "required": true
                })),

                _ => parse(&args[4..].join(" ")),
            };

            match column {
                Ok(value) => params["column"] = value,

                Err(error) => {
                    return format!("Data action failed: {}.\n> ", sentence(error.to_string()));
                }
            }
        }

        ["column", "remove", database, table, column] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
            params["column_name"] = Value::String((*column).into());
        }

        ["table", _, database, table] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
        }

        ["insert", database, table, ..] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());

            match parse(&args[3..].join(" ")) {
                Ok(value) => params["record"] = value,

                Err(error) => {
                    return format!("Data action failed: {}.\n> ", sentence(error.to_string()));
                }
            }
        }

        ["query", database, table] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
        }

        ["query", database, table, ..] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
            let after_id = args.last().and_then(|value| value.parse::<i64>().ok());
            let filter_end = if after_id.is_some() { args.len() - 1 } else { args.len() };

            match parse(&args[3..filter_end].join(" ")) {
                Ok(value) => params["filter"] = value,

                Err(error) => {
                    return format!("Data action failed: {}.\n> ", sentence(error.to_string()));
                }
            }

            if let Some(after_id) = after_id {
                params["after_id"] = Value::from(after_id);
            }
        }

        ["update", database, table, id, ..] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
            params["id"] = match id.parse::<i64>() {
                Ok(value) => Value::from(value),
                Err(_) => return "Data record ID must be a positive integer.\n> ".into(),
            };

            match parse(&args[4..].join(" ")) {
                Ok(value) => params["record"] = value,

                Err(error) => {
                    return format!("Data action failed: {}.\n> ", sentence(error.to_string()));
                }
            }
        }

        ["delete", database, table, id] => {
            params["database"] = Value::String((*database).into());
            params["table"] = Value::String((*table).into());
            params["id"] = match id.parse::<i64>() {
                Ok(value) => Value::from(value),
                Err(_) => return "Data record ID must be a positive integer.\n> ".into(),
            };
        }

        _ => return "Unknown data command. Run /data help for usage.\n> ".into(),
    }

    match capability(home, "sqlite", "data", params, host).await {
        Ok(value) => {
            let rendered = format_data_output(&args, &value);
            let shown = rendered.chars().take(4_096).collect::<String>();

            if rendered.chars().count() > 4_096 {
                format!("{shown}\n[data output truncated]\n> ")
            } else {
                format!("{shown}\n> ")
            }
        }

        Err(error) => format!("Data action failed: {}.\n> ", sentence(error.to_string())),
    }
}

fn format_data_output(args: &[&str], value: &Value) -> String {
    match args {
        ["database", "list"] => {
            let Some(databases) = value["databases"].as_array() else {
                return value.to_string();
            };

            if databases.is_empty() {
                "No databases found.".into()
            } else {
                format!(
                    "Databases:\n{}",
                    format_data_tree_list(
                        &databases
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    )
                )
            }
        }

        ["database", "create", database] => {
            if value["created"] == true {
                format!("Created database {database}.")
            } else {
                format!("Database {database} already exists.")
            }
        }

        ["database", "drop", database] => {
            if value["deleted"] == true {
                format!("Deleted database {database}.")
            } else {
                format!("Database {database} was not found.")
            }
        }

        ["table", "list", database] => {
            let Some(tables) = value["tables"].as_array() else {
                return value.to_string();
            };

            if tables.is_empty() {
                format!("No tables found in database {database}.")
            } else {
                format!(
                    "Tables in {database}:\n{}",
                    format_data_tree_list(
                        &tables
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    )
                )
            }
        }

        ["table", "create", database, table, ..] => {
            let Some(columns) = value["columns"].as_array() else {
                return value.to_string();
            };

            if columns.is_empty() {
                format!(
                    "Created empty table {database}.{table}.\n\nAdd a column with:\n  /data column add {database} {table} NAME TYPE"
                )
            } else {
                format!("Created table {database}.{table}.")
            }
        }

        ["table", "describe", database, table] => {
            let Some(columns) = value["columns"].as_array() else {
                return value.to_string();
            };

            if columns.is_empty() {
                format!("Table {database}.{table} has no columns yet.")
            } else {
                format!(
                    "Columns in {database}.{table}:\n{}",
                    format_data_tree_list(
                        &columns.iter().map(format_data_column).collect::<Vec<_>>()
                    )
                )
            }
        }

        ["column", "add", database, table, ..] => {
            format!("Added column {} to {database}.{table}.", format_data_column(&value["column"]))
        }

        ["column", "remove", database, table, column] => {
            format!("Removed column {column} from {database}.{table}.")
        }

        ["table", "drop", database, table] => {
            if value["deleted"] == true {
                format!("Deleted table {database}.{table}.")
            } else {
                format!("Table {database}.{table} was not found.")
            }
        }

        ["insert", database, table, ..] => {
            format!("Inserted record #{} into {database}.{table}.", value["id"])
        }

        ["query", database, table, ..] => {
            let Some(items) = value["items"].as_array() else {
                return value.to_string();
            };

            if items.is_empty() {
                return format!("No records found in {database}.{table}.");
            }

            let records = items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let details = item["record"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .map(|(name, value)| format!("{name}={}", format_data_value(value)))
                        .collect::<Vec<_>>();

                    format_tree_entry(
                        &format!("#{}", item["id"]),
                        &details,
                        index + 1 == items.len(),
                    )
                })
                .collect::<Vec<_>>();

            let mut output = format!("Records in {database}.{table}:\n{}", records.join("\n"));
            output.push_str(&format!("\nShowing {} record(s).", items.len()));

            if value["truncated"] == true {
                output.push_str(&format!(" Continue after ID {}.", value["next_after"]));
            }

            output
        }

        ["update", database, table, id, ..] => {
            format!("Updated record #{id} in {database}.{table}.")
        }

        ["delete", database, table, id] => {
            if value["deleted"] == true {
                format!("Deleted record #{id} from {database}.{table}.")
            } else {
                format!("Record #{id} was not found in {database}.{table}.")
            }
        }

        _ => value.to_string(),
    }
}

fn format_data_tree_list(entries: &[String]) -> String {
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| format_tree_entry(entry, &[], index + 1 == entries.len()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_data_column(value: &Value) -> String {
    let name = value["name"].as_str().unwrap_or("column");

    let kind = value["type"].as_str().unwrap_or("json");
    let required = if value["required"] == true { ", required" } else { "" };

    format!("{name} ({kind}{required})")
}

fn format_data_value(value: &Value) -> String {
    if let Some(value) = value.as_str() {
        return value.replace('\n', "\\n");
    }

    value.to_string()
}

fn data_help() -> &'static str {
    r#"Manage SQLite values and structured data for this TUI conversation.

Usage: /data <COMMAND>

Commands:
  database list                                  List databases
  database create <DB>                           Create a database
  database drop <DB>                             Delete a database and its tables

  value put KEY=VALUE                            Save a session-scoped text value
  value get KEY                                  Read a session-scoped value
  value delete KEY                               Delete a session-scoped value

  table list <DB>                                List tables in a database
  table create <DB> <TABLE>                      Create an empty table
  table describe <DB> <TABLE>                    Show table columns
  table drop <DB> <TABLE>                        Delete a table and its records

  column add <DB> <TABLE> <NAME> <TYPE>          Add a column
  column remove <DB> <TABLE> <COLUMN>            Remove a column

  insert <DB> <TABLE> <RECORD_JSON>              Insert a record
  query <DB> <TABLE> [FILTER_JSON] [AFTER_ID]    Query records
  update <DB> <TABLE> <ID> <CHANGES_JSON>        Update a record
  delete <DB> <TABLE> <ID>                       Delete a record

  help                                           Show this help

Options:
  -h, --help  Show this help

Notes:
  Run /data value help for key/value storage. Create a table without a schema,
  then add columns one at a time. JSON column definitions and table schemas are
  also accepted. Column types are text (also accepted as string), integer,
  real, boolean, and json. Use --required with column add to require a value on
  insert. Required columns can only be added before records exist.
  Each database supports 100 tables; each table supports 64 columns and 10,000
  records. Queries match exact values and return at most 20 records/128 KiB.
  Continue a truncated query with its next_after ID; use '{}' as the filter
  when paging without a field filter. Data is scoped to the active TUI session.
> "#
}

fn column_type(kind: &str) -> &str {
    if kind == "string" { "text" } else { kind }
}

async fn memory_command<C, F>(home: String, session: String, command: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
{
    if command.is_empty() || command == "help" {
        return memory_help().into();
    }

    let local_scope = format!("tui:{session}");

    let result = if command == "list" || command.starts_with("list ") {
        match parse_memory_list(command) {
            Ok((page, record_type)) => {
                let mut params = serde_json::json!({
                    "page": page,
                    "scopes": ["global", local_scope, format!("provider:tui:{session}")]
                });

                if let Some(record_type) = record_type {
                    params["type"] = Value::String(record_type.into());
                }

                capability(home, "memory", "list", params, host)
                    .await
                    .map(|value| format_memories(&value))
            }

            Err(()) => Ok("Usage: /memory list [page] [--type global|session].\n> ".into()),
        }
    } else if let Some(value) = command.strip_prefix("remember ") {
        match value.split_once('=') {
            Some((key, value)) if !key.trim().is_empty() && !value.trim().is_empty() => capability(
                home,
                "memory",
                "remember",
                serde_json::json!({
                    "key": key.trim(),
                    "value": value.trim(),
                    "scope": local_scope,
                    "mode": "suggest",
                    "approved": true
                }),
                host,
            )
            .await
            .map(|_| "Memory saved for this session.\n> ".into()),

            _ => Ok("Usage: /memory remember KEY=VALUE. Run /memory help for details.\n> ".into()),
        }
    } else if let Some(key) = command.strip_prefix("forget ") {
        if key.trim().is_empty() {
            Ok("Usage: /memory forget [global|session] KEY. Run /memory help for details.\n> "
                .into())
        } else {
            let (requested_scope, key) = key
                .trim()
                .strip_prefix("global ")
                .map(|key| (Some("global"), key.trim()))
                .or_else(|| {
                    key.trim().strip_prefix("session ").map(|key| (Some("session"), key.trim()))
                })
                .unwrap_or((None, key.trim()));

            async {
                let legacy_scope = format!("provider:tui:{session}");
                let target_scopes = match requested_scope {
                    Some("global") => vec!["global".to_owned()],
                    Some("session") => vec![local_scope.clone(), legacy_scope.clone()],

                    _ => {
                        let mut found_session = false;
                        let mut found_global = false;

                        for scope in [
                            local_scope.clone(),
                            legacy_scope.clone(),
                            "global".to_owned(),
                        ] {
                            let recalled = capability(
                                home.clone(),
                                "memory",
                                "recall",
                                serde_json::json!({"key": key, "scope": scope}),
                                host,
                            )
                            .await?;

                            if !recalled["item"].is_null() {
                                if scope == "global" {
                                    found_global = true;
                                } else {
                                    found_session = true;
                                }
                            }
                        }

                        match (found_session, found_global) {
                            (true, true) => {
                                return Err(krabb_core::Error::Denied(
                                    "This key exists in global and session memory. Choose `/memory forget global KEY` or `/memory forget session KEY`.".into(),
                                ));
                            }

                            (true, false) => vec![local_scope.clone(), legacy_scope.clone()],
                            (false, true) => vec!["global".into()],
                            (false, false) => return Ok(serde_json::json!({"deleted": false})),
                        }
                    }
                };

                let resolved_scope =
                    if target_scopes.iter().any(|scope| scope == "global") && target_scopes.len() == 1 {
                        "global"
                    } else {
                        "session"
                    };

                let mut deleted = false;

                for scope in target_scopes {
                    let forgotten = capability(
                        home.clone(),
                        "memory",
                        "forget",
                        serde_json::json!({"key": key, "scope": scope}),
                        host,
                    )
                    .await?;
                    deleted |= forgotten["deleted"] == true;
                }

                Ok(serde_json::json!({"deleted": deleted, "scope": resolved_scope}))
            }
            .await
            .map(|value| {
                if value["deleted"] == true && value["scope"] == "global" {
                    "Global memory removed.\n> ".into()
                } else if value["deleted"] == true {
                    "Session memory removed.\n> ".into()
                } else {
                    "Memory was not found.\n> ".into()
                }
            })
        }
    } else {
        Ok("Unknown memory command. Run /memory help for usage.\n> ".into())
    };

    result.unwrap_or_else(|error| {
        format!("Memory action failed: {}.\n> ", sentence(error.to_string()))
    })
}

fn memory_help() -> &'static str {
    r#"Manage memories for this session.

Commands:
  /memory help                               Show this help
  /memory list [PAGE] [options]              List memories
  /memory remember KEY=VALUE                 Save a memory for this session
  /memory forget KEY                         Remove a unique matching memory
  /memory forget global KEY                  Remove a global memory
  /memory forget session KEY                 Remove a session memory

Options:
  --type global|session                      Filter the list by memory type

Notes:
  List rows show their type. If a key exists in both scopes, specify global or
  session when forgetting it. Selecting /memory in the command picker shows
  this help.
> "#
}

fn parse_memory_list(command: &str) -> Result<(usize, Option<&'static str>), ()> {
    let mut parts = command.split_whitespace();

    if parts.next() != Some("list") {
        return Err(());
    }

    let mut page = 1;
    let mut page_seen = false;
    let mut record_type = None;

    while let Some(part) = parts.next() {
        if part == "--type" {
            if record_type.is_some() {
                return Err(());
            }

            record_type = match parts.next() {
                Some("global") => Some("global"),
                Some("session") => Some("session"),
                _ => return Err(()),
            };
        } else if !page_seen {
            page = part.parse::<usize>().ok().filter(|page| *page > 0).ok_or(())?;
            page_seen = true;
        } else {
            return Err(());
        }
    }

    Ok((page, record_type))
}

fn format_schedules(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "There are no schedules to list.\n> ".into();
    };

    if items.is_empty() {
        return "There are no schedules to list.\n> ".into();
    }

    let total = value["total"].as_u64().unwrap_or(items.len() as u64);
    let page = value["page"].as_u64().unwrap_or(1);
    let pages = value["pages"].as_u64().unwrap_or(1);
    let from = value["from"].as_u64().unwrap_or(1);
    let to = value["to"].as_u64().unwrap_or(items.len() as u64);
    let items = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let id = item["id"].as_u64()?;
            let status = item["status"].as_str().unwrap_or("pending");
            let mut details = Vec::new();

            if let Some(due) = item["due"].as_u64() {
                details.push(format!(
                    "Next run: {}",
                    krabb_date::format_datetime_in_timezone(
                        due,
                        item["zone"].as_str().unwrap_or("UTC"),
                    )
                ));
            } else {
                details.push("No upcoming run".into());
            }

            if let Some(cron) = item["cron"].as_str() {
                details.push(format!(
                    "Repeats on cron {cron} in {}",
                    item["zone"].as_str().unwrap_or("UTC")
                ));
            } else if let Some(repeat) = item["repeat"].as_u64().filter(|repeat| *repeat > 0) {
                details.push(format!("Repeats every {}", krabb_date::format_duration(repeat)));
            }

            let retries = item["retries"].as_u64().unwrap_or_default();

            if retries > 0 {
                let used =
                    item["attempts"].as_u64().unwrap_or_default().saturating_sub(1).min(retries);
                details.push(format!("Automatic retries: {used} of {retries} used"));
            } else {
                details.push("Automatic retries are off".into());
            }

            if item["on_failure"] == "continue" {
                details.push("Continues after failed occurrences".into());
            }

            if let Some(retry_at) = item["retry_at"].as_u64() {
                details.push(format!(
                    "Next retry: {}",
                    krabb_date::format_datetime_in_timezone(
                        retry_at,
                        item["zone"].as_str().unwrap_or("UTC"),
                    )
                ));
            }

            if item["run_requested"] == true {
                details.push("A manual run is queued".into());
            }

            if let Some(outcome) = item["last_outcome"].as_str() {
                let label = match outcome {
                    "completed" => "completed successfully",
                    "retrying" => "failed; retry queued",
                    "continuing" => "failed; next occurrence remains scheduled",
                    "cancelled" => "cancelled",
                    "skipped" => "skipped",
                    _ => "failed; task paused",
                };

                details.push(format!(
                    "Last result: {label}. Details: krabb schedule history {}",
                    item["name"].as_str().unwrap_or("Scheduled task")
                ));
            }

            Some(format_tree_entry(
                &format!("{id} | {} | {status}", item["name"].as_str().unwrap_or("Scheduled task")),
                &details,
                index + 1 == items.len(),
            ))
        })
        .collect::<Vec<_>>();

    format!(
        "Schedules ({from}–{to} of {total}, page {page} of {pages})\n{}\n> ",
        items.join("\n\n")
    )
}

fn format_memories(value: &Value) -> String {
    let Some(items) = value["items"].as_array() else {
        return "Memory list returned an invalid response.\n> ".into();
    };

    let total = value["total"].as_u64().unwrap_or(items.len() as u64) as usize;

    if total == 0 || items.is_empty() {
        let page = value["page"].as_u64().unwrap_or(1);
        let pages = value["pages"].as_u64().unwrap_or(1);

        return format!("Memories | 0–0 of 0 | page {page}/{pages}:\nNo memories found.\n> ");
    }

    let page = value["page"].as_u64().unwrap_or(1) as usize;
    let pages =
        value["pages"].as_u64().unwrap_or(total.div_ceil(PLUGIN_LIST_PAGE_SIZE) as u64) as usize;

    let start = (page - 1) * PLUGIN_LIST_PAGE_SIZE;
    let end = (start + items.len()).min(total);
    let mut output = format!("Memories | {}–{} of {total} | page {page}/{pages}:", start + 1, end);

    for (index, item) in items.iter().enumerate() {
        let key = short(item["key"].as_str().unwrap_or("unknown"));
        let record_type = item["type"].as_str().unwrap_or("session");
        let value = short(item["value"].as_str().unwrap_or_default());
        let metadata = [format!("type: {record_type}"), value];

        output.push_str(&format!(
            "\n\n{}",
            format_tree_entry(&key, &metadata, index + 1 == items.len())
        ));
    }

    output.push_str("\n> ");
    output
}

fn short(value: &str) -> String {
    let mut chars = value.chars();
    let mut short = chars.by_ref().take(120).collect::<String>();

    if chars.next().is_some() {
        short.push('…');
    }

    short
}

fn schedule_id() -> u64 {
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

fn terminal(mode: &str) -> krabb_core::Result<std::fs::File> {
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

    options.open(path).map_err(krabb_core::Error::from)
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
            krabb_core::Error::Denied(format!("The daemon port is invalid: {error}."))
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
            .map_err(|error| ControlError::Failed(krabb_core::Error::Denied(error.to_string())))?;

        let system_output = method == "session.command";
        let mut streamed = String::new();

        loop {
            let response: IpcResponse = jsonl::read(&mut input, jsonl::MAX)
                .await
                .map_err(ControlError::Failed)?
                .ok_or_else(|| {
                    ControlError::Failed(krabb_core::Error::Denied(
                        "The daemon closed the IPC connection.".into(),
                    ))
                })?;

            if !response.valid() || response.id != 1 {
                return Err(ControlError::Failed(krabb_core::Error::Denied(
                    "The daemon returned an invalid response.".into(),
                )));
            }

            if let Some(error) = response.error {
                return Err(ControlError::Failed(krabb_core::Error::Denied(error.message)));
            }

            let Some(value) = response.result else {
                return Err(ControlError::Failed(krabb_core::Error::Denied(
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
        ControlError::Failed(krabb_core::Error::Denied(
            "The daemon did not respond in time.".into(),
        ))
    })?
}

fn daemon_streaming_control(
    home: String,
    method: String,
    params: Value,
    events: mpsc::UnboundedSender<EngineEvent>,
) -> Pin<Box<dyn Future<Output = krabb_core::Result<Value>> + Send>> {
    Box::pin(async move {
        control_with_stream(&home, &method, params, Some(events))
            .await
            .map_err(ControlError::into_core)
    })
}

async fn delivery_action<C, F>(home: &str, method: &str, id: &str, host: C) -> String
where
    C: Fn(String, String, Value) -> F + Copy,
    F: Future<Output = krabb_core::Result<Value>>,
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
            .strip_prefix("Krabb requests approval to ")
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
    F: Future<Output = krabb_core::Result<Value>>,
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

    if parts.next() != Some("/plugin") {
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

            if let Some(created) = item["created"].as_u64().map(krabb_date::format_date) {
                details.push(format!("created: {created}"));
            }

            if let Some(updated) = item["updated"].as_u64().map(krabb_date::format_date) {
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

            let manifest_path = entry.path().join("krabb-plugin.toml");
            let manifest = krabb_file::load(&manifest_path, 64 * 1024)
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

    #[tokio::test]
    async fn stores_values_through_the_session_scoped_sqlite_capability() {
        let calls = std::sync::Mutex::new(Vec::new());
        let host = |_: String, method: String, params: serde_json::Value| {
            let calls = &calls;

            async move {
                assert_eq!(method, "capability.call");
                assert_eq!(params["service"], "sqlite");
                calls.lock().unwrap().push(params.clone());

                match params["method"].as_str().unwrap() {
                    "put" => Ok(json!({"ok": true})),
                    "get" => Ok(json!({"found": true, "value": "stored text"})),
                    "delete" => Ok(json!({"deleted": true})),
                    method => panic!("unexpected SQLite method {method}"),
                }
            }
        };

        assert_eq!(
            super::store_command(
                "/tmp/krabb".into(),
                "default".into(),
                "put note=stored text",
                host,
            )
            .await,
            "Stored session value.\n> "
        );

        assert_eq!(
            super::store_command("/tmp/krabb".into(), "default".into(), "get note", host).await,
            "\"stored text\"\n> "
        );

        assert_eq!(
            super::store_command("/tmp/krabb".into(), "default".into(), "delete note", host,).await,
            "Deleted session value.\n> "
        );

        let calls = calls.lock().unwrap();

        assert_eq!(calls[0]["method"], "put");
        assert_eq!(calls[0]["params"]["key"], "tui:default:note");
        assert_eq!(calls[0]["params"]["value"], "stored text");
        assert_eq!(calls[1]["params"]["key"], "tui:default:note");
        assert_eq!(calls[2]["method"], "delete");
        assert_eq!(calls[2]["params"]["key"], "tui:default:note");
    }

    #[tokio::test]
    async fn manages_structured_data_in_the_active_tui_scope() {
        let calls = std::sync::Mutex::new(Vec::new());
        let host = |_: String, method: String, params: serde_json::Value| {
            let calls = &calls;
            async move {
                assert_eq!(method, "capability.call");
                calls.lock().unwrap().push(params.clone());
                Ok(match params["params"]["operation"].as_str() {
                    Some("create_table") => serde_json::json!({"columns": []}),

                    Some("add_column") => {
                        serde_json::json!({"column": params["params"]["column"]})
                    }

                    Some("drop_column") => serde_json::json!({"records_updated": 0}),
                    _ => serde_json::json!({"created": true}),
                })
            }
        };

        for command in ["", "help", "--help", "-h"] {
            let help =
                super::data_command("/tmp/krabb".into(), "default".into(), command, host).await;

            assert!(help.contains("Usage: /data <COMMAND>"));
            assert!(help.contains("Commands:"));
            assert!(help.contains("Options:"));
            assert!(help.contains("Notes:"));
            assert!(help.contains("table create <DB> <TABLE>"));
            assert!(help.contains("column add <DB> <TABLE> <NAME> <TYPE>"));
            assert!(help.contains("column remove <DB> <TABLE> <COLUMN>"));
            assert!(help.contains("\n\n  table list <DB>"));
            assert!(help.contains("value put KEY=VALUE"));
            assert!(!help.contains("/store"));
        }

        let value_help =
            super::data_command("/tmp/krabb".into(), "default".into(), "value help", host).await;

        assert!(value_help.contains("/data value get KEY"));
        assert!(!value_help.contains("/store"));

        assert!(calls.lock().unwrap().is_empty());

        let response = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "table create contacts people",
            host,
        )
        .await;

        assert!(response.contains(
            "Created empty table contacts.people.\n\nAdd a column with:\n  /data column add contacts people NAME TYPE"
        ));

        let response = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "column add contacts people name string --required",
            host,
        )
        .await;

        assert!(response.contains("Added column name (text, required) to contacts.people."));

        let response = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "column add contacts people active boolean",
            host,
        )
        .await;

        assert!(response.contains("Added column active (boolean) to contacts.people."));

        let response = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "column remove contacts people active",
            host,
        )
        .await;

        assert!(response.contains("Removed column active from contacts.people."));

        let response = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "value put note=stored",
            host,
        )
        .await;

        assert!(response.contains("Stored session value."));
        let calls = calls.lock().unwrap();

        assert_eq!(calls[0]["service"], "sqlite");
        assert_eq!(calls[0]["method"], "data");
        assert_eq!(calls[0]["params"]["scope"], "tui:default");
        assert_eq!(calls[0]["params"]["operation"], "create_table");
        assert_eq!(calls[0]["params"]["columns"], serde_json::json!([]));
        assert_eq!(calls[1]["params"]["operation"], "add_column");
        assert_eq!(calls[1]["params"]["column"]["name"], "name");
        assert_eq!(calls[1]["params"]["column"]["required"], true);
        assert_eq!(calls[2]["params"]["column"]["name"], "active");
        assert_eq!(calls[3]["params"]["operation"], "drop_column");
        assert_eq!(calls[3]["params"]["column_name"], "active");
        assert_eq!(calls[4]["method"], "put");
    }

    #[tokio::test]
    async fn formats_tui_data_lists_with_tree_branches() {
        let host = |_: String, _: String, params: serde_json::Value| async move {
            Ok(match params["params"]["operation"].as_str() {
                Some("list_databases") => serde_json::json!({"databases": ["foo", "bar"]}),
                Some("list_tables") => serde_json::json!({"tables": ["people", "notes"]}),
                Some("describe_table") => serde_json::json!({
                    "columns": [{"name": "name", "type": "text"}]
                }),
                Some("query") => serde_json::json!({
                    "items": [{"id": 1, "record": {"name": "Ada"}}],
                    "truncated": false
                }),
                _ => serde_json::json!({}),
            })
        };

        let databases =
            super::data_command("/tmp/krabb".into(), "default".into(), "database list", host).await;
        assert!(databases.contains("Databases:\n├─ foo\n└─ bar\n> "));

        let tables =
            super::data_command("/tmp/krabb".into(), "default".into(), "table list foo", host)
                .await;
        assert!(tables.contains("Tables in foo:\n├─ people\n└─ notes\n> "));

        let columns = super::data_command(
            "/tmp/krabb".into(),
            "default".into(),
            "table describe foo people",
            host,
        )
        .await;

        assert!(columns.contains("Columns in foo.people:\n└─ name (text)\n> "));

        let records =
            super::data_command("/tmp/krabb".into(), "default".into(), "query foo people", host)
                .await;
        assert!(
            records.contains("Records in foo.people:\n└─ #1\n   └─ name=Ada\nShowing 1 record(s).")
        );
    }

    #[tokio::test]
    async fn forgets_matching_session_memories_from_both_scopes() {
        let legacy_called = std::sync::atomic::AtomicBool::new(false);
        let host = |_: String, method: String, params: serde_json::Value| {
            let legacy_called = &legacy_called;

            async move {
                assert_eq!(method, "capability.call");
                assert_eq!(params["service"], "memory");
                let action = params["method"].as_str().unwrap();
                let scope = params["params"]["scope"].as_str().unwrap();

                assert_eq!(params["params"]["key"], "duplicate");

                match action {
                    "recall" => Ok(serde_json::json!({
                        "item": if scope == "tui:default" || scope == "provider:tui:default" {
                            serde_json::json!({"key": "duplicate", "scope": scope})
                        } else {
                            serde_json::Value::Null
                        }
                    })),

                    "forget" => {
                        if scope == "provider:tui:default" {
                            legacy_called.store(true, std::sync::atomic::Ordering::SeqCst);
                        }

                        Ok(serde_json::json!({"deleted": true}))
                    }

                    _ => panic!("unexpected memory method {action}"),
                }
            }
        };

        let response =
            super::memory_command("/tmp/krabb".into(), "default".into(), "forget duplicate", host)
                .await;

        assert_eq!(response, "Session memory removed.\n> ");
        assert!(legacy_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn forgets_a_unique_global_memory_by_its_listed_key() {
        let global_deleted = std::sync::atomic::AtomicBool::new(false);
        let host = |_: String, method: String, params: serde_json::Value| {
            let global_deleted = &global_deleted;

            async move {
                assert_eq!(method, "capability.call");
                assert_eq!(params["service"], "memory");
                let action = params["method"].as_str().unwrap();
                let scope = params["params"]["scope"].as_str().unwrap();

                assert_eq!(params["params"]["key"], "foo");

                match action {
                    "recall" => Ok(serde_json::json!({
                        "item": if scope == "global" {
                            serde_json::json!({"key": "foo", "scope": "global"})
                        } else {
                            serde_json::Value::Null
                        }
                    })),

                    "forget" => {
                        assert_eq!(scope, "global");
                        global_deleted.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(serde_json::json!({"deleted": true}))
                    }

                    _ => panic!("unexpected memory method {action}"),
                }
            }
        };

        let response =
            super::memory_command("/tmp/krabb".into(), "default".into(), "forget foo", host).await;

        assert_eq!(response, "Global memory removed.\n> ");
        assert!(global_deleted.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn asks_for_scope_when_a_memory_key_is_ambiguous() {
        let deleted = std::sync::atomic::AtomicBool::new(false);
        let host = |_: String, method: String, params: serde_json::Value| {
            let deleted = &deleted;

            async move {
                assert_eq!(method, "capability.call");
                assert_eq!(params["service"], "memory");
                let action = params["method"].as_str().unwrap();
                let scope = params["params"]["scope"].as_str().unwrap();

                assert_eq!(params["params"]["key"], "foo");

                match action {
                    "recall" => Ok(serde_json::json!({
                        "item": if scope == "global" || scope == "tui:default" {
                            serde_json::json!({"key": "foo", "scope": scope})
                        } else {
                            serde_json::Value::Null
                        }
                    })),

                    "forget" => {
                        deleted.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(serde_json::json!({"deleted": true}))
                    }

                    _ => panic!("unexpected memory method {action}"),
                }
            }
        };

        let response =
            super::memory_command("/tmp/krabb".into(), "default".into(), "forget foo", host).await;

        assert!(response.contains("exists in global and session memory"));
        assert!(!deleted.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saves_session_memories_outside_the_global_scope() {
        let scoped = std::sync::atomic::AtomicBool::new(false);
        let host = |_: String, method: String, params: serde_json::Value| {
            let scoped = &scoped;

            async move {
                assert_eq!(method, "capability.call");
                assert_eq!(params["service"], "memory");
                assert_eq!(params["method"], "remember");
                assert_eq!(params["params"]["scope"], "tui:global");
                scoped.store(true, std::sync::atomic::Ordering::SeqCst);

                Ok(serde_json::json!({"ok": true}))
            }
        };

        let response = super::memory_command(
            "/tmp/krabb".into(),
            "global".into(),
            "remember favorite=tea",
            host,
        )
        .await;

        assert_eq!(response, "Memory saved for this session.\n> ");
        assert!(scoped.load(std::sync::atomic::Ordering::SeqCst));
    }

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
        assert_eq!(hello.capabilities, vec![krabb_core::types::Capability::Client]);
        assert_eq!(hello.commands[0].name, "tui");
        assert!(hello.commands[0].description.contains("daemon must be running"));
    }

    #[test]
    fn bounds_saved_replies_without_splitting_utf8() {
        let text = format!("{}étail", "a".repeat(super::SAVED_REPLY_LIMIT - 1));
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
    fn temporary_run_names_are_random_tui_sessions() {
        let first = super::temporary_run_namespace().unwrap();
        let second = super::temporary_run_namespace().unwrap();

        assert_ne!(first, second);
        assert!(first.starts_with("tui-once-"));
        assert!(super::tui_session_name(&first).is_some());
    }

    #[test]
    fn defaults_to_the_default_session_and_keeps_explicit_selection() {
        assert_eq!(super::selected_session_id(None), "tui-default");
        assert_eq!(super::selected_session_id(Some("work".into())), "tui-work");
    }

    #[test]
    fn resumes_last_session_or_most_recent_active_tui_session() {
        let sessions = json!({
            "items": [
                {"id": "tui-old", "archived": false, "updated": 10},
                {"id": "tui-recent", "archived": false, "updated": 20},
                {"id": "tui-archived", "archived": true, "updated": 30},
                {"id": "other", "archived": false, "updated": 40}
            ]
        });

        assert_eq!(super::resume_session_id(&sessions, Some("tui-old")), "tui-old");
        assert_eq!(super::resume_session_id(&sessions, Some("tui-missing")), "tui-recent");
    }

    #[test]
    fn defaults_when_no_active_tui_session_exists() {
        let sessions = json!({
            "items": [
                {"id": "tui-archived", "archived": true, "updated": 30},
                {"id": "other", "archived": false, "updated": 40}
            ]
        });

        assert_eq!(super::resume_session_id(&sessions, Some("tui-archived")), "tui-default");
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
            std::env::temp_dir().join(format!("krabb-tui-deep-delete-{}", std::process::id()));

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
        assert!(basic.contains("Plugin commands run in the CLI: krabb <command>."));
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
        assert!(model_commands.contains("/plugin [page]"));
        assert!(model_commands.contains("Conditional commands (shown only when usable):"));
        assert!(model_commands.contains("\n\nPlugin commands run in the CLI:"));
        assert!(!model_commands.contains("`krabb"));
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
            std::env::temp_dir().join(format!("krabb-tui-capability-{}", std::process::id()));

        let plugin = root.join("plugins/scheduler");
        let binary = krabb_core::plugin::binary_name("scheduler");

        std::fs::create_dir_all(plugin.join("bin")).unwrap();
        std::fs::write(plugin.join("krabb-plugin.toml"), "capabilities = ['scheduler']\n").unwrap();
        std::fs::write(plugin.join("bin").join(binary), "test").unwrap();

        let memory = root.join("plugins/memory");
        let memory_binary = krabb_core::plugin::binary_name("memory");

        std::fs::create_dir_all(memory.join("bin")).unwrap();
        std::fs::write(memory.join("krabb-plugin.toml"), "capabilities = ['memory']\n").unwrap();

        std::fs::write(memory.join("bin").join(memory_binary), "test").unwrap();

        assert!(super::has_capability(root.to_str().unwrap(), "scheduler"));
        assert!(super::has_capability(root.to_str().unwrap(), "memory"));

        let help = super::command_help(root.to_str().unwrap(), true, true);

        assert!(help.contains("/memory"));
        assert!(!help.contains("/memory <"));

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

        let timeout = super::ControlError::Failed(krabb_core::Error::Denied(
            "The daemon did not respond in time.".into(),
        ));

        assert!(!timeout.is_unavailable());

        let connection_reset = super::ControlError::Failed(krabb_core::Error::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        ));

        assert!(!connection_reset.is_unavailable());

        let protocol_error = super::ControlError::Failed(krabb_core::Error::Denied(
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
                "Krabb requests approval to run this shell command: rm foo.",
            ),
            "Approval needed.\nTool: shell\nCommand: rm foo\nApprove: /approve approval-1\nDeny: /deny approval-1"
        );

        assert_eq!(
            super::format_approval_notice(
                "approval-3",
                "shell",
                "{\"command\":\"rm foo.\"}",
                Some("rm foo."),
                "Krabb requests approval to run this shell command: rm foo..",
            ),
            "Approval needed.\nTool: shell\nCommand: rm foo.\nApprove: /approve approval-3\nDeny: /deny approval-3"
        );

        assert_eq!(
            super::format_approval_notice(
                "approval-2",
                "write",
                "{\"path\":\"foo\"}",
                None,
                "Krabb requests approval to write to foo..",
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
        assert_eq!(super::parse_plugin_list_page("/plugin"), Ok(1));
        assert_eq!(super::parse_plugin_list_page("/plugin 3"), Ok(3));
        assert!(super::parse_plugin_list_page("/plugin 0").is_err());
        assert!(super::parse_plugin_list_page("/plugin next").is_err());
        assert!(super::parse_plugin_list_page("/plugin 2 extra").is_err());
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
    fn formats_schedule_and_memory_records() {
        assert_eq!(
            super::format_schedules(&json!({"items": []})),
            "There are no schedules to list.\n> "
        );

        assert_eq!(
            super::format_schedules(&json!({"items": [{"id": 7, "due": 42, "text": "call back"}]})),
            "Schedules (1–1 of 1, page 1 of 1)\n└─ 7 | Scheduled task | pending\n   ├─ Next run: 1970-01-01 00:00:42 UTC\n   └─ Automatic retries are off\n> "
        );

        let local = super::format_schedules(&json!({
            "items": [{
                "id": 8,
                "name": "rome-report",
                "due": 1_700_000_000,
                "cron": "0 9 * * *",
                "zone": "Europe/Rome",
                "status": "active"
            }]
        }));

        assert!(local.contains(
            "Next run: 2023-11-14 23:13:20 +01:00 (Europe/Rome; 2023-11-14 22:13:20 UTC)"
        ));

        assert_eq!(
            super::format_memories(&json!({
                "items": [
                    {"key": "close-up", "value": "banana", "type": "global"},
                    {"key": "drink", "value": "tea", "type": "session"},
                    {"key": "legacy", "value": "saved before upgrade", "type": "session"}
                ],
                "total": 3,
                "page": 1,
                "pages": 1
            })),
            "Memories | 1–3 of 3 | page 1/1:\n\n├─ close-up\n│  ├─ type: global\n│  └─ banana\n\n├─ drink\n│  ├─ type: session\n│  └─ tea\n\n└─ legacy\n   ├─ type: session\n   └─ saved before upgrade\n> "
        );

        assert_eq!(
            super::format_memories(&json!({"items": [], "total": 0, "page": 1, "pages": 1})),
            "Memories | 0–0 of 0 | page 1/1:\nNo memories found.\n> "
        );
    }

    #[test]
    fn tui_schedule_history_paginates_five_runs() {
        let root =
            std::env::temp_dir().join(format!("krabb-tui-schedule-history-{}", std::process::id()));
        let directory = root.join("logs/schedules");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("morning-check-42.jsonl");
        let records = (1..=12)
            .map(|run_id| {
                json!({
                    "timestamp": "2026-10-19T14:00:00Z",
                    "schedule": {"id": 42, "name": "morning-check", "timezone": "UTC"},
                    "run": {"id": run_id, "status": "completed", "scheduled_for": 1_800_000_000},
                    "agent": {"provider": "codex", "model": "gpt-6-luna"},
                    "result": format!("run-{run_id}")
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &path,
            records.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"),
        )
        .unwrap();

        let (page, total, found_path) =
            super::read_schedule_history(root.to_str().unwrap(), 42, 2).unwrap();
        let output =
            super::format_schedule_history(&page, total, 42, "morning-check", 2, &found_path);

        assert_eq!(page.len(), 5);
        assert_eq!(total, 12);
        assert!(output.contains("Schedule history: morning-check | 6–10 of 12 | page 2/3:"));
        assert!(
            output.find("Run 7 | completed successfully").unwrap()
                < output.find("Run 3 | completed successfully").unwrap()
        );

        assert!(output.contains("Raw JSONL: "));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tui_schedule_add_shows_name_access_and_frequency() {
        let host = |_: String, _: String, _: serde_json::Value| async move {
            Ok(json!({
                "id": 77,
                "name": "morning-check",
                "status": "active",
                "due": 1_800_000_000_u64,
                "repeat": 300,
                "zone": "UTC",
                "context": {"tools": false, "shell": false}
            }))
        };

        let output = super::schedule_command(
            "/tmp/krabb".into(),
            "add --every 5m --name morning-check Review the report",
            "default".into(),
            "codex".into(),
            "gpt-6-luna".into(),
            Some("/work/reports".into()),
            host,
        )
        .await;

        assert!(output.starts_with("Schedule morning-check ("));
        assert!(output.contains(") is active.\n\nInstruction: Review the report"));
        assert!(output.contains("\nTools: disabled\nShell: disabled\n"));
        assert!(output.contains("Frequency: every 5 minutes"));
        assert!(output.contains("\n\nCheck it with /schedule list;"));
    }

    #[tokio::test]
    async fn schedule_selectors_resolve_names_across_pages() {
        let host = |_: String, _: String, params: serde_json::Value| async move {
            let page = params["params"]["page"].as_u64().unwrap();
            let value = if page == 1 {
                json!({"items": [{"id": 10, "name": "first-task"}], "pages": 2})
            } else {
                json!({"items": [{"id": 11, "name": "mighty-raccoon"}], "pages": 2})
            };

            Ok(value)
        };

        assert_eq!(
            super::schedule_selector_id("/home/krabb", "mighty-raccoon", host).await,
            Ok(11)
        );

        assert_eq!(super::schedule_selector_id("/home/krabb", "10", host).await, Ok(10));
        assert!(super::schedule_selector_id("/home/krabb", "missing", host).await.is_err());
    }

    #[test]
    fn schedule_list_shows_repeat_interval_and_last_outcome() {
        let output = super::format_schedules(&json!({
            "items": [{
                "id": 8,
                "name": "send-report",
                "due": 1_700_000_000,
                "repeat": 300,
                "status": "paused",
                "text": "send report",
                "last_outcome": "failed",
                "last_result": "PAUSED: The channel is not available."
            }]
        }));

        assert!(output.contains("Repeats every 5 minutes"));
        assert!(output.contains("Last result: failed; task paused."));
        assert!(output.contains("krabb schedule history send-report"));
        assert!(!output.contains("The channel is not available"));
    }

    #[test]
    fn schedule_list_shows_continue_after_retries_policy() {
        let output = super::format_schedules(&json!({
            "items": [{
                "id": 10,
                "due": 1_700_000_000,
                "repeat": 300,
                "status": "active",
                "on_failure": "continue",
                "text": "check the backup"
            }]
        }));

        assert!(output.contains("Continues after failed occurrences"));
    }

    #[test]
    fn parses_memory_list_pages_and_type_filters() {
        assert_eq!(super::parse_memory_list("list"), Ok((1, None)));
        assert_eq!(super::parse_memory_list("list 3"), Ok((3, None)));
        assert_eq!(super::parse_memory_list("list --type global"), Ok((1, Some("global"))));
        assert_eq!(super::parse_memory_list("list 2 --type session"), Ok((2, Some("session"))));
        assert_eq!(super::parse_memory_list("list --type global 2"), Ok((2, Some("global"))));
        assert!(super::parse_memory_list("list 0").is_err());
        assert!(super::parse_memory_list("list --type invalid").is_err());
        assert!(super::parse_memory_list("list 2 3").is_err());
        assert!(super::parse_memory_list("list --type global --type session").is_err());
    }

    #[tokio::test]
    async fn lists_memories_with_page_and_type_filter() {
        let host = |_: String, method: String, params: serde_json::Value| async move {
            assert_eq!(method, "capability.call");
            assert_eq!(params["service"], "memory");
            assert_eq!(params["method"], "list");
            assert_eq!(params["params"]["page"], 2);
            assert_eq!(params["params"]["type"], "session");
            assert_eq!(
                params["params"]["scopes"],
                json!(["global", "tui:default", "provider:tui:default"])
            );

            Ok(json!({
                "items": [{"key": "note", "value": "session detail", "type": "session"}],
                "total": 6,
                "page": 2,
                "pages": 2
            }))
        };

        let response = super::memory_command(
            "/tmp/krabb".into(),
            "default".into(),
            "list 2 --type session",
            host,
        )
        .await;

        assert!(response.contains("Memories | 6–6 of 6 | page 2/2:"));
        assert!(response.contains("└─ note\n   ├─ type: session\n   └─ session detail"));
    }

    #[test]
    fn schedule_help_uses_enterable_arguments() {
        let help = super::schedule_help();

        assert!(help.contains("/schedule add TIMING [options] TEXT"));
        assert!(help.contains("--after DURATION"));
        assert!(help.contains("--at DATE-TIME"));
        assert!(help.contains("--every DURATION"));
        assert!(help.contains("--name NAME"));
        assert!(help.contains("--cron 'FIELDS'"));
        assert!(help.contains("--timezone AREA/CITY"));
        assert!(help.contains("/schedule run ID|NAME"));
        assert!(help.contains("/schedule retry ID|NAME"));
        assert!(help.contains("--missed skip|coalesce|replay"));
        assert!(help.contains("--retries 0..5"));
        assert!(help.contains("--on-failure pause|continue"));
        assert!(!help.contains('<'));

        let memory_help = super::memory_help();

        assert!(memory_help.contains("/memory list [PAGE] [options]"));
        assert!(memory_help.contains("--type global|session"));
        assert!(memory_help.contains("Notes:"));
        assert!(!memory_help.contains('<'));
    }

    #[test]
    fn tui_schedule_add_accepts_cli_timing_and_retry_options() {
        let (params, instruction) = super::parse_schedule_add(
            "--every 300 --at 2030-10-20T09:00:00+02:00 --missed replay --retries 2 --on-failure continue 'Check user's backup folder'",
            "/missing-krabb-home",
        )
        .unwrap();
        let expected_at = u64::try_from(
            "2030-10-20T09:00:00+02:00".parse::<jiff::Timestamp>().unwrap().as_second(),
        )
        .unwrap();

        assert_eq!(instruction, "Check user's backup folder");
        assert_eq!(params["repeat"], 300);
        assert_eq!(params["at"], expected_at);
        assert_eq!(params["missed"], "replay");
        assert_eq!(params["retries"], 2);
        assert_eq!(params["on_failure"], "continue");
    }

    #[test]
    fn tui_schedule_add_accepts_human_durations_and_options_after_instruction() {
        let (params, instruction) = super::parse_schedule_add(
            "run a backup --after '1m30s' --every 1mo --name Backup",
            "/missing-krabb-home",
        )
        .unwrap();

        assert_eq!(instruction, "run a backup");
        assert_eq!(params["delay"], 90);
        assert_eq!(params["repeat"], 2_592_000);
        assert_eq!(params["name"], "Backup");

        let (params, _) = super::parse_schedule_add(
            "--after '1 month' do the monthly backup",
            "/missing-krabb-home",
        )
        .unwrap();

        assert_eq!(params["delay"], 2_592_000);

        let (params, _) = super::parse_schedule_add(
            "--at '2030-10-20 09:00' --timezone UTC Run the report",
            "/missing-krabb-home",
        )
        .unwrap();

        assert_eq!(
            params["at"],
            "2030-10-20T09:00:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64
        );

        assert!(
            super::parse_schedule_add("--after 5m --name 'Backup job' Send report", "/tmp")
                .is_err()
        );
    }

    #[test]
    fn tui_schedule_add_parses_quoted_cron_and_validates_options() {
        let (params, instruction) = super::parse_schedule_add(
            "--cron '0 9 * * 1-5' --timezone Europe/Rome --retries 1 Send the weekday report",
            "/missing-krabb-home",
        )
        .unwrap();

        assert_eq!(params["cron"], "0 9 * * 1-5");
        assert_eq!(params["zone"], "Europe/Rome");
        assert_eq!(instruction, "Send the weekday report");
        assert!(
            super::parse_schedule_add("--every 300 --cron '* * * * *' --timezone UTC Run", "/tmp")
                .unwrap_err()
                .contains("either --every or --cron")
        );

        assert!(super::parse_schedule_add("--at yesterday Run", "/tmp").is_err());
        assert!(super::parse_schedule_add("30 --retries 8 Run", "/tmp").is_err());
    }

    #[tokio::test]
    async fn manual_schedule_controls_use_scheduler_ipc_methods() {
        let host = |_: String, _: String, params: serde_json::Value| async move {
            if params["method"] == "list" {
                Ok(json!({
                    "items": [
                        {"id": 7, "name": "morning-check"},
                        {"id": 8, "name": "evening-check"},
                        {"id": 9, "name": "night-check"}
                    ],
                    "pages": 1
                }))
            } else {
                Ok(json!({"id": params["params"]["id"], "status": "active"}))
            }
        };

        let run = super::schedule_command(
            "/tmp/krabb".into(),
            "run 7",
            "default".into(),
            "codex".into(),
            "gpt-6-luna".into(),
            None,
            host,
        )
        .await;
        let retry = super::schedule_command(
            "/tmp/krabb".into(),
            "retry 8",
            "default".into(),
            "codex".into(),
            "gpt-6-luna".into(),
            None,
            host,
        )
        .await;
        let remove_host = |_: String, _: String, params: serde_json::Value| async move {
            if params["method"] == "list" {
                Ok(json!({
                    "items": [{"id": 9, "name": "night-check"}],
                    "pages": 1
                }))
            } else {
                Ok(json!({"deleted": false, "cancel_requested": true}))
            }
        };

        let remove = super::schedule_command(
            "/tmp/krabb".into(),
            "remove 9",
            "default".into(),
            "codex".into(),
            "gpt-6-luna".into(),
            None,
            remove_host,
        )
        .await;

        assert!(run.contains("Schedule morning-check (7) queued to run now."), "{run}");
        assert!(retry.contains("Schedule evening-check (8) retry queued."));
        assert!(remove.contains("Cancellation requested for schedule night-check (9)."));
        assert!(remove.contains("side effect may already have happened"));
    }

    #[test]
    fn scheduled_tui_tasks_capture_the_configured_shell_setting() {
        let root =
            std::env::temp_dir().join(format!("krabb-tui-schedule-shell-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("config.toml");

        std::fs::write(&config, "[clients.tui]\nshell = true\n").unwrap();

        assert!(super::configured_tui_shell(root.to_str().unwrap()));

        std::fs::write(&config, "shell = true\n[clients.tui]\nshell = false\n").unwrap();

        assert!(!super::configured_tui_shell(root.to_str().unwrap()));

        std::fs::write(&config, "shell = true\n").unwrap();

        assert!(super::configured_tui_shell(root.to_str().unwrap()));

        std::fs::remove_dir_all(root).unwrap();
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
            "/tmp/krabb",
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
            "/tmp/krabb",
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
    ) -> krabb_core::Result<serde_json::Value> {
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
                (Some("scheduler"), Some("list")) => Ok(json!({
                    "items": [{"id": 7, "name": "morning-check", "due": 42, "text": "call back"}]
                })),

                (Some("scheduler"), Some("add")) => Ok(json!({"id": params["params"]["id"]})),
                (Some("scheduler"), Some("remove")) => Ok(json!({"deleted": true})),

                (Some("memory"), Some("list")) => {
                    assert_eq!(
                        params["params"]["scopes"],
                        json!(["global", "tui:default", "provider:tui:default"])
                    );

                    Ok(json!({
                        "items": [
                            {"key": "close-up", "value": "banana at the end", "type": "global"},
                            {"key": "drink", "value": "tea", "type": "session"},
                            {"key": "old-note", "value": "saved before upgrade", "type": "session"}
                        ],
                        "total": 3,
                        "page": params["params"]["page"],
                        "pages": 1
                    }))
                }

                (Some("memory"), Some("remember")) => Ok(json!({"ok": true})),
                (Some("memory"), Some("recall"))
                    if params["params"]["key"] == "drink"
                        && params["params"]["scope"] == "tui:default" =>
                {
                    Ok(json!({"item": {"key": "drink", "scope": "tui:default"}}))
                }

                (Some("memory"), Some("recall"))
                    if params["params"]["key"] == "old-note"
                        && params["params"]["scope"] == "provider:tui:default" =>
                {
                    Ok(json!({"item": {"key": "old-note", "scope": "provider:tui:default"}}))
                }

                (Some("memory"), Some("recall")) => Ok(json!({"item": null})),
                (Some("memory"), Some("forget"))
                    if params["params"]["key"] == "old-note"
                        && params["params"]["scope"] == "tui:default" =>
                {
                    Ok(json!({"deleted": false}))
                }

                (Some("memory"), Some("forget"))
                    if params["params"]["key"] == "old-note"
                        && params["params"]["scope"] == "provider:tui:default" =>
                {
                    Ok(json!({"deleted": true}))
                }

                (Some("memory"), Some("forget")) => Ok(json!({"deleted": true})),
                _ => Err(krabb_core::Error::Denied("Method not found.".into())),
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
                    Err(krabb_core::Error::Denied("Session was not found.".into()))
                } else {
                    Ok(json!({"id": params["id"], "changed": true}))
                }
            }

            _ => Err(krabb_core::Error::Denied("Method not found.".into())),
        }
    }

    async fn delayed_control(
        home: String,
        method: String,
        params: serde_json::Value,
    ) -> krabb_core::Result<serde_json::Value> {
        if method == "session.answer" {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            return Ok(json!({"text": "Delayed reply"}));
        }

        mock_control(home, method, params).await
    }

    async fn session_id_error_control(
        home: String,
        method: String,
        params: serde_json::Value,
    ) -> krabb_core::Result<serde_json::Value> {
        if method == "session.answer" {
            let id = params["id"].as_str().unwrap_or_default();

            return Err(krabb_core::Error::Denied(format!("Request for session {id} failed.")));
        }

        mock_control(home, method, params).await
    }

    async fn mock_deep_delete(
        _home: String,
        method: String,
        params: serde_json::Value,
    ) -> krabb_core::Result<serde_json::Value> {
        assert_eq!(method, "session.delete");
        assert_eq!(params["deep"], true);

        Ok(json!({"id": params["id"], "worktree": {"status": "removed"}}))
    }

    async fn delayed_answer_control(
        home: String,
        method: String,
        params: serde_json::Value,
    ) -> krabb_core::Result<serde_json::Value> {
        if method == "session.answer" {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            return Ok(json!({"text": "late reply"}));
        }

        super::local_aware(home, method, params).await
    }

    #[test]
    fn extracts_text_stream_events() {
        let event = krabb_core::types::Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: serde_json::json!({"event": {"kind": "text", "text": "part"}}),
        };

        assert_eq!(super::stream_text(event), Some("part".into()));
        assert!(
            super::stream_text(krabb_core::types::Request::call(1, "event", serde_json::json!({})))
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
            super::workspace_root("/tmp/krabb-home", None),
            super::PathBuf::from("/tmp/krabb-home/workspace")
        );

        assert_eq!(
            super::workspace_root("/tmp/krabb-home", Some("/work/project")),
            super::PathBuf::from("/work/project")
        );
    }

    #[test]
    fn relative_workspace_paths_resolve_against_the_tui_launch_directory() {
        let root = std::env::temp_dir().join(format!("krabb-tui-workspace-{}", std::process::id()));
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
            krabb_core::types::Request::call(
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

        let process = krabb_core::plugin::Process::start_with("sh", ["-c", script]).await.unwrap();

        let tools =
            std::sync::Arc::new(tokio::sync::Mutex::new(Some(super::ToolsProcess { process })));

        let response = super::serve_read_only_tool(
            std::sync::Arc::clone(&tools),
            krabb_core::types::Request::call(
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
            krabb_core::types::Request::call(
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
        let root = std::env::temp_dir().join(format!("krabb-tui-tools-{nonce}"));
        let codex = root.join("plugins/codex/bin/krabb-plugin-codex");
        let tools = root.join("plugins/tools/bin/krabb-plugin-tools");
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

        let root = std::env::temp_dir().join(format!("krabb-tui-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let binary = root.join("plugins/codex/bin/krabb-plugin-codex");
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
                "/help\n/status\n/approval\n/approvals\n/approve 0123456789abcdef01234567\n/deny 0123456789abcdef01234567\n/session list\n/session help\n/deliveries\n/retry bad id\n/drop bad id\n/plugin\n/workspace\n/workspace {}\n/workspace reset\n/schedule list\n/schedule add 30 break\n/schedule remove 7\n/memory\n/memory help\n/memory list\n/memory remember drink=tea\n/memory forget drink\n/memory forget old-note\n/model \n/model test\n/session bad!\n/new two\n/session create test-one\n/session one\n/session rename renamed\n/session archive two\n/session unarchive two\n/session unarchive missing\n/session delete two -y\n/session delete renamed -y\n/session archive renamed\n/compact\n/clear\n\nhello\n/quit\n",
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
        assert!(
            text.contains(
                "Schedules (1–1 of 1, page 1 of 1)\n└─ 7 | morning-check | pending\n   ├─ Next run: 1970-01-01 00:00:42 UTC\n   └─ Automatic retries are off"
            )
        );

        assert!(text.contains("is active."));
        assert!(text.contains("Schedule morning-check (7) was removed."));
        assert!(text.contains("Memories | 1–3 of 3 | page 1/1:"));
        assert!(text.contains("├─ close-up\n│  ├─ type: global\n│  └─ banana at the end"));
        assert!(text.contains("├─ drink\n│  ├─ type: session\n│  └─ tea"));
        assert!(text.contains("old-note"));
        assert!(text.matches("Manage memories for this session.").count() >= 2);
        assert!(text.contains("Memory saved for this session."));
        assert!(text.matches("Session memory removed.").count() >= 2);

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

    #[tokio::test]
    async fn one_shot_waits_for_a_delayed_model_response() {
        let root =
            std::env::temp_dir().join(format!("krabb-tui-one-shot-delay-{}", std::process::id()));

        let reply = super::run_once(
            root.display().to_string(),
            "codex".into(),
            "base".into(),
            None,
            "once-delayed".into(),
            "hello once".into(),
            delayed_control,
        )
        .await
        .unwrap();

        assert_eq!(reply, "Delayed reply");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn one_shot_does_not_print_session_ids_from_diagnostic_events() {
        let root =
            std::env::temp_dir().join(format!("krabb-tui-one-shot-error-{}", std::process::id()));
        let session = "tui-once-sensitive-session-id";

        let error = super::run_once(
            root.display().to_string(),
            "codex".into(),
            "base".into(),
            None,
            session.into(),
            "hello once".into(),
            session_id_error_control,
        )
        .await
        .unwrap_err();

        let krabb_core::Error::Denied(message) = error else {
            panic!("one-shot diagnostics should be reported with a generic message");
        };

        assert_eq!(message, "The one-shot request did not complete.");
        assert!(!message.contains(session));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keeps_the_session_engine_alive_after_a_model_error() {
        use std::{fs, os::unix::fs::PermissionsExt};
        use tokio::io::duplex;

        let root =
            std::env::temp_dir().join(format!("krabb-tui-model-error-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);

        let binary = root.join("plugins/codex/bin/krabb-plugin-codex");
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
            std::env::temp_dir().join(format!("krabb-tui-model-list-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);

        let binary = root.join("plugins/codex/bin/krabb-plugin-codex");
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

        assert!(output.contains("Manage the selected model.\n\nCommands:"));
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

        let root = std::env::temp_dir().join(format!("krabb-tui-interrupt-{}", std::process::id()));

        let _ = fs::remove_dir_all(&root);
        let binary = root.join("plugins/codex/bin/krabb-plugin-codex");
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
        let root = std::env::temp_dir().join(format!("krabb-tui-offline-{}", std::process::id()));
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
        let root = std::env::temp_dir().join(format!("krabb-tui-banner-{}", std::process::id()));

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
        let root = std::env::temp_dir().join(format!("krabb-tui-backend-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let home = root.display().to_string();
        let error = super::require_daemon(&home).await.unwrap_err();

        assert!(error.to_string().contains("daemon is not running"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn formats_denied_cli_errors_without_debug_wrappers() {
        let error = krabb_core::Error::Denied(
            "The Krabb daemon is not running.\nStart it with `krabb service start`.".into(),
        );

        assert_eq!(
            super::cli_error_message(&error),
            "The Krabb daemon is not running.\nStart it with `krabb service start`."
        );
    }

    #[tokio::test]
    async fn applies_an_explicit_model_override_to_an_existing_session() {
        let root = std::env::temp_dir().join(format!("krabb-tui-model-{}", std::process::id()));
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
