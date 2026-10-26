#![forbid(unsafe_code)]

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{collections::BTreeMap, fs, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use base64::Engine;
use krabb_core::{
    jsonl,
    plugin::serve_events_concurrent,
    types::{Capability, Hello, Protocol, Request, Response, ToolSpec},
};

#[cfg(unix)]
use rustix::process::{Pid, Signal, kill_process_group};

use serde_json::{Value, json};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, timeout},
};

const ACTION_TIME: Duration = Duration::from_secs(10);
const START_TIME: Duration = Duration::from_secs(5);
const SESSION_IDLE: Duration = Duration::from_secs(600);
const SESSION_REAPER: Duration = Duration::from_secs(30);
const IMAGE_LIMIT: usize = 4 * 1024 * 1024;
const COORDINATE_LIMIT: i64 = 4095;
const SCREEN: &str = "1280x720x24";

#[cfg(test)]
static TEST_COMMAND_DIRECTORY: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("computer", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    let state = Arc::new(Mutex::new(ComputerState::default()));
    let reaper_state = Arc::clone(&state);
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(SESSION_REAPER);

        loop {
            interval.tick().await;

            let mut state = reaper_state.lock().await;
            let _ = expire_session(&mut state).await;
        }
    });

    let result = serve_events_concurrent(
        Hello {
            protocol: Protocol::CURRENT,
            id: "computer".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Tool],
            commands: Vec::new(),
        },
        move |request, _emitter| {
            let state = Arc::clone(&state);
            async move { handle(&state, request).await }
        },
    )
    .await;

    reaper.abort();

    result
}

#[derive(Default)]
struct ComputerState {
    owner: Option<String>,
    session_id: Option<String>,
    last_used: Option<std::time::Instant>,
    display: Option<String>,
    server: Option<Child>,
    server_group: Option<u32>,
    application: Option<Child>,
    application_group: Option<u32>,
    auth_dir: Option<PathBuf>,
}

impl Drop for ComputerState {
    fn drop(&mut self) {
        kill_owned_group(self.server_group);
        kill_owned_group(self.application_group);

        if let Some(directory) = self.auth_dir.take() {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

async fn handle(
    state: &Mutex<ComputerState>,
    request: Request,
) -> krabb_core::Result<Option<Response>> {
    let Request::Call { id, method, params, .. } = request else {
        return Ok(None);
    };

    if method == "tools/list" {
        return Ok(Some(Response::ok(id, json!({"tools": tool_specs()}))));
    }

    let owner = params["host"]["session"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or_else(|| denied("Trusted Krabb session context is required."))?;

    let arguments = &params["arguments"];
    let mut state = state.lock().await;
    expire_session(&mut state).await?;

    if method != "computer_open" && method != "computer_close" {
        owned_display(&state, owner, arguments)?;
        state.last_used = Some(std::time::Instant::now());
    }

    let result = match method.as_str() {
        "computer_open" => open(&mut state, owner).await?,
        "computer_close" => close(&mut state, owner, arguments).await?,

        "computer_displays" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let output = run("xdotool", &["getdisplaygeometry"], display, &authority).await?;
            let dimensions = output
                .split_whitespace()
                .filter_map(|part| part.parse::<u32>().ok())
                .collect::<Vec<_>>();

            if dimensions.len() != 2 {
                return Err(denied("Could not read isolated display dimensions."));
            }

            json!({"display": display, "width": dimensions[0], "height": dimensions[1]})
        }

        "computer_windows" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let output =
                run("xdotool", &["search", "--onlyvisible", "--name", "."], display, &authority)
                    .await
                    .unwrap_or_default();
            let ids = output
                .lines()
                .filter(|id| {
                    !id.is_empty() && id.len() <= 32 && id.bytes().all(|byte| byte.is_ascii_digit())
                })
                .take(65)
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let truncated = ids.len() > 64;
            let mut windows = Vec::new();

            for id in ids.iter().take(64) {
                let Ok(title) = run("xdotool", &["getwindowname", id], display, &authority).await
                else {
                    continue;
                };

                let Ok(geometry) =
                    run("xdotool", &["getwindowgeometry", "--shell", id], display, &authority)
                        .await
                else {
                    continue;
                };

                if let Some(bounds) = window_bounds(&geometry) {
                    windows.push(json!({
                        "title": title.trim().chars().take(512).collect::<String>(),
                        "x": bounds.0,
                        "y": bounds.1,
                        "width": bounds.2,
                        "height": bounds.3
                    }));
                }
            }

            json!({"windows": windows, "truncated": truncated})
        }

        "computer_screenshot" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            screenshot(display, &authority, arguments["format"].as_str().unwrap_or("png")).await?
        }

        "computer_move" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let (x, y) = coordinates(arguments)?;
            let mut release = InputRelease::new(display, authority.clone());
            run("xdotool", &["mousemove", &x.to_string(), &y.to_string()], display, &authority)
                .await?;
            release.disarm();
            json!({"moved": true})
        }

        "computer_click" | "computer_double_click" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let (x, y) = coordinates(arguments)?;
            let button = mouse_button(arguments["button"].as_str().unwrap_or("left"))?;
            let mut release = InputRelease::new(display, authority.clone());
            run("xdotool", &["mousemove", &x.to_string(), &y.to_string()], display, &authority)
                .await?;
            let count = if method == "computer_double_click" { "2" } else { "1" };

            run("xdotool", &["click", "--repeat", count, button], display, &authority).await?;
            release.disarm();
            json!({"clicked": true})
        }

        "computer_drag" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let (x, y) = coordinates(arguments)?;
            let (to_x, to_y) = coordinates_at(arguments, "to_x", "to_y")?;
            let button = mouse_button(arguments["button"].as_str().unwrap_or("left"))?;
            let mut release = InputRelease::new(display, authority.clone());
            run("xdotool", &["mousemove", &x.to_string(), &y.to_string()], display, &authority)
                .await?;
            run("xdotool", &["mousedown", button], display, &authority).await?;
            let drag = run(
                "xdotool",
                &["mousemove", "--sync", &to_x.to_string(), &to_y.to_string(), "mouseup", button],
                display,
                &authority,
            )
            .await;

            if drag.is_err() {
                let _ = run("xdotool", &["mouseup", button], display, &authority).await;
            }

            drag?;
            release.disarm();
            json!({"dragged": true})
        }

        "computer_scroll" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let (x, y) = coordinates(arguments)?;
            let amount = bounded_integer(arguments, "amount", -20, 20)?;
            let button = if amount < 0 { "4" } else { "5" };

            let mut release = InputRelease::new(display, authority.clone());

            run("xdotool", &["mousemove", &x.to_string(), &y.to_string()], display, &authority)
                .await?;

            if amount != 0 {
                run(
                    "xdotool",
                    &["click", "--repeat", &amount.unsigned_abs().to_string(), button],
                    display,
                    &authority,
                )
                .await?;
            }

            release.disarm();
            json!({"scrolled": true})
        }

        "computer_keypress" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let key = arguments["key"]
                .as_str()
                .filter(|key| valid_key(key))
                .ok_or_else(|| denied("key must use supported X11 key names."))?;
            let mut release = InputRelease::new(display, authority.clone());
            run("xdotool", &["key", "--clearmodifiers", key], display, &authority).await?;
            release.disarm();
            json!({"pressed": true})
        }

        "computer_type" => {
            let display = owned_display(&state, owner, arguments)?;
            let authority = owned_authority(&state, owner, arguments)?;
            let text = arguments["text"]
                .as_str()
                .filter(|text| !text.is_empty() && text.len() <= 4096)
                .ok_or_else(|| denied("text must contain 1 to 4096 bytes."))?;
            let mut release = InputRelease::new(display, authority.clone());
            run(
                "xdotool",
                &["type", "--clearmodifiers", "--delay", "0", text],
                display,
                &authority,
            )
            .await?;
            release.disarm();
            json!({"typed": true})
        }

        _ => return Err(denied("Unknown computer tool.")),
    };

    Ok(Some(Response::ok(id, result)))
}

async fn open(state: &mut ComputerState, owner: &str) -> krabb_core::Result<Value> {
    let mode = std::env::var("KRABB_COMPUTER_MODE").unwrap_or_default();
    let display = std::env::var("KRABB_COMPUTER_DISPLAY").unwrap_or_default();
    let xvfb = std::env::var("KRABB_XVFB").unwrap_or_else(|_| "Xvfb".into());
    let startup = startup_config()?;

    open_with_config(state, owner, &mode, &display, &xvfb, startup).await
}

async fn open_with_config(
    state: &mut ComputerState,
    owner: &str,
    mode: &str,
    display: &str,
    xvfb: &str,
    startup: Option<(std::ffi::OsString, Vec<std::ffi::OsString>)>,
) -> krabb_core::Result<Value> {
    if !cfg!(target_os = "linux") {
        return Err(denied("The computer plugin currently supports Linux X11 only."));
    }

    if mode != "isolated" || !valid_display(display) {
        return Err(denied(
            "Computer access requires KRABB_COMPUTER_MODE=isolated and a dedicated X11 display above :0.",
        ));
    }

    if state.owner.is_some() {
        return Err(denied("The isolated desktop is already owned by another session."));
    }

    let session_id = session_id()?;
    let auth_dir = auth_directory()?;
    let authority = auth_dir.join("Xauthority");
    let mut cookie = [0_u8; 16];

    if let Err(error) = getrandom::fill(&mut cookie) {
        let _ = fs::remove_dir_all(&auth_dir);

        return Err(denied(format!("Could not create X11 authorization: {error}")));
    }

    let cookie = cookie.iter().map(|byte| format!("{byte:02x}")).collect::<String>();

    let auth = timeout(
        ACTION_TIME,
        command("xauth")
            .env_clear()
            .arg("-f")
            .arg(&authority)
            .args(["add", display, "MIT-MAGIC-COOKIE-1", &cookie])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status(),
    )
    .await;

    let auth = match auth {
        Ok(Ok(status)) => status,

        Ok(Err(_)) => {
            let _ = fs::remove_dir_all(&auth_dir);

            return Err(denied("The xauth dependency is unavailable."));
        }

        Err(_) => {
            let _ = fs::remove_dir_all(&auth_dir);

            return Err(denied("Creating X11 authorization timed out."));
        }
    };

    if !auth.success() {
        let _ = fs::remove_dir_all(&auth_dir);
        return Err(denied("Could not create X11 authorization."));
    }

    #[cfg(unix)]
    if let Err(error) = fs::set_permissions(&authority, fs::Permissions::from_mode(0o600)) {
        let _ = fs::remove_dir_all(&auth_dir);

        return Err(krabb_core::Error::Io(error));
    }

    let mut server_command = command(xvfb);
    server_command
        .env_clear()
        .env("DISPLAY", display)
        .env("XAUTHORITY", &authority)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .arg(display)
        .args(["-screen", "0", SCREEN, "-nolisten", "tcp", "-auth"])
        .arg(&authority)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    isolate_process(&mut server_command);

    let mut server = server_command.spawn().map_err(|_| {
        let _ = fs::remove_dir_all(&auth_dir);
        denied("Could not start Xvfb. Check the configured Xvfb installation.")
    })?;

    let server_group = server.id();

    let ready = timeout(START_TIME, async {
        loop {
            if server.try_wait().map_err(krabb_core::Error::Io)?.is_some() {
                return Err(denied("Xvfb exited before the isolated display became ready."));
            }

            if run_with_authority("xdotool", &["getdisplaygeometry"], display, &authority)
                .await
                .is_ok()
            {
                return Ok(());
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    if !matches!(ready, Ok(Ok(()))) {
        stop_owned_process(server, server_group).await;
        let _ = fs::remove_dir_all(&auth_dir);

        return match ready {
            Ok(Err(error)) => Err(error),
            _ => Err(denied("Xvfb did not become ready within five seconds.")),
        };
    }

    let application = if let Some((program, arguments)) = startup {
        match desktop_application(program, arguments, display, &authority, &auth_dir).spawn() {
            Ok(application) => Some(application),

            Err(_) => {
                stop_owned_process(server, server_group).await;
                let _ = fs::remove_dir_all(&auth_dir);

                return Err(denied("Could not start the configured desktop application."));
            }
        }
    } else {
        None
    };

    let application_group = application.as_ref().and_then(Child::id);
    state.owner = Some(owner.into());
    state.session_id = Some(session_id.clone());
    state.last_used = Some(std::time::Instant::now());
    state.display = Some(display.into());
    state.server = Some(server);
    state.server_group = server_group;
    state.application = application;
    state.application_group = application_group;
    state.auth_dir = Some(auth_dir);

    Ok(json!({"opened": true, "session_id": session_id, "display": display}))
}

async fn close(
    state: &mut ComputerState,
    owner: &str,
    arguments: &Value,
) -> krabb_core::Result<Value> {
    if owned_display(state, owner, arguments).is_err() {
        return Err(denied("Computer session is unavailable to this Krabb session."));
    }

    if let Some(application) = state.application.take() {
        stop_owned_process(application, state.application_group.take()).await;
    }

    if let Some(server) = state.server.take() {
        stop_owned_process(server, state.server_group.take()).await;
    }

    let cleanup =
        state.auth_dir.take().map(fs::remove_dir_all).transpose().map_err(krabb_core::Error::Io);

    state.owner = None;
    state.session_id = None;
    state.last_used = None;
    state.display = None;

    cleanup?;

    Ok(json!({"closed": true}))
}

fn owned_display<'a>(
    state: &'a ComputerState,
    owner: &str,
    arguments: &Value,
) -> krabb_core::Result<&'a str> {
    if state.owner.as_deref() != Some(owner)
        || state.session_id.as_deref() != arguments["session_id"].as_str()
    {
        return Err(denied("Computer session is unavailable to this Krabb session."));
    }

    state.display.as_deref().ok_or_else(|| denied("Computer session is closed."))
}

async fn expire_session(state: &mut ComputerState) -> krabb_core::Result<()> {
    if !state.last_used.is_some_and(|last_used| last_used.elapsed() >= SESSION_IDLE) {
        return Ok(());
    }

    if let Some(application) = state.application.take() {
        stop_owned_process(application, state.application_group.take()).await;
    }

    if let Some(server) = state.server.take() {
        stop_owned_process(server, state.server_group.take()).await;
    }

    let cleanup =
        state.auth_dir.take().map(fs::remove_dir_all).transpose().map_err(krabb_core::Error::Io);

    state.owner = None;
    state.session_id = None;
    state.last_used = None;
    state.display = None;

    cleanup.map(|_| ())
}

fn owned_authority(
    state: &ComputerState,
    owner: &str,
    arguments: &Value,
) -> krabb_core::Result<PathBuf> {
    if state.owner.as_deref() != Some(owner)
        || state.session_id.as_deref() != arguments["session_id"].as_str()
    {
        return Err(denied("Computer session is unavailable to this Krabb session."));
    }

    state
        .auth_dir
        .as_ref()
        .map(|directory| directory.join("Xauthority"))
        .ok_or_else(|| denied("Computer session has no X11 authorization."))
}

async fn screenshot(
    display: &str,
    authority: &std::path::Path,
    format: &str,
) -> krabb_core::Result<Value> {
    let (format, mime, signature) = match format {
        "png" => ("png", "image/png", b"\x89PNG\r\n\x1a\n".as_slice()),
        "jpeg" => ("jpeg", "image/jpeg", b"\xff\xd8\xff".as_slice()),
        _ => return Err(denied("format must be png or jpeg.")),
    };

    let geometry = run("xdotool", &["getdisplaygeometry"], display, authority).await?;

    let dimensions =
        geometry.split_whitespace().filter_map(|part| part.parse::<u32>().ok()).collect::<Vec<_>>();

    if dimensions.len() != 2 {
        return Err(denied("Could not read isolated display dimensions."));
    }

    let output_format = format!("{format}:-");
    let output = timeout(
        ACTION_TIME,
        desktop_command("import", display, authority)
            .args(["-window", "root", output_format.as_str()])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| denied("Screen capture timed out."))?
    .map_err(|_| denied("ImageMagick import is unavailable."))?;

    if !output.status.success() || output.stdout.len() > IMAGE_LIMIT {
        return Err(denied("Screen capture failed or exceeded the 4 MiB limit."));
    }

    if !output.stdout.starts_with(signature) {
        return Err(denied(format!("Screen capture returned invalid {format} data.")));
    }

    Ok(json!({
        "content": [
            {"kind":"text","text":"Isolated desktop screenshot captured."},
            {"kind":"image","uri":format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(output.stdout)),"alt":"Isolated desktop screenshot"}
        ],
        "metadata": {
            "width": dimensions[0],
            "height": dimensions[1],
            "format":mime,
            "coordinate_space":"display",
            "scale":1
        }
    }))
}

async fn run(
    program: &str,
    arguments: &[&str],
    display: &str,
    authority: &std::path::Path,
) -> krabb_core::Result<String> {
    run_with_authority(program, arguments, display, authority).await
}

async fn run_with_authority(
    program: &str,
    arguments: &[&str],
    display: &str,
    authority: &std::path::Path,
) -> krabb_core::Result<String> {
    let output = timeout(
        ACTION_TIME,
        desktop_command(program, display, authority)
            .args(arguments)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| denied(format!("{program} operation timed out.")))?
    .map_err(|_| denied(format!("Required desktop dependency '{program}' is unavailable.")))?;

    if !output.status.success() {
        return Err(denied(format!("{program} failed on the isolated display.")));
    }

    if output.stdout.len() > jsonl::MAX {
        return Err(krabb_core::Error::Limit(format!(
            "{program} output exceeded the protocol limit."
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

struct InputRelease {
    display: String,
    authority: PathBuf,
    armed: bool,
}

impl InputRelease {
    fn new(display: &str, authority: PathBuf) -> Self {
        Self { display: display.into(), authority, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InputRelease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let display = self.display.clone();
        let authority = self.authority.clone();

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for button in ["1", "2", "3", "4", "5"] {
                    let _ = run("xdotool", &["mouseup", button], &display, &authority).await;
                }

                let _ = run(
                    "xdotool",
                    &["keyup", "Shift_L", "Control_L", "Alt_L", "Super_L"],
                    &display,
                    &authority,
                )
                .await;
            });
        }
    }
}

fn desktop_application(
    program: std::ffi::OsString,
    arguments: Vec<std::ffi::OsString>,
    display: &str,
    authority: &std::path::Path,
    home: &std::path::Path,
) -> Command {
    let mut command = Command::new(program);
    command
        .env_clear()
        .env("DISPLAY", display)
        .env("XAUTHORITY", authority)
        .env("HOME", home)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    isolate_process(&mut command);

    command
}

fn isolate_process(command: &mut Command) {
    #[cfg(unix)]
    command.process_group(0);

    #[cfg(not(unix))]
    let _ = command;
}

fn kill_owned_group(process_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(id) = process_id.and_then(|id| i32::try_from(id).ok()).and_then(Pid::from_raw) {
        let _ = kill_process_group(id, Signal::KILL);
    }

    #[cfg(not(unix))]
    let _ = process_id;
}

async fn stop_owned_process(mut child: Child, process_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(id) = process_id.and_then(|id| i32::try_from(id).ok()).and_then(Pid::from_raw) {
        let _ = kill_process_group(id, Signal::TERM);
    }

    let stopped = tokio::time::timeout(Duration::from_millis(200), child.wait()).await.is_ok();
    kill_owned_group(process_id);

    if !stopped {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

fn session_id() -> krabb_core::Result<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes)
        .map_err(|error| denied(format!("Session ID generation failed: {error}")))?;

    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn startup_config() -> krabb_core::Result<Option<(std::ffi::OsString, Vec<std::ffi::OsString>)>> {
    let Some(program) = std::env::var_os("KRABB_COMPUTER_STARTUP") else {
        return Ok(None);
    };

    if program.is_empty() || program.len() > 4096 || !PathBuf::from(&program).is_absolute() {
        return Err(denied("KRABB_COMPUTER_STARTUP must name one absolute executable path."));
    }

    let arguments = std::env::var("KRABB_COMPUTER_STARTUP_ARGS").unwrap_or_else(|_| "[]".into());
    let arguments = serde_json::from_str::<Vec<String>>(&arguments)
        .map_err(|_| denied("KRABB_COMPUTER_STARTUP_ARGS must be a JSON array of strings."))?;

    if arguments.len() > 64
        || arguments.iter().any(|argument| argument.len() > 1024)
        || arguments.iter().map(String::len).sum::<usize>() > 8192
    {
        return Err(denied("Configured desktop application arguments exceed their limits."));
    }

    Ok(Some((program, arguments.into_iter().map(Into::into).collect())))
}

fn command(program: &str) -> Command {
    #[cfg(test)]
    if let Some(directory) = TEST_COMMAND_DIRECTORY.get() {
        return Command::new(directory.join(program));
    }

    Command::new(program)
}

fn desktop_command(program: &str, display: &str, authority: &std::path::Path) -> Command {
    let mut command = command(program);

    command
        .env_clear()
        .env("DISPLAY", display)
        .env("XAUTHORITY", authority)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default());

    command
}

fn auth_directory() -> krabb_core::Result<PathBuf> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| denied(format!("Could not create X11 session: {error}")))?;
    let name = random.iter().map(|byte| format!("{byte:02x}")).collect::<String>();

    let directory = std::env::temp_dir().join(format!("krabb-computer-{name}"));
    fs::create_dir(&directory).map_err(krabb_core::Error::Io)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Err(error) = fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)) {
            let _ = fs::remove_dir_all(&directory);

            return Err(krabb_core::Error::Io(error));
        }
    }

    Ok(directory)
}

fn valid_display(display: &str) -> bool {
    let Some(number) = display.strip_prefix(':').and_then(|rest| rest.split('.').next()) else {
        return false;
    };

    number.parse::<u16>().is_ok_and(|value| value > 0) && display == format!(":{number}")
}

fn coordinates(arguments: &Value) -> krabb_core::Result<(i64, i64)> {
    Ok((
        bounded_integer(arguments, "x", 0, COORDINATE_LIMIT)?,
        bounded_integer(arguments, "y", 0, COORDINATE_LIMIT)?,
    ))
}

fn coordinates_at(arguments: &Value, x: &str, y: &str) -> krabb_core::Result<(i64, i64)> {
    Ok((
        bounded_integer(arguments, x, 0, COORDINATE_LIMIT)?,
        bounded_integer(arguments, y, 0, COORDINATE_LIMIT)?,
    ))
}

fn bounded_integer(
    arguments: &Value,
    name: &str,
    minimum: i64,
    maximum: i64,
) -> krabb_core::Result<i64> {
    arguments[name]
        .as_i64()
        .filter(|value| (minimum..=maximum).contains(value))
        .ok_or_else(|| denied(format!("{name} must be between {minimum} and {maximum}.")))
}

fn window_bounds(output: &str) -> Option<(i32, i32, u32, u32)> {
    let mut values = BTreeMap::new();

    for line in output.lines() {
        let (key, value) = line.split_once('=')?;

        if matches!(key, "X" | "Y" | "WIDTH" | "HEIGHT") {
            values.insert(key, value.parse::<i32>().ok()?);
        }
    }

    let x = *values.get("X")?;
    let y = *values.get("Y")?;
    let width = *values.get("WIDTH")?;
    let height = *values.get("HEIGHT")?;

    if !(-4096..=4096).contains(&x)
        || !(-4096..=4096).contains(&y)
        || !(1..=4096).contains(&width)
        || !(1..=4096).contains(&height)
    {
        return None;
    }

    Some((x, y, width as u32, height as u32))
}

fn mouse_button(button: &str) -> krabb_core::Result<&'static str> {
    match button {
        "left" => Ok("1"),
        "middle" => Ok("2"),
        "right" => Ok("3"),
        _ => Err(denied("button must be left, middle, or right.")),
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'_' | b'-'))
}

fn denied(message: impl Into<String>) -> krabb_core::Error {
    krabb_core::Error::Denied(message.into())
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

fn tool_specs() -> Vec<ToolSpec> {
    let coordinate = json!({"type":"integer","minimum":0,"maximum":4095});

    let point = || {
        json!({
            "session_id":{"type":"string"},
            "x":coordinate.clone(),
            "y":coordinate.clone()
        })
    };

    [
        (
            "computer_open",
            "Claim the configured isolated X11 display.",
            object_schema(json!({}), &[]),
        ),
        (
            "computer_close",
            "Release the owned isolated display session.",
            object_schema(json!({"session_id":{"type":"string"}}), &["session_id"]),
        ),
        (
            "computer_displays",
            "Read dimensions of the owned isolated display.",
            object_schema(json!({"session_id":{"type":"string"}}), &["session_id"]),
        ),
        (
            "computer_windows",
            "List bounded visible window titles and bounds on the owned display.",
            object_schema(json!({"session_id":{"type":"string"}}), &["session_id"]),
        ),
        (
            "computer_screenshot",
            "Capture the owned isolated display as an image.",
            object_schema(
                json!({"session_id":{"type":"string"},"format":{"type":"string","enum":["png","jpeg"]}}),
                &["session_id"],
            ),
        ),
        (
            "computer_move",
            "Move the pointer to bounded display coordinates.",
            object_schema(point(), &["session_id", "x", "y"]),
        ),
        (
            "computer_click",
            "Click a mouse button at bounded display coordinates.",
            object_schema(
                {
                    let mut value = point();
                    value["button"] = json!({"type":"string","enum":["left","middle","right"]});
                    value
                },
                &["session_id", "x", "y"],
            ),
        ),
        (
            "computer_double_click",
            "Double click at bounded display coordinates.",
            object_schema(
                {
                    let mut value = point();
                    value["button"] = json!({"type":"string","enum":["left","middle","right"]});
                    value
                },
                &["session_id", "x", "y"],
            ),
        ),
        (
            "computer_drag",
            "Drag between bounded display coordinates.",
            object_schema(
                {
                    let mut value = point();
                    value["to_x"] = coordinate.clone();
                    value["to_y"] = coordinate.clone();
                    value["button"] = json!({"type":"string","enum":["left","middle","right"]});
                    value
                },
                &["session_id", "x", "y", "to_x", "to_y"],
            ),
        ),
        (
            "computer_scroll",
            "Scroll by a bounded number of display steps.",
            object_schema(
                {
                    let mut value = point();
                    value["amount"] = json!({"type":"integer","minimum":-20,"maximum":20});
                    value
                },
                &["session_id", "x", "y", "amount"],
            ),
        ),
        (
            "computer_keypress",
            "Press a bounded X11 key name or combination.",
            object_schema(
                json!({"session_id":{"type":"string"},"key":{"type":"string","minLength":1,"maxLength":64}}),
                &["session_id", "key"],
            ),
        ),
        (
            "computer_type",
            "Type bounded text into the focused application.",
            object_schema(
                json!({"session_id":{"type":"string"},"text":{"type":"string","minLength":1,"maxLength":4096}}),
                &["session_id", "text"],
            ),
        ),
    ]
    .into_iter()
    .map(|(name, description, schema)| ToolSpec {
        name: name.into(),
        description: Some(description.into()),
        schema,
    })
    .collect()
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        COORDINATE_LIMIT, ComputerState, InputRelease, SESSION_IDLE, TEST_COMMAND_DIRECTORY,
        bounded_integer, mouse_button, open_with_config, owned_display, tool_specs, valid_display,
        valid_key,
    };

    use krabb_core::types::Request;
    use serde_json::json;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    use tokio::sync::Mutex;

    fn fake_commands() -> &'static PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        TEST_COMMAND_DIRECTORY.get_or_init(|| {
            let directory = std::env::temp_dir().join(format!(
                "krabb-computer-commands-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&directory).unwrap();
            let xdotool = directory.join("xdotool");
            let calls = directory.join("xdotool-calls.log");
            fs::write(
                &xdotool,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in getdisplaygeometry) printf '1280 720\\n' ;; search) printf '1\\n' ;; getwindowname) printf 'Fixture window\\n' ;; getwindowgeometry) printf 'X=12\\nY=34\\nWIDTH=640\\nHEIGHT=480\\n' ;; esac\n",
                    calls.display()
                ),
            )
            .unwrap();
            fs::set_permissions(&xdotool, fs::Permissions::from_mode(0o700)).unwrap();
            let xauth = directory.join("xauth");
            fs::write(&xauth, "#!/bin/sh\ntouch \"$2\"\n").unwrap();
            fs::set_permissions(&xauth, fs::Permissions::from_mode(0o700)).unwrap();
            let xvfb = directory.join("Xvfb");
            fs::write(&xvfb, "#!/bin/sh\nexec sleep 30\n").unwrap();
            fs::set_permissions(&xvfb, fs::Permissions::from_mode(0o700)).unwrap();
            let app = directory.join("desktop-app");
            fs::write(&app, "#!/bin/sh\nexec sleep 30\n").unwrap();
            fs::set_permissions(&app, fs::Permissions::from_mode(0o700)).unwrap();
            let import = directory.join("import");
            fs::write(
                &import,
                "#!/bin/sh\ncase \"$*\" in *jpeg:-*) printf '\\377\\330\\377' ;; *) printf '\\211PNG\\r\\n\\032\\n' ;; esac\n",
            )
            .unwrap();
            fs::set_permissions(&import, fs::Permissions::from_mode(0o700)).unwrap();
            directory
        })
    }

    #[test]
    fn validates_isolated_display_values() {
        assert!(valid_display(":1"));

        assert!(!valid_display(":42.0"));
        assert!(!valid_display(":0"));
        assert!(!valid_display("host:1"));
        assert!(!valid_display(":1/tcp"));
        assert!(ComputerState::default().owner.is_none());
    }

    #[test]
    fn bounds_coordinates_buttons_and_keys() {
        assert_eq!(bounded_integer(&json!({"x":4095}), "x", 0, COORDINATE_LIMIT).unwrap(), 4095);
        assert!(bounded_integer(&json!({"x":4096}), "x", 0, COORDINATE_LIMIT).is_err());
        assert_eq!(mouse_button("right").unwrap(), "3");
        assert!(mouse_button("shell-command").is_err());
        assert!(valid_key("CTRL+L"));
        assert!(!valid_key("key;rm"));
    }

    #[test]
    fn advertises_only_implemented_computer_tools() {
        let specs = tool_specs();
        let names = specs.iter().map(|spec| spec.name.as_str()).collect::<Vec<_>>();

        assert_eq!(names.len(), 12);
        assert!(names.contains(&"computer_screenshot"));
        assert!(names.contains(&"computer_windows"));
    }

    #[test]
    fn enforces_owner_for_a_claimed_display() {
        let mut state = ComputerState::default();
        state.owner = Some("owner-a".into());
        state.session_id = Some("session-a".into());
        state.display = Some(":99".into());

        assert_eq!(
            owned_display(&state, "owner-a", &json!({"session_id":"session-a"})).unwrap(),
            ":99"
        );

        assert!(owned_display(&state, "owner-a", &json!({"session_id":"session-b"})).is_err());
        assert!(owned_display(&state, "owner-b", &json!({"session_id":"session-a"})).is_err());
    }

    #[tokio::test]
    async fn lists_tools_without_accessing_a_display() {
        let state = Mutex::new(ComputerState::default());
        let response = super::handle(&state, Request::call(1, "tools/list", json!({})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.result.unwrap()["tools"].as_array().unwrap().len(), 12);
    }

    #[tokio::test]
    async fn handles_isolated_display_observation_and_input_actions() {
        let _ = fake_commands();
        let mut initial = ComputerState::default();
        let application = TEST_COMMAND_DIRECTORY.get().unwrap().join("desktop-app");
        let opened = open_with_config(
            &mut initial,
            "owner-a",
            "isolated",
            ":99",
            "Xvfb",
            Some((application.into_os_string(), Vec::new())),
        )
        .await
        .unwrap();

        assert_eq!(opened["display"], ":99");
        assert!(opened["session_id"].as_str().is_some());
        assert!(initial.application.is_some());
        let authority = initial.auth_dir.as_ref().unwrap().join("Xauthority");
        let auth_dir = initial.auth_dir.clone().unwrap();

        assert!(authority.is_file());
        let state = Mutex::new(initial);

        let displays = invoke(&state, "owner-a", "computer_displays", json!({})).await.unwrap();

        assert_eq!(displays["width"], 1280);
        assert_eq!(displays["height"], 720);

        let screenshot = invoke(&state, "owner-a", "computer_screenshot", json!({})).await.unwrap();

        assert_eq!(screenshot["content"][1]["kind"], "image");
        assert_eq!(screenshot["metadata"]["width"], 1280);

        let jpeg = invoke(&state, "owner-a", "computer_screenshot", json!({"format":"jpeg"}))
            .await
            .unwrap();

        assert!(jpeg["content"][1]["uri"].as_str().unwrap().starts_with("data:image/jpeg;base64,"));
        assert_eq!(jpeg["metadata"]["format"], "image/jpeg");

        let windows = invoke(&state, "owner-a", "computer_windows", json!({})).await.unwrap();

        assert_eq!(windows["windows"][0]["title"], "Fixture window");
        assert_eq!(windows["windows"][0]["width"], 640);

        for (method, arguments) in [
            ("computer_move", json!({"x":1,"y":2})),
            ("computer_click", json!({"x":1,"y":2})),
            ("computer_double_click", json!({"x":1,"y":2,"button":"right"})),
            ("computer_drag", json!({"x":1,"y":2,"to_x":3,"to_y":4})),
            ("computer_scroll", json!({"x":1,"y":2,"amount":2})),
            ("computer_keypress", json!({"key":"CTRL+L"})),
            ("computer_type", json!({"text":"hello"})),
        ] {
            assert!(invoke(&state, "owner-a", method, arguments).await.is_ok());
        }

        let closed = invoke(&state, "owner-a", "computer_close", json!({})).await.unwrap();

        assert_eq!(closed["closed"], true);
        assert!(state.lock().await.owner.is_none());
        assert!(!auth_dir.exists());
    }

    #[tokio::test]
    async fn expires_idle_desktop_sessions_and_removes_their_processes_and_credentials() {
        let _ = fake_commands();
        let mut state = ComputerState::default();
        let opened =
            open_with_config(&mut state, "owner-a", "isolated", ":99", "Xvfb", None).await.unwrap();

        let auth_dir = state.auth_dir.clone().unwrap();
        state.last_used = Some(Instant::now() - SESSION_IDLE - Duration::from_secs(1));
        let state = Mutex::new(state);

        assert!(invoke(&state, "owner-a", "computer_displays", json!({})).await.is_err());
        assert!(state.lock().await.owner.is_none());
        assert!(!auth_dir.exists());
        assert!(opened["session_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn releases_held_input_after_an_action_is_canceled() {
        let calls = fake_commands().join("xdotool-calls.log");
        fs::write(&calls, "").unwrap();
        drop(InputRelease::new(":99", PathBuf::from("/tmp/authority")));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let log = fs::read_to_string(&calls).unwrap();

                if log.contains("mouseup 1")
                    && log.contains("keyup Shift_L Control_L Alt_L Super_L")
                {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("canceled input cleanup should release buttons and modifiers");
    }

    async fn invoke(
        state: &Mutex<ComputerState>,
        owner: &str,
        method: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let mut arguments = arguments;

        if method != "computer_open"
            && let Some(session_id) = state.lock().await.session_id.clone()
        {
            arguments["session_id"] = json!(session_id);
        }

        let response = super::handle(
            state,
            Request::call(
                2,
                method,
                json!({"host":{"session":owner,"turn":"turn-1"},"arguments":arguments}),
            ),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "call returned no response".to_owned())?;

        response.result.ok_or_else(|| {
            response
                .error
                .map_or_else(|| "response returned no result".to_owned(), |error| error.message)
        })
    }
}
