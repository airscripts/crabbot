#![forbid(unsafe_code)]

use std::sync::atomic::AtomicU64;
use std::{
    ffi::OsString,
    io::{ErrorKind, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crabbot_core::{
    plugin::serve_with,
    policy::{Policy, Shell},
    types::{Capability, Hello, Protocol, Request, Response},
};
#[cfg(unix)]
use rustix::{
    fd::OwnedFd,
    fs::{AtFlags, Dir, Mode, OFlags, openat, renameat, unlinkat},
    process::{Pid, Signal, getpgid, getpgrp, kill_process, kill_process_group},
};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::Semaphore,
    time::timeout,
};

const FRAME_HEADROOM: usize = 64 * 1024;
const FILE_LIMIT: usize = (crabbot_core::jsonl::MAX - FRAME_HEADROOM) / 2;
const OUTPUT_LIMIT: usize = (crabbot_core::jsonl::MAX - FRAME_HEADROOM) / 4;
const ENTRY_LIMIT: usize = 10_000;
const HIT_LIMIT: usize = 10_000;
const SEARCH_NODES: usize = 20_000;
const SEARCH_DIRECTORIES: usize = 10_000;
const SEARCH_FILES: usize = 10_000;
const SEARCH_BYTES: u64 = 256 * 1024 * 1024;
const SEARCH_DEPTH: usize = 64;
const SEARCH_TIME: Duration = Duration::from_secs(10);
const COMMAND_LIMIT: Duration = Duration::from_secs(120);
static SEARCHES: OnceLock<Arc<Semaphore>> = OnceLock::new();
static TEMP_FILES: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let root = std::env::var_os("CRABBOT_ROOT")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("CRABBOT_ROOT is not configured.".into()))?;
    let shell = match std::env::var("CRABBOT_SHELL").as_deref() {
        Ok("on") => Shell::On,
        Ok("ask") => Shell::Ask,
        _ => Shell::Off,
    };
    let sandbox = Sandbox::from_env()?;
    let policy = Arc::new(Policy { shell, root: Some(root) });

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "tools".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Tool],
            commands: vec![],
        },
        move |request| {
            let policy = Arc::clone(&policy);
            let sandbox = sandbox.clone();
            async move { call_with_sandbox(&policy, request, sandbox.as_ref()).await }
        },
    )
    .await
}

#[cfg(test)]
async fn call(policy: &Policy, request: Request) -> crabbot_core::Result<Option<Response>> {
    Box::pin(call_with_sandbox(policy, request, None)).await
}

async fn call_with_sandbox(
    policy: &Policy,
    request: Request,
    sandbox: Option<&Sandbox>,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let result = match method.as_str() {
        "read" => {
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let path = confined(
                policy,
                params["path"].as_str().ok_or_else(|| denied("read.path is required"))?,
                workspace.as_deref(),
                false,
            )?;
            json!({
                "text": read_confined(&path).map_err(|error| denied(format!("Read failed: {error}")))?
            })
        }
        "write" => {
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let path = params["path"].as_str().ok_or_else(|| denied("write.path is required"))?;
            let text = params["text"].as_str().ok_or_else(|| denied("write.text is required"))?;

            if params["approve"].as_bool() != Some(true) {
                return Err(denied("Write approval is required"));
            }

            let path = confined(policy, path, workspace.as_deref(), true)?;
            write_confined(&path, text)
                .map_err(|error| denied(format!("Write failed: {error}")))?;
            json!({"ok": true})
        }
        "list" => {
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let path = confined(
                policy,
                params["path"].as_str().unwrap_or("."),
                workspace.as_deref(),
                false,
            )?;
            let mut items =
                list_confined(&path).map_err(|error| denied(format!("List failed: {error}")))?;
            items.sort();
            json!({"items": items})
        }
        "search" => {
            let path = params["path"].as_str().unwrap_or(".").to_owned();
            let needle = params["text"]
                .as_str()
                .ok_or_else(|| denied("search.text is required"))?
                .to_owned();
            let workspace = params["workspace"].as_str().map(str::to_owned);
            let policy = policy.clone();
            let cancel = Arc::new(AtomicBool::new(false));
            let worker_cancel = Arc::clone(&cancel);
            let permit = timeout(SEARCH_TIME, searches().acquire_owned())
                .await
                .map_err(|_| denied("Search exceeded the hard time limit"))?
                .map_err(|_| denied("Search worker is unavailable"))?;
            let task = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                search_request(&policy, &path, &needle, workspace.as_deref(), worker_cancel)
            });
            let result = match timeout(SEARCH_TIME, task).await {
                Ok(result) => result.map_err(|error| denied(format!("Search failed: {error}")))?,
                Err(_) => {
                    cancel.store(true, Ordering::Relaxed);
                    return Err(denied("Search exceeded the hard time limit"));
                }
            };
            let hits = result?;
            json!({"hits": hits})
        }
        "patch" => {
            if params["approve"].as_bool() != Some(true) {
                return Err(denied("Patch approval is required"));
            }
            let text = params["text"].as_str().ok_or_else(|| denied("patch.text is required"))?;
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let root = file_at(policy, ".", workspace.as_deref())?;
            patch_paths_at(policy, text, workspace.as_deref())?;
            let checked = apply(&root, text, true)
                .await
                .map_err(|error| denied(format!("Patch check failed: {error}")))?;
            if !checked.status.success() {
                return Err(denied(format!(
                    "Patch check failed: {}",
                    String::from_utf8_lossy(&checked.stderr).trim()
                )));
            }
            let applied = apply(&root, text, false)
                .await
                .map_err(|error| denied(format!("Patch failed: {error}")))?;
            if !applied.status.success() {
                return Err(denied(format!(
                    "Patch failed: {}",
                    String::from_utf8_lossy(&applied.stderr).trim()
                )));
            }
            json!({"ok": true})
        }
        "git" => {
            let args = params["args"]
                .as_array()
                .ok_or_else(|| denied("git.args is required"))?
                .iter()
                .map(|value| value.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| denied("git.args must contain strings"))?;
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let changes = matches!(args.as_slice(), [command, action, _] if command == "worktree" && action == "add")
                || matches!(args.as_slice(), [command, action, _] if command == "worktree" && action == "remove");
            if changes {
                if params["approve"].as_bool() != Some(true) {
                    return Err(denied("Worktree approval is required"));
                }
                if let Some(path) = args.get(2) {
                    file_at(policy, path, workspace.as_deref())?;
                }
            } else if !matches!(args.as_slice(), [command] if command == "status" || command == "diff")
                && !matches!(args.as_slice(), [command, list] if command == "worktree" && list == "list")
            {
                return Err(denied("Only git status, diff, and worktree operations are allowed"));
            }
            let root = file_at(policy, ".", workspace.as_deref())?;
            let output =
                git(&args, &root).await.map_err(|error| denied(format!("Git failed: {error}")))?;
            json!({
                "status": output.status.code(),
                "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr)
            })
        }
        "shell" => {
            policy.shell(params["approve"].as_bool() == Some(true))?;
            let command =
                params["command"].as_str().ok_or_else(|| denied("shell.command is required"))?;
            let workspace = workspace(policy, params["workspace"].as_str())?;
            let root = file_at(policy, ".", workspace.as_deref())?;
            let output = shell_with(command, &root, sandbox)
                .await
                .map_err(|error| denied(format!("Shell failed: {error}")))?;
            json!({
                "status": output.status.code(),
                "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr)
            })
        }
        _ => return Ok(None),
    };

    response(id, result)
}

fn response(id: u64, result: serde_json::Value) -> crabbot_core::Result<Option<Response>> {
    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(denied("Tool response exceeds the protocol frame limit"));
    }
    Ok(Some(response))
}

#[cfg(test)]
async fn shell(command: &str, root: &Path) -> std::io::Result<std::process::Output> {
    shell_with(command, root, None).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Sandbox {
    runtime: String,
    image: String,
}

impl Sandbox {
    fn from_env() -> std::io::Result<Option<Self>> {
        let runtime = std::env::var("CRABBOT_SANDBOX_RUNTIME").ok();
        let image = std::env::var("CRABBOT_SANDBOX_IMAGE").ok();
        Self::parse(runtime.as_deref(), image.as_deref())
    }

    fn parse(runtime: Option<&str>, image: Option<&str>) -> std::io::Result<Option<Self>> {
        let Some(runtime) = runtime else {
            if image.is_some() {
                return Err(invalid("CRABBOT_SANDBOX_IMAGE requires CRABBOT_SANDBOX_RUNTIME."));
            }
            return Ok(None);
        };
        if runtime == "off" {
            if image.is_some() {
                return Err(invalid("CRABBOT_SANDBOX_IMAGE cannot be set when sandboxing is off."));
            }
            return Ok(None);
        }
        if !matches!(runtime, "docker" | "podman") {
            return Err(invalid("CRABBOT_SANDBOX_RUNTIME must be docker, podman, or off."));
        }
        let image = image
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 255
                    && !value.starts_with('-')
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"./:@_-".contains(&byte))
            })
            .ok_or_else(|| invalid("CRABBOT_SANDBOX_IMAGE must name a valid local image."))?;
        Ok(Some(Self { runtime: runtime.into(), image: image.into() }))
    }

    fn process(&self, command: &str, root: &Path, cidfile: &Path) -> std::io::Result<Command> {
        let root = root
            .to_str()
            .filter(|path| !path.contains(',') && !path.chars().any(char::is_control))
            .ok_or_else(|| invalid("Sandbox workspace path cannot be represented safely."))?;
        let mut process = Command::new(&self.runtime);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            process.as_std_mut().process_group(0);
        }
        process.arg("run");
        #[cfg(unix)]
        process.arg("--user").arg(format!(
            "{}:{}",
            rustix::process::getuid().as_raw(),
            rustix::process::getgid().as_raw()
        ));
        process
            .args([
                "--rm",
                "--pull=never",
                "--read-only",
                "--network=none",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--pids-limit=128",
                "--memory=1g",
                "--cpus=2",
                "--tmpfs=/tmp:rw,noexec,nosuid,size=64m",
                "--entrypoint=sh",
                "--mount",
            ])
            .arg(format!("type=bind,source={root},target=/workspace"))
            .arg("--cidfile")
            .arg(cidfile)
            .args(["--workdir", "/workspace"])
            .arg(&self.image)
            .args(["-lc", command])
            .current_dir(root);
        Ok(process)
    }

    async fn remove(&self, cidfile: &Path) -> std::io::Result<()> {
        let id = match std::fs::read_to_string(cidfile) {
            Ok(id) => id.trim().to_owned(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !(12..=128).contains(&id.len()) || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid("Container runtime returned an invalid container ID."));
        }
        let mut process = Command::new(&self.runtime);
        process
            .args(["rm", "--force", &id])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let status = timeout(Duration::from_secs(10), process.status()).await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Container cleanup exceeded the time limit.",
            )
        })??;
        if status.success() {
            Ok(())
        } else {
            Err(std::io::Error::other("Container runtime could not stop the timed-out command."))
        }
    }
}

async fn shell_with(
    command: &str,
    root: &Path,
    sandbox: Option<&Sandbox>,
) -> std::io::Result<std::process::Output> {
    if let Some(sandbox) = sandbox {
        return sandbox_shell(command, root, sandbox).await;
    }

    #[cfg(windows)]
    {
        let mut process = Command::new("cmd");
        process.args(["/C", command]).current_dir(root);
        capture(process, None, true).await
    }

    #[cfg(not(windows))]
    {
        let mut process = Command::new("sh");
        process.process_group(0).args(["-c", command]);
        process.current_dir(root);
        capture(process, None, true).await
    }
}

async fn sandbox_shell(
    command: &str,
    root: &Path,
    sandbox: &Sandbox,
) -> std::io::Result<std::process::Output> {
    let cid_dir = cid_dir()?;
    let cidfile = cid_dir.join("id");
    let process = match sandbox.process(command, root, &cidfile) {
        Ok(process) => process,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&cid_dir);
            return Err(error);
        }
    };
    let result = capture(process, None, true).await;
    let cleanup = if result.is_err() { sandbox.remove(&cidfile).await } else { Ok(()) };
    let remove_dir = std::fs::remove_dir_all(&cid_dir);
    match (result, cleanup, remove_dir) {
        (Ok(output), Ok(()), Ok(())) => Ok(output),
        (Err(error), Ok(()), Ok(())) => Err(error),
        (result, cleanup, remove_dir) => {
            let message = [
                result.err().map(|error| error.to_string()),
                cleanup.err().map(|error| format!("container cleanup failed: {error}")),
                remove_dir.err().map(|error| format!("temporary cleanup failed: {error}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("; ");
            Err(std::io::Error::other(message))
        }
    }
}

fn cid_dir() -> std::io::Result<std::path::PathBuf> {
    let parent = std::env::temp_dir();
    for _ in 0..16 {
        let nonce = TEMP_FILES.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("crabbot-container-{}-{nonce}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
                }
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "Could not allocate a private container ID file.",
    ))
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(windows)]
const DISABLED_HOOKS: &str = "NUL";
#[cfg(not(windows))]
const DISABLED_HOOKS: &str = "/dev/null";

fn git_command(args: &[String]) -> Command {
    let mut process = Command::new("git");
    process.args(["--no-pager", "--no-optional-locks", "-c"]);
    process.arg(format!("core.hooksPath={DISABLED_HOOKS}"));
    process.args(["-c", "core.fsmonitor=false", "-c", "diff.external="]);
    if args.first().is_some_and(|argument| argument == "diff") {
        process.arg("diff").args(["--no-ext-diff", "--no-textconv"]).args(&args[1..]);
    } else {
        process.args(args);
    }
    process.env("GIT_CONFIG_NOSYSTEM", "1");
    process.env("GIT_CONFIG_COUNT", "0");
    process.env_remove("GIT_DIR");
    process.env_remove("GIT_WORK_TREE");
    process.env_remove("GIT_INDEX_FILE");
    process.env_remove("GIT_COMMON_DIR");
    process.env_remove("GIT_OBJECT_DIRECTORY");
    process.env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES");
    process.env_remove("GIT_EXTERNAL_DIFF");
    process.env_remove("GIT_DIFF_OPTS");
    process.env_remove("GIT_PAGER");
    process.env_remove("GIT_SSH");
    process.env_remove("GIT_SSH_COMMAND");
    process.env_remove("GIT_EDITOR");
    process.env_remove("GIT_SEQUENCE_EDITOR");
    process.env_remove("GIT_ASKPASS");
    process
}

async fn git(args: &[String], root: &Path) -> std::io::Result<std::process::Output> {
    let mut process = git_command(args);
    process.current_dir(root);
    capture(process, None, false).await
}

async fn apply(root: &Path, text: &str, check: bool) -> std::io::Result<std::process::Output> {
    let mut command = git_command(&["apply".into()]);
    command.arg("--whitespace=error");
    if check {
        command.arg("--check");
    }
    command
        .arg("-")
        .current_dir(root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    capture(command, Some(text.as_bytes()), false).await
}

async fn capture(
    mut command: Command,
    input: Option<&[u8]>,
    group: bool,
) -> std::io::Result<std::process::Output> {
    command
        .stdin(if input.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn()?;
    let process_id = child.id();

    #[cfg(unix)]
    let process_group = process_id.and_then(child_group);

    #[cfg(windows)]
    let process_group = process_id;

    if let Some(input) = input {
        let mut pipe = child.stdin.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stdin.")
        })?;
        pipe.write_all(input).await?;
        pipe.shutdown().await?;
    }

    let stdout = child.stdout.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stdout.")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stderr.")
    })?;
    let result = timeout(COMMAND_LIMIT, async {
        let stdout = limited(stdout);
        let stderr = limited(stderr);
        let status = child.wait();
        let (stdout, stderr, status) = tokio::try_join!(stdout, stderr, status)?;
        Ok::<_, std::io::Error>(std::process::Output { status, stdout, stderr })
    })
    .await;

    match result {
        Ok(Ok(output)) => {
            if group {
                stop(&mut child, process_group, true).await;
            }
            Ok(output)
        }

        Ok(Err(error)) => {
            stop(&mut child, process_group, group).await;
            Err(error)
        }

        Err(_) => {
            stop(&mut child, process_group, group).await;

            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Process exceeded the execution time limit.",
            ))
        }
    }
}

async fn limited<R: AsyncRead + Unpin>(mut input: R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = input.read(&mut buffer).await?;
        if count == 0 {
            return Ok(bytes);
        }
        if count > OUTPUT_LIMIT.saturating_sub(bytes.len()) {
            return Err(std::io::Error::new(
                ErrorKind::FileTooLarge,
                "Process output exceeds the size limit.",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

async fn stop(child: &mut Child, process_id: Option<u32>, group: bool) {
    #[cfg(unix)]
    if group && let Some(group) = process_id.and_then(external_group) {
        let _ = kill_process_group(group, Signal::TERM);
        tokio::time::sleep(Duration::from_millis(100)).await;
        kill_group(group, Signal::KILL);
    }

    #[cfg(windows)]
    if group && let Some(id) = process_id {
        let _ = Command::new("taskkill").args(["/PID", &id.to_string(), "/T", "/F"]).status().await;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
fn child_group(id: u32) -> Option<u32> {
    let child = Pid::from_raw(i32::try_from(id).ok()?)?;
    (getpgid(Some(child)).ok()? == child).then_some(id)
}

#[cfg(unix)]
fn external_group(id: u32) -> Option<Pid> {
    let group = Pid::from_raw(i32::try_from(id).ok()?)?;
    (group != getpgrp()).then_some(group)
}

#[cfg(unix)]
fn kill_group(group: Pid, signal: Signal) {
    if group == getpgrp() {
        return;
    }

    let _ = kill_process_group(group, signal);

    let Ok(output) = std::process::Command::new("ps").args(["-eo", "pid=,pgid="]).output() else {
        return;
    };
    let current = std::process::id();

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut fields = line.split_whitespace();

        let Some(pid) =
            fields.next().and_then(|value| value.parse::<i32>().ok()).and_then(Pid::from_raw)
        else {
            continue;
        };

        let Some(pgid) =
            fields.next().and_then(|value| value.parse::<i32>().ok()).and_then(Pid::from_raw)
        else {
            continue;
        };

        if pgid == group && pid.as_raw_pid() as u32 != current {
            let _ = kill_process(pid, signal);
        }
    }
}

fn denied(error: impl Into<String>) -> crabbot_core::Error {
    let error = error.into();
    let message = if error.ends_with('.') { error } else { format!("{error}.") };
    crabbot_core::Error::Denied(message)
}

#[cfg(test)]
fn file(policy: &Policy, path: &str) -> crabbot_core::Result<PathBuf> {
    file_at(policy, path, None)
}

fn workspace(policy: &Policy, path: Option<&str>) -> crabbot_core::Result<Option<PathBuf>> {
    let root = policy.root.as_ref().ok_or_else(|| denied("Workspace root is unset"))?;
    let root = std::fs::canonicalize(root)
        .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
    let Some(path) = path.filter(|path| !path.trim().is_empty()) else {
        return Ok(None);
    };
    let workspace = std::fs::canonicalize(path)
        .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
    if workspace.starts_with(&root) {
        Ok(Some(workspace))
    } else {
        Err(denied("Workspace leaves the configured root"))
    }
}

fn file_at(policy: &Policy, path: &str, workspace: Option<&Path>) -> crabbot_core::Result<PathBuf> {
    let root = policy.root.as_ref().ok_or_else(|| denied("Workspace root is unset"))?;
    let root = std::fs::canonicalize(root)
        .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
    let root = if let Some(workspace) = workspace {
        let workspace = std::fs::canonicalize(workspace)
            .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
        if !workspace.starts_with(&root) {
            return Err(denied("Workspace leaves the configured root"));
        }
        workspace
    } else {
        root
    };
    let candidate =
        if Path::new(path).is_absolute() { PathBuf::from(path) } else { root.join(path) };
    if Path::new(path).components().any(|component| component == Component::ParentDir) {
        return Err(denied("Path traversal is not allowed"));
    }
    match std::fs::symlink_metadata(&candidate) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(denied("Path cannot be a symbolic link"));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(denied(format!("Path is unavailable: {error}"))),
    }
    let clean = if candidate.exists() {
        std::fs::canonicalize(candidate)
            .map_err(|error| denied(format!("Path is unavailable: {error}")))?
    } else {
        let mut parent = candidate.parent().ok_or_else(|| denied("Path has no parent"))?;
        let mut missing = Vec::new();
        while !parent.exists() {
            missing.push(parent.file_name().ok_or_else(|| denied("Path has no name"))?.to_owned());
            parent = parent.parent().ok_or_else(|| denied("Path has no parent"))?;
        }
        let mut clean = std::fs::canonicalize(parent)
            .map_err(|error| denied(format!("Path parent is unavailable: {error}")))?;
        for part in missing.into_iter().rev() {
            clean.push(part);
        }
        clean.push(candidate.file_name().ok_or_else(|| denied("Path has no name"))?);
        clean
    };

    if clean.starts_with(&root) { Ok(clean) } else { Err(denied("Path leaves the workspace")) }
}

struct Confined {
    display: PathBuf,
    #[cfg(unix)]
    directory: OwnedFd,
    #[cfg(unix)]
    name: Option<OsString>,
    #[cfg(not(unix))]
    path: PathBuf,
}

#[cfg(unix)]
fn io_error(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}

#[cfg(unix)]
fn components(path: &Path) -> crabbot_core::Result<Vec<OsString>> {
    path.components()
        .map(|component| match component {
            Component::Normal(value) => Ok(value.to_owned()),
            Component::CurDir => Ok(OsString::new()),
            _ => Err(denied("Path is not relative to the workspace")),
        })
        .filter(|component| component.as_ref().map_or(true, |value| !value.is_empty()))
        .collect()
}

#[cfg(unix)]
fn open_directory(path: &Path) -> std::io::Result<OwnedFd> {
    openat(
        rustix::fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io_error)
}

#[cfg(unix)]
fn descend(mut directory: OwnedFd, parts: &[OsString], create: bool) -> std::io::Result<OwnedFd> {
    for part in parts {
        if create {
            match rustix::fs::mkdirat(&directory, part, Mode::from_raw_mode(0o700)) {
                Ok(()) => {}
                Err(error) if io_error(error).kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        directory = openat(
            &directory,
            part,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io_error)?;
    }
    Ok(directory)
}

fn confined(
    policy: &Policy,
    path: &str,
    workspace: Option<&Path>,
    create: bool,
) -> crabbot_core::Result<Confined> {
    let display = file_at(policy, path, workspace)?;

    #[cfg(unix)]
    {
        let configured = policy.root.as_ref().ok_or_else(|| denied("Workspace root is unset"))?;
        let root = std::fs::canonicalize(configured)
            .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
        let base = workspace.map_or_else(
            || Ok(root.clone()),
            |path| {
                std::fs::canonicalize(path)
                    .map_err(|error| denied(format!("Workspace is unavailable: {error}")))
            },
        )?;
        let base_parts = components(
            base.strip_prefix(&root).map_err(|_| denied("Workspace leaves the configured root"))?,
        )?;
        let target =
            display.strip_prefix(&base).map_err(|_| denied("Path leaves the workspace"))?;
        let mut target_parts = components(target)?;
        let name = target_parts.pop();
        let root_fd = open_directory(&root)
            .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
        let base_fd = descend(root_fd, &base_parts, false)
            .map_err(|error| denied(format!("Workspace is unavailable: {error}")))?;
        let directory = descend(base_fd, &target_parts, create)
            .map_err(|error| denied(format!("Path is unavailable: {error}")))?;
        Ok(Confined { display, directory, name })
    }

    #[cfg(not(unix))]
    {
        if create && let Some(parent) = display.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| denied(format!("Path is unavailable: {error}")))?;
        }
        Ok(Confined { path: display.clone(), display })
    }
}

#[cfg(unix)]
impl Confined {
    fn open(&self, flags: OFlags) -> std::io::Result<OwnedFd> {
        match &self.name {
            Some(name) => openat(
                &self.directory,
                name,
                flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(io_error),
            None => self.directory.try_clone(),
        }
    }

    fn child(&self, name: OsString) -> std::io::Result<Self> {
        let directory = self.open(OFlags::RDONLY | OFlags::DIRECTORY)?;
        Ok(Self { display: self.display.join(&name), directory, name: Some(name) })
    }
}

#[cfg(not(unix))]
impl Confined {
    fn child(&self, name: OsString) -> std::io::Result<Self> {
        Ok(Self { display: self.display.join(&name), path: self.path.join(name) })
    }
}

fn read_confined(path: &Confined) -> std::io::Result<String> {
    #[cfg(unix)]
    {
        read_text(std::fs::File::from(path.open(OFlags::RDONLY)?))
    }
    #[cfg(not(unix))]
    {
        read_text(open_confined(&path.path, false)?)
    }
}

fn write_confined(path: &Confined, value: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let Some(name) = path.name.as_ref() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write a workspace directory.",
            ));
        };
        for _ in 0..100 {
            let number = TEMP_FILES.fetch_add(1, Ordering::Relaxed);
            let temporary = format!(".crabbot-write-{}-{number}", std::process::id());
            let fd = match openat(
                &path.directory,
                &temporary,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            ) {
                Ok(fd) => fd,
                Err(error) if io_error(error).kind() == std::io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => return Err(io_error(error)),
            };
            let mut file = std::fs::File::from(fd);
            let result = file.write_all(value.as_bytes()).and_then(|()| file.sync_all());
            if let Err(error) = result {
                let _ = unlinkat(&path.directory, &temporary, AtFlags::empty());
                return Err(error);
            }
            if let Err(error) = renameat(&path.directory, &temporary, &path.directory, name) {
                let _ = unlinkat(&path.directory, &temporary, AtFlags::empty());
                return Err(io_error(error));
            }
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Could not allocate a confined temporary file.",
        ))
    }
    #[cfg(not(unix))]
    {
        let mut file = open_confined(&path.path, true)?;
        file.write_all(value.as_bytes())?;
        file.sync_all()
    }
}

#[cfg(windows)]
fn open_confined(path: &Path, write: bool) -> std::io::Result<std::fs::File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    let mut options = OpenOptions::new();
    options.read(!write).write(write).custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    if write {
        options.create(true).truncate(true);
    }
    options.open(path)
}

#[cfg(all(not(unix), not(windows)))]
fn open_confined(path: &Path, write: bool) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(!write).write(write);
    if write {
        options.create(true).truncate(true);
    }
    options.open(path)
}

fn list_confined(path: &Confined) -> std::io::Result<Vec<String>> {
    #[cfg(unix)]
    {
        let mut directory =
            Dir::new(path.open(OFlags::RDONLY | OFlags::DIRECTORY)?).map_err(io_error)?;
        let mut names = Vec::new();
        for entry in &mut directory {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name().to_string_lossy();
            if name != "." && name != ".." {
                if names.len() >= ENTRY_LIMIT {
                    return Err(std::io::Error::other("List exceeds the entry limit"));
                }
                names.push(name.into_owned());
            }
        }
        Ok(names)
    }
    #[cfg(not(unix))]
    {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&path.path)? {
            let entry = entry?;
            if names.len() >= ENTRY_LIMIT {
                return Err(std::io::Error::other("List exceeds the entry limit"));
            }
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(names)
    }
}

#[cfg(test)]
fn patch_paths(policy: &Policy, text: &str) -> crabbot_core::Result<()> {
    patch_paths_at(policy, text, None)
}

fn patch_paths_at(
    policy: &Policy,
    text: &str,
    workspace: Option<&Path>,
) -> crabbot_core::Result<()> {
    let mut paths = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("diff --git ") {
            let mut parts = value.split_whitespace();
            if let (Some(from), Some(to)) = (parts.next(), parts.next()) {
                paths.extend([from, to]);
            }
        } else if let Some(value) = line
            .strip_prefix("--- ")
            .or_else(|| line.strip_prefix("+++ "))
            .or_else(|| line.strip_prefix("rename from "))
            .or_else(|| line.strip_prefix("rename to "))
            .or_else(|| line.strip_prefix("copy from "))
            .or_else(|| line.strip_prefix("copy to "))
            && let Some(path) = value.split_whitespace().next()
        {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Err(denied("Patch does not declare any file paths"));
    }
    for path in paths {
        if path == "/dev/null" {
            continue;
        }
        let path = path.strip_prefix("a/").or_else(|| path.strip_prefix("b/")).unwrap_or(path);
        file_at(policy, path, workspace)?;
    }
    Ok(())
}

#[cfg(test)]
fn search(
    path: &Path,
    policy: &Policy,
    needle: &str,
    hits: &mut Vec<String>,
) -> crabbot_core::Result<()> {
    let mut budget = Search::new();
    search_at(path, policy, needle, hits, None, &mut budget, 0)
}

fn searches() -> Arc<Semaphore> {
    Arc::clone(SEARCHES.get_or_init(|| Arc::new(Semaphore::new(1))))
}

struct Search {
    nodes: usize,
    directories: usize,
    files: usize,
    bytes: u64,
    started: Instant,
    cancel: Arc<AtomicBool>,
}

impl Search {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_cancel(Arc::new(AtomicBool::new(false)))
    }

    fn with_cancel(cancel: Arc<AtomicBool>) -> Self {
        Self { nodes: 0, directories: 0, files: 0, bytes: 0, started: Instant::now(), cancel }
    }

    fn check(&self) -> crabbot_core::Result<()> {
        if self.cancel.load(Ordering::Relaxed) {
            Err(denied("Search was cancelled"))
        } else {
            Ok(())
        }
    }

    fn visit(&mut self, depth: usize, directory: bool) -> crabbot_core::Result<()> {
        self.check()?;
        if depth > SEARCH_DEPTH {
            return Err(denied("Search exceeds the directory depth limit"));
        }
        if self.started.elapsed() > SEARCH_TIME {
            return Err(denied("Search exceeds the time limit"));
        }
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > SEARCH_NODES {
            return Err(denied("Search exceeds the entry limit"));
        }
        let count = if directory { &mut self.directories } else { &mut self.files };
        *count = count.saturating_add(1);
        let limit = if directory { SEARCH_DIRECTORIES } else { SEARCH_FILES };
        if *count > limit {
            return Err(denied(if directory {
                "Search exceeds the directory limit"
            } else {
                "Search exceeds the file limit"
            }));
        }
        Ok(())
    }

    fn read(&mut self, size: u64) -> crabbot_core::Result<()> {
        self.check()?;
        self.bytes = self.bytes.saturating_add(size);
        if self.bytes > SEARCH_BYTES {
            return Err(denied("Search exceeds the byte limit"));
        }
        Ok(())
    }
}

fn search_at(
    path: &Path,
    policy: &Policy,
    needle: &str,
    hits: &mut Vec<String>,
    workspace: Option<&Path>,
    budget: &mut Search,
    depth: usize,
) -> crabbot_core::Result<()> {
    let display = file_at(policy, path.to_string_lossy().as_ref(), workspace)?;
    if !display.exists() {
        return Ok(());
    }
    let path = confined(policy, display.to_string_lossy().as_ref(), workspace, false)?;
    search_confined(&path, needle, hits, budget, depth)
}

fn search_confined(
    path: &Confined,
    needle: &str,
    hits: &mut Vec<String>,
    budget: &mut Search,
    depth: usize,
) -> crabbot_core::Result<()> {
    #[cfg(unix)]
    {
        let fd = path
            .open(OFlags::RDONLY | OFlags::NONBLOCK)
            .map_err(|error| denied(format!("Search failed: {error}")))?;
        let metadata = std::fs::File::from(
            fd.try_clone().map_err(|error| denied(format!("Search failed: {error}")))?,
        )
        .metadata()
        .map_err(|error| denied(format!("Search failed: {error}")))?;
        budget.visit(depth, metadata.is_dir())?;

        if metadata.is_dir() {
            let mut directory =
                Dir::new(fd).map_err(|error| denied(format!("Search failed: {error}")))?;
            for entry in &mut directory {
                let entry = entry.map_err(|error| denied(format!("Search failed: {error}")))?;
                let entry_name = entry.file_name().to_string_lossy();
                if entry_name == "." || entry_name == ".." {
                    continue;
                }
                if entry.file_type().is_symlink() {
                    continue;
                }
                let name = OsString::from(entry_name.as_ref());
                let child =
                    path.child(name).map_err(|error| denied(format!("Search failed: {error}")))?;
                search_confined(&child, needle, hits, budget, depth + 1)?;
            }
        } else if metadata.is_file() {
            budget.read(metadata.len())?;
            if let Ok(text) = read_text(std::fs::File::from(fd)) {
                for (line, value) in text.lines().enumerate() {
                    budget.check()?;
                    if value.contains(needle) {
                        if hits.len() >= HIT_LIMIT {
                            return Err(denied("Search exceeds the result limit"));
                        }
                        hits.push(format!("{}:{}", path.display.display(), line + 1));
                    }
                }
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let metadata = std::fs::metadata(&path.path)
            .map_err(|error| denied(format!("Search failed: {error}")))?;
        budget.visit(depth, metadata.is_dir())?;
        if metadata.is_dir() {
            for entry in std::fs::read_dir(&path.path)
                .map_err(|error| denied(format!("Search failed: {error}")))?
            {
                let entry = entry.map_err(|error| denied(format!("Search failed: {error}")))?;
                if entry
                    .file_type()
                    .map_err(|error| denied(format!("Search failed: {error}")))?
                    .is_symlink()
                {
                    continue;
                }
                let child = path
                    .child(entry.file_name())
                    .map_err(|error| denied(format!("Search failed: {error}")))?;
                search_confined(&child, needle, hits, budget, depth + 1)?;
            }
        } else if metadata.is_file() {
            budget.read(metadata.len())?;
            if let Ok(text) = read_text(
                std::fs::File::open(&path.path)
                    .map_err(|error| denied(format!("Search failed: {error}")))?,
            ) {
                for (line, value) in text.lines().enumerate() {
                    budget.check()?;
                    if value.contains(needle) {
                        if hits.len() >= HIT_LIMIT {
                            return Err(denied("Search exceeds the result limit"));
                        }
                        hits.push(format!("{}:{}", path.display.display(), line + 1));
                    }
                }
            }
        }
        Ok(())
    }
}

fn search_request(
    policy: &Policy,
    path: &str,
    needle: &str,
    workspace_path: Option<&str>,
    cancel: Arc<AtomicBool>,
) -> crabbot_core::Result<Vec<String>> {
    let workspace = workspace(policy, workspace_path)?;
    let path = file_at(policy, path, workspace.as_deref())?;
    let mut hits = Vec::new();
    let mut budget = Search::with_cancel(cancel);
    search_at(&path, policy, needle, &mut hits, workspace.as_deref(), &mut budget, 0)?;
    Ok(hits)
}

fn read_text(file: std::fs::File) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    file.take((FILE_LIMIT as u64).saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() > FILE_LIMIT {
        return Err(std::io::Error::new(ErrorKind::FileTooLarge, "file exceeds the size limit"));
    }
    String::from_utf8(bytes).map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::{
        ENTRY_LIMIT, Policy, Sandbox, Shell, apply, call, confined, denied, file, git,
        list_confined, patch_paths, search, shell, shell_with,
    };
    use crabbot_core::types::{Request, Response};
    use serde_json::json;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    static TEST_TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn test_root(prefix: &str) -> PathBuf {
        let nonce = TEST_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{}-{nonce}", std::process::id()))
    }

    fn policy(root: &Path) -> Policy {
        Policy { shell: Shell::Off, root: Some(root.to_path_buf()) }
    }

    #[test]
    fn validates_container_sandbox_configuration() {
        assert_eq!(Sandbox::parse(None, None).unwrap(), None);
        assert_eq!(Sandbox::parse(Some("off"), None).unwrap(), None);
        assert!(Sandbox::parse(None, Some("image:latest")).is_err());
        assert!(Sandbox::parse(Some("docker"), None).is_err());
        assert!(Sandbox::parse(Some("containerd"), Some("image:latest")).is_err());
        assert!(Sandbox::parse(Some("podman"), Some("unsafe image")).is_err());
        assert_eq!(
            Sandbox::parse(Some("docker"), Some("local/tool:latest")).unwrap(),
            Some(Sandbox { runtime: "docker".into(), image: "local/tool:latest".into() })
        );
    }

    #[test]
    fn restricts_sandbox_command_to_the_active_workspace() {
        let root = test_root("crabbot-tools-sandbox");
        let sandbox = Sandbox { runtime: "docker".into(), image: "local/tool:latest".into() };
        let command = sandbox.process("touch note", &root, Path::new("/tmp/container-id")).unwrap();
        let args = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        for argument in [
            "--pull=never",
            "--read-only",
            "--network=none",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--pids-limit=128",
            "--memory=1g",
            "--cpus=2",
            "--tmpfs=/tmp:rw,noexec,nosuid,size=64m",
            "--entrypoint=sh",
            &format!("type=bind,source={},target=/workspace", root.display()),
            "local/tool:latest",
            "-lc",
            "touch note",
        ] {
            assert!(args.iter().any(|value| value == argument), "Missing argument: {argument}");
        }
        assert!(!args.iter().any(|value| value.contains("/var/run/docker.sock")));
        assert!(
            sandbox.process("true", Path::new("/tmp/work,space"), Path::new("/tmp/id")).is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn configured_sandbox_does_not_fall_back_to_host_shell() {
        let root = test_root("crabbot-tools-sandbox");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let sandbox = Sandbox {
            runtime: "crabbot-missing-container-runtime".into(),
            image: "local/tool:latest".into(),
        };

        assert!(shell_with("touch host-marker", &root, Some(&sandbox)).await.is_err());
        assert!(!root.join("host-marker").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sandbox_smoke_runs_through_the_configured_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_root("crabbot-tools-sandbox-smoke");
        let runtime = root.join("runtime");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &runtime,
            "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --cidfile) printf '%s\\n' 0123456789abcdef > \"$2\"; shift 2 ;;\n    -lc) shift; /bin/sh -c \"$1\"; exit $? ;;\n    *) shift ;;\n  esac\ndone\n",
        )
        .unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
        let sandbox =
            Sandbox { runtime: runtime.display().to_string(), image: "local/tool:latest".into() };

        let output = shell_with("printf sandboxed", &root, Some(&sandbox)).await.unwrap();

        assert_eq!(String::from_utf8_lossy(&output.stdout), "sandboxed");
        assert!(!root.join("host-marker").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tools_read_write_list_and_search_within_root() {
        let root = test_root("crabbot-tools");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let policy = Arc::new(policy(&root));

        let write = call(
            &policy,
            Request::call(
                1,
                "write",
                json!({"path": "note.txt", "text": "hello", "approve": true}),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(write.result.unwrap()["ok"], true);
        call(
            &policy,
            Request::call(
                11,
                "write",
                json!({"path": "a/b/note.txt", "text": "nested", "approve": true}),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fs::read_to_string(root.join("a/b/note.txt")).unwrap(), "nested");

        let read = call(&policy, Request::call(2, "read", json!({"path": "note.txt"})))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.result.unwrap()["text"], "hello");

        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("nested.txt"), "nested").unwrap();
        let nested_read = call(
            &policy,
            Request::call(
                8,
                "read",
                json!({"path": "nested.txt", "workspace": nested.display().to_string()}),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(nested_read.result.unwrap()["text"], "nested");
        assert!(call(
            &policy,
            Request::call(
                9,
                "read",
                json!({"path": "nested.txt", "workspace": root.parent().unwrap().display().to_string()}),
            ),
        )
        .await
        .is_err());

        let listed =
            call(&policy, Request::call(3, "list", json!({"path": "."}))).await.unwrap().unwrap();
        assert!(
            listed.result.unwrap()["items"]
                .as_array()
                .is_some_and(|items| { items.iter().any(|item| item == "note.txt") })
        );

        let mut hits = Vec::new();
        search(&root, &policy, "ell", &mut hits).unwrap();
        assert_eq!(hits.len(), 1);

        assert!(file(&policy, "../outside").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn list_enforces_entry_limit_while_reading() {
        let root = test_root("crabbot-tools-list");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        for index in 0..=ENTRY_LIMIT {
            fs::File::create(root.join(format!("entry-{index}"))).unwrap();
        }

        let path = confined(&policy(&root), ".", None, false).unwrap();
        let error = list_confined(&path).unwrap_err();
        assert_eq!(error.to_string(), "List exceeds the entry limit");

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn search_skips_symlinks() {
        use std::os::unix::fs::symlink;

        let root = test_root("crabbot-tools-link");
        let outside = test_root("crabbot-tools-outside");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_file(&outside);
        fs::create_dir_all(&root).unwrap();
        fs::write(&outside, "secret").unwrap();
        symlink(&outside, root.join("outside")).unwrap();

        let mut hits = Vec::new();
        search(&root, &policy(&root), "secret", &mut hits).unwrap();
        assert!(hits.is_empty());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_file(outside);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_dangling_write_symlinks() {
        use std::os::unix::fs::symlink;

        let root = test_root("crabbot-tools-dangling");
        let outside = test_root("crabbot-tools-dangling-target");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_file(&outside);
        fs::create_dir_all(&root).unwrap();
        symlink(&outside, root.join("dangling")).unwrap();

        let result = call(
            &Arc::new(policy(&root)),
            Request::call(
                1,
                "write",
                json!({"path": "dangling", "text": "secret", "approve": true}),
            ),
        )
        .await;

        assert!(result.is_err());
        assert!(!outside.exists());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_file(outside);
    }

    #[test]
    fn search_enforces_workload_limits() {
        let cancel = Arc::new(super::AtomicBool::new(true));
        let mut budget = super::Search::with_cancel(cancel);
        assert!(budget.visit(0, false).is_err());
        let mut budget = super::Search::new();
        budget.nodes = super::SEARCH_NODES;
        assert!(budget.visit(0, false).is_err());
        let mut budget = super::Search::new();
        budget.bytes = super::SEARCH_BYTES;
        assert!(budget.read(1).is_err());
        let mut budget = super::Search::new();
        assert!(budget.visit(super::SEARCH_DEPTH + 1, false).is_err());
        let mut budget = super::Search::new();
        budget.files = super::SEARCH_FILES;
        assert!(budget.visit(0, false).is_err());
        let mut budget = super::Search::new();
        budget.directories = super::SEARCH_DIRECTORIES;
        assert!(budget.visit(0, true).is_err());
    }

    #[tokio::test]
    async fn shell_requires_approval() {
        let root = test_root("crabbot-tools-shell");
        fs::create_dir_all(&root).unwrap();
        let policy = Arc::new(policy(&root));
        let error =
            call(&policy, Request::call(1, "shell", json!({"command": "pwd"}))).await.unwrap_err();
        assert!(error.to_string().contains("disabled"));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tools_validate_requests_and_can_run_approved_shell() {
        let root = test_root("crabbot-tools-approved");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let mut approved = policy(&root);
        approved.shell = Shell::On;

        let output =
            call(&approved, Request::call(1, "shell", json!({"command": "pwd", "approve": true})))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(output.result.unwrap()["status"], 0);

        assert!(call(&approved, Request::call(2, "read", json!({}))).await.is_err());
        assert!(call(&approved, Request::call(3, "write", json!({"path": "x"}))).await.is_err());
        assert!(
            call(
                &approved,
                Request::call(3, "write", json!({"path": "blocked/file", "text": "x"}))
            )
            .await
            .is_err()
        );
        assert!(!root.join("blocked").exists());
        assert!(
            call(&approved, Request::call(4, "write", json!({"path": "x", "text": "x"})))
                .await
                .is_err()
        );
        assert!(call(&approved, Request::call(5, "search", json!({}))).await.is_err());
        assert!(
            call(&approved, Request::call(6, "list", json!({"path": "missing"}))).await.is_err()
        );
        let git = call(&approved, Request::call(7, "git", json!({"args": ["status"]})))
            .await
            .unwrap()
            .unwrap();
        assert!(git.result.unwrap()["status"].is_number());
        assert!(call(&approved, Request::call(7, "git", json!({"args": ["diff"]}))).await.is_ok());
        assert!(
            call(&approved, Request::call(7, "git", json!({"args": ["worktree", "list"]})))
                .await
                .is_ok()
        );
        assert!(
            call(
                &approved,
                Request::call(7, "git", json!({"args": ["worktree", "add", "../copy"]}))
            )
            .await
            .is_err()
        );
        assert!(
            call(
                &approved,
                Request::call(
                    7,
                    "git",
                    json!({"args": ["worktree", "add", "--detach", "../copy"]})
                )
            )
            .await
            .is_err()
        );
        assert!(
            call(
                &approved,
                Request::call(7, "git", json!({"args": ["worktree", "remove", "../copy"]}))
            )
            .await
            .is_err()
        );
        let active = root.join("active");
        let sibling = root.join("sibling");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&sibling).unwrap();
        assert!(
            call(
                &approved,
                Request::call(
                    7,
                    "git",
                    json!({
                        "args": ["worktree", "remove", sibling.display().to_string()],
                        "workspace": active.display().to_string(),
                        "approve": true
                    }),
                ),
            )
            .await
            .is_err()
        );
        assert!(
            call(&approved, Request::call(7, "git", json!({"args": ["status", 1]}))).await.is_err()
        );
        assert!(
            call(&approved, Request::call(8, "git", json!({"args": ["reset"]}))).await.is_err()
        );
        assert!(call(&approved, Request::call(9, "unknown", json!({}))).await.unwrap().is_none());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "list".into(), params: json!({}) };
        assert!(call(&approved, note).await.unwrap().is_none());
        assert!(file(&Policy { root: None, ..approved }, ".").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_worktrees_do_not_run_hooks() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_root("crabbot-tools-worktree");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("README"), "test\n").unwrap();
        for args in [
            vec!["init".into()],
            vec!["config".into(), "user.email".into(), "test@example.com".into()],
            vec!["config".into(), "user.name".into(), "Crabbot Test".into()],
            vec!["config".into(), "commit.gpgsign".into(), "false".into()],
            vec!["add".into(), "README".into()],
            vec!["commit".into(), "-qm".into(), "initial".into()],
        ] {
            let output = git(&args, &root).await.unwrap();
            assert!(
                output.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let marker = root.join("hook-ran");
        let hook = root.join(".git/hooks/post-checkout");
        fs::write(&hook, format!("#!/bin/sh\nprintf ran > '{}'\n", marker.display())).unwrap();
        let mut permissions = fs::metadata(&hook).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&hook, permissions).unwrap();

        let target = root.join("copy");
        let result = call(
            &Arc::new(policy(&root)),
            Request::call(
                1,
                "git",
                json!({
                    "args": ["worktree", "add", target.display().to_string()],
                    "approve": true
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        let payload = result.result.unwrap();
        assert_eq!(
            payload["status"], 0,
            "git worktree add failed: stdout={} stderr={}",
            payload["stdout"], payload["stderr"]
        );
        assert!(target.join("README").is_file());
        assert!(!marker.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tools_cover_command_helpers_and_missing_workspace() {
        let root = test_root("crabbot-tools-helpers");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        assert!(shell("printf hello", &root).await.unwrap().status.success());
        assert!(git(&["status".into()], &root).await.is_ok());
        assert!(denied("Already denied.").to_string().ends_with("Already denied."));
        assert!(
            file(&Policy { root: Some(root.join("missing")), shell: Shell::Off }, ".").is_err()
        );
        assert!(search(&root.join("missing"), &policy(&root), "x", &mut Vec::new()).is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_stops_background_processes_after_completion() {
        let root = test_root("crabbot-tools-group");
        let marker = root.join("marker");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();

        shell("(sleep 1; touch marker) >/dev/null 2>&1 &", &root).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert!(!marker.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn process_cleanup_rejects_its_own_group() {
        let group = u32::try_from(rustix::process::getpgrp().as_raw_pid()).unwrap();
        assert!(super::external_group(group).is_none());
        assert!(super::external_group(u32::MAX).is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounds_command_output() {
        let mut command = tokio::process::Command::new("sh");
        command.args(["-c", "yes output"]);
        let error = super::capture(command, None, false).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
        let response =
            Response::ok(1, json!({"stdout": "x".repeat(super::OUTPUT_LIMIT), "stderr": ""}));
        assert!(serde_json::to_vec(&response).unwrap().len() < crabbot_core::jsonl::MAX);
        assert!(super::response(1, json!({"stdout": "x"})).unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn applies_approved_patches() {
        let root = test_root("crabbot-tools-patch");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("note.txt"), "before\n").unwrap();
        assert!(git(&["init".into()], &root).await.unwrap().status.success());
        assert!(git(&["add".into(), "note.txt".into()], &root).await.unwrap().status.success());
        let patch = "diff --git a/note.txt b/note.txt\nindex 8f6f5f0..9d5e3f4 100644\n--- a/note.txt\n+++ b/note.txt\n@@ -1 +1 @@\n-before\n+after\n";
        assert!(apply(&root, patch, true).await.unwrap().status.success());
        let policy = Arc::new(policy(&root));
        assert!(call(&policy, Request::call(1, "patch", json!({"text": patch}))).await.is_err());
        assert!(
            patch_paths(
                &policy,
                "diff --git a/note.txt b/../../outside\n--- a/note.txt\n+++ b/../../outside\n"
            )
            .is_err()
        );
        let result =
            call(&policy, Request::call(2, "patch", json!({"text": patch, "approve": true})))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(result.result.unwrap()["ok"], true);
        assert_eq!(fs::read_to_string(root.join("note.txt")).unwrap(), "after\n");
        let _ = fs::remove_dir_all(root);
    }
}
