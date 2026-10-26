#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    process::Stdio,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use krabb_core::{
    jsonl,
    plugin::serve_events_concurrent,
    types::{Capability, Hello, Protocol, Request, Response, ToolSpec},
};

#[cfg(unix)]
use rustix::process::{Pid, Signal, getpgid, kill_process_group};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};

const SESSION_LIMIT: usize = 2;
const SESSION_IDLE: Duration = Duration::from_secs(600);
const SESSION_REAPER: Duration = Duration::from_secs(30);
const WORKER_TIME: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("browser", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    let state = Arc::new(Mutex::new(BrowserState::default()));
    let reaper_state = Arc::clone(&state);
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(SESSION_REAPER);

        loop {
            interval.tick().await;

            let mut state = reaper_state.lock().await;
            let _ = expire_sessions(&mut state).await;
        }
    });

    let result = serve_events_concurrent(
        Hello {
            protocol: Protocol::CURRENT,
            id: "browser".into(),
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
struct BrowserState {
    worker: Option<Worker>,
    sessions: BTreeMap<String, BrowserSession>,
}

struct BrowserSession {
    owner: String,
    last_used: Instant,
}

struct Worker {
    child: Child,
    process_id: u32,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
    next_id: u64,
    time_limit: Duration,
    stopped: Arc<AtomicBool>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        terminate_worker_group(self.process_id);
    }
}

impl Worker {
    async fn start() -> krabb_core::Result<Self> {
        let module = std::env::var_os("KRABB_PLAYWRIGHT_MODULE").ok_or_else(|| {
            denied("KRABB_PLAYWRIGHT_MODULE must point to the installed Playwright package.")
        })?;

        let node = std::env::var_os("KRABB_NODE").unwrap_or_else(|| "node".into());
        Self::start_with(node, module, WORKER_TIME).await
    }

    async fn start_with(
        node: impl AsRef<std::ffi::OsStr>,
        module: impl AsRef<std::ffi::OsStr>,
        time_limit: Duration,
    ) -> krabb_core::Result<Self> {
        let mut command = Command::new(node);
        command
            .arg("--input-type=module")
            .arg("-e")
            .arg(include_str!("worker.mjs"))
            .env_clear()
            .env("KRABB_PLAYWRIGHT_MODULE", module)
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        for key in [
            "HOME",
            "USERPROFILE",
            "TMPDIR",
            "TMP",
            "TEMP",
            "SystemRoot",
            "WINDIR",
            "APPDATA",
            "LOCALAPPDATA",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }

        for key in ["KRABB_BROWSER_ALLOWED_HOSTS", "KRABB_BROWSER_EGRESS_PROXY"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }

        if let Some(path) = std::env::var_os("KRABB_PLAYWRIGHT_BROWSERS_PATH") {
            command.env("PLAYWRIGHT_BROWSERS_PATH", path);
        }

        #[cfg(unix)]
        command.process_group(0);

        let mut child = None;
        let mut last_error = None;

        for _attempt in 0..3 {
            match command.spawn() {
                Ok(process) => {
                    child = Some(process);
                    break;
                }

                Err(error) => {
                    #[cfg(unix)]
                    let retry = error.raw_os_error() == Some(26) && _attempt < 2;
                    #[cfg(not(unix))]
                    let retry = false;

                    if !retry {
                        last_error = Some(error);
                        break;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        let mut child = child.ok_or_else(|| {
            let error = last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "executable remained busy".into());
            denied(format!(
                "Could not start the Playwright worker. Check Node.js installation: {error}"
            ))
        })?;

        let process_id =
            child.id().ok_or_else(|| denied("Could not identify the Playwright worker."))?;

        let input =
            child.stdin.take().ok_or_else(|| denied("Playwright worker stdin is unavailable."))?;

        let output = child
            .stdout
            .take()
            .ok_or_else(|| denied("Playwright worker stdout is unavailable."))?;

        #[cfg(unix)]
        {
            let id = process_id;
            let id = i32::try_from(id)
                .ok()
                .and_then(Pid::from_raw)
                .ok_or_else(|| denied("Could not isolate the Playwright worker process."))?;

            if getpgid(Some(id)).ok() != Some(id) {
                return Err(denied("Could not isolate the Playwright worker process."));
            }
        }

        Ok(Self {
            child,
            process_id,
            input: BufWriter::new(input),
            output: BufReader::new(output),
            next_id: 1,
            time_limit,
            stopped: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn call(&mut self, method: &str, params: Value) -> krabb_core::Result<Value> {
        let mut cancellation = WorkerCancellation::new(self.process_id, Arc::clone(&self.stopped));

        let result = self.call_inner(method, params).await;

        cancellation.disarm();

        result
    }

    async fn call_inner(&mut self, method: &str, params: Value) -> krabb_core::Result<Value> {
        if self.child.try_wait().map_err(krabb_core::Error::Io)?.is_some() {
            return Err(denied("Playwright worker stopped unexpectedly."));
        }

        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
            krabb_core::Error::Limit("Playwright worker exhausted request IDs.".into())
        })?;

        let request = json!({"id": id, "method": method, "params": params});
        let mut frame = serde_json::to_vec(&request)?;
        frame.push(b'\n');

        if frame.len() > jsonl::MAX {
            return Err(krabb_core::Error::Limit(
                "Playwright request exceeded the frame limit.".into(),
            ));
        }

        if let Err(error) = self.input.write_all(&frame).await {
            self.terminate();

            return Err(krabb_core::Error::Io(error));
        }

        if let Err(error) = self.input.flush().await {
            self.terminate();

            return Err(krabb_core::Error::Io(error));
        }

        let mut response = Vec::new();
        let read = timeout(
            self.time_limit,
            (&mut self.output).take((jsonl::MAX + 1) as u64).read_until(b'\n', &mut response),
        )
        .await;

        let read = match read {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => return Err(krabb_core::Error::Io(error)),

            Err(_) => {
                self.terminate();

                return Err(denied("Playwright operation timed out."));
            }
        };

        if read == 0 || response.len() > jsonl::MAX || response.last() != Some(&b'\n') {
            self.terminate();

            return Err(denied("Playwright worker returned an invalid or oversized frame."));
        }

        let response: Value = match serde_json::from_slice(&response) {
            Ok(response) => response,

            Err(error) => {
                self.terminate();

                return Err(krabb_core::Error::Json(error));
            }
        };

        if response["id"].as_u64() != Some(id) {
            self.terminate();

            return Err(denied("Playwright worker returned a mismatched response ID."));
        }

        if response["ok"] != true {
            let message = response["error"].as_str().unwrap_or("Browser operation failed.");
            return Err(denied(message.to_owned()));
        }

        Ok(response["result"].clone())
    }

    fn terminate(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        terminate_worker_group(self.process_id);
    }
}

struct WorkerCancellation {
    process_id: u32,
    stopped: Arc<AtomicBool>,
    armed: bool,
}

impl WorkerCancellation {
    fn new(process_id: u32, stopped: Arc<AtomicBool>) -> Self {
        Self { process_id, stopped, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WorkerCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.stopped.store(true, Ordering::SeqCst);
            terminate_worker_group(self.process_id);
        }
    }
}

fn terminate_worker_group(process_id: u32) {
    #[cfg(unix)]
    if let Ok(raw_id) = i32::try_from(process_id)
        && let Some(id) = Pid::from_raw(raw_id)
    {
        let _ = kill_process_group(id, Signal::KILL);
    }

    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
    }

    #[cfg(not(any(unix, windows)))]
    let _ = process_id;
}

async fn handle(
    state: &Mutex<BrowserState>,
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
        .filter(|owner| !owner.is_empty() && owner.len() <= 256)
        .ok_or_else(|| denied("Trusted Krabb session context is required."))?;

    let turn_id = params["host"]["turn"]
        .as_str()
        .filter(|turn| !turn.is_empty() && turn.len() <= 512)
        .ok_or_else(|| denied("Trusted Krabb turn context is required."))?;

    let mut state = state.lock().await;
    let mut params = params["arguments"].clone();

    if let Value::Object(arguments) = &mut params {
        arguments.insert("turn_id".into(), Value::String(turn_id.into()));
    } else {
        return Err(denied("Browser arguments must be an object."));
    }

    expire_sessions(&mut state).await?;

    let result = match method.as_str() {
        "browser_open" => open(&mut state, owner).await?,
        "browser_close" => close(&mut state, owner, &params).await?,

        _ => {
            let session_id =
                params["session_id"].as_str().ok_or_else(|| denied("session_id is required."))?;
            let session = state
                .sessions
                .get_mut(session_id)
                .filter(|session| session.owner == owner)
                .ok_or_else(|| denied("Browser session is unavailable to this Krabb session."))?;
            session.last_used = Instant::now();

            let worker = worker(&mut state).await?;
            worker.call(&method, params).await?
        }
    };

    Ok(Some(Response::ok(id, result)))
}

async fn open(state: &mut BrowserState, owner: &str) -> krabb_core::Result<Value> {
    expire_sessions(state).await?;

    if state.sessions.len() >= SESSION_LIMIT {
        return Err(denied("Browser session capacity is full."));
    }

    let session_id = session_id()?;
    let worker = worker(state).await?;
    let result = worker.call("browser_open", json!({"session_id": session_id})).await?;
    state.sessions.insert(
        session_id.clone(),
        BrowserSession { owner: owner.into(), last_used: Instant::now() },
    );

    Ok(json!({
        "session_id": session_id,
        "tab_id": result["tab_id"],
        "viewport": result["viewport"]
    }))
}

async fn close(state: &mut BrowserState, owner: &str, params: &Value) -> krabb_core::Result<Value> {
    let session_id =
        params["session_id"].as_str().ok_or_else(|| denied("session_id is required."))?;

    if !state.sessions.get(session_id).is_some_and(|session| session.owner == owner) {
        return Err(denied("Browser session is unavailable to this Krabb session."));
    }

    if let Some(worker) = state.worker.as_mut() {
        worker.call("browser_close", json!({"session_id": session_id})).await?;
    }

    state.sessions.remove(session_id);

    if state.sessions.is_empty() {
        state.worker = None;
    }

    Ok(json!({"closed": true}))
}

async fn expire_sessions(state: &mut BrowserState) -> krabb_core::Result<()> {
    let expired = state
        .sessions
        .iter()
        .filter_map(|(id, session)| {
            (session.last_used.elapsed() >= SESSION_IDLE).then_some(id.clone())
        })
        .collect::<Vec<_>>();

    let had_expired = !expired.is_empty();

    for session_id in expired {
        if let Some(worker) = state.worker.as_mut() {
            let _ = worker.call("browser_close", json!({"session_id": session_id})).await;
        }

        state.sessions.remove(&session_id);
    }

    if had_expired && state.sessions.is_empty() {
        state.worker = None;
    }

    Ok(())
}

async fn worker(state: &mut BrowserState) -> krabb_core::Result<&mut Worker> {
    let stopped = if let Some(worker) = state.worker.as_mut() {
        worker.stopped.load(Ordering::SeqCst)
            || worker.child.try_wait().map_err(krabb_core::Error::Io)?.is_some()
    } else {
        false
    };

    if stopped {
        state.worker = None;
        state.sessions.clear();
    }

    if state.worker.is_none() {
        state.worker = Some(Worker::start().await?);
    }

    state.worker.as_mut().ok_or_else(|| denied("Playwright worker is unavailable."))
}

fn session_id() -> krabb_core::Result<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes)
        .map_err(|error| denied(format!("Session ID generation failed: {error}")))?;

    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn denied(message: impl Into<String>) -> krabb_core::Error {
    krabb_core::Error::Denied(message.into())
}

fn tool_specs() -> Vec<ToolSpec> {
    [
        ("browser_open", "Start an isolated browser session.", json!({"type":"object","properties":{},"required":[],"additionalProperties":false})),
        ("browser_close", "Close an owned browser session.", object_schema(json!({"session_id":{"type":"string","minLength":1,"maxLength":64}}), &["session_id"])),
        ("browser_navigate", "Navigate an owned tab to an allowed HTTP or HTTPS URL.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"url":{"type":"string","maxLength":4096}}), &["session_id","url"])),
        ("browser_tabs", "List tabs in an owned browser session.", object_schema(json!({"session_id":{"type":"string"}}), &["session_id"])),
        ("browser_new_tab", "Create a tab in an owned browser session.", object_schema(json!({"session_id":{"type":"string"}}), &["session_id"])),
        ("browser_close_tab", "Close a tab in an owned browser session.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"}}), &["session_id","tab_id"])),
        ("browser_snapshot", "Return bounded visible text and interactive controls.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"}}), &["session_id"])),
        ("browser_screenshot", "Capture a bounded viewport, full-page, or element PNG or JPEG image for a vision-capable model.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"format":{"type":"string","enum":["png","jpeg"]},"full_page":{"type":"boolean"},"by":{"type":"string","enum":["role","label","placeholder","text","test_id","css"]},"target":{"type":"string","maxLength":2048},"role":{"type":"string","maxLength":64},"name":{"type":"string","maxLength":1024},"exact":{"type":"boolean"}}), &["session_id"])),
        ("browser_click", "Click a semantic locator or a point from the latest screenshot.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"by":{"type":"string","enum":["role","label","placeholder","text","test_id","css","coordinate"]},"target":{"type":"string","maxLength":2048},"role":{"type":"string","maxLength":64},"name":{"type":"string","maxLength":1024},"exact":{"type":"boolean"},"x":{"type":"integer","minimum":0,"maximum":4096},"y":{"type":"integer","minimum":0,"maximum":4096},"observation_id":{"type":"string","maxLength":64}}), &["session_id","by"])),
        ("browser_fill", "Fill an editable semantic locator.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"by":{"type":"string","enum":["label","placeholder","test_id","css"]},"target":{"type":"string","maxLength":2048},"value":{"type":"string","maxLength":4096}}), &["session_id","by","target","value"])),
        ("browser_select", "Select an option from a semantic locator.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"by":{"type":"string","enum":["label","test_id","css"]},"target":{"type":"string","maxLength":2048},"value":{"type":"string","maxLength":1024}}), &["session_id","by","target","value"])),
        ("browser_press", "Press a key or key combination in an owned tab.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"key":{"type":"string","maxLength":64}}), &["session_id","key"])),
        ("browser_scroll", "Scroll an owned tab by a bounded viewport delta.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"delta_x":{"type":"integer","minimum":-4096,"maximum":4096},"delta_y":{"type":"integer","minimum":-4096,"maximum":4096}}), &["session_id","delta_y"])),
        ("browser_wait", "Wait for a bounded semantic locator.", object_schema(json!({"session_id":{"type":"string"},"tab_id":{"type":"string"},"by":{"type":"string","enum":["role","label","text","test_id","css"]},"target":{"type":"string","maxLength":2048},"role":{"type":"string","maxLength":64},"name":{"type":"string","maxLength":1024},"timeout_ms":{"type":"integer","minimum":1,"maximum":15000}}), &["session_id","by","target"])),
    ]
    .into_iter()
    .map(|(name, description, schema)| ToolSpec {
        name: name.into(),
        description: Some(description.into()),
        schema,
    })
    .collect()
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        BrowserSession, BrowserState, SESSION_LIMIT, Worker, handle, session_id, tool_specs,
    };

    use krabb_core::types::Request;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        fs,
        io::Write,
        os::unix::fs::PermissionsExt,
        process::Command as StdCommand,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    use tokio::sync::Mutex;

    static NEXT_WORKER: AtomicU64 = AtomicU64::new(1);

    fn fake_worker(script: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "krabb-browser-worker-{}-{}-{}",
            std::process::id(),
            NEXT_WORKER.fetch_add(1, Ordering::Relaxed),
            session_id().unwrap()
        ));

        let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
        file.write_all(format!("#!/bin/sh\n{script}\n").as_bytes()).unwrap();
        drop(file);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    async fn lifecycle_worker() -> Worker {
        let path = fake_worker(
            r#"while IFS= read -r line; do
                id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
                case "$line" in
                    *browser_open*) printf '{"id":%s,"ok":true,"result":{"tab_id":"tab-1","viewport":{"width":1280,"height":720}}}\n' "$id" ;;
                    *browser_tabs*) printf '{"id":%s,"ok":true,"result":{"tabs":[]}}\n' "$id" ;;
                    *) printf '{"id":%s,"ok":true,"result":{"closed":true}}\n' "$id" ;;
                esac
            done"#,
        );

        Worker::start_with(&path, "unused", Duration::from_millis(100)).await.unwrap()
    }

    #[tokio::test]
    async fn lists_browser_tools_without_starting_node() {
        let state = Mutex::new(BrowserState::default());
        let response =
            handle(&state, Request::call(1, "tools/list", json!({}))).await.unwrap().unwrap();

        let tools = response.result.unwrap()["tools"].as_array().unwrap().clone();
        let names = tools.iter().filter_map(|tool| tool["name"].as_str()).collect::<Vec<_>>();

        for expected in [
            "browser_open",
            "browser_navigate",
            "browser_snapshot",
            "browser_screenshot",
            "browser_click",
            "browser_fill",
            "browser_select",
            "browser_press",
            "browser_scroll",
            "browser_wait",
        ] {
            assert!(names.contains(&expected));
        }

        assert_eq!(names.len(), tool_specs().len());

        assert!(!names.iter().any(|name| name.contains("evaluate")));
    }

    #[tokio::test]
    async fn rejects_browser_session_ids_from_another_owner() {
        let state = Mutex::new(BrowserState {
            worker: None,
            sessions: BTreeMap::from([(
                "opaque-session".into(),
                BrowserSession { owner: "owner-a".into(), last_used: Instant::now() },
            )]),
        });

        let result = handle(
            &state,
            Request::call(
                2,
                "browser_tabs",
                json!({
                    "host": {"session":"owner-b", "turn":"turn-2"},
                    "arguments": {"session_id":"opaque-session"}
                }),
            ),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn owns_routes_and_closes_browser_sessions() {
        let state =
            BrowserState { worker: Some(lifecycle_worker().await), sessions: BTreeMap::new() };

        let state = Mutex::new(state);

        let opened = handle(
            &state,
            Request::call(
                3,
                "browser_open",
                json!({"host":{"session":"owner-a","turn":"turn-1"},"arguments":{}}),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        let session_id = opened["session_id"].as_str().unwrap().to_owned();

        assert_eq!(opened["tab_id"], "tab-1");

        let tabs = handle(
            &state,
            Request::call(
                4,
                "browser_tabs",
                json!({
                    "host":{"session":"owner-a","turn":"turn-1"},
                    "arguments":{"session_id":session_id}
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(tabs.result.unwrap()["tabs"], json!([]));

        handle(
            &state,
            Request::call(
                5,
                "browser_close",
                json!({
                    "host":{"session":"owner-a","turn":"turn-1"},
                    "arguments":{"session_id":session_id}
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(state.lock().await.sessions.is_empty());
        assert!(state.lock().await.worker.is_none());
    }

    #[tokio::test]
    async fn expires_idle_sessions_before_opening_another() {
        let state = BrowserState {
            worker: Some(lifecycle_worker().await),
            sessions: BTreeMap::from([(
                "expired-session".into(),
                BrowserSession {
                    owner: "owner-a".into(),
                    last_used: Instant::now() - super::SESSION_IDLE - Duration::from_secs(1),
                },
            )]),
        };

        let state = Mutex::new(state);

        let result = handle(
            &state,
            Request::call(
                6,
                "browser_tabs",
                json!({
                    "host":{"session":"owner-a","turn":"turn-2"},
                    "arguments":{"session_id":"expired-session"}
                }),
            ),
        )
        .await;

        assert!(result.is_err());
        let state = state.lock().await;

        assert!(state.sessions.is_empty());
        assert!(state.worker.is_none());
    }

    #[test]
    fn generates_unpredictable_session_identifiers() {
        let first = session_id().unwrap();
        let second = session_id().unwrap();

        assert_eq!(first.len(), 48);
        assert_ne!(first, second);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(SESSION_LIMIT, 2);
    }

    #[tokio::test]
    async fn worker_correlates_jsonl_responses_and_reports_worker_errors() {
        let path = fake_worker(
            r#"while IFS= read -r line; do
                case "$line" in
                    *mismatch*) printf '%s\n' '{"id":99,"ok":true,"result":{}}' ;;
                    *failure*) printf '%s\n' '{"id":1,"ok":false,"error":"fixture failure"}' ;;
                    *) printf '%s\n' '{"id":1,"ok":true,"result":{"ready":true}}' ;;
                esac
            done"#,
        );

        let mut worker =
            Worker::start_with(&path, "unused", Duration::from_millis(100)).await.unwrap();

        assert_eq!(worker.call("ok", json!({})).await.unwrap(), json!({"ready":true}));
        assert!(worker.call("mismatch", json!({})).await.is_err());

        let path = fake_worker(
            r#"while IFS= read -r line; do
                printf '%s\n' '{"id":1,"ok":false,"error":"fixture failure"}'
            done"#,
        );

        let mut worker =
            Worker::start_with(&path, "unused", Duration::from_millis(100)).await.unwrap();

        assert!(
            worker
                .call("failure", json!({}))
                .await
                .unwrap_err()
                .to_string()
                .contains("fixture failure")
        );
    }

    #[tokio::test]
    async fn worker_times_out_when_child_does_not_answer() {
        let path = fake_worker("while IFS= read -r line; do sleep 1; done");
        let mut worker =
            Worker::start_with(&path, "unused", Duration::from_millis(10)).await.unwrap();

        assert!(worker.call("slow", json!({})).await.is_err());
        let _ = worker.child.kill().await;
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn dropping_worker_terminates_its_process_group() {
        let pid_path =
            std::env::temp_dir().join(format!("krabb-browser-child-{}", session_id().unwrap()));

        let script = format!(
            "sleep 30 &\necho $! > '{}'\nwhile IFS= read -r line; do printf '%s\\n' '{{\"id\":1,\"ok\":true,\"result\":{{}}}}'; done",
            pid_path.display(),
        );

        let path = fake_worker(&script);
        let worker = Worker::start_with(&path, "unused", Duration::from_secs(1)).await.unwrap();
        let mut child_pid = None;

        for _ in 0..100 {
            if let Ok(value) = fs::read_to_string(&pid_path) {
                child_pid = value.trim().parse::<u32>().ok();

                if child_pid.is_some() {
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let child_pid = child_pid.expect("worker should start its fixture child");
        drop(worker);

        let mut stopped = false;

        for _ in 0..100 {
            let status = StdCommand::new("ps")
                .args(["-o", "stat=", "-p", &child_pid.to_string()])
                .output()
                .unwrap();

            let state = String::from_utf8_lossy(&status.stdout);

            if state.trim().is_empty() || state.trim().starts_with('Z') {
                stopped = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let _ = fs::remove_file(pid_path);
        let _ = fs::remove_file(path);

        assert!(stopped, "worker child process should stop when its owner drops");
    }
}
