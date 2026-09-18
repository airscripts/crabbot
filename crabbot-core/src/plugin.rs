use std::{
    collections::BTreeMap,
    ffi::OsString,
    future::Future,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncWrite, BufReader},
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, timeout},
};

use crate::{
    Error, Result, jsonl,
    types::{Hello, Protocol, Request, Response},
};

const FRAME: usize = jsonl::MAX;
const NOTES: usize = 256;
const OUTPUT: usize = 32;
const EVENTS: usize = 256;

#[derive(Clone)]
pub struct Emitter {
    output: mpsc::Sender<Value>,
    events: Arc<AtomicUsize>,
    calls: Arc<AtomicU64>,
    replies: Arc<Mutex<BTreeMap<u64, oneshot::Sender<crate::types::Response>>>>,
}

impl Emitter {
    pub fn new(output: mpsc::Sender<Value>) -> Self {
        Self {
            output,
            events: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicU64::new(1)),
            replies: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn event(&mut self, event: Value) -> Result<()> {
        if self.events.fetch_add(1, Ordering::Relaxed) >= EVENTS {
            return Err(Error::Limit("plugin emitted too many events".into()));
        }

        let note = Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: json!({"event": event}),
        };
        if serde_json::to_vec(&note)?.len().saturating_add(1) > FRAME {
            return Err(Error::Limit("plugin event exceeds the frame limit".into()));
        }
        let value = serde_json::to_value(note)?;
        self.output
            .send(value)
            .await
            .map_err(|_| Error::Protocol("plugin output is unavailable".into()))
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<crate::types::Response> {
        let id = self.calls.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(Error::Limit("plugin exhausted its request IDs".into()));
        }
        let request = crate::types::Request::call(id, method, params);
        if serde_json::to_vec(&request)?.len().saturating_add(1) > FRAME {
            return Err(Error::Limit("plugin request exceeds the frame limit".into()));
        }

        let (sender, receiver) = oneshot::channel();
        self.replies.lock().await.insert(id, sender);
        let value = serde_json::to_value(request)?;
        if self.output.send(value).await.is_err() {
            self.replies.lock().await.remove(&id);
            return Err(Error::Protocol("plugin output is unavailable".into()));
        }

        match timeout(Duration::from_secs(120), receiver).await {
            Ok(Ok(response)) if response.valid() && response.id == id => Ok(response),
            Ok(Ok(_)) => Err(Error::Protocol("host returned an invalid server response".into())),
            Ok(Err(_)) => Err(Error::Protocol("host response channel was closed".into())),
            Err(_) => {
                self.replies.lock().await.remove(&id);
                Err(Error::Protocol("host response timed out".into()))
            }
        }
    }
}

pub async fn serve(hello: Hello) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    serve_io(hello, BufReader::new(stdin), stdout, |_| async { Ok(None) }).await
}

pub async fn serve_with<F, Fut>(hello: Hello, handle: F) -> Result<()>
where
    F: FnMut(Request) -> Fut,
    Fut: Future<Output = Result<Option<Response>>>,
{
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    serve_io(hello, BufReader::new(stdin), stdout, handle).await
}

pub async fn serve_events<F, Fut>(hello: Hello, mut handle: F) -> Result<()>
where
    F: FnMut(Request, Emitter) -> Fut,
    Fut: Future<Output = Result<Option<Response>>>,
{
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    serve_io_events(hello, BufReader::new(stdin), stdout, &mut handle).await
}

pub async fn serve_io_events<R, W, F, Fut>(
    hello: Hello,
    input: R,
    output: W,
    handle: &mut F,
) -> Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    F: FnMut(Request, Emitter) -> Fut,
    Fut: Future<Output = Result<Option<Response>>>,
{
    let (sender, mut receiver) = mpsc::channel::<Value>(OUTPUT);
    let (incoming, mut requests) = mpsc::channel::<Incoming>(OUTPUT);
    let replies = Arc::new(Mutex::new(BTreeMap::new()));
    let reader = tokio::spawn(read_input(input, incoming, Arc::clone(&replies)));
    let writer = tokio::spawn(async move {
        let mut output = output;
        while let Some(value) = receiver.recv().await {
            jsonl::write(&mut output, &value).await?;
        }
        Ok::<(), Error>(())
    });

    let result = async {
        loop {
            let request = match requests.recv().await {
                Some(Incoming::Request(request)) => request,
                Some(Incoming::Error(error)) => return Err(error),
                None => break,
            };
            let id = request.id();
            if !request.valid() {
                if let Some(id) = id {
                    write_output(&sender, Response::fail(id, -32600, "invalid request")).await?;
                }
                continue;
            }

            let method = match &request {
                Request::Call { method, .. } | Request::Note { method, .. } => method.as_str(),
            };
            match method {
                "hello" => {
                    if let Some(id) = id {
                        write_output(&sender, Response::ok(id, serde_json::to_value(&hello)?))
                            .await?;
                    }
                }
                "ping" => {
                    if let Some(id) = id {
                        write_output(&sender, Response::ok(id, json!({"ok": true}))).await?;
                    }
                }
                "shutdown" => {
                    if let Some(id) = id {
                        write_output(&sender, Response::ok(id, json!({"ok": true}))).await?;
                    }
                    break;
                }
                _ => {
                    let emitter = Emitter {
                        output: sender.clone(),
                        events: Arc::new(AtomicUsize::new(0)),
                        calls: Arc::new(AtomicU64::new(1)),
                        replies: Arc::clone(&replies),
                    };
                    match handle(request, emitter).await {
                        Ok(Some(response)) => write_output(&sender, response).await?,
                        Ok(None) => {
                            if let Some(id) = id {
                                write_output(
                                    &sender,
                                    Response::fail(id, -32601, "method not found"),
                                )
                                .await?;
                            }
                        }
                        Err(error) => {
                            if let Some(id) = id {
                                write_output(
                                    &sender,
                                    Response::fail(id, -32000, error.to_string()),
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    reader.abort();
    drop(sender);
    let written = writer
        .await
        .map_err(|error| Error::Protocol(format!("plugin output task failed: {error}")))?;
    result?;
    written
}

enum Incoming {
    Request(Request),
    Error(Error),
}

async fn read_input<R>(
    mut input: R,
    incoming: mpsc::Sender<Incoming>,
    replies: Arc<Mutex<BTreeMap<u64, oneshot::Sender<crate::types::Response>>>>,
) where
    R: AsyncBufRead + Unpin,
{
    loop {
        let value = match jsonl::read::<Value>(&mut input, FRAME).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                let _ = incoming
                    .send(Incoming::Error(Error::Protocol("plugin input is closed".into())))
                    .await;
                return;
            }
            Err(Error::Json(_)) => continue,
            Err(error) => {
                let _ = incoming.send(Incoming::Error(error)).await;
                return;
            }
        };

        if value.get("method").is_some() {
            match serde_json::from_value::<Request>(value) {
                Ok(request) if request.valid() => {
                    if incoming.send(Incoming::Request(request)).await.is_err() {
                        return;
                    }
                }
                _ => {
                    let _ = incoming
                        .send(Incoming::Error(Error::Protocol(
                            "host sent an invalid request".into(),
                        )))
                        .await;
                    return;
                }
            }
            continue;
        }

        let response = match serde_json::from_value::<crate::types::Response>(value) {
            Ok(response) if response.valid() => response,
            _ => {
                let _ = incoming
                    .send(Incoming::Error(Error::Protocol("host sent an invalid response".into())))
                    .await;
                return;
            }
        };
        let reply = replies.lock().await.remove(&response.id);
        let Some(reply) = reply else {
            let _ = incoming
                .send(Incoming::Error(Error::Protocol(
                    "host sent an unknown server response".into(),
                )))
                .await;
            return;
        };
        let _ = reply.send(response);
    }
}

async fn write_output(sender: &mpsc::Sender<Value>, value: impl serde::Serialize) -> Result<()> {
    let value = serde_json::to_value(value)?;
    sender.send(value).await.map_err(|_| Error::Protocol("plugin output is unavailable".into()))
}

pub async fn serve_io<R, W, F, Fut>(
    hello: Hello,
    mut input: R,
    mut output: W,
    mut handle: F,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut(Request) -> Fut,
    Fut: Future<Output = Result<Option<Response>>>,
{
    loop {
        let request = match jsonl::read::<Request>(&mut input, FRAME).await {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(Error::Json(_)) => continue,
            Err(error) => return Err(error),
        };
        let id = request.id();
        if !request.valid() {
            if let Some(id) = id {
                jsonl::write(&mut output, &Response::fail(id, -32600, "invalid request")).await?;
            }
            continue;
        }

        let method = match &request {
            Request::Call { method, .. } | Request::Note { method, .. } => method.as_str(),
        };
        match method {
            "hello" => {
                if let Some(id) = id {
                    jsonl::write(&mut output, &Response::ok(id, serde_json::to_value(&hello)?))
                        .await?;
                }
            }
            "ping" => {
                if let Some(id) = id {
                    jsonl::write(&mut output, &Response::ok(id, serde_json::json!({"ok": true})))
                        .await?;
                }
            }
            "shutdown" => {
                if let Some(id) = id {
                    jsonl::write(&mut output, &Response::ok(id, serde_json::json!({"ok": true})))
                        .await?;
                }
                break;
            }
            _ => match handle(request).await {
                Ok(Some(response)) => jsonl::write(&mut output, &response).await?,
                Ok(None) => {
                    if let Some(id) = id {
                        jsonl::write(&mut output, &Response::fail(id, -32601, "method not found"))
                            .await?;
                    }
                }
                Err(error) => {
                    if let Some(id) = id {
                        jsonl::write(&mut output, &Response::fail(id, -32000, error.to_string()))
                            .await?;
                    }
                }
            },
        }
    }
    Ok(())
}

pub struct Process {
    pub hello: Hello,
    child: Child,
    group: Option<u32>,
    input: BufReader<tokio::process::ChildStdout>,
    output: tokio::process::ChildStdin,
    path: PathBuf,
    args: Vec<OsString>,
    env: Option<Vec<(OsString, OsString)>>,
}

impl Drop for Process {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(id) = self.group {
            kill_group(id, "-KILL");
        }
        #[cfg(windows)]
        if let Some(id) = self.group {
            let _ = std::process::Command::new("taskkill")
                .args(["/PID", &id.to_string(), "/T", "/F"])
                .status();
        }
        let _ = self.child.start_kill();
    }
}

impl Process {
    pub async fn start(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::start_with(path, std::iter::empty::<&std::ffi::OsStr>()).await
    }

    pub async fn start_with_env<I, S, E, K, V>(
        path: impl AsRef<std::path::Path>,
        args: I,
        env: E,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
        E: IntoIterator<Item = (K, V)>,
        K: AsRef<std::ffi::OsStr>,
        V: AsRef<std::ffi::OsStr>,
    {
        let path = path.as_ref().to_path_buf();
        let args = args.into_iter().map(|arg| arg.as_ref().to_os_string()).collect::<Vec<_>>();
        let env = env
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_os_string(), value.as_ref().to_os_string()))
            .collect::<Vec<_>>();
        Self::start_owned(path, args, Some(env)).await
    }

    pub async fn start_with<I, S>(path: impl AsRef<std::path::Path>, args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let path = path.as_ref().to_path_buf();
        let args = args.into_iter().map(|arg| arg.as_ref().to_os_string()).collect::<Vec<_>>();
        Self::start_owned(path, args, None).await
    }

    async fn start_owned(
        path: PathBuf,
        args: Vec<OsString>,
        env: Option<Vec<(OsString, OsString)>>,
    ) -> Result<Self> {
        #[cfg(unix)]
        let mut command = {
            let mut command = Command::new(&path);
            command.process_group(0);
            command
        };
        #[cfg(not(unix))]
        let mut command = Command::new(&path);
        command.args(&args);
        if let Some(ref env) = env {
            command
                .env_clear()
                .envs(env.iter().map(|(key, value)| (key, value)))
                .env("CRABBOT_PLUGIN_GROUP", "1");
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let group = child.id();
        let result = async {
            let stdout =
                child.stdout.take().ok_or_else(|| Error::Handshake("missing stdout".into()))?;
            let mut input = BufReader::new(stdout);
            let request = Request::call(1, "hello", json!({"protocol": Protocol::CURRENT}));
            let mut stdin =
                child.stdin.take().ok_or_else(|| Error::Handshake("missing stdin".into()))?;
            jsonl::write(&mut stdin, &request).await?;
            let response: Response =
                timeout(Duration::from_secs(5), jsonl::read(&mut input, FRAME))
                    .await
                    .map_err(|_| Error::Handshake("timeout".into()))??
                    .ok_or_else(|| Error::Handshake("plugin exited before hello".into()))?;
            if response.id != 1 || !response.valid() {
                return Err(Error::Handshake("invalid hello response".into()));
            }
            let value = response.result.ok_or_else(|| {
                Error::Handshake(
                    response.error.map_or_else(|| "empty response".into(), |e| e.message),
                )
            })?;
            let hello: Hello = serde_json::from_value(value)?;
            if hello.id.trim().is_empty() || hello.version.trim().is_empty() {
                return Err(Error::Handshake("invalid hello metadata".into()));
            }
            if !Protocol::CURRENT.compatible(hello.protocol) {
                return Err(Error::Protocol(format!("{} {:?}.", hello.id, hello.protocol)));
            }
            Ok::<_, Error>((hello, input, stdin))
        }
        .await;

        match result {
            Ok((hello, input, output)) => {
                Ok(Self { hello, child, group, input, output, path, args, env })
            }
            Err(error) => {
                stop_tree(&mut child, group).await;
                Err(error)
            }
        }
    }

    pub async fn call(&mut self, request: Request) -> Result<Response> {
        self.call_stream(request, |_| {}).await
    }

    pub async fn call_stream<F>(&mut self, request: Request, mut note: F) -> Result<Response>
    where
        F: FnMut(Request),
    {
        self.call_stream_async(request, move |request| {
            note(request);
            std::future::ready(Ok(()))
        })
        .await
    }

    pub async fn call_stream_async<F, Fut>(&mut self, request: Request, note: F) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.call_stream_timeout_async(request, Duration::from_secs(120), note).await
    }

    pub async fn call_stream_timeout_async<F, Fut>(
        &mut self,
        request: Request,
        limit: Duration,
        note: F,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.call_full_timeout_async(request, limit, note, |request| async move {
            let method = match request {
                Request::Call { method, .. } | Request::Note { method, .. } => method,
            };
            Err(Error::Protocol(format!("plugin sent an unsupported request: {method}")))
        })
        .await
    }

    pub async fn call_full_async<F, Fut, C, Cfut>(
        &mut self,
        request: Request,
        note: F,
        call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        self.call_full_timeout_async(request, Duration::from_secs(120), note, call).await
    }

    pub async fn call_full_timeout_async<F, Fut, C, Cfut>(
        &mut self,
        request: Request,
        limit: Duration,
        mut note: F,
        mut call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        if !request.valid() {
            return Err(Error::Protocol("requests must use JSON-RPC 2.0".into()));
        }

        let id = request.id().ok_or_else(|| Error::Protocol("calls need an id".into()))?;
        let result = timeout(limit, async {
            jsonl::write(&mut self.output, &request).await?;
            let mut notes = 0_usize;
            let mut calls = 0_usize;
            loop {
                let value: Value = jsonl::read(&mut self.input, FRAME)
                    .await?
                    .ok_or_else(|| Error::Protocol("plugin closed stdout".into()))?;
                if value.get("method").is_some() {
                    let request: Request = serde_json::from_value(value)?;
                    if !request.valid() {
                        return Err(Error::Protocol("plugin sent an invalid request".into()));
                    }
                    if let Some(id) = request.id() {
                        calls = calls.saturating_add(1);
                        if calls > NOTES {
                            return Err(Error::Limit(
                                "plugin sent too many server requests".into(),
                            ));
                        }
                        let response = call(request).await?;
                        if !response.valid() || response.id != id {
                            return Err(Error::Protocol(
                                "server request handler returned an invalid response".into(),
                            ));
                        }
                        jsonl::write(&mut self.output, &response).await?;
                    } else {
                        notes = notes.saturating_add(1);
                        if notes > NOTES {
                            return Err(Error::Limit(
                                "plugin emitted too many stream notifications".into(),
                            ));
                        }
                        note(request).await?;
                    }
                    continue;
                }
                let response: Response = serde_json::from_value(value)?;
                if !response.valid() {
                    return Err(Error::Protocol("plugin sent an invalid response".into()));
                }

                if response.id == id {
                    return Ok(response);
                }
            }
        })
        .await;
        result.map_err(|_| Error::Protocol("plugin response timed out".into()))?
    }

    pub async fn restart(&mut self) -> Result<()> {
        stop_tree(&mut self.child, self.group).await;
        let replacement =
            Self::start_owned(self.path.clone(), self.args.clone(), self.env.clone()).await?;
        *self = replacement;
        Ok(())
    }

    pub async fn stop(mut self) -> Result<()> {
        let shutdown = self.call(Request::call(9_999, "shutdown", serde_json::json!({})));
        let _ = timeout(Duration::from_secs(2), shutdown).await;

        let _ = timeout(Duration::from_secs(2), self.child.wait()).await;
        stop_tree(&mut self.child, self.group).await;
        Ok(())
    }
}

async fn stop_tree(child: &mut Child, group: Option<u32>) {
    #[cfg(unix)]
    if let Some(id) = group {
        let group = format!("-{id}");
        let _ = Command::new("kill").args(["-TERM", &group]).status().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = Command::new("kill").args(["-KILL", &group]).status().await;
        kill_group(id, "-KILL");
    }
    #[cfg(windows)]
    if let Some(id) = group {
        let _ = Command::new("taskkill").args(["/PID", &id.to_string(), "/T", "/F"]).status().await;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
fn kill_group(group: u32, signal: &str) {
    let group_arg = format!("-{group}");
    let _ = std::process::Command::new("kill").args([signal, &group_arg]).status();

    let Ok(output) = std::process::Command::new("ps").args(["-eo", "pid=,pgid="]).output() else {
        return;
    };

    let current = std::process::id();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut fields = line.split_whitespace();
        let Some(pid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(pgid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        if pgid == group && pid != current {
            let _ = std::process::Command::new("kill").args([signal, &pid.to_string()]).status();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Emitter, Process, serve_io, serve_io_events};
    use crate::{
        jsonl,
        types::{Capability, Hello, Protocol, Request, Response},
    };
    use serde_json::json;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split},
        time::{Duration, timeout},
    };

    fn hello() -> Hello {
        Hello {
            protocol: Protocol::CURRENT,
            id: "test".into(),
            version: "0.1.0".into(),
            capabilities: vec![Capability::Model],
            commands: vec![],
        }
    }

    #[cfg(unix)]
    fn temp_path(name: &str, suffix: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("crabbot-plugin-{name}-{}-{nonce}.{suffix}", std::process::id()))
    }

    #[cfg(unix)]
    async fn start_test_plugin(_mode: &str, script: &str) -> crate::Result<Process> {
        Process::start_with("sh", ["-c", script]).await
    }

    #[cfg(windows)]
    fn windows_test_plugin() -> std::path::PathBuf {
        static PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

        PATH.get_or_init(|| {
            let root = std::env::temp_dir().join(format!("crabbot-plugin-{}", std::process::id()));
            std::fs::create_dir_all(&root).unwrap();
            let source = root.join("fixture.rs");
            let binary = root.join("fixture.exe");
            std::fs::write(&source, WINDOWS_TEST_PLUGIN).unwrap();

            let output = std::process::Command::new("rustc")
                .args(["--edition", "2024"])
                .arg(&source)
                .arg("-o")
                .arg(&binary)
                .output()
                .unwrap();

            assert!(
                output.status.success(),
                "could not compile the Windows plugin fixture: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let _ = std::fs::remove_file(source);
            binary
        })
        .clone()
    }

    #[cfg(windows)]
    async fn start_test_plugin(mode: &str, _script: &str) -> crate::Result<Process> {
        Process::start_with(windows_test_plugin(), [mode]).await
    }

    #[cfg(windows)]
    const WINDOWS_TEST_PLUGIN: &str = r###"
use std::io::{BufRead, Write};

fn send(value: &str) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let mut lines = std::io::stdin().lock().lines();

    match mode.as_str() {
        "bad_handshake" => send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"","version":"0.1.0","capabilities":[]}}"#),

        "bad_version" => {
            send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":1,"minor":0},"id":"test","version":"0.1.0","capabilities":[]}}"#);
            std::thread::sleep(std::time::Duration::from_secs(5));
        }

        "bad_jsonrpc" => {
            send(r#"{"jsonrpc":"1.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}"#);
            std::thread::sleep(std::time::Duration::from_secs(5));
        }

        "bad_result" => {
            send(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"failed"}}"#);
            std::thread::sleep(std::time::Duration::from_secs(5));
        }

        "invalid_calls" => {
            for line in lines.by_ref().map_while(Result::ok) {
                if line.contains("\"method\":\"hello\"") {
                    send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}"#);
                } else if line.contains("\"method\":\"ping\"") {
                    send(r#"{"jsonrpc":"1.0","id":2,"result":{}}"#);
                } else if line.contains("\"method\":\"shutdown\"") {
                    break;
                }
            }
        }

        "stream" => {
            for line in lines.by_ref().map_while(Result::ok) {
                if line.contains("\"method\":\"hello\"") {
                    send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":["model"]}}"#);
                } else if line.contains("\"method\":\"generate\"") {
                    send(r#"{"jsonrpc":"2.0","method":"event","params":{"kind":"text","text":"part"}}"#);
                    send(r#"{"jsonrpc":"2.0","id":2,"result":{"ok":true}}"#);
                } else if line.contains("\"method\":\"shutdown\"") {
                    send(r#"{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}"#);
                    break;
                }
            }
        }

        "server_request" => {
            while let Some(Ok(line)) = lines.next() {
                if line.contains("\"method\":\"hello\"") {
                    send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":["model"]}}"#);
                } else if line.contains("\"method\":\"generate\"") {
                    send(r#"{"jsonrpc":"2.0","id":7,"method":"host/tool","params":{"name":"read"}}"#);

                    if lines.next().map(|reply| reply.unwrap_or_default().contains("accepted")).unwrap_or(false) {
                        send(r#"{"jsonrpc":"2.0","id":2,"result":{"ok":true}}"#);
                    } else {
                        break;
                    }
                } else if line.contains("\"method\":\"shutdown\"") {
                    send(r#"{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}"#);
                    break;
                }
            }
        }

        _ => {
            for line in lines.map_while(Result::ok) {
                if line.contains("\"method\":\"hello\"") {
                    send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":["model"]}}"#);
                } else if line.contains("\"method\":\"ping\"") {
                    send(r#"{"jsonrpc":"2.0","id":2,"result":{"ok":true}}"#);
                } else if line.contains("\"method\":\"shutdown\"") {
                    send(r#"{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}"#);
                    break;
                }
            }
        }
    }
}
"###;

    #[tokio::test]
    async fn serves_protocol_and_handler_paths() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let task = tokio::spawn(serve_io(
            hello(),
            BufReader::new(server_read),
            server_write,
            |request| async move {
                match request {
                    Request::Call { id, method, .. } if method == "echo" => {
                        Ok(Some(Response::ok(id, json!({"echo": true}))))
                    }
                    Request::Call { method, .. } if method == "none" => Ok(None),
                    Request::Call { .. } => Err(crate::Error::Denied("handler failed".into())),
                    Request::Note { .. } => Ok(None),
                }
            },
        ));

        jsonl::write(&mut client_write, &Request::call(1, "hello", json!({}))).await.unwrap();
        jsonl::write(
            &mut client_write,
            &Request::Note { jsonrpc: "2.0".into(), method: "hello".into(), params: json!({}) },
        )
        .await
        .unwrap();
        jsonl::write(&mut client_write, &Request::call(2, "ping", json!({}))).await.unwrap();
        jsonl::write(
            &mut client_write,
            &Request::Note { jsonrpc: "2.0".into(), method: "ping".into(), params: json!({}) },
        )
        .await
        .unwrap();
        jsonl::write(&mut client_write, &Request::call(3, "echo", json!({}))).await.unwrap();
        jsonl::write(&mut client_write, &Request::call(4, "bad", json!({}))).await.unwrap();
        jsonl::write(&mut client_write, &Request::call(7, "none", json!({}))).await.unwrap();
        jsonl::write(
            &mut client_write,
            &Request::Note { jsonrpc: "2.0".into(), method: "note".into(), params: json!({}) },
        )
        .await
        .unwrap();
        client_write.write_all(b"not-json\n").await.unwrap();
        client_write
            .write_all(b"{\"jsonrpc\":\"1.0\",\"id\":5,\"method\":\"ping\",\"params\":{}}\n")
            .await
            .unwrap();
        jsonl::write(&mut client_write, &Request::call(6, "shutdown", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut responses = Vec::new();
        for _ in 0..7 {
            let mut line = String::new();
            input.read_line(&mut line).await.unwrap();
            responses.push(serde_json::from_str::<Response>(&line).unwrap());
        }
        assert_eq!(responses[0].id, 1);
        assert_eq!(responses[1].id, 2);
        assert_eq!(responses[2].result.as_ref().unwrap()["echo"], true);
        assert_eq!(responses[3].error.as_ref().unwrap().code, -32000);
        assert_eq!(responses[4].error.as_ref().unwrap().code, -32601);
        assert_eq!(responses[5].error.as_ref().unwrap().code, -32600);
        assert_eq!(responses[6].id, 6);

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn streams_bounded_events_before_the_response() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let mut handle = |request: Request, mut emitter: Emitter| async move {
            let Request::Call { id, .. } = request else {
                return Ok(None);
            };
            emitter.event(json!({"kind": "text", "text": "part"})).await?;
            Ok(Some(Response::ok(id, json!({"done": true}))))
        };
        let task = tokio::spawn(async move {
            serve_io_events(hello(), BufReader::new(server_read), server_write, &mut handle).await
        });

        jsonl::write(&mut client_write, &Request::call(1, "generate", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        input.read_line(&mut line).await.unwrap();
        let event: Request = serde_json::from_str(&line).unwrap();
        assert!(matches!(event, Request::Note { method, .. } if method == "event"));

        line.clear();
        input.read_line(&mut line).await.unwrap();
        let response: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(response.id, 1);
        assert_eq!(response.result.unwrap()["done"], true);

        jsonl::write(&mut client_write, &Request::call(2, "shutdown", json!({}))).await.unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        let shutdown: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(shutdown.id, 2);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn waits_for_plugin_server_call_responses() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let mut handle = |request: Request, emitter: Emitter| async move {
            let Request::Call { id, .. } = request else {
                return Ok(None);
            };
            let response = emitter.call("host/tool", json!({"name": "read"})).await?;
            Ok(Some(Response::ok(id, response.result.unwrap())))
        };
        let task = tokio::spawn(async move {
            serve_io_events(hello(), BufReader::new(server_read), server_write, &mut handle).await
        });

        jsonl::write(&mut client_write, &Request::call(1, "generate", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        input.read_line(&mut line).await.unwrap();
        let call: Request = serde_json::from_str(&line).unwrap();
        let Request::Call { id, method, params, .. } = call else {
            panic!("plugin server call must be a JSON-RPC request");
        };
        assert_eq!(method, "host/tool");
        assert_eq!(params["name"], "read");
        jsonl::write(&mut client_write, &Response::ok(id, json!({"text": "contents"})))
            .await
            .unwrap();

        line.clear();
        input.read_line(&mut line).await.unwrap();
        let response: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(response.id, 1);
        assert_eq!(response.result.unwrap()["text"], "contents");

        jsonl::write(&mut client_write, &Request::call(2, "shutdown", json!({}))).await.unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Response>(&line).unwrap().id, 2);
        task.await.unwrap().unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn supervises_a_plugin_process() {
        let script = "#!/bin/sh\nwhile IFS= read -r line; do\ncase \"$line\" in\n*hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"test\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;;\n*ping*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}' ;;\n*shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;;\nesac\ndone\n";
        let mut process = start_test_plugin("normal", script).await.unwrap();
        let response = process.call(Request::call(2, "ping", json!({}))).await.unwrap();
        assert_eq!(response.result.unwrap()["ok"], true);
        process.restart().await.unwrap();
        let response = process.call(Request::call(2, "ping", json!({}))).await.unwrap();
        assert_eq!(response.result.unwrap()["ok"], true);
        timeout(Duration::from_secs(2), process.stop()).await.unwrap().unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn stops_plugin_after_bad_handshake() {
        #[cfg(windows)]
        {
            assert!(start_test_plugin("bad_handshake", "").await.is_err());
            return;
        }

        #[cfg(unix)]
        {
            let marker = temp_path("bad", "pid");
            let script = format!(
                "#!/bin/sh\necho $$ > '{}'\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocol\":{{\"major\":0,\"minor\":1}},\"id\":\"\",\"version\":\"0.1.0\",\"capabilities\":[]}}}}'\nwhile IFS= read -r line; do :; done\n",
                marker.display()
            );

            assert!(start_test_plugin("bad_handshake", &script).await.is_err());
            for _ in 0..100 {
                if marker.is_file() {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let pid = std::fs::read_to_string(&marker).unwrap();
            let status = std::process::Command::new("kill")
                .args(["-0", pid.trim()])
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap();

            assert!(!status.success());
            let _ = std::fs::remove_file(marker);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stops_descendants_after_the_plugin_exits() {
        let marker = temp_path("descendant", "pid");
        let script = format!(
            "sleep 30 & echo $! > '{}'; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocol\":{{\"major\":0,\"minor\":1}},\"id\":\"test\",\"version\":\"0.1.0\",\"capabilities\":[]}}}}'; exit 0\n",
            marker.display()
        );
        let script = format!("IFS= read -r _; {script}");
        let process = Process::start_with("sh", ["-c", &script]).await.unwrap();
        for _ in 0..100 {
            if marker.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pid = std::fs::read_to_string(&marker).unwrap();
        drop(process);
        let mut running = true;
        for _ in 0..200 {
            let status = std::process::Command::new("ps")
                .args(["-o", "stat=", "-p", pid.trim()])
                .output()
                .unwrap();
            let state = String::from_utf8_lossy(&status.stdout);
            if !status.status.success()
                || state.trim().is_empty()
                || state.trim_start().starts_with('Z')
            {
                running = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!running);
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn rejects_incompatible_and_malformed_handshakes() {
        for (mode, response) in [
            (
                "bad_version",
                r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":1,"minor":0},"id":"test","version":"0.1.0","capabilities":[]}}"#,
            ),
            (
                "bad_jsonrpc",
                r#"{"jsonrpc":"1.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}"#,
            ),
            ("bad_result", r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"failed"}}"#),
        ] {
            let script = format!("#!/bin/sh\nprintf '%s\\n' '{}'\nsleep 5\n", response);
            assert!(start_test_plugin(mode, &script).await.is_err());
        }
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn rejects_invalid_calls_and_responses() {
        let script = "#!/bin/sh\nwhile IFS= read -r line; do case \"$line\" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"test\",\"version\":\"0.1.0\",\"capabilities\":[]}}' ;; *ping*) printf '%s\\n' '{\"jsonrpc\":\"1.0\",\"id\":2,\"result\":{}}' ;; *shutdown*) exit 0 ;; esac; done\n";
        let mut process = start_test_plugin("invalid_calls", script).await.unwrap();
        assert!(
            process
                .call(Request::Note {
                    jsonrpc: "2.0".into(),
                    method: "ping".into(),
                    params: json!({})
                })
                .await
                .is_err()
        );
        assert!(process.call(Request::call(2, "ping", json!({}))).await.is_err());
        process.stop().await.unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn forwards_stream_notifications_before_response() {
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":["model"]}}' ;; *generate*) printf '%s\n' '{"jsonrpc":"2.0","method":"event","params":{"kind":"text","text":"part"}}'; printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"ok":true}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let mut process = start_test_plugin("stream", script).await.unwrap();
        let mut notes = Vec::new();
        let response = process
            .call_stream(Request::call(2, "generate", json!({})), |note| notes.push(note))
            .await
            .unwrap();
        assert_eq!(response.result.unwrap()["ok"], true);
        assert_eq!(notes.len(), 1);
        assert!(matches!(
            &notes[0],
            Request::Note { method, .. } if method == "event"
        ));
        process.stop().await.unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn answers_plugin_server_requests() {
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":["model"]}}' ;; *generate*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"host/tool","params":{"name":"read"}}'; IFS= read -r reply; case "$reply" in *"accepted"*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"ok":true}}' ;; *) exit 1 ;; esac ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let mut process = start_test_plugin("server_request", script).await.unwrap();
        let response = process
            .call_full_async(
                Request::call(2, "generate", json!({})),
                |_| async { Ok(()) },
                |request| async move {
                    assert!(matches!(
                        request,
                        Request::Call { id: 7, method, params, .. }
                            if method == "host/tool" && params["name"] == "read"
                    ));
                    Ok(Response::ok(7, json!({"accepted": true})))
                },
            )
            .await
            .unwrap();
        assert_eq!(response.result.unwrap()["ok"], true);
        process.stop().await.unwrap();
    }
}
