use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    future::Future,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as SyncMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

#[cfg(unix)]
use rustix::process::{Pid, Signal, getpgid, getpgrp, getsid, kill_process, kill_process_group};
use serde_json::{Value, json};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot},
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
const CONCURRENT_HANDLERS: usize = 4;

pub fn binary_name(id: &str) -> String {
    format!("krabb-plugin-{id}{}", std::env::consts::EXE_SUFFIX)
}

#[derive(Clone)]
pub struct Emitter {
    output: mpsc::Sender<Value>,
    events: Arc<AtomicUsize>,
    calls: Arc<AtomicU64>,
    replies: Arc<SyncMutex<BTreeMap<u64, oneshot::Sender<crate::types::Response>>>>,
    streams: Arc<SyncMutex<BTreeMap<u64, mpsc::UnboundedSender<Value>>>>,
}

impl Emitter {
    pub fn new(output: mpsc::Sender<Value>) -> Self {
        Self {
            output,
            events: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicU64::new(1)),
            replies: Arc::new(SyncMutex::new(BTreeMap::new())),
            streams: Arc::new(SyncMutex::new(BTreeMap::new())),
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
        self.call_stream(method, params, |_| std::future::ready(Ok(()))).await
    }

    pub async fn call_stream<F, Fut>(
        &self,
        method: &str,
        params: Value,
        mut event: F,
    ) -> Result<crate::types::Response>
    where
        F: FnMut(Value) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let id = self.calls.fetch_add(1, Ordering::Relaxed);

        if id == u64::MAX {
            return Err(Error::Limit("plugin exhausted its request IDs".into()));
        }

        let request = crate::types::Request::call(id, method, params);

        if serde_json::to_vec(&request)?.len().saturating_add(1) > FRAME {
            return Err(Error::Limit("plugin request exceeds the frame limit".into()));
        }

        let (sender, mut receiver) = oneshot::channel();
        self.replies.lock().expect("call registry poisoned").insert(id, sender);
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
        self.streams.lock().expect("call registry poisoned").insert(id, event_sender);
        let mut registration = Registration { emitter: self.clone(), id, sent: false };
        let value = serde_json::to_value(request)?;
        self.output
            .send(value)
            .await
            .map_err(|_| Error::Protocol("plugin output is unavailable".into()))?;

        registration.sent = true;

        let result = timeout(Duration::from_secs(120), async {
            loop {
                tokio::select! {
                    response = &mut receiver => {
                        let response = match response {
                            Ok(response) if response.valid() && response.id == id => Ok(response),
                            Ok(_) => Err(Error::Protocol("host returned an invalid server response".into())),
                            Err(_) => Err(Error::Protocol("host response channel was closed".into())),
                        }?;

                        while let Ok(event_value) = event_receiver.try_recv() {
                            event(event_value).await?;
                        }

                        return Ok(response);
                    }

                    event_value = event_receiver.recv() => {
                        let Some(event_value) = event_value else {
                            return Err(Error::Protocol("host event channel was closed".into()));
                        };

                        event(event_value).await?;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| Err(Error::Protocol("host response timed out".into())));

        if result.is_ok() {
            registration.sent = false;
        }

        result
    }
}

struct Registration {
    emitter: Emitter,
    id: u64,
    sent: bool,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.emitter.replies.lock().expect("call registry poisoned").remove(&self.id);
        self.emitter.streams.lock().expect("call registry poisoned").remove(&self.id);

        if self.sent {
            let cancel = json!({"jsonrpc": "2.0", "method": "cancel", "params": {"id": self.id}});

            if let Err(mpsc::error::TrySendError::Full(cancel)) =
                self.emitter.output.try_send(cancel)
            {
                let output = self.emitter.output.clone();
                tokio::spawn(async move {
                    let _ = output.send(cancel).await;
                });
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

pub async fn serve_events_concurrent<F, Fut>(hello: Hello, handle: F) -> Result<()>
where
    F: Fn(Request, Emitter) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Response>>> + Send + 'static,
{
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    serve_io_events_concurrent(hello, BufReader::new(stdin), stdout, handle).await
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
    let replies = Arc::new(SyncMutex::new(BTreeMap::new()));
    let streams = Arc::new(SyncMutex::new(BTreeMap::new()));
    let calls = Arc::new(AtomicU64::new(1));
    let reader = tokio::spawn(read_input(
        input,
        incoming,
        Arc::clone(&replies),
        Arc::clone(&streams),
        Arc::clone(&calls),
    ));

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
                        calls: Arc::clone(&calls),
                        replies: Arc::clone(&replies),
                        streams: Arc::clone(&streams),
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

pub async fn serve_io_events_concurrent<R, W, F, Fut>(
    hello: Hello,
    input: R,
    output: W,
    handle: F,
) -> Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    F: Fn(Request, Emitter) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Response>>> + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<Value>(OUTPUT);
    let (incoming, mut requests) = mpsc::channel::<Incoming>(OUTPUT);
    let replies = Arc::new(SyncMutex::new(BTreeMap::new()));
    let streams = Arc::new(SyncMutex::new(BTreeMap::new()));
    let calls = Arc::new(AtomicU64::new(1));
    let reader = tokio::spawn(read_input(
        input,
        incoming,
        Arc::clone(&replies),
        Arc::clone(&streams),
        Arc::clone(&calls),
    ));

    let writer = tokio::spawn(async move {
        let mut output = output;

        while let Some(value) = receiver.recv().await {
            jsonl::write(&mut output, &value).await?;
        }

        Ok::<(), Error>(())
    });

    let handle = Arc::new(handle);
    let mut workers = tokio::task::JoinSet::new();
    let mut active = BTreeMap::<u64, tokio::task::AbortHandle>::new();
    let result = async {
        loop {
            tokio::select! {
                request = requests.recv() => {
                    let Some(request) = request else {
                        break;
                    };

                    let request = match request {
                        Incoming::Request(request) => request,
                        Incoming::Error(error) => return Err(error),
                    };

                    let id = request.id();

                    if !request.valid() {
                        if let Some(id) = id {
                            write_output(&sender, Response::fail(id, -32600, "invalid request"))
                                .await?;
                        }

                        continue;
                    }

                    if let Request::Note { method, params, .. } = &request
                        && method == "cancel"
                    {
                        if let Some(handle) = params["id"].as_u64().and_then(|id| active.get(&id)) {
                            handle.abort();
                        }

                        continue;
                    }

                    let method = match &request {
                        Request::Call { method, .. } | Request::Note { method, .. } => method.as_str(),
                    };

                    if workers.len() >= CONCURRENT_HANDLERS
                        && !matches!(method, "hello" | "ping" | "shutdown")
                    {
                        if let Some(id) = id {
                            write_output(
                                &sender,
                                Response::fail(id, -32000, "plugin is at its concurrent request limit"),
                            )
                            .await?;
                        }

                        continue;
                    }

                    active.retain(|_, handle| !handle.is_finished());

                    if id.is_some_and(|id| active.contains_key(&id)) {
                        if let Some(id) = id {
                            write_output(
                                &sender,
                                Response::fail(id, -32600, "request ID is already active"),
                            )
                            .await?;
                        }

                        continue;
                    }

                    match method {
                        "hello" => {
                            if let Some(id) = id {
                                write_output(
                                    &sender,
                                    Response::ok(id, serde_json::to_value(&hello)?),
                                )
                                .await?;
                            }
                        }

                        "ping" => {
                            if let Some(id) = id {
                                write_output(&sender, Response::ok(id, json!({"ok": true})))
                                    .await?;
                            }
                        }

                        "shutdown" => {
                            if let Some(id) = id {
                                write_output(&sender, Response::ok(id, json!({"ok": true})))
                                    .await?;
                            }

                            break;
                        }

                        _ => {
                            let handle = Arc::clone(&handle);
                            let sender = sender.clone();
                            let emitter = Emitter {
                                output: sender.clone(),
                                events: Arc::new(AtomicUsize::new(0)),
                                calls: Arc::clone(&calls),
                                replies: Arc::clone(&replies),
                                streams: Arc::clone(&streams),
                            };

                            let worker = workers.spawn(async move {
                                match handle(request, emitter).await {
                                    Ok(Some(response)) => write_output(&sender, response).await,

                                    Ok(None) => {
                                        if let Some(id) = id {
                                            write_output(
                                                &sender,
                                                Response::fail(id, -32601, "method not found"),
                                            )
                                            .await?;
                                        }

                                        Ok(())
                                    }

                                    Err(error) => {
                                        if let Some(id) = id {
                                            write_output(
                                                &sender,
                                                Response::fail(id, -32000, error.to_string()),
                                            )
                                            .await?;
                                        }

                                        Ok(())
                                    }
                                }
                            });

                            if let Some(id) = id {
                                active.insert(id, worker);
                            }
                        }
                    }
                }

                result = workers.join_next(), if !workers.is_empty() => {
                    match result {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => return Err(error),

                        Some(Err(error)) => {
                            if error.is_cancelled() {
                                active.retain(|_, handle| !handle.is_finished());
                                continue;
                            }

                            return Err(Error::Protocol(format!("plugin request task failed: {error}")));
                        }

                        None => {}
                    }

                    active.retain(|_, handle| !handle.is_finished());
                }
            }
        }

        while let Some(result) = workers.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),

                Err(error) => {
                    if error.is_cancelled() {
                        continue;
                    }

                    return Err(Error::Protocol(format!("plugin request task failed: {error}")));
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
    replies: Arc<SyncMutex<BTreeMap<u64, oneshot::Sender<crate::types::Response>>>>,
    streams: Arc<SyncMutex<BTreeMap<u64, mpsc::UnboundedSender<Value>>>>,
    calls: Arc<AtomicU64>,
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
                    if let Request::Note { method, params, .. } = &request
                        && method == "event"
                        && let Some(id) = params["callId"].as_u64()
                        && let Some(stream) =
                            streams.lock().expect("call registry poisoned").get(&id).cloned()
                    {
                        let _ = stream.send(params["event"].clone());
                        continue;
                    }

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

        let reply = replies.lock().expect("call registry poisoned").remove(&response.id);
        let Some(reply) = reply else {
            // IDs are never reused. An issued ID without a registration is retired.
            if response.id > 0 && response.id < calls.load(Ordering::Relaxed) {
                continue;
            }

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

async fn read_frame(input: &mut (impl AsyncBufRead + Unpin), frame: &mut Vec<u8>) -> Result<Value> {
    let remaining = FRAME.saturating_add(1).saturating_sub(frame.len());
    let count = input.take(remaining as u64).read_until(b'\n', frame).await?;

    if frame.len() > FRAME {
        return Err(Error::Limit("plugin frame exceeds the size limit".into()));
    }

    if count == 0 || frame.last() != Some(&b'\n') {
        return Err(Error::Protocol("plugin closed stdout during a frame".into()));
    }

    let value = serde_json::from_slice(frame)?;
    frame.clear();
    Ok(value)
}

pub struct Process {
    pub hello: Hello,
    tree: Tree,
    input: BufReader<tokio::process::ChildStdout>,
    frame: Vec<u8>,
    output: tokio::process::ChildStdin,
    stderr: Option<mpsc::Receiver<Vec<u8>>>,
    path: PathBuf,
    args: Vec<OsString>,
    env: Option<Vec<(OsString, OsString)>>,
}

struct Tree {
    child: Child,
    group: Option<u32>,
}

impl Drop for Tree {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self.group.and_then(external_group) {
            kill_session(group, Signal::KILL);
        }

        #[cfg(windows)]
        if let Some(id) = self.group {
            let _ = std::process::Command::new("taskkill")
                .args(["/PID", &id.to_string(), "/T", "/F"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
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
            krabb_process::isolate_session(command.as_std_mut());
            command
        };

        #[cfg(not(unix))]
        let mut command = Command::new(&path);
        command.args(&args);

        if let Some(ref env) = env {
            command
                .env_clear()
                .envs(env.iter().map(|(key, value)| (key, value)))
                .env("KRABB_PLUGIN_GROUP", "1");
        }

        let child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        #[cfg(unix)]
        let group = child.id().and_then(child_group);

        #[cfg(windows)]
        let group = child.id();

        let mut tree = Tree { child, group };

        let stderr = tree.child.stderr.take().map(|mut stream| {
            let (sender, receiver) = mpsc::channel(64);

            tokio::spawn(async move {
                let mut buffer = [0; 1024];

                loop {
                    match tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await {
                        Ok(0) | Err(_) => break,

                        Ok(count) => {
                            let _ = sender.try_send(buffer[..count].to_vec());
                        }
                    }
                }
            });

            receiver
        });

        let result = async {
            let stdout = tree
                .child
                .stdout
                .take()
                .ok_or_else(|| Error::Handshake("missing stdout".into()))?;

            let mut input = BufReader::new(stdout);
            let request = Request::call(1, "hello", json!({"protocol": Protocol::CURRENT}));

            let mut stdin =
                tree.child.stdin.take().ok_or_else(|| Error::Handshake("missing stdin".into()))?;

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
                Ok(Self { hello, tree, input, frame: Vec::new(), output, stderr, path, args, env })
            }

            Err(error) => {
                stop_tree(&mut tree.child, tree.group).await;
                Err(error)
            }
        }
    }

    pub fn take_stderr(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.stderr.take()
    }

    pub async fn call(&mut self, request: Request) -> Result<Response> {
        self.call_stream(request, |_| {}).await
    }

    pub async fn notify(&mut self, request: Request) -> Result<()> {
        if !request.valid() || request.id().is_some() {
            return Err(Error::Protocol("notifications must be valid JSON-RPC notes".into()));
        }

        jsonl::write(&mut self.output, &request).await
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
        note: F,
        mut call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        self.call_stream_with_lifetime(request, Some(limit), note, move |request, _| call(request))
            .await
    }

    pub async fn call_full_timeout_stream_async<F, Fut, C, Cfut>(
        &mut self,
        request: Request,
        limit: Duration,
        note: F,
        call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request, mpsc::UnboundedSender<Request>) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        self.call_stream_with_lifetime(request, Some(limit), note, call).await
    }

    pub async fn listen_stream_async<F, Fut, C, Cfut>(
        &mut self,
        request: Request,
        note: F,
        call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request, mpsc::UnboundedSender<Request>) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        self.call_stream_with_lifetime(request, None, note, call).await
    }

    async fn call_stream_with_lifetime<F, Fut, C, Cfut>(
        &mut self,
        request: Request,
        limit: Option<Duration>,
        mut note: F,
        mut call: C,
    ) -> Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = Result<()>>,
        C: FnMut(Request, mpsc::UnboundedSender<Request>) -> Cfut,
        Cfut: Future<Output = Result<Response>>,
    {
        if !request.valid() {
            return Err(Error::Protocol("requests must use JSON-RPC 2.0".into()));
        }

        let id = request.id().ok_or_else(|| Error::Protocol("calls need an id".into()))?;
        let (outgoing, mut outgoing_events) = mpsc::unbounded_channel();

        let operation = async {
            jsonl::write(&mut self.output, &request).await?;
            let mut notes = 0_usize;
            let mut calls = 0_usize;
            let mut events_closed = false;
            let mut queued = VecDeque::new();

            loop {
                let (value, received) = match queued.pop_front() {
                    Some(value) => value,
                    None => (
                        read_frame(&mut self.input, &mut self.frame).await?,
                        tokio::time::Instant::now(),
                    ),
                };

                if value.get("method").is_some() {
                    let request: Request = serde_json::from_value(value)?;

                    if !request.valid() {
                        return Err(Error::Protocol("plugin sent an invalid request".into()));
                    }

                    if let Request::Note { method, params, .. } = &request
                        && method == "cancel"
                    {
                        if let Some(cancelled) = params["id"].as_u64() {
                            queued.retain(|(value, _)| value["id"].as_u64() != Some(cancelled));
                        }

                        continue;
                    }

                    if let Some(server_id) = request.id() {
                        let deadline = received + limit.unwrap_or(Duration::from_secs(120));

                        if tokio::time::Instant::now() >= deadline {
                            jsonl::write(
                                &mut self.output,
                                &Response::fail(server_id, -32800, "host request expired"),
                            )
                            .await?;
                            continue;
                        }

                        calls = calls.saturating_add(1);

                        if limit.is_some() && calls > NOTES {
                            return Err(Error::Limit(
                                "plugin sent too many server requests".into(),
                            ));
                        }

                        let handler = async {
                            let result =
                                tokio::time::timeout_at(deadline, call(request, outgoing.clone()))
                                    .await
                                    .unwrap_or_else(|_| {
                                        Err(Error::Protocol("host request timed out".into()))
                                    });

                            if limit.is_none() {
                                Ok(result.unwrap_or_else(|error| {
                                    Response::fail(server_id, -32000, error.to_string())
                                }))
                            } else {
                                result
                            }
                        };

                        let mut emitted = 0_usize;
                        tokio::pin!(handler);
                        let response = loop {
                            tokio::select! {
                                biased;

                                incoming = read_frame(&mut self.input, &mut self.frame) => {
                                    let value = incoming?;

                                    if value["method"] == "cancel" && value.get("id").is_none() {
                                        let cancel: Request = serde_json::from_value(value)?;

                                        if !cancel.valid() {
                                            return Err(Error::Protocol("plugin sent an invalid cancellation".into()));
                                        }

                                        if let Request::Note { params, .. } = cancel
                                            && let Some(cancelled) = params["id"].as_u64()
                                        {
                                            queued.retain(|(value, _)| value["id"].as_u64() != Some(cancelled));

                                            if cancelled == server_id {
                                                break Response::fail(server_id, -32800, "host request cancelled");
                                            }
                                        }
                                    } else {
                                        if queued.len() >= OUTPUT {
                                            return Err(Error::Limit("plugin sent too many queued requests".into()));
                                        }

                                        queued.push_back((value, tokio::time::Instant::now()));
                                    }
                                }

                                event = outgoing_events.recv(), if !events_closed => {
                                    match event {
                                        Some(event) => {
                                            if !matches!(event, Request::Note { .. }) || !event.valid() {
                                                return Err(Error::Protocol(
                                                    "host emitted an invalid stream notification".into(),
                                                ));
                                            }

                                            if serde_json::to_vec(&event)?.len().saturating_add(1) > FRAME {
                                                return Err(Error::Limit(
                                                    "host stream notification exceeds the frame limit".into(),
                                                ));
                                            }

                                            emitted += 1;

                                            if emitted > NOTES {
                                                return Err(Error::Limit(
                                                    "host emitted too many stream notifications".into(),
                                                ));
                                            }

                                            jsonl::write(&mut self.output, &event).await?;
                                        }

                                        None => events_closed = true,
                                    }
                                }

                                response = &mut handler => break response?,
                            }
                        };

                        while let Ok(event) = outgoing_events.try_recv() {
                            if !matches!(event, Request::Note { .. }) || !event.valid() {
                                return Err(Error::Protocol(
                                    "host emitted an invalid stream notification".into(),
                                ));
                            }

                            if serde_json::to_vec(&event)?.len().saturating_add(1) > FRAME {
                                return Err(Error::Limit(
                                    "host stream notification exceeds the frame limit".into(),
                                ));
                            }

                            emitted += 1;

                            if emitted > NOTES {
                                return Err(Error::Limit(
                                    "host emitted too many stream notifications".into(),
                                ));
                            }

                            jsonl::write(&mut self.output, &event).await?;
                        }

                        if !response.valid() || response.id != server_id {
                            return Err(Error::Protocol(
                                "server request handler returned an invalid response".into(),
                            ));
                        }

                        jsonl::write(&mut self.output, &response).await?;

                        if limit.is_none() {
                            notes = 0;
                        }
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
        };

        match limit {
            Some(limit) => timeout(limit, operation)
                .await
                .map_err(|_| Error::Protocol("plugin response timed out".into()))?,
            None => operation.await,
        }
    }

    pub async fn restart(&mut self) -> Result<()> {
        stop_tree(&mut self.tree.child, self.tree.group).await;

        let replacement =
            Self::start_owned(self.path.clone(), self.args.clone(), self.env.clone()).await?;

        *self = replacement;
        Ok(())
    }

    pub async fn stop(mut self) -> Result<()> {
        let shutdown = self.call(Request::call(9_999, "shutdown", serde_json::json!({})));
        let _ = timeout(Duration::from_secs(2), shutdown).await;

        let _ = timeout(Duration::from_secs(2), self.tree.child.wait()).await;
        stop_tree(&mut self.tree.child, self.tree.group).await;
        Ok(())
    }
}

async fn stop_tree(child: &mut Child, group: Option<u32>) {
    #[cfg(unix)]
    if let Some(group) = group.and_then(external_group) {
        kill_session(group, Signal::TERM);
        tokio::time::sleep(Duration::from_millis(100)).await;
        kill_session(group, Signal::KILL);
    }

    #[cfg(windows)]
    if let Some(id) = group {
        let _ = Command::new("taskkill")
            .args(["/PID", &id.to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
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
fn kill_session(group: Pid, signal: Signal) {
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

        if pid.as_raw_pid() as u32 != current
            && (pgid == group || getsid(Some(pid)).ok() == Some(group))
        {
            let _ = kill_process_group(pgid, signal);
            let _ = kill_process(pid, signal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Emitter, Process, binary_name, serve_io, serve_io_events, serve_io_events_concurrent,
    };

    #[cfg(unix)]
    use super::{external_group, getpgrp};

    use crate::{
        jsonl,
        types::{Capability, Hello, Protocol, Request, Response},
    };

    use serde_json::json;
    use std::sync::Arc;
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

    #[test]
    fn plugin_binary_names_use_the_platform_suffix() {
        assert_eq!(
            binary_name("model"),
            format!("krabb-plugin-model{}", std::env::consts::EXE_SUFFIX)
        );
    }

    #[cfg(unix)]
    fn temp_path(name: &str, suffix: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();

        std::env::temp_dir()
            .join(format!("krabb-plugin-{name}-{}-{nonce}.{suffix}", std::process::id()))
    }

    #[cfg(unix)]
    async fn start_test_plugin(_mode: &str, script: &str) -> crate::Result<Process> {
        Process::start_with("sh", ["-c", script]).await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captures_plugin_stderr_without_blocking_the_protocol() {
        let script = r#"printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}'; printf 'plugin diagnostic\n' >&2; cat >/dev/null"#;
        let mut process = Process::start_with("sh", ["-c", script]).await.unwrap();
        let mut stderr = process.take_stderr().unwrap();
        let diagnostic = timeout(Duration::from_secs(1), async {
            let mut diagnostic = Vec::new();

            while !diagnostic.ends_with(b"\n") {
                diagnostic.extend(stderr.recv().await.unwrap());
            }

            diagnostic
        })
        .await
        .unwrap();

        assert_eq!(diagnostic, b"plugin diagnostic\n");
        process.stop().await.unwrap();
    }

    #[cfg(windows)]
    fn windows_test_plugin() -> std::path::PathBuf {
        static PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

        PATH.get_or_init(|| {
            let root = std::env::temp_dir().join(format!("krabb-plugin-{}", std::process::id()));
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

        "stream_server_request" | "listener_budget" | "operation_budget" => {
            while let Some(Ok(line)) = lines.next() {
                if line.contains("\"method\":\"hello\"") {
                    send(r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}"#);
                } else if line.contains("\"method\":\"command\"") {
                    let count = if mode == "stream_server_request" { 1 } else { 300 };

                    for _ in 0..count {
                        if mode != "stream_server_request" {
                            send(r#"{"jsonrpc":"2.0","method":"event","params":{}}"#);
                        }

                        send(r#"{"jsonrpc":"2.0","id":7,"method":"host/model_stream","params":{}}"#);
                        let _ = lines.next();

                        if mode == "stream_server_request" {
                            let _ = lines.next();
                        }
                    }

                    send(r#"{"jsonrpc":"2.0","id":2,"result":{"done":true}}"#);
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
    async fn concurrent_handlers_return_responses_by_request_id() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let handle = |request: Request, _emitter: Emitter| async move {
            let Request::Call { id, .. } = request else {
                return Ok(None);
            };

            if id == 1 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            Ok(Some(Response::ok(id, json!({"done": true}))))
        };

        let task = tokio::spawn(async move {
            serve_io_events_concurrent(hello(), BufReader::new(server_read), server_write, handle)
                .await
        });

        jsonl::write(&mut client_write, &Request::call(1, "generate", json!({}))).await.unwrap();
        jsonl::write(&mut client_write, &Request::call(2, "generate", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        input.read_line(&mut line).await.unwrap();
        let first: Response = serde_json::from_str(&line).unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        let second: Response = serde_json::from_str(&line).unwrap();

        assert_eq!((first.id, second.id), (2, 1));

        jsonl::write(&mut client_write, &Request::call(3, "shutdown", json!({}))).await.unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        let shutdown: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(shutdown.id, 3);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn concurrent_handlers_accept_sequential_id_reuse() {
        use std::{future::Future, task::Poll};

        let (client, server) = duplex(16 * 1024);
        let (client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let server = serve_io_events_concurrent(
            hello(),
            BufReader::new(server_read),
            server_write,
            |request, _| async move {
                Ok(Some(Response::ok(request.id().unwrap(), json!({"done": true}))))
            },
        );
        tokio::pin!(server);
        let mut input = BufReader::new(client_read);

        std::future::poll_fn(|cx| {
            assert!(server.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        for _ in 0..64 {
            jsonl::write(&mut client_write, &Request::call(1, "generate", json!({})))
                .await
                .unwrap();
            tokio::task::yield_now().await;

            // Leave completed workers unjoined until the next request is queued.
            std::future::poll_fn(|cx| {
                assert!(server.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            let response: Response =
                timeout(Duration::from_secs(1), jsonl::read(&mut input, super::FRAME))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();

            assert!(response.error.is_none(), "{response:?}");
            assert_eq!(response.result.unwrap()["done"], true);
        }

        jsonl::write(&mut client_write, &Request::call(2, "shutdown", json!({}))).await.unwrap();
        timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancellation_aborts_a_request_without_stopping_the_plugin() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let handle = |request: Request, _emitter: Emitter| async move {
            let Request::Call { id, .. } = request else {
                return Ok(None);
            };

            if id == 1 {
                std::future::pending::<()>().await;
            }

            Ok(Some(Response::ok(id, json!({"done": true}))))
        };

        let task = tokio::spawn(async move {
            serve_io_events_concurrent(hello(), BufReader::new(server_read), server_write, handle)
                .await
        });

        jsonl::write(&mut client_write, &Request::call(1, "run", json!({}))).await.unwrap();
        jsonl::write(
            &mut client_write,
            &Request::Note {
                jsonrpc: "2.0".into(),
                method: "cancel".into(),
                params: json!({"id": 1}),
            },
        )
        .await
        .unwrap();
        jsonl::write(&mut client_write, &Request::call(2, "shutdown", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        timeout(Duration::from_secs(1), input.read_line(&mut line)).await.unwrap().unwrap();
        let shutdown: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(shutdown.id, 2);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropped_host_calls_release_registrations_and_accept_late_responses() {
        let (output, mut sent) = tokio::sync::mpsc::channel(1);
        let emitter = Emitter::new(output);
        let task_emitter = emitter.clone();
        let call =
            tokio::spawn(async move { task_emitter.call("host/model_stream", json!({})).await });
        let request = sent.recv().await.unwrap();
        let id = request["id"].as_u64().unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());

        assert!(emitter.replies.lock().unwrap().is_empty());
        assert!(emitter.streams.lock().unwrap().is_empty());
        let cancel = sent.recv().await.unwrap();

        assert_eq!(cancel["method"], "cancel");
        assert_eq!(cancel["params"]["id"], id);

        let mut input = Vec::new();
        jsonl::write(&mut input, &Response::ok(id, json!({}))).await.unwrap();
        jsonl::write(&mut input, &Request::call(90, "ping", json!({}))).await.unwrap();
        jsonl::write(&mut input, &Response::ok(id + 1, json!({}))).await.unwrap();
        let (incoming, mut received) = tokio::sync::mpsc::channel(8);
        super::read_input(
            BufReader::new(input.as_slice()),
            incoming,
            emitter.replies.clone(),
            emitter.streams.clone(),
            emitter.calls.clone(),
        )
        .await;

        assert!(matches!(
            received.recv().await,
            Some(super::Incoming::Request(Request::Call { id: 90, .. }))
        ));
        assert!(matches!(received.recv().await, Some(super::Incoming::Error(_))));
    }

    #[tokio::test]
    async fn preserves_partial_frames_when_reading_is_interrupted() {
        let (mut writer, reader) = duplex(1024);
        let mut input = BufReader::new(reader);
        let mut frame = Vec::new();
        writer.write_all(b"{\"jsonrpc\":").await.unwrap();

        assert!(
            timeout(Duration::from_millis(10), super::read_frame(&mut input, &mut frame))
                .await
                .is_err()
        );
        writer.write_all(b"\"2.0\",\"id\":1,\"result\":{}}\n").await.unwrap();
        let value = super::read_frame(&mut input, &mut frame).await.unwrap();

        assert_eq!(value["id"], 1);
        assert!(frame.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancels_active_and_queued_host_requests_while_listening() {
        let script = r#"while IFS= read -r line; do
            case "$line" in
                *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}' ;;
                *command*)
                    printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"host/model_stream","params":{}}'
                    IFS= read -r event
                    printf '%s\n' '{"jsonrpc":"2.0","id":8,"method":"host/model_stream","params":{}}'
                    printf '%s\n' '{"jsonrpc":"2.0","method":"cancel","params":{"id":8}}'
                    printf '%s\n' '{"jsonrpc":"2.0","method":"cancel","params":{"id":7}}'
                    IFS= read -r cancelled
                    printf '%s\n' '{"jsonrpc":"2.0","id":9,"method":"host/model_stream","params":{}}'
                    IFS= read -r reply
                    printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"done":true}}' ;;
                *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{}}'; exit 0 ;;
            esac
        done"#;
        let mut process = start_test_plugin("host_cancel", script).await.unwrap();
        let (dropped, mut cancelled) = tokio::sync::oneshot::channel::<()>();
        let mut dropped = Some(dropped);
        let mut calls = Vec::new();
        let result = timeout(
            Duration::from_secs(5),
            process.listen_stream_async(
                Request::call(2, "command", json!({})),
                |_| async { Ok(()) },
                |request, events| {
                    let id = request.id().unwrap();
                    calls.push(id);
                    let guard = dropped.take();
                    async move {
                        if id == 7 {
                            let _guard = guard;
                            events
                                .send(Request::Note {
                                    jsonrpc: "2.0".into(),
                                    method: "event".into(),
                                    params: json!({}),
                                })
                                .unwrap();
                            std::future::pending::<()>().await;
                        }

                        Ok(Response::ok(id, json!({})))
                    }
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(calls, vec![7, 9]);
        assert!(matches!(
            cancelled.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));

        assert_eq!(result.result.unwrap()["done"], true);
        process.stop().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_handlers_use_distinct_ids_for_host_calls() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let handle = |request: Request, emitter: Emitter| async move {
            let Request::Call { id, .. } = request else {
                return Ok(None);
            };

            let response = emitter.call("host/tool", json!({"request": id})).await?;
            Ok(Some(Response::ok(id, response.result.unwrap())))
        };

        let task = tokio::spawn(async move {
            serve_io_events_concurrent(hello(), BufReader::new(server_read), server_write, handle)
                .await
        });

        jsonl::write(&mut client_write, &Request::call(1, "generate", json!({}))).await.unwrap();
        jsonl::write(&mut client_write, &Request::call(2, "generate", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        input.read_line(&mut line).await.unwrap();
        let first: Request = serde_json::from_str(&line).unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        let second: Request = serde_json::from_str(&line).unwrap();

        let Request::Call { id: first_id, params: first_params, .. } = first else {
            panic!("plugin server call must be a JSON-RPC request");
        };

        let Request::Call { id: second_id, params: second_params, .. } = second else {
            panic!("plugin server call must be a JSON-RPC request");
        };

        assert_ne!(first_id, second_id);

        jsonl::write(&mut client_write, &Response::ok(second_id, second_params)).await.unwrap();
        jsonl::write(&mut client_write, &Response::ok(first_id, first_params)).await.unwrap();

        let mut responses = std::collections::BTreeMap::new();

        for _ in 0..2 {
            line.clear();
            input.read_line(&mut line).await.unwrap();
            let response: Response = serde_json::from_str(&line).unwrap();
            responses.insert(response.id, response.result.unwrap());
        }

        assert_eq!(responses[&1]["request"], 1);
        assert_eq!(responses[&2]["request"], 2);

        jsonl::write(&mut client_write, &Request::call(3, "shutdown", json!({}))).await.unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Response>(&line).unwrap().id, 3);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn forwards_host_stream_events_to_the_matching_plugin_call() {
        let (client, server) = duplex(16 * 1024);
        let (mut client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server);
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let handle = {
            let captured = Arc::clone(&captured);
            move |request: Request, emitter: Emitter| {
                let captured = Arc::clone(&captured);
                async move {
                    let Request::Call { id, .. } = request else {
                        return Ok(None);
                    };

                    let response = emitter
                        .call_stream("host/model_stream", json!({}), move |event| {
                            let captured = Arc::clone(&captured);
                            async move {
                                captured.lock().unwrap().push(event);
                                Ok(())
                            }
                        })
                        .await?;

                    Ok(Some(Response::ok(id, response.result.unwrap())))
                }
            }
        };

        let task = tokio::spawn(async move {
            serve_io_events_concurrent(hello(), BufReader::new(server_read), server_write, handle)
                .await
        });

        jsonl::write(&mut client_write, &Request::call(1, "run", json!({}))).await.unwrap();

        let mut input = BufReader::new(&mut client_read);
        let mut line = String::new();
        input.read_line(&mut line).await.unwrap();
        let Request::Call { id, .. } = serde_json::from_str(&line).unwrap() else {
            panic!("host stream must start with a request");
        };

        jsonl::write(
            &mut client_write,
            &Request::Note {
                jsonrpc: "2.0".into(),
                method: "event".into(),
                params: json!({"callId":id,"event":{"kind":"text","text":"piece"}}),
            },
        )
        .await
        .unwrap();
        jsonl::write(&mut client_write, &Response::ok(id, json!({"done":true}))).await.unwrap();

        line.clear();
        input.read_line(&mut line).await.unwrap();
        let response: Response = serde_json::from_str(&line).unwrap();
        assert_eq!(response.result.unwrap()["done"], true);
        assert_eq!(*captured.lock().unwrap(), vec![json!({"kind":"text","text":"piece"})]);

        jsonl::write(&mut client_write, &Request::call(2, "shutdown", json!({}))).await.unwrap();
        line.clear();
        input.read_line(&mut line).await.unwrap();
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
    async fn cancelling_handshakes_stops_and_reaps_starting_processes() {
        for restart in [false, true] {
            let marker = temp_path("cancelled-handshake", "pid");
            let script = format!("sleep 30 & printf '%s\\n' $$ $! > '{}'; wait", marker.display());

            let task = tokio::spawn(async move {
                if restart {
                    let ready = r#"printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}'; cat >/dev/null"#;
                    let mut process = Process::start_with("sh", ["-c", ready]).await.unwrap();
                    process.args = vec!["-c".into(), script.into()];
                    process.restart().await
                } else {
                    Process::start_with("sh", ["-c", &script]).await.map(|_| ())
                }
            });

            let pids = timeout(Duration::from_secs(3), async {
                loop {
                    let pids = std::fs::read_to_string(&marker).unwrap_or_default();
                    let pids = pids.split_whitespace().map(str::to_owned).collect::<Vec<_>>();

                    if pids.len() == 2 {
                        break pids;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();

            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());

            timeout(Duration::from_secs(3), async {
                for (index, pid) in pids.iter().enumerate() {
                    loop {
                        let status = std::process::Command::new("ps")
                            .args(["-o", "stat=", "-p", pid])
                            .output()
                            .unwrap();

                        let state = String::from_utf8_lossy(&status.stdout);

                        // The direct child must be reaped; orphan descendants may await init.
                        if state.trim().is_empty()
                            || (index > 0 && state.trim_start().starts_with('Z'))
                        {
                            break;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .await
            .expect("cancelled handshake left a live process or an unreaped child");

            std::fs::remove_file(marker).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn process_cleanup_rejects_its_own_group() {
        let group = u32::try_from(getpgrp().as_raw_pid()).unwrap();
        assert!(external_group(group).is_none());
        assert!(external_group(u32::MAX).is_none());
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

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn listener_budgets_are_per_request_and_handler_errors_are_recoverable() {
        let script = r#"while IFS= read -r line; do
            case "$line" in
                *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}' ;;
                *command*)
                    count=0
                    while [ "$count" -lt 300 ]; do
                        printf '%s\n' '{"jsonrpc":"2.0","method":"event","params":{}}'
                        printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"host/model_stream","params":{}}'
                        IFS= read -r reply
                        count=$((count + 1))
                    done
                    printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"done":true}}' ;;
                *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;;
            esac
        done"#;

        let mut process = start_test_plugin("listener_budget", script).await.unwrap();
        let mut calls = 0;
        let mut notes = 0;

        let response = process
            .listen_stream_async(
                Request::call(2, "command", json!({})),
                |_| {
                    notes += 1;
                    std::future::ready(Ok(()))
                },
                |_, _| {
                    calls += 1;
                    std::future::ready(if calls == 1 {
                        Err(crate::Error::Denied("failed run".into()))
                    } else {
                        Ok(Response::ok(7, json!({"text":"done"})))
                    })
                },
            )
            .await
            .unwrap();

        assert_eq!(calls, 300);
        assert_eq!(notes, 300);
        assert_eq!(response.result.unwrap()["done"], true);
        process.stop().await.unwrap();

        let mut process = start_test_plugin("operation_budget", script).await.unwrap();
        let result = process
            .call_full_timeout_stream_async(
                Request::call(2, "command", json!({})),
                Duration::from_secs(5),
                |_| async { Ok(()) },
                |_, _| async { Ok(Response::ok(7, json!({"text":"done"}))) },
            )
            .await;

        assert!(matches!(result, Err(crate::Error::Limit(_))));
        drop(process);
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn forwards_host_events_while_answering_a_plugin_server_request() {
        let script = r#"while IFS= read -r line; do id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\),"method".*/\1/p'); case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"test","version":"0.1.0","capabilities":[]}}' ;; *command*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"method":"host/model_stream","params":{}}'; IFS= read -r reply; IFS= read -r stream_event; printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"done":true}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let mut process = start_test_plugin("stream_server_request", script).await.unwrap();
        let mut notes = Vec::new();
        let response = process
            .call_full_timeout_stream_async(
                Request::call(2, "command", json!({})),
                Duration::from_secs(5),
                |note| {
                    notes.push(note);
                    async { Ok(()) }
                },
                |request, events| async move {
                    let Some(id) = request.id() else {
                        return Err(crate::Error::Protocol("expected a server call".into()));
                    };

                    events
                        .send(Request::Note {
                            jsonrpc: "2.0".into(),
                            method: "event".into(),
                            params: json!({"callId":id,"event":{"kind":"text","text":"live"}}),
                        })
                        .map_err(|_| crate::Error::Protocol("event receiver closed".into()))?;

                    Ok(Response::ok(id, json!({"accepted":true})))
                },
            )
            .await
            .unwrap();

        assert_eq!(response.result.unwrap()["done"], true);
        assert!(notes.is_empty());

        process.stop().await.unwrap();
    }
}
