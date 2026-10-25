#![forbid(unsafe_code)]

use krabb_core::{
    plugin::{Emitter, serve_events_concurrent},
    types::{
        Capability, CommandSpec, Content, Event, Hello, Message, ModelReply, ModelRequest,
        Protocol, Request, Response, Role, ToolSpec,
    },
};

use serde_json::{Value, json};
use std::future::Future;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
};

const HEADER_LIMIT: usize = 8 * 1024;
const BODY_LIMIT: usize = 128 * 1024;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("ag-ui", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    serve_events_concurrent(hello(), |request, emitter| async move { call(request, emitter).await })
        .await
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "ag-ui".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Agent],
        commands: vec![CommandSpec {
            name: "ag-ui".into(),
            description: "Serve the local AG-UI run endpoint.".into(),
            interactive: false,
        }],
    }
}

async fn call(request: Request, emitter: Emitter) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method != "command" || params["name"] != "ag-ui" {
        return Ok(None);
    }

    let address = listen_address(std::env::var("KRABB_AG_UI_LISTEN").ok().as_deref())?;
    start_command(id, address, emitter).await
}

async fn start_command(
    id: u64,
    address: SocketAddr,
    emitter: Emitter,
) -> krabb_core::Result<Option<Response>> {
    let bound_address = start_listener(address, emitter.clone()).await?;
    let mut emitter = emitter;
    emitter.event(json!({"text":format!("AG-UI listening on {bound_address}")})).await?;

    tokio::signal::ctrl_c().await.map_err(|error| {
        krabb_core::Error::Protocol(format!("AG-UI shutdown signal failed: {error}."))
    })?;

    Ok(Some(Response::ok(id, json!({"stopped": true}))))
}

fn listen_address(value: Option<&str>) -> krabb_core::Result<SocketAddr> {
    value.unwrap_or("127.0.0.1:8787").parse::<SocketAddr>().map_err(|error| {
        krabb_core::Error::Denied(format!("Invalid AG-UI listen address: {error}."))
    })
}

async fn start_listener(address: SocketAddr, emitter: Emitter) -> krabb_core::Result<SocketAddr> {
    if !address.ip().is_loopback() {
        return Err(krabb_core::Error::Denied("AG-UI can only bind to a loopback address.".into()));
    }

    let listener = TcpListener::bind(address)
        .await
        .map_err(|error| krabb_core::Error::Denied(format!("AG-UI listener failed: {error}.")))?;

    let bound_address = listener.local_addr().map_err(|error| {
        krabb_core::Error::Protocol(format!("AG-UI listener address failed: {error}."))
    })?;

    tokio::spawn(serve(listener, emitter));

    Ok(bound_address)
}

async fn serve(listener: TcpListener, emitter: Emitter) {
    let capacity = Arc::new(Semaphore::new(4));

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(connection) => connection,

            Err(error) => {
                tracing::warn!(%error, "AG-UI accept failed");
                return;
            }
        };

        let permit = match Arc::clone(&capacity).try_acquire_owned() {
            Ok(permit) => permit,

            Err(_) => {
                let mut stream = stream;

                if let Err(error) = write_response(
                    &mut stream,
                    503,
                    "Service Unavailable",
                    "application/json",
                    "{}",
                    None,
                )
                .await
                {
                    tracing::warn!(%error, "AG-UI busy response failed");
                }

                continue;
            }
        };

        let emitter = emitter.clone();

        tokio::spawn(async move {
            let _permit = permit;

            if let Err(error) = handle_connection(stream, |params, events| async move {
                let response = emitter
                    .call_stream("host/model_stream", params, |event| {
                        let _ = events.send(event);
                        std::future::ready(Ok(()))
                    })
                    .await?;
                let result = response.result.ok_or_else(|| {
                    krabb_core::Error::Protocol("Host model request failed.".into())
                })?;

                serde_json::from_value(result).map_err(|error| {
                    krabb_core::Error::Protocol(format!("Invalid host model response: {error}."))
                })
            })
            .await
            {
                tracing::warn!(%error, "AG-UI request failed");
            }
        });
    }
}

async fn handle_connection<F, Fut>(mut stream: TcpStream, generate: F) -> krabb_core::Result<()>
where
    F: FnOnce(Value, mpsc::UnboundedSender<Value>) -> Fut,
    Fut: Future<Output = krabb_core::Result<ModelReply>>,
{
    let request = read_request(&mut stream).await?;
    let origin = request.origin.as_deref().filter(|origin| loopback_origin(origin));

    if request.origin.is_some() && origin.is_none() {
        return write_response(&mut stream, 403, "Forbidden", "application/json", "{}", None).await;
    }

    if request.method == "OPTIONS" {
        return write_response(&mut stream, 204, "No Content", "", "", origin).await;
    }

    if request.method != "POST" || request.path != "/" {
        return write_response(&mut stream, 404, "Not Found", "application/json", "{}", origin)
            .await;
    }

    let input: Value = match serde_json::from_slice(&request.body) {
        Ok(input) => input,

        Err(_) => {
            return write_response(
                &mut stream,
                400,
                "Bad Request",
                "application/json",
                "{\"error\":\"Invalid AG-UI run input.\"}",
                origin,
            )
            .await;
        }
    };

    let run = match RunInput::parse(&input) {
        Ok(run) => run,

        Err(_) => {
            return write_response(
                &mut stream,
                400,
                "Bad Request",
                "application/json",
                "{\"error\":\"Invalid AG-UI run input.\"}",
                origin,
            )
            .await;
        }
    };

    write_sse_headers(&mut stream, origin).await?;
    write_sse_event(
        &mut stream,
        &json!({"type":"RUN_STARTED","threadId":run.thread_id,"runId":run.run_id}),
    )
    .await?;

    if let Some(state) = run.state.as_ref() {
        write_sse_event(&mut stream, &json!({"type":"STATE_SNAPSHOT","snapshot":state})).await?;
    }

    let params = run.model_request();
    let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
    let generation = generate(params, event_sender);
    tokio::pin!(generation);
    let mut stream_state = RunStream::new(&run);
    let mut disconnect_probe = [0_u8; 1];

    loop {
        tokio::select! {
            disconnected = stream.read(&mut disconnect_probe) => {
                if disconnected.is_err() || disconnected.is_ok_and(|read| read == 0) {
                    break;
                }
            }

            result = &mut generation => {
                match result {
                    Ok(reply) => {
                        for event in stream_state.fallback(&run, &reply)? {
                            write_sse_event(&mut stream, &event).await?;
                        }

                        for event in stream_state.finish(&run, &reply) {
                            write_sse_event(&mut stream, &event).await?;
                        }
                    }

                    Err(_) => {
                        let event = json!({
            "type":"RUN_ERROR",
            "threadId":run.thread_id,
            "runId":run.run_id,
            "message":"The configured model request failed.",
            "code":"MODEL_REQUEST_FAILED"
                        });

                        write_sse_event(&mut stream, &event).await?;
                    }
                }

                break;
            }

            Some(event) = event_receiver.recv() => {
                for event in stream_state.push(&run, event)? {
                    write_sse_event(&mut stream, &event).await?;
                }
            }
        }
    }

    finish_sse(&mut stream).await
}

struct RunStream {
    message_id: String,
    message_open: bool,
    text_seen: bool,
    event_seen: bool,
    pending_tools: Vec<String>,
    error: Option<String>,
    index: usize,
}

impl RunStream {
    fn new(run: &RunInput) -> Self {
        Self {
            message_id: format!("{}-message", run.run_id),
            message_open: false,
            text_seen: false,
            event_seen: false,
            pending_tools: Vec::new(),
            error: None,
            index: 0,
        }
    }

    fn push(&mut self, run: &RunInput, value: Value) -> krabb_core::Result<Vec<Value>> {
        let event: Event = serde_json::from_value(value).map_err(|error| {
            krabb_core::Error::Protocol(format!("Invalid model stream event: {error}."))
        })?;
        self.event_seen = true;

        let mapped = match event {
            Event::Text { text } | Event::Done { text } if !text.is_empty() => {
                self.text_seen = true;
                let mut events = Vec::new();

                if !self.message_open {
                    self.message_open = true;
                    events.push(json!({"type":"TEXT_MESSAGE_START","messageId":self.message_id,"role":"assistant"}));
                }

                events.push(
                    json!({"type":"TEXT_MESSAGE_CONTENT","messageId":self.message_id,"delta":text}),
                );
                events
            }

            Event::Tool { name, args, id, .. } => {
                let tool_call_id =
                    id.unwrap_or_else(|| format!("{}-tool-{}", run.run_id, self.index));
                self.index += 1;
                self.pending_tools.push(tool_call_id.clone());
                vec![
                    json!({"type":"TOOL_CALL_START","toolCallId":tool_call_id,"toolCallName":name,"parentMessageId":self.message_id}),
                    json!({"type":"TOOL_CALL_ARGS","toolCallId":tool_call_id,"delta":args.to_string()}),
                    json!({"type":"TOOL_CALL_END","toolCallId":tool_call_id}),
                ]
            }

            Event::Error { message } => {
                self.error = Some(message);
                Vec::new()
            }

            _ => Vec::new(),
        };

        Ok(mapped)
    }

    fn fallback(&mut self, run: &RunInput, reply: &ModelReply) -> krabb_core::Result<Vec<Value>> {
        let mut events = Vec::new();

        if !self.event_seen {
            for event in &reply.events {
                events.extend(self.push(run, serde_json::to_value(event)?)?);
            }
        }

        if self.text_seen || reply.text.is_empty() {
            return Ok(events);
        }

        self.message_open = true;
        self.text_seen = true;
        events.extend([
            json!({"type":"TEXT_MESSAGE_START","messageId":self.message_id,"role":"assistant"}),
            json!({"type":"TEXT_MESSAGE_CONTENT","messageId":self.message_id,"delta":reply.text}),
        ]);

        Ok(events)
    }

    fn finish(&mut self, run: &RunInput, reply: &ModelReply) -> Vec<Value> {
        if self.error.is_none() {
            self.error = reply.events.iter().find_map(|event| match event {
                Event::Error { message } => Some(message.clone()),
                _ => None,
            });
        }

        if self.pending_tools.is_empty() {
            self.pending_tools = reply
                .events
                .iter()
                .enumerate()
                .filter_map(|(index, event)| {
                    if let Event::Tool { id, .. } = event {
                        Some(id.clone().unwrap_or_else(|| format!("{}-tool-{index}", run.run_id)))
                    } else {
                        None
                    }
                })
                .collect();
        }

        let mut events = Vec::new();

        if self.message_open {
            events.push(json!({"type":"TEXT_MESSAGE_END","messageId":self.message_id}));
        }

        if let Some(message) = self.error.take() {
            events.push(json!({"type":"RUN_ERROR","threadId":run.thread_id,"runId":run.run_id,"message":message,"code":"MODEL_RUN_FAILED"}));
        } else {
            let outcome = if self.pending_tools.is_empty() {
                json!({"type":"success"})
            } else {
                json!({"type":"success","pendingToolCallIds":self.pending_tools})
            };

            events.push(json!({"type":"RUN_FINISHED","threadId":run.thread_id,"runId":run.run_id,"outcome":outcome}));
        }

        events
    }
}

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    origin: Option<String>,
}

async fn read_request(stream: &mut TcpStream) -> krabb_core::Result<HttpRequest> {
    let mut bytes = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 1024];
    let header_end;

    loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|error| krabb_core::Error::Protocol(format!("AG-UI read failed: {error}.")))?;

        if read == 0 {
            return Err(krabb_core::Error::Protocol("AG-UI request ended early.".into()));
        }

        bytes.extend_from_slice(&buffer[..read]);

        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = end + 4;
            break;
        }

        if bytes.len() > HEADER_LIMIT {
            return Err(krabb_core::Error::Limit("AG-UI request headers are too large.".into()));
        }
    }

    if header_end > HEADER_LIMIT {
        return Err(krabb_core::Error::Limit("AG-UI request headers are too large.".into()));
    }

    let header_text = std::str::from_utf8(&bytes[..header_end - 4]).map_err(|error| {
        krabb_core::Error::Protocol(format!("AG-UI headers are invalid: {error}."))
    })?;

    let mut lines = header_text.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let mut content_length = 0_usize;
    let mut origin = None;

    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().map_err(|_| {
                    krabb_core::Error::Protocol("AG-UI Content-Length is invalid.".into())
                })?;
            } else if name.eq_ignore_ascii_case("origin") {
                origin = Some(value.trim().to_owned());
            }
        }
    }

    if content_length > BODY_LIMIT {
        return Err(krabb_core::Error::Limit("AG-UI request body is too large.".into()));
    }

    let mut body = bytes[header_end..].to_vec();

    if body.len() > content_length {
        body.truncate(content_length);
    } else if body.len() < content_length {
        let start = body.len();
        body.resize(content_length, 0);
        stream.read_exact(&mut body[start..]).await.map_err(|error| {
            krabb_core::Error::Protocol(format!("AG-UI request body ended early: {error}."))
        })?;
    }

    Ok(HttpRequest { method, path, body, origin })
}

fn loopback_origin(origin: &str) -> bool {
    let Some((scheme, remainder)) = origin.split_once("://") else {
        return false;
    };

    let authority = remainder.split(['/', '?', '#']).next().unwrap_or_default();

    if !matches!(scheme, "http" | "https") || authority.contains('@') {
        return false;
    }

    let host = authority
        .strip_prefix('[')
        .and_then(|value| value.split_once(']').map(|(host, _)| host))
        .or_else(|| authority.split_once(':').map(|(host, _)| host))
        .unwrap_or(authority);

    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &str,
    origin: Option<&str>,
) -> krabb_core::Result<()> {
    let content_length = body.len();
    let access_control_origin = origin
        .map(|value| format!("Access-Control-Allow-Origin: {value}\r\nVary: Origin\r\n"))
        .unwrap_or_default();

    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\n{access_control_origin}Access-Control-Allow-Methods: POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Accept\r\nConnection: close\r\n\r\n{body}"
    );

    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|error| krabb_core::Error::Protocol(format!("AG-UI write failed: {error}.")))
}

async fn write_sse_headers(stream: &mut TcpStream, origin: Option<&str>) -> krabb_core::Result<()> {
    let access_control_origin = origin
        .map(|value| format!("Access-Control-Allow-Origin: {value}\r\nVary: Origin\r\n"))
        .unwrap_or_default();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n{access_control_origin}Access-Control-Allow-Methods: POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Accept\r\nConnection: close\r\n\r\n"
    );

    stream.write_all(headers.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

async fn write_sse_event(stream: &mut TcpStream, event: &Value) -> krabb_core::Result<()> {
    let data = format!("data: {event}\n\n");
    let chunk = format!("{:x}\r\n{data}\r\n", data.len());

    stream.write_all(chunk.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

async fn finish_sse(stream: &mut TcpStream) -> krabb_core::Result<()> {
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await?;

    Ok(())
}

struct RunInput {
    thread_id: String,
    run_id: String,
    messages: Vec<Message>,
    tools: Vec<ToolSpec>,
    state: Option<Value>,
}

impl RunInput {
    fn parse(input: &Value) -> krabb_core::Result<Self> {
        let thread_id = input["threadId"].as_str().filter(|value| !value.is_empty());

        let run_id = input["runId"].as_str().filter(|value| !value.is_empty());
        let source = input["messages"].as_array().ok_or_else(|| {
            krabb_core::Error::Protocol("AG-UI messages must be an array.".into())
        })?;

        let mut messages = Vec::new();

        for (index, item) in source.iter().enumerate() {
            let role = match item["role"].as_str().unwrap_or_default() {
                "system" => Role::System,
                "assistant" => Role::Assistant,
                "user" => Role::User,
                "tool" => Role::Tool,

                _ => {
                    return Err(krabb_core::Error::Protocol(
                        "AG-UI message role is unsupported.".into(),
                    ));
                }
            };

            let text = item["content"].as_str().map(str::to_owned).unwrap_or_else(|| {
                item["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            });

            let mut content = Vec::new();

            if !text.is_empty() {
                content.push(Content::Text { text });
            }

            for (call_index, tool_call) in
                item["toolCalls"].as_array().into_iter().flatten().enumerate()
            {
                let function = &tool_call["function"];
                let name =
                    function["name"].as_str().filter(|name| !name.is_empty()).ok_or_else(|| {
                        krabb_core::Error::Protocol("AG-UI tool call name is required.".into())
                    })?;
                let args = match &function["arguments"] {
                    Value::String(arguments) => {
                        serde_json::from_str(arguments).map_err(|error| {
                            krabb_core::Error::Protocol(format!(
                                "AG-UI tool call arguments are invalid JSON: {error}."
                            ))
                        })?
                    }

                    value => value.clone(),
                };

                content.push(Content::ToolCall {
                    name: name.to_owned(),
                    args,
                    id: tool_call["id"]
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| Some(format!("ag-ui-{index}-tool-{call_index}"))),
                    thought_signature: None,
                });
            }

            if content.is_empty() {
                continue;
            }

            messages.push(Message {
                id: item["id"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("ag-ui-{index}")),
                session: thread_id.unwrap_or("ag-ui").to_owned(),
                role,
                sender: item["toolCallId"].as_str().map(str::to_owned),
                content,
            });
        }

        if messages.is_empty() {
            return Err(krabb_core::Error::Protocol(
                "AG-UI run input has no text messages.".into(),
            ));
        }

        let tools = input["tools"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|tool| {
                let name =
                    tool["name"].as_str().filter(|name| !name.is_empty()).ok_or_else(|| {
                        krabb_core::Error::Protocol("AG-UI tool name is required.".into())
                    })?;

                if name.len() > 128 {
                    return Err(krabb_core::Error::Limit("AG-UI tool name is too long.".into()));
                }

                Ok(ToolSpec {
                    name: name.to_owned(),
                    description: tool["description"].as_str().map(str::to_owned),
                    schema: tool
                        .get("parameters")
                        .or_else(|| tool.get("schema"))
                        .cloned()
                        .unwrap_or_else(|| json!({"type":"object"})),
                })
            })
            .collect::<krabb_core::Result<Vec<_>>>()?;

        if tools.len() > 64 {
            return Err(krabb_core::Error::Limit("AG-UI has too many tools.".into()));
        }

        let state = input.get("state").filter(|value| !value.is_null()).cloned();
        let context = input["context"].as_array().cloned().unwrap_or_default();

        if state.as_ref().is_some_and(|state| {
            serde_json::to_vec(state).is_ok_and(|bytes| bytes.len() > BODY_LIMIT)
        }) {
            return Err(krabb_core::Error::Limit("AG-UI state is too large.".into()));
        }

        if !context.is_empty() || state.is_some() {
            let serialized = serde_json::to_string(&json!({"state":state,"context":context}))?;

            messages.push(Message {
                id: format!("{}-context-{}", thread_id.unwrap_or("ag-ui"), run_id.unwrap_or("run")),
                session: thread_id.unwrap_or("ag-ui").to_owned(),
                role: Role::User,
                sender: None,
                content: vec![Content::Text {
                    text: format!("Untrusted AG-UI client context data: {serialized}"),
                }],
            });
        }

        Ok(Self {
            thread_id: thread_id
                .ok_or_else(|| krabb_core::Error::Protocol("AG-UI threadId is required.".into()))?
                .to_owned(),
            run_id: run_id
                .ok_or_else(|| krabb_core::Error::Protocol("AG-UI runId is required.".into()))?
                .to_owned(),
            messages,
            tools,
            state,
        })
    }

    fn model_request(&self) -> Value {
        json!(ModelRequest {
            model: std::env::var("KRABB_MODEL").unwrap_or_default(),
            workspace: std::env::var("KRABB_ROOT").ok(),
            messages: self.messages.clone(),
            stream: true,
            tools: self.tools.clone(),
        })
    }

    #[cfg(test)]
    fn events(&self, reply: &ModelReply) -> Vec<Value> {
        let mut events = Vec::new();
        let mut message_open = false;
        let mut text_seen = false;
        let mut pending_tools = Vec::new();
        let message_id = format!("{}-message", self.run_id);
        let mut error = None;

        for (index, event) in reply.events.iter().enumerate() {
            match event {
                Event::Text { text } | Event::Done { text } => {
                    if !text.is_empty() {
                        if !message_open {
                            events.push(json!({
                                "type":"TEXT_MESSAGE_START",
                                "messageId":message_id,
                                "role":"assistant"
                            }));
                            message_open = true;
                        }

                        text_seen = true;
                        events.push(json!({
                            "type":"TEXT_MESSAGE_CONTENT",
                            "messageId":message_id,
                            "delta":text
                        }));
                    }
                }

                Event::Tool { name, args, id, .. } => {
                    let tool_call_id =
                        id.clone().unwrap_or_else(|| format!("{}-tool-{index}", self.run_id));
                    pending_tools.push(tool_call_id.clone());
                    events.push(json!({
                        "type":"TOOL_CALL_START",
                        "toolCallId":tool_call_id,
                        "toolCallName":name,
                        "parentMessageId":message_id
                    }));
                    events.push(json!({
                        "type":"TOOL_CALL_ARGS",
                        "toolCallId":tool_call_id,
                        "delta":args.to_string()
                    }));
                    events.push(json!({"type":"TOOL_CALL_END","toolCallId":tool_call_id}));
                }

                Event::Error { message } => error = Some(message.clone()),
            }
        }

        if !text_seen && !reply.text.is_empty() {
            events.push(json!({
                "type":"TEXT_MESSAGE_START",
                "messageId":message_id,
                "role":"assistant"
            }));
            events.push(json!({
                "type":"TEXT_MESSAGE_CONTENT",
                "messageId":message_id,
                "delta":reply.text
            }));
            message_open = true;
        }

        if message_open {
            events.push(json!({"type":"TEXT_MESSAGE_END","messageId":message_id}));
        }

        if let Some(message) = error {
            events.push(json!({
                "type":"RUN_ERROR",
                "threadId":self.thread_id,
                "runId":self.run_id,
                "message":message,
                "code":"MODEL_RUN_FAILED"
            }));
        } else {
            let outcome = if pending_tools.is_empty() {
                json!({"type":"success"})
            } else {
                json!({"type":"success","pendingToolCallIds":pending_tools})
            };

            events.push(json!({
                "type":"RUN_FINISHED",
                "threadId":self.thread_id,
                "runId":self.run_id,
                "outcome":outcome
            }));
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    async fn exchange(request: String, reply: ModelReply) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let handler = tokio::spawn(handle_connection(server, move |_, _| async move { Ok(reply) }));

        client.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        handler.await.unwrap().unwrap();

        String::from_utf8(response).unwrap()
    }

    async fn exchange_failure(request: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let handler = tokio::spawn(handle_connection(server, |_, _| async {
            Err(krabb_core::Error::Denied("model unavailable".into()))
        }));

        client.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        handler.await.unwrap().unwrap();

        String::from_utf8(response).unwrap()
    }

    async fn parse_request(bytes: &[u8]) -> krabb_core::Result<HttpRequest> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        client.write_all(bytes).await.unwrap();
        read_request(&mut server).await
    }

    fn reply(text: &str) -> ModelReply {
        ModelReply {
            text: text.into(),
            stop: "stop".into(),
            input: None,
            output: None,
            context_usage: None,
            events: Vec::new(),
        }
    }

    fn emitter() -> Emitter {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        Emitter::new(sender)
    }

    #[test]
    fn parses_standard_run_input_and_maps_text_messages() {
        let run = RunInput::parse(&json!({
            "threadId":"thread-1",
            "runId":"run-1",
            "messages":[{"id":"m1","role":"user","content":"hello"}]
        }))
        .unwrap();

        assert_eq!(run.messages[0].content, vec![Content::Text { text: "hello".into() }]);
        assert_eq!(run.events(&reply("answer"))[0]["type"], "TEXT_MESSAGE_START");
        assert_eq!(run.events(&reply("answer")).last().unwrap()["type"], "RUN_FINISHED");
    }

    #[test]
    fn maps_frontend_tools_state_and_tool_results() {
        let run = RunInput::parse(&json!({
            "threadId":"thread-1",
            "runId":"run-2",
            "state":{"count":2},
            "tools":[{"name":"confirm","description":"Confirm action","parameters":{"type":"object"}}],
            "messages":[
                {"id":"assistant-1","role":"assistant","toolCalls":[{"id":"call-1","function":{"name":"confirm","arguments":{"ok":true}}}]},
                {"id":"tool-1","role":"tool","toolCallId":"call-1","content":"approved"}
            ]
        }))
        .unwrap();

        let request: ModelRequest = serde_json::from_value(run.model_request()).unwrap();

        assert!(request.stream);
        assert_eq!(request.tools[0].name, "confirm");
        assert_eq!(request.messages[0].role, Role::Assistant);
        assert!(matches!(request.messages[0].content[0], Content::ToolCall { .. }));
        assert_eq!(request.messages[1].role, Role::Tool);
        assert_eq!(request.messages[1].sender.as_deref(), Some("call-1"));
        assert_eq!(run.state, Some(json!({"count":2})));
    }

    #[test]
    fn maps_model_tool_calls_to_frontend_lifecycle_and_error_events() {
        let run = RunInput::parse(&json!({
            "threadId":"thread-1",
            "runId":"run-3",
            "messages":[{"role":"user","content":"approve this"}]
        }))
        .unwrap();
        let mut tool_reply = reply("");

        tool_reply.events.push(Event::Tool {
            name: "confirm".into(),
            args: json!({"amount":4}),
            id: Some("call-4".into()),
            thought_signature: None,
        });

        let events = run.events(&tool_reply);

        assert_eq!(events[0]["type"], "TOOL_CALL_START");
        assert_eq!(events[1]["type"], "TOOL_CALL_ARGS");
        assert_eq!(events[2]["type"], "TOOL_CALL_END");
        assert_eq!(events[3]["outcome"]["pendingToolCallIds"][0], "call-4");

        let mut failed_reply = reply("");

        failed_reply.events.push(Event::Error { message: "failed".into() });

        assert_eq!(run.events(&failed_reply).last().unwrap()["type"], "RUN_ERROR");
    }

    #[test]
    fn preserves_model_text_stream_chunks() {
        let run = RunInput::parse(&json!({
            "threadId":"thread-1",
            "runId":"run-4",
            "messages":[{"role":"user","content":"hello"}]
        }))
        .unwrap();
        let mut streamed = reply("hello world");

        streamed.events =
            vec![Event::Text { text: "hello ".into() }, Event::Text { text: "world".into() }];

        let events = run.events(&streamed);
        let deltas = events
            .iter()
            .filter(|event| event["type"] == "TEXT_MESSAGE_CONTENT")
            .filter_map(|event| event["delta"].as_str())
            .collect::<Vec<_>>();

        assert_eq!(deltas, ["hello ", "world"]);
    }

    #[test]
    fn rejects_missing_run_identifiers() {
        let error = RunInput::parse(&json!({"messages":[{"role":"user","content":"hello"}]}));

        assert!(error.is_err());
    }

    #[test]
    fn accepts_only_loopback_browser_origins() {
        assert!(loopback_origin("http://localhost:3000"));
        assert!(loopback_origin("https://127.0.0.1:8080"));
        assert!(loopback_origin("http://[::1]:3000"));
        assert!(!loopback_origin("https://example.com"));
        assert!(!loopback_origin("http://localhost.example.com"));
    }

    #[test]
    fn describes_the_agent_command() {
        let hello = hello();

        assert_eq!(hello.id, "ag-ui");
        assert_eq!(hello.capabilities, vec![Capability::Agent]);
        assert_eq!(hello.commands[0].name, "ag-ui");
        assert!(!hello.commands[0].interactive);
    }

    #[test]
    fn parses_loopback_listen_addresses() {
        assert_eq!(listen_address(None).unwrap(), "127.0.0.1:8787".parse().unwrap());
        assert_eq!(listen_address(Some("[::1]:3000")).unwrap(), "[::1]:3000".parse().unwrap());
        assert!(listen_address(Some("nope")).is_err());
    }

    #[tokio::test]
    async fn starts_only_a_loopback_listener() {
        let result = start_listener("0.0.0.0:0".parse().unwrap(), emitter()).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn keeps_the_bound_endpoint_available() {
        let address = start_listener("127.0.0.1:0".parse().unwrap(), emitter()).await.unwrap();
        let _connection = TcpStream::connect(address).await.unwrap();
    }

    #[tokio::test]
    async fn ignores_notes_and_unrelated_calls() {
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "event".into(), params: json!({}) };
        let unrelated = Request::call(2, "ping", json!({"name":"other"}));

        assert!(call(note, emitter()).await.unwrap().is_none());
        assert!(call(unrelated, emitter()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn serves_ag_ui_events_from_a_text_run() {
        let body = json!({
            "threadId":"thread-1",
            "runId":"run-1",
            "messages":[{"id":"m1","role":"user","content":"hello"}]
        })
        .to_string();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nOrigin: http://localhost:3000\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = exchange(request, reply("answer")).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: text/event-stream"));
        assert!(response.contains("Access-Control-Allow-Origin: http://localhost:3000"));
        assert!(response.contains("Transfer-Encoding: chunked"));
        assert!(response.contains("\"type\":\"RUN_STARTED\""));
        assert!(response.contains("\"type\":\"TEXT_MESSAGE_CONTENT\""));
        assert!(response.contains("\"delta\":\"answer\""));
        assert!(response.contains("\"type\":\"RUN_FINISHED\""));
    }

    #[tokio::test]
    async fn reports_model_failures_with_a_run_error_event() {
        let body = json!({
            "threadId":"thread-1",
            "runId":"run-error",
            "messages":[{"id":"m1","role":"user","content":"hello"}]
        })
        .to_string();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = exchange_failure(request).await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"type\":\"RUN_STARTED\""));
        assert!(response.contains("\"type\":\"RUN_ERROR\""));
        assert!(response.contains("MODEL_REQUEST_FAILED"));
    }

    #[tokio::test]
    async fn streams_model_events_before_the_model_reply_finishes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (release, wait) = oneshot::channel();
        let handler = tokio::spawn(handle_connection(server, move |_, events| async move {
            events.send(json!({"kind":"text","text":"live"})).unwrap();
            let _ = wait.await;
            Ok(reply("live"))
        }));
        let body = r#"{"threadId":"thread-1","runId":"run-live","messages":[{"role":"user","content":"hello"}]}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );

        client.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        let mut buffer = [0_u8; 1024];

        loop {
            let read = client.read(&mut buffer).await.unwrap();

            assert_ne!(read, 0);
            response.extend_from_slice(&buffer[..read]);

            if response
                .windows(b"TEXT_MESSAGE_CONTENT".len())
                .any(|window| window == b"TEXT_MESSAGE_CONTENT")
            {
                break;
            }
        }

        assert!(String::from_utf8_lossy(&response).contains("live"));
        release.send(()).unwrap();

        let mut remaining = Vec::new();
        client.read_to_end(&mut remaining).await.unwrap();
        response.extend(remaining);
        handler.await.unwrap().unwrap();

        let response = String::from_utf8(response).unwrap();

        assert_eq!(response.matches("TEXT_MESSAGE_CONTENT").count(), 1);
        assert!(response.contains("RUN_FINISHED"));
    }

    #[tokio::test]
    async fn drops_a_pending_model_request_when_the_client_disconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (cancelled, cancelled_rx) = oneshot::channel::<()>();
        let handler = tokio::spawn(handle_connection(server, move |_, _| async move {
            let _cancelled = cancelled;
            std::future::pending::<()>().await;
            Ok(reply("unreachable"))
        }));
        let body = r#"{"threadId":"thread-1","runId":"run-cancel","messages":[{"role":"user","content":"hello"}]}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );

        client.write_all(request.as_bytes()).await.unwrap();

        let mut response = [0_u8; 1024];
        let read = client.read(&mut response).await.unwrap();

        assert!(String::from_utf8_lossy(&response[..read]).contains("RUN_STARTED"));
        drop(client);

        tokio::time::timeout(std::time::Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap_err();
        handler.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn returns_http_errors_for_unavailable_routes_and_untrusted_origins() {
        let not_found =
            exchange("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n".into(), reply("unused"))
                .await;
        let forbidden = exchange(
            "OPTIONS / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\n\r\n".into(),
            reply("unused"),
        )
        .await;

        assert!(not_found.starts_with("HTTP/1.1 404 Not Found"));
        assert!(forbidden.starts_with("HTTP/1.1 403 Forbidden"));
    }

    #[tokio::test]
    async fn accepts_loopback_preflight_requests() {
        let response = exchange(
            "OPTIONS / HTTP/1.1\r\nHost: localhost\r\nOrigin: http://127.0.0.1:3000\r\n\r\n".into(),
            reply("unused"),
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 204 No Content"));
        assert!(response.contains("Access-Control-Allow-Methods: POST, OPTIONS"));
    }

    #[tokio::test]
    async fn serves_http_requests_from_the_background_listener() {
        let address = start_listener("127.0.0.1:0".parse().unwrap(), emitter()).await.unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();

        assert!(String::from_utf8(response).unwrap().starts_with("HTTP/1.1 404 Not Found"));
    }

    #[tokio::test]
    async fn reads_bounded_http_headers_and_request_bodies() {
        let oversized_body =
            parse_request(b"POST / HTTP/1.1\r\nContent-Length: 131073\r\n\r\n").await;
        let invalid_length =
            parse_request(b"POST / HTTP/1.1\r\nContent-Length: nope\r\n\r\n").await;
        let oversized_headers =
            parse_request(&[vec![b'a'; HEADER_LIMIT + 1], b"\r\n\r\n".to_vec()].concat()).await;

        assert!(oversized_body.is_err());
        assert!(invalid_length.is_err());
        assert!(oversized_headers.is_err());
    }
}
