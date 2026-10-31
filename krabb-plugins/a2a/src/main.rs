#![forbid(unsafe_code)]

use krabb_core::{
    plugin::{Emitter, serve_events},
    types::{Capability, CommandSpec, Hello, Protocol, Request, Response},
};

use futures_util::StreamExt;
use reqwest::Url;
use serde_json::{Value, json};
use std::time::Duration;

const BODY_LIMIT: usize = krabb_core::jsonl::MAX / 2;
const RPC_TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("a2a", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(RPC_TIMEOUT)
        .build()
        .map_err(|error| krabb_core::Error::Denied(format!("A2A client failed: {error}.")))?;

    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "a2a".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Agent],
            commands: vec![CommandSpec {
                name: "a2a".into(),
                description: "Discover and collaborate with a remote A2A agent.".into(),
                interactive: false,
            }],
        },
        move |request, emitter| {
            let client = client.clone();
            async move { call(&client, request, emitter).await }
        },
    )
    .await
}

async fn call(
    client: &reqwest::Client,
    request: Request,
    mut emitter: Emitter,
) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method != "command" || params["name"] != "a2a" {
        return Ok(None);
    }

    let args = params["args"].as_array().cloned().unwrap_or_default();
    let request = A2aRequest::parse(&args)?;
    let result = execute(client, request, &mut emitter).await?;

    Ok(Some(Response::ok(id, result)))
}

struct A2aRequest {
    action: String,
    card: Option<String>,
    context: Option<String>,
    task: Option<String>,
    prompt: String,
    stream: Option<bool>,
    return_immediately: bool,
}

impl A2aRequest {
    fn parse(args: &[Value]) -> krabb_core::Result<Self> {
        let mut index = 0;
        let action = args
            .first()
            .and_then(Value::as_str)
            .filter(|value| !value.starts_with('-'))
            .unwrap_or("send")
            .to_owned();

        if matches!(action.as_str(), "send" | "discover" | "status" | "cancel")
            && args.first().and_then(Value::as_str) == Some(action.as_str())
        {
            index += 1;
        }

        let mut card = None;

        let mut context = None;
        let mut task = None;
        let mut stream = None;
        let mut return_immediately = false;
        let mut prompt = Vec::new();

        while index < args.len() {
            let value = args[index].as_str().unwrap_or_default();

            match value {
                "--card" => {
                    index += 1;
                    card = args.get(index).and_then(Value::as_str).map(str::to_owned);
                }

                "--context" => {
                    index += 1;
                    context = args.get(index).and_then(Value::as_str).map(str::to_owned);
                }

                "--task" => {
                    index += 1;
                    task = args.get(index).and_then(Value::as_str).map(str::to_owned);
                }

                "--stream" => stream = Some(true),
                "--no-stream" => stream = Some(false),
                "--return-immediately" => return_immediately = true,
                value if !value.starts_with('-') => prompt.push(value.to_owned()),

                value => {
                    return Err(krabb_core::Error::Denied(format!("Unknown A2A option: {value}.")));
                }
            }

            index += 1;
        }

        if !matches!(action.as_str(), "send" | "discover" | "status" | "cancel") {
            return Err(krabb_core::Error::Denied(format!("Unknown A2A action: {action}.")));
        }

        let prompt = prompt.join(" ").trim().to_owned();

        if action == "send" && prompt.is_empty() {
            return Err(krabb_core::Error::Denied("An A2A message is required.".into()));
        }

        if matches!(action.as_str(), "status" | "cancel")
            && task.as_deref().is_none_or(str::is_empty)
        {
            return Err(krabb_core::Error::Denied("An A2A task ID is required.".into()));
        }

        Ok(Self { action, card, context, task, prompt, stream, return_immediately })
    }
}

async fn execute(
    client: &reqwest::Client,
    request: A2aRequest,
    emitter: &mut Emitter,
) -> krabb_core::Result<Value> {
    let card_url =
        request.card.or_else(|| std::env::var("KRABB_A2A_AGENT_CARD").ok()).ok_or_else(|| {
            krabb_core::Error::Denied("KRABB_A2A_AGENT_CARD is not configured.".into())
        })?;

    let card_url = validate_url(&card_url)?;
    let card = discover(client, &card_url).await?;

    if request.action == "discover" {
        return Ok(card);
    }

    let endpoint = rpc_endpoint(&card)?;
    let token = std::env::var("KRABB_A2A_TOKEN").ok();

    match request.action.as_str() {
        "send" => {
            let streaming = request.stream.unwrap_or(card["capabilities"]["streaming"] == true);
            let mut message = json!({
                "messageId": unique_id(),
                "contextId": request.context,
                "role": "ROLE_USER",
                "parts": [{"text": request.prompt}]
            });

            if message["contextId"].is_null() {
                message.as_object_mut().unwrap().remove("contextId");
            }

            let params = json!({
                "message": message,
                "configuration": {
                    "acceptedOutputModes": ["text/plain"],
                    "returnImmediately": request.return_immediately
                }
            });

            let result = if streaming {
                stream_message(client, &endpoint, params, token.as_deref(), emitter).await?
            } else {
                rpc(client, &endpoint, "SendMessage", params, token.as_deref()).await?
            };

            Ok(task_summary(&result))
        }

        "status" => {
            let result =
                rpc(client, &endpoint, "GetTask", json!({"id": request.task}), token.as_deref())
                    .await?;

            Ok(task_summary(&result))
        }

        "cancel" => {
            let result =
                rpc(client, &endpoint, "CancelTask", json!({"id": request.task}), token.as_deref())
                    .await?;

            Ok(task_summary(&result))
        }

        _ => Err(krabb_core::Error::Denied("Unsupported A2A operation.".into())),
    }
}

async fn discover(client: &reqwest::Client, card_url: &Url) -> krabb_core::Result<Value> {
    let url = if card_url.path().ends_with("agent-card.json") {
        card_url.clone()
    } else {
        card_url
            .join("/.well-known/agent-card.json")
            .map_err(|_| krabb_core::Error::Denied("The A2A agent-card URL is invalid.".into()))?
    };

    let response =
        client.get(url).send().await.map_err(|error| {
            krabb_core::Error::Denied(format!("A2A discovery failed: {error}."))
        })?;

    read_json(response, "A2A agent card").await
}

fn rpc_endpoint(card: &Value) -> krabb_core::Result<Url> {
    let interfaces = card["supportedInterfaces"].as_array().ok_or_else(|| {
        krabb_core::Error::Protocol("A2A agent card has no supported interfaces.".into())
    })?;

    let endpoint = interfaces
        .iter()
        .find(|interface| {
            matches!(interface["protocolBinding"].as_str(), Some("JSONRPC" | "JSON-RPC"))
        })
        .and_then(|interface| interface["url"].as_str())
        .ok_or_else(|| {
            krabb_core::Error::Denied("A2A agent does not advertise a JSON-RPC interface.".into())
        })?;

    validate_url(endpoint)
}

fn validate_url(value: &str) -> krabb_core::Result<Url> {
    let url =
        Url::parse(value).map_err(|_| krabb_core::Error::Denied("A2A URL is invalid.".into()))?;

    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));

    if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        return Err(krabb_core::Error::Denied(
            "A2A URLs cannot contain credentials or fragments.".into(),
        ));
    }

    if url.scheme() != "https" && !loopback {
        return Err(krabb_core::Error::Denied(
            "A2A endpoints must use HTTPS outside loopback.".into(),
        ));
    }

    Ok(url)
}

async fn rpc(
    client: &reqwest::Client,
    endpoint: &Url,
    method: &str,
    params: Value,
    token: Option<&str>,
) -> krabb_core::Result<Value> {
    let mut request = client
        .post(endpoint.clone())
        .header("A2A-Version", "1.0")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}));

    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .map_err(|error| krabb_core::Error::Denied(format!("A2A request failed: {error}.")))?;

    let value = read_json(response, "A2A response").await?;

    if let Some(error) = value.get("error") {
        return Err(krabb_core::Error::Denied(format!(
            "A2A {method} failed: {}.",
            error["message"].as_str().unwrap_or("remote request failed")
        )));
    }

    value
        .get("result")
        .cloned()
        .ok_or_else(|| krabb_core::Error::Protocol("A2A JSON-RPC response had no result.".into()))
}

async fn stream_message(
    client: &reqwest::Client,
    endpoint: &Url,
    params: Value,
    token: Option<&str>,
    emitter: &mut Emitter,
) -> krabb_core::Result<Value> {
    let mut request = client
        .post(endpoint.clone())
        .header("A2A-Version", "1.0")
        .header("Accept", "text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":params}));

    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .map_err(|error| krabb_core::Error::Denied(format!("A2A stream failed: {error}.")))?;

    if !response.status().is_success() {
        return Err(krabb_core::Error::Denied(format!(
            "A2A streaming request was rejected with {}.",
            response.status()
        )));
    }

    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut bytes = 0_usize;
    let mut result = Value::Null;
    let mut pending = String::new();
    let mut emitted = 0_usize;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| krabb_core::Error::Denied(format!("A2A stream failed: {error}.")))?;

        bytes = bytes.saturating_add(chunk.len());

        if bytes > BODY_LIMIT {
            return Err(krabb_core::Error::Limit("A2A stream exceeded its response limit.".into()));
        }

        buffer.extend_from_slice(&chunk);

        while let Some((end, delimiter)) = event_boundary(&buffer) {
            let event = buffer.drain(..end + delimiter).collect::<Vec<_>>();
            let event = std::str::from_utf8(&event)
                .map_err(|_| krabb_core::Error::Protocol("A2A stream was not UTF-8.".into()))?;

            let data = event
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim)
                .collect::<Vec<_>>()
                .join("\n");

            if !data.is_empty() {
                let value: Value = serde_json::from_str(&data)?;

                if let Some(error) = value.get("error") {
                    return Err(krabb_core::Error::Denied(format!(
                        "A2A stream failed: {}.",
                        error["message"].as_str().unwrap_or("remote request failed")
                    )));
                }

                let event = value.get("result").unwrap_or(&value);
                accumulate_event(&mut result, event);
                emit_stream_event(event, emitter, &mut pending, &mut emitted).await?;
            }
        }
    }

    if result.is_null() {
        return Err(krabb_core::Error::Protocol("A2A stream returned no events.".into()));
    }

    if !pending.is_empty() {
        emitter.event(json!({"kind":"text","text":pending})).await?;
    }

    Ok(result)
}

fn accumulate_event(result: &mut Value, event: &Value) {
    let Some(update) = event.get("statusUpdate").or_else(|| event.get("artifactUpdate")) else {
        *result = event.get("task").or_else(|| event.get("message")).unwrap_or(event).clone();
        return;
    };

    if !result.is_object() {
        *result = json!({});
    }

    result["id"] = update["taskId"].clone();
    result["contextId"] = update["contextId"].clone();

    if event.get("statusUpdate").is_some() {
        result["status"] = update["status"].clone();
        return;
    }

    let artifact = &update["artifact"];

    if !result["artifacts"].is_array() {
        result["artifacts"] = json!([]);
    }

    let artifacts = result["artifacts"].as_array_mut().unwrap();

    if let Some(previous) = artifacts.iter_mut().find(|previous| {
        artifact["artifactId"].is_string() && previous["artifactId"] == artifact["artifactId"]
    }) {
        if update["append"] == true {
            if !previous["parts"].is_array() {
                previous["parts"] = json!([]);
            }

            previous["parts"]
                .as_array_mut()
                .unwrap()
                .extend(artifact["parts"].as_array().into_iter().flatten().cloned());
        } else {
            *previous = artifact.clone();
        }
    } else {
        artifacts.push(artifact.clone());
    }
}

async fn read_json(response: reqwest::Response, label: &str) -> krabb_core::Result<Value> {
    let status = response.status();
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| krabb_core::Error::Denied(format!("{label} read failed: {error}.")))?;

        if chunk.len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(krabb_core::Error::Limit(format!("{label} exceeded its response limit.")));
        }

        bytes.extend_from_slice(&chunk);
    }

    if !status.is_success() {
        return Err(krabb_core::Error::Denied(format!("{label} was rejected with {status}.")));
    }

    serde_json::from_slice(&bytes)
        .map_err(|error| krabb_core::Error::Protocol(format!("{label} was invalid JSON: {error}.")))
}

async fn emit_stream_event(
    event: &Value,
    emitter: &mut Emitter,
    pending: &mut String,
    emitted: &mut usize,
) -> krabb_core::Result<()> {
    let status = &event["statusUpdate"]["status"];

    let task = event.get("task").unwrap_or(event);

    if let Some(text) = task_text(task) {
        pending.push_str(&text);
    }

    if let Some(state) = status["state"].as_str() {
        pending.push_str(&format!("[A2A task: {state}]"));
    }

    if !pending.is_empty() && *emitted < 255 {
        emitter.event(json!({"kind":"text","text":std::mem::take(pending)})).await?;
        *emitted += 1;
    }

    Ok(())
}

fn task_text(value: &Value) -> Option<String> {
    let mut parts = Vec::new();

    value["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str().map(str::to_owned))
        .for_each(|text| parts.push(text));

    for artifact in value["artifacts"].as_array().into_iter().flatten() {
        if let Some(text) = artifact_text(artifact) {
            parts.push(text);
        }
    }

    value["status"]["message"]["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str().map(str::to_owned))
        .for_each(|text| parts.push(text));

    value["statusUpdate"]["status"]["message"]["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str().map(str::to_owned))
        .for_each(|text| parts.push(text));

    if let Some(text) = artifact_text(&value["artifactUpdate"]["artifact"]) {
        parts.push(text);
    }

    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn artifact_text(value: &Value) -> Option<String> {
    value["parts"]
        .as_array()?
        .iter()
        .filter_map(|part| part["text"].as_str())
        .map(str::to_owned)
        .reduce(|mut output, text| {
            output.push_str(&text);

            output
        })
}

fn task_summary(value: &Value) -> Value {
    if let Some(update) = value.get("statusUpdate") {
        return json!({
            "taskId": update["taskId"],
            "contextId": update["contextId"],
            "status": update["status"]["state"],
            "text": task_text(value).unwrap_or_default()
        });
    }

    let task = value.get("task").or_else(|| value.get("message")).unwrap_or(value);

    if task["id"].is_null() && task["status"].is_null() {
        return json!({
            "taskId": null,
            "contextId": task["contextId"],
            "status": "message",
            "text": task_text(task).unwrap_or_default()
        });
    }

    json!({
        "taskId": task["id"],
        "contextId": task["contextId"],
        "status": task["status"]["state"],
        "text": task_text(task).unwrap_or_default()
    })
}

fn unique_id() -> String {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());

    format!("krabb-{time}-{}", std::process::id())
}

fn event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");

    match (lf, crlf) {
        (Some(lf), Some(crlf)) if crlf <= lf => Some((crlf, 4)),
        (Some(lf), _) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        A2aRequest, artifact_text, execute, rpc_endpoint, task_summary, task_text, validate_url,
    };

    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn mock_server(
        responses: Vec<(String, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();

            for (content_type, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                requests.push(read_http_request(&mut stream).await);
                let body = body.replace("MOCK_ENDPOINT", &format!("http://{address}"));
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }

            requests
        });

        (format!("http://{address}"), handle)
    }

    async fn read_http_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2048];
        let header_end;

        loop {
            let count = stream.read(&mut buffer).await.unwrap();

            assert_ne!(count, 0);
            bytes.extend_from_slice(&buffer[..count]);

            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                header_end = end + 4;
                break;
            }
        }

        let header = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = header
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or_default();

        let remaining = header_end + content_length - bytes.len();

        if remaining > 0 {
            let start = bytes.len();
            bytes.resize(start + remaining, 0);
            stream.read_exact(&mut bytes[start..]).await.unwrap();
        }

        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn card(endpoint: &str, streaming: bool) -> String {
        json!({
            "name":"fixture",
            "capabilities":{"streaming":streaming},
            "supportedInterfaces":[{"protocolBinding":"JSONRPC","url":format!("{endpoint}/rpc")}]
        })
        .to_string()
    }

    fn task() -> String {
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "result":{"id":"task-1","contextId":"context-1","status":{"state":"TASK_STATE_COMPLETED","message":{"parts":[{"text":"answer"}]}},"artifacts":[]}
        })
        .to_string()
    }

    #[test]
    fn parses_send_status_cancel_and_discovery_actions() {
        let send = A2aRequest::parse(&[
            json!("send"),
            json!("--return-immediately"),
            json!("check"),
            json!("this"),
        ])
        .unwrap();

        assert_eq!(send.action, "send");
        assert_eq!(send.prompt, "check this");
        assert!(send.return_immediately);

        let status =
            A2aRequest::parse(&[json!("status"), json!("--task"), json!("task-1")]).unwrap();

        assert_eq!(status.task.as_deref(), Some("task-1"));
        assert!(A2aRequest::parse(&[json!("send")]).is_err());
        assert!(A2aRequest::parse(&[json!("cancel")]).is_err());
    }

    #[test]
    fn validates_supported_agent_url_and_json_rpc_binding() {
        assert!(validate_url("https://agent.example/a2a").is_ok());
        assert!(validate_url("http://127.0.0.1:9000/a2a").is_ok());
        assert!(validate_url("http://agent.example/a2a").is_err());
        assert!(validate_url("https://user:pass@agent.example/a2a").is_err());
        let card = json!({"supportedInterfaces":[
            {"protocolBinding":"HTTP+JSON","url":"https://agent.example/rest"},
            {"protocolBinding":"JSONRPC","url":"https://agent.example/rpc"}
        ]});

        assert_eq!(rpc_endpoint(&card).unwrap().as_str(), "https://agent.example/rpc");
    }

    #[test]
    fn extracts_task_text_and_status_without_assuming_optional_fields() {
        let task = json!({
            "id":"task-1",
            "contextId":"context-1",
            "status":{"state":"TASK_STATE_COMPLETED","message":{"parts":[{"text":"done"}]}},
            "artifacts":[{"parts":[{"text":"result"}]}]
        });

        assert_eq!(task_text(&task).as_deref(), Some("result\ndone"));
        assert_eq!(
            artifact_text(&json!({"parts":[{"text":"a"},{"text":"b"}]})).as_deref(),
            Some("ab")
        );

        let summary = task_summary(&task);

        assert_eq!(summary["taskId"], "task-1");
        assert_eq!(summary["status"], "TASK_STATE_COMPLETED");

        let message = json!({"contextId":"context-2","parts":[{"text":"instant"}]});
        let summary = task_summary(&message);

        assert!(summary["taskId"].is_null());
        assert_eq!(summary["contextId"], "context-2");
        assert_eq!(summary["status"], "message");
        assert_eq!(summary["text"], "instant");

        let update = json!({
            "statusUpdate": {
                "taskId":"task-2",
                "contextId":"context-3",
                "status":{"state":"TASK_STATE_WORKING","message":{"parts":[{"text":"working"}]}}
            }
        });

        let summary = task_summary(&update);

        assert_eq!(summary["taskId"], "task-2");
        assert_eq!(summary["contextId"], "context-3");
        assert_eq!(summary["status"], "TASK_STATE_WORKING");
        assert_eq!(summary["text"], "working");
    }

    #[tokio::test]
    async fn discovers_and_sends_to_a_json_rpc_agent() {
        let task = task();
        let (endpoint, server) = mock_server(vec![
            ("application/json".into(), card("MOCK_ENDPOINT", false)),
            ("application/json".into(), card("MOCK_ENDPOINT", false)),
            ("application/json".into(), task),
        ])
        .await;

        let client = reqwest::Client::new();
        let (output, _events) = tokio::sync::mpsc::channel(8);
        let mut emitter = krabb_core::plugin::Emitter::new(output);
        let discovery =
            A2aRequest::parse(&[json!("discover"), json!("--card"), json!(endpoint)]).unwrap();

        let found = execute(&client, discovery, &mut emitter).await.unwrap();

        assert_eq!(found["name"], "fixture");

        let send = A2aRequest::parse(&[
            json!("send"),
            json!("--card"),
            json!(endpoint),
            json!("--no-stream"),
            json!("--return-immediately"),
            json!("hello"),
        ])
        .unwrap();

        let result = execute(&client, send, &mut emitter).await.unwrap();

        assert_eq!(result["taskId"], "task-1");
        assert_eq!(result["text"], "answer");

        let requests = server.await.unwrap();

        assert!(requests[0].starts_with("GET /.well-known/agent-card.json"));
        assert!(requests[2].contains("\"method\":\"SendMessage\""));
        assert!(requests[2].contains("\"returnImmediately\":true"));
        assert!(requests[2].to_ascii_lowercase().contains("a2a-version: 1.0"));
    }

    #[tokio::test]
    async fn coalesces_external_updates_without_losing_artifact_text() {
        let (output, mut events) = tokio::sync::mpsc::channel(256);
        let mut emitter = krabb_core::plugin::Emitter::new(output);
        let mut pending = String::new();
        let mut emitted = 0;

        for _ in 0..300 {
            super::emit_stream_event(
                &json!({"artifactUpdate":{"artifact":{"parts":[{"text":"piece"}]}}}),
                &mut emitter,
                &mut pending,
                &mut emitted,
            )
            .await
            .unwrap();
        }

        emitter.event(json!({"kind":"text","text":pending})).await.unwrap();
        let mut text = String::new();
        let mut count = 0;

        while let Ok(event) = events.try_recv() {
            text.push_str(event["params"]["event"]["text"].as_str().unwrap());
            count += 1;
        }

        assert_eq!(count, 256);
        assert_eq!(text, "piece".repeat(300));
    }

    #[tokio::test]
    async fn streams_agent_task_updates_as_normalized_text_events() {
        let event = format!("data: {}\n\n", task());
        let (endpoint, server) = mock_server(vec![
            ("application/json".into(), card("MOCK_ENDPOINT", true)),
            ("text/event-stream".into(), event),
        ])
        .await;

        let client = reqwest::Client::new();
        let (output, mut events) = tokio::sync::mpsc::channel(8);
        let mut emitter = krabb_core::plugin::Emitter::new(output);
        let request =
            A2aRequest::parse(&[json!("send"), json!("--card"), json!(endpoint), json!("hello")])
                .unwrap();

        let result = execute(&client, request, &mut emitter).await.unwrap();

        assert_eq!(result["text"], "answer");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "answer");
        assert!(server.await.unwrap()[1].contains("SendStreamingMessage"));
    }

    #[tokio::test]
    async fn retains_streamed_artifacts_in_the_final_summary() {
        let updates = [
            json!({"artifactUpdate":{
                "taskId":"task-1", "contextId":"context-1",
                "artifact":{"artifactId":"answer", "parts":[{"text":"answer"}]}
            }}),
            json!({"statusUpdate":{
                "taskId":"task-1", "contextId":"context-1",
                "status":{"state":"TASK_STATE_COMPLETED"}
            }}),
        ];

        let body = updates
            .into_iter()
            .map(|result| format!("data: {}\n\n", json!({"jsonrpc":"2.0","id":1,"result":result})))
            .collect::<String>();

        let (endpoint, server) = mock_server(vec![
            ("application/json".into(), card("MOCK_ENDPOINT", true)),
            ("text/event-stream".into(), body),
        ])
        .await;

        let client = reqwest::Client::new();
        let (output, mut events) = tokio::sync::mpsc::channel(8);
        let mut emitter = krabb_core::plugin::Emitter::new(output);
        let request =
            A2aRequest::parse(&[json!("send"), json!("--card"), json!(endpoint), json!("hello")])
                .unwrap();

        let result = execute(&client, request, &mut emitter).await.unwrap();

        assert_eq!(
            result,
            json!({
                "taskId":"task-1", "contextId":"context-1",
                "status":"TASK_STATE_COMPLETED", "text":"answer"
            })
        );

        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "answer");
        assert_eq!(
            events.recv().await.unwrap()["params"]["event"]["text"],
            "[A2A task: TASK_STATE_COMPLETED]"
        );

        assert!(events.try_recv().is_err());
        assert!(server.await.unwrap()[1].contains("SendStreamingMessage"));
    }

    #[test]
    fn accumulates_task_status_and_artifact_replacements_and_appends() {
        let mut result = serde_json::Value::Null;
        super::accumulate_event(
            &mut result,
            &json!({"task":{
                "id":"task-1", "contextId":"context-1",
                "status":{"state":"TASK_STATE_WORKING"},
                "artifacts":[{"artifactId":"answer", "parts":[{"text":"draft"}]}]
            }}),
        );

        for (id, text, append) in [
            ("answer", " discarded", true),
            ("other", "second", true),
            ("answer", "final", false),
            ("answer", " answer", true),
        ] {
            super::accumulate_event(
                &mut result,
                &json!({"artifactUpdate":{
                    "taskId":"task-1", "contextId":"context-1", "append":append,
                    "artifact":{"artifactId":id, "parts":[{"text":text}]}
                }}),
            );
        }

        super::accumulate_event(
            &mut result,
            &json!({"statusUpdate":{
                "taskId":"task-1", "contextId":"context-1",
                "status":{"state":"TASK_STATE_COMPLETED"}
            }}),
        );

        assert_eq!(
            task_summary(&result),
            json!({
                "taskId":"task-1", "contextId":"context-1",
                "status":"TASK_STATE_COMPLETED", "text":"final answer\nsecond"
            })
        );

        super::accumulate_event(
            &mut result,
            &json!({"task":{
                "id":"task-1", "contextId":"context-1",
                "status":{"state":"TASK_STATE_COMPLETED"},
                "artifacts":[{"artifactId":"answer", "parts":[{"text":"snapshot"}]}]
            }}),
        );

        assert_eq!(task_summary(&result)["text"], "snapshot");

        super::accumulate_event(
            &mut result,
            &json!({"message":{
                "contextId":"context-2", "parts":[{"text":"instant"}]
            }}),
        );

        assert_eq!(task_summary(&result)["text"], "instant");
        assert_eq!(task_summary(&result)["status"], "message");
    }

    #[tokio::test]
    async fn reads_and_cancels_remote_tasks() {
        let task = task();
        let (endpoint, server) = mock_server(vec![
            ("application/json".into(), card("MOCK_ENDPOINT", false)),
            ("application/json".into(), task.clone()),
            ("application/json".into(), card("MOCK_ENDPOINT", false)),
            ("application/json".into(), task),
        ])
        .await;

        let client = reqwest::Client::new();
        let (output, _events) = tokio::sync::mpsc::channel(8);
        let mut emitter = krabb_core::plugin::Emitter::new(output);

        for action in ["status", "cancel"] {
            let request = A2aRequest::parse(&[
                json!(action),
                json!("--card"),
                json!(endpoint),
                json!("--task"),
                json!("task-1"),
            ])
            .unwrap();

            let result = execute(&client, request, &mut emitter).await.unwrap();

            assert_eq!(result["taskId"], "task-1");
        }

        let requests = server.await.unwrap();

        assert!(requests[1].contains("GetTask"));
        assert!(requests[3].contains("CancelTask"));
    }
}
