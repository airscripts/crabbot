#![forbid(unsafe_code)]

use std::{process::Stdio, time::Duration};

use krabb_core::{
    jsonl,
    plugin::serve_with,
    types::{Capability, Hello, Protocol, Request, Response},
};

use futures_util::{Stream, StreamExt};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufReader},
    process::Command,
    time::timeout,
};

const VERSION: &str = "2025-06-18";
const FRAME: usize = 1024 * 1024;
const BODY_LIMIT: usize = krabb_core::jsonl::MAX / 2;
const DEADLINE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("mcp", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| krabb_core::Error::Denied(format!("MCP client failed: {error}.")))?;

    serve_with(hello(), move |request| {
        let client = client.clone();
        async move { call(&client, request).await }
    })
    .await
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "mcp".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Mcp, Capability::Resource],
        commands: vec![],
    }
}

async fn call(client: &Client, request: Request) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let result = match method.as_str() {
        "stdio" => stdio(&params).await?,
        "http" => http(client, &params).await?,
        "describe" => json!({
            "transport": ["stdio", "http"],
            "methods": ["tools/list", "tools/call", "resources/list", "prompts/list"]
        }),

        _ => return Ok(None),
    };

    let response = Response::ok(id, result);

    if serde_json::to_vec(&response)?.len().saturating_add(1) > krabb_core::jsonl::MAX {
        return Err(denied("MCP response exceeds the protocol frame limit"));
    }

    Ok(Some(response))
}

async fn stdio(params: &Value) -> krabb_core::Result<Value> {
    if params["approved"] != true {
        return Err(denied("stdio.approved must be true"));
    }

    let command = params["command"]
        .as_str()
        .filter(|command| !command.trim().is_empty())
        .ok_or_else(|| denied("stdio.command is required"))?;

    let args = match params["args"].as_array() {
        Some(args) => args
            .iter()
            .map(|arg| arg.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| denied("stdio.args must contain strings"))?,
        None => Vec::new(),
    };

    let payload =
        params.get("request").cloned().ok_or_else(|| denied("stdio.request is required"))?;

    let mut child = Command::new(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| denied(format!("MCP process failed to start: {error}")))?;

    let result = timeout(DEADLINE, async {
        let mut input = child.stdin.take().ok_or_else(|| denied("MCP process has no stdin"))?;
        let output = child.stdout.take().ok_or_else(|| denied("MCP process has no stdout"))?;
        let mut output = BufReader::new(output);
        let initialization = if payload["method"] == "initialize" {
            payload.clone()
        } else {
            json!({
                "jsonrpc": "2.0",
                "id": "krabb-initialize",
                "method": "initialize",
                "params": {
                    "protocolVersion": VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "krabb", "version": env!("CARGO_PKG_VERSION")}
                }
            })
        };

        jsonl::write(&mut input, &initialization).await?;
        let initialized = receive(&mut input, &mut output, &initialization["id"]).await?;

        if initialized.get("error").is_some() || initialized["result"]["protocolVersion"] != VERSION
        {
            return Err(denied("MCP initialization failed or returned an unsupported version"));
        }

        jsonl::write(
            &mut input,
            &json!({
                "jsonrpc": "2.0", "method": "notifications/initialized"
            }),
        )
        .await?;

        let response = if payload["method"] == "initialize" {
            initialized
        } else {
            jsonl::write(&mut input, &payload).await?;

            if let Some(id) = payload.get("id") {
                receive(&mut input, &mut output, id).await?
            } else {
                Value::Null
            }
        };

        input.shutdown().await.map_err(|error| denied(format!("MCP input failed: {error}")))?;
        Ok(response)
    })
    .await
    .map_err(|_| denied("MCP process timed out"))
    .and_then(|result| result);

    match result {
        Ok(response) => {
            if timeout(Duration::from_secs(2), child.wait()).await.is_err() {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }

            Ok(response)
        }

        Err(error) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(error)
        }
    }
}

async fn receive<W, R>(input: &mut W, output: &mut R, id: &Value) -> krabb_core::Result<Value>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    for _ in 0..128 {
        let message = jsonl::read::<Value>(output, FRAME)
            .await?
            .ok_or_else(|| denied("MCP process returned no matching response"))?;

        if matches_response(&message, id) {
            return Ok(message);
        }

        if let Some(id) = message.get("id")
            && message.get("method").is_some()
        {
            let response = if message["method"] == "ping" {
                json!({"jsonrpc": "2.0", "id": id, "result": {}})
            } else {
                json!({"jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": "Unsupported client method"}})
            };

            jsonl::write(input, &response).await?;
        }
    }

    Err(denied("MCP process sent too many messages without a matching response"))
}

fn matches_response(message: &Value, id: &Value) -> bool {
    message["jsonrpc"] == "2.0"
        && message.get("id") == Some(id)
        && message.get("method").is_none()
        && (message.get("result").is_some() || message.get("error").is_some())
}

async fn http(client: &Client, params: &Value) -> krabb_core::Result<Value> {
    timeout(DEADLINE, http_request(client, params))
        .await
        .map_err(|_| denied("MCP HTTP request timed out"))?
}

async fn http_request(client: &Client, params: &Value) -> krabb_core::Result<Value> {
    let url = params["url"]
        .as_str()
        .filter(|url| safe_url(url))
        .ok_or_else(|| denied("http.url must use HTTPS or loopback HTTP"))?;

    let payload =
        params.get("request").cloned().ok_or_else(|| denied("http.request is required"))?;

    let mut request = client
        .post(url)
        .json(&payload)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", params["protocol_version"].as_str().unwrap_or(VERSION));

    if let Some(token) = params["token"].as_str().filter(|token| !token.trim().is_empty()) {
        request = request.bearer_auth(token);
    }

    if let Some(session) = params["session_id"].as_str() {
        request = request.header("Mcp-Session-Id", session);
    }

    let response = request
        .send()
        .await
        .map_err(|error| denied(format!("MCP HTTP request failed: {error}")))?;
    let status = response.status();

    if !status.is_success() {
        return Err(denied(format!("MCP HTTP request was rejected with {status}")));
    }

    let session = response
        .headers()
        .get("Mcp-Session-Id")
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| denied("MCP HTTP session identifier was invalid"))?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let request_id = payload.get("id").filter(|_| payload.get("method").is_some());

    if status == reqwest::StatusCode::ACCEPTED && request_id.is_none() {
        if !read(response).await?.is_empty() {
            return Err(denied("MCP HTTP acknowledgement must be empty"));
        }

        return Ok(Value::Null);
    }

    let id = request_id.ok_or_else(|| denied("MCP HTTP notification was not acknowledged"))?;
    let mut value = match content_type.as_str() {
        "application/json" => {
            let body = read(response).await?;
            let value = serde_json::from_str::<Value>(&body)
                .map_err(|error| denied(format!("MCP HTTP response failed: {error}")))?;

            if !matches_response(&value, id) {
                return Err(denied("MCP HTTP response did not match the request"));
            }

            value
        }

        "text/event-stream" => events(response.bytes_stream(), id).await?,
        _ => return Err(denied("MCP HTTP response has an unsupported content type")),
    };

    if let Some(session) = session {
        value["session_id"] = Value::String(session);
    }

    Ok(value)
}

async fn events<S, C, E>(mut stream: S, id: &Value) -> krabb_core::Result<Value>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut total = 0;
    let mut line = Vec::new();
    let mut data = String::new();
    let mut carriage = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| denied(format!("MCP HTTP response failed: {error}")))?;

        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(total) {
            return Err(denied("MCP HTTP response was too large"));
        }

        total += chunk.as_ref().len();

        for &byte in chunk.as_ref() {
            if byte == b'\n' && carriage {
                carriage = false;
                continue;
            }

            carriage = byte == b'\r';

            if !matches!(byte, b'\r' | b'\n') {
                line.push(byte);
                continue;
            }

            let text = std::str::from_utf8(&line)
                .map_err(|_| denied("MCP HTTP response was not UTF-8"))?;

            if text.is_empty() && !data.is_empty() {
                let value: Value = serde_json::from_str(&data)
                    .map_err(|error| denied(format!("MCP SSE response failed: {error}")))?;
                data.clear();

                if matches_response(&value, id) {
                    return Ok(value);
                }
            } else if let Some(value) = text.strip_prefix("data:") {
                data.push_str(value.strip_prefix(' ').unwrap_or(value));
                data.push('\n');
            }

            line.clear();
        }
    }

    Err(denied("MCP SSE stream ended without a matching response"))
}

async fn read(response: reqwest::Response) -> krabb_core::Result<String> {
    collect(response.bytes_stream()).await
}

async fn collect<S, C, E>(mut stream: S) -> krabb_core::Result<String>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| denied(format!("MCP HTTP response failed: {error}")))?;

        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(denied("MCP HTTP response was too large"));
        }

        bytes.extend_from_slice(chunk.as_ref());
    }

    String::from_utf8(bytes).map_err(|_| denied("MCP HTTP response was not UTF-8"))
}

fn safe_url(value: &str) -> bool {
    if value.starts_with("https://") {
        return value
            .strip_prefix("https://")
            .and_then(|value| value.split(['/', '?', '#']).next())
            .is_some_and(|authority| !authority.is_empty() && !authority.contains('@'));
    }

    let Some(authority) =
        value.strip_prefix("http://").and_then(|value| value.split(['/', '?', '#']).next())
    else {
        return false;
    };

    let host = if authority.starts_with('[') {
        let Some(end) = authority.find(']') else { return false };

        if authority[end + 1..].is_empty()
            || authority[end + 1..].strip_prefix(':').is_some_and(|port| {
                !port.is_empty() && port.chars().all(|value| value.is_ascii_digit())
            })
        {
            &authority[..=end]
        } else {
            ""
        }
    } else {
        authority.rsplit_once(':').map_or(authority, |(host, port)| {
            if port.chars().all(|value| value.is_ascii_digit()) { host } else { "" }
        })
    };

    matches!(host, "127.0.0.1" | "localhost" | "[::1]")
}

fn denied(message: impl Into<String>) -> krabb_core::Error {
    let message = message.into();
    krabb_core::Error::Denied(if message.ends_with('.') { message } else { format!("{message}.") })
}

#[cfg(test)]
mod tests {
    use super::{BODY_LIMIT, call, collect, denied, hello, http, safe_url, stdio};
    use krabb_core::types::{Capability, Request};
    use reqwest::Client;
    use serde_json::json;

    #[test]
    fn describes_mcp_capability() {
        let hello = hello();

        assert_eq!(hello.id, "mcp");
        assert_eq!(hello.capabilities, vec![Capability::Mcp, Capability::Resource]);
        assert!(denied("failed").to_string().ends_with('.'));
        assert!(safe_url("https://example.test/mcp"));
        assert!(!safe_url("https://user:password@example.test/mcp"));
        assert!(safe_url("http://127.0.0.1:8080/mcp"));
        assert!(safe_url("http://localhost/mcp"));
        assert!(safe_url("http://[::1]:8080/mcp"));
        assert!(safe_url("http://[::1]/mcp"));
        assert!(!safe_url("http://127.0.0.1.evil.test/mcp"));
        assert!(!safe_url("http://127.0.0.1x/mcp"));
    }

    #[tokio::test]
    async fn validates_calls_and_stdio() {
        let client = Client::new();

        assert!(call(&client, Request::call(1, "stdio", json!({}))).await.is_err());
        assert!(
            call(&client, Request::call(2, "stdio", json!({"command":"sh","args":[1]})))
                .await
                .is_err()
        );

        assert!(call(&client, Request::call(3, "stdio", json!({"command":"sh"}))).await.is_err());
        assert!(
            call(
                &client,
                Request::call(4, "stdio", json!({"command":"definitely-missing","request":{}}))
            )
            .await
            .is_err()
        );

        assert!(call(&client, Request::call(5, "unknown", json!({}))).await.unwrap().is_none());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "describe".into(), params: json!({}) };

        assert!(call(&client, note).await.unwrap().is_none());
        let description =
            call(&client, Request::call(6, "describe", json!({}))).await.unwrap().unwrap();

        assert_eq!(description.result.unwrap()["transport"][0], "stdio");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executes_stdio_requests_after_initialization() {
        let script = r#"
read line
case "$line" in *'"method":"initialize"'*) ;; *) exit 1;; esac
printf '%s\n' '{"jsonrpc":"2.0","id":"krabb-initialize","result":{"protocolVersion":"2025-06-18"}}'
read line
case "$line" in *'"method":"notifications/initialized"'*) ;; *) exit 2;; esac
read line
case "$line" in *'"method":"tools/list"'*) ;; *) exit 3;; esac
printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/message"}'
printf '%s\n' '{"jsonrpc":"2.0","id":99,"result":{}}'
printf '%s\n' '{"jsonrpc":"2.0","id":4,"method":"ping"}'
read line
case "$line" in *'"result":{}'*) ;; *) exit 4;; esac
printf '%s\n' '{"jsonrpc":"2.0","id":5,"method":"sampling/createMessage"}'
read line
case "$line" in *'"code":-32601'*) ;; *) exit 5;; esac
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}'
read line && exit 6
exit 0
"#;

        let value = stdio(&json!({
            "approved": true,
            "command": "sh", "args": ["-c", script],
            "request": {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
        }))
        .await
        .unwrap();

        assert_eq!(value["result"]["tools"], json!([]));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn handles_stdio_initialization_and_failures() {
        let initialize = r#"read line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18"}}'
read line
case "$line" in *'"method":"notifications/initialized"'*) ;; *) exit 1;; esac
"#;

        let response = stdio(&json!({
            "approved": true, "command": "sh", "args": ["-c", initialize],
            "request": {"jsonrpc":"2.0", "id":1, "method":"initialize"}
        }))
        .await
        .unwrap();

        assert_eq!(response["result"]["protocolVersion"], super::VERSION);

        for script in [
            "exit 0",
            "read line; printf '%s\\n' 'malformed'",
            r#"read line; printf '%s\n' '{"jsonrpc":"2.0","id":1,"error":{"code":-1}}'"#,
            r#"read line; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"unknown"}}'"#,
        ] {
            assert!(
                stdio(&json!({
                    "approved": true, "command": "sh", "args": ["-c", script],
                    "request": {"jsonrpc":"2.0", "id":1, "method":"initialize"}
                }))
                .await
                .is_err()
            );
        }

        for params in [
            json!({"approved": true}),
            json!({"approved": true, "command":"sh", "args":[1]}),
            json!({"approved": true, "command":"sh"}),
            json!({"approved": true, "command":"definitely-missing", "request":{}}),
        ] {
            assert!(stdio(&params).await.is_err());
        }
    }

    #[tokio::test]
    async fn validates_http_requests() {
        let client = Client::new();

        assert!(http(&client, &json!({})).await.is_err());
        assert!(http(&client, &json!({"url":"file:///tmp/mcp","request":{}})).await.is_err());
        assert!(
            http(&client, &json!({"url":"http://127.0.0.1.evil.test","request":{}})).await.is_err()
        );

        assert!(http(&client, &json!({"url":"http://127.0.0.1:1","request":{}})).await.is_err());
    }

    #[tokio::test]
    async fn forwards_http_requests_to_a_loopback_mcp_server() {
        let body = json!({"jsonrpc":"2.0", "id":7,
            "result":{"tools":[{"name":"fixture_tool"}]}})
        .to_string();

        let (url, server) = http_fixture(vec![http_response(
            "200 OK",
            "Content-Type: application/json\r\n",
            &body,
        )])
        .await;

        let response = call(
            &Client::new(),
            Request::call(
                1,
                "http",
                json!({
                    "url":url, "request":{"jsonrpc":"2.0", "id":7, "method":"tools/list"}
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.result.unwrap()["result"]["tools"][0]["name"], "fixture_tool");
        let requests = server.await.unwrap();

        assert_eq!(requests[0].1["method"], "tools/list");
    }

    async fn http_fixture(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<(String, serde_json::Value)>>) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            for response in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut headers = String::new();

                loop {
                    let mut line = String::new();

                    assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
                    headers.push_str(&line);

                    if line == "\r\n" {
                        break;
                    }
                }

                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                requests.push((headers, serde_json::from_slice(&body).unwrap()));
                stream.get_mut().write_all(response.as_bytes()).await.unwrap();
            }

            requests
        });

        (url, server)
    }

    fn http_response(status: &str, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[tokio::test]
    async fn forwards_http_session_from_initialization_through_operations() {
        let initialized = json!({"jsonrpc":"2.0", "id":1,
            "result":{"protocolVersion":"2025-06-18"}})
        .to_string();

        let (url, server) = http_fixture(vec![
            http_response(
                "200 OK",
                "Content-Type: application/json\r\nMcp-Session-Id: fixture-session\r\n",
                &initialized,
            ),
            http_response("202 Accepted", "", ""),
            http_response(
                "200 OK",
                "Content-Type: text/event-stream; charset=utf-8\r\n",
                concat!(
                    ": keepalive\r\n\r\n",
                    "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}\r\n\r\n",
                    "data: {\"jsonrpc\":\"2.0\",\"id\":2,\r\n",
                    "data: \"result\":{\"tools\":[]}}\r\n\r\n"
                ),
            ),
        ])
        .await;

        let client = Client::new();
        let mut params = json!({"url":url, "token":"fixture-token",
            "request":{"jsonrpc":"2.0", "id":1, "method":"initialize"}});

        let initialized = http(&client, &params).await.unwrap();

        assert_eq!(initialized["session_id"], "fixture-session");
        params["session_id"] = initialized["session_id"].clone();
        params["protocol_version"] = initialized["result"]["protocolVersion"].clone();
        params["request"] = json!({"jsonrpc":"2.0", "method":"notifications/initialized"});

        assert!(http(&client, &params).await.unwrap().is_null());
        params["request"] = json!({"jsonrpc":"2.0", "id":2, "method":"tools/list"});
        let response = http(&client, &params).await.unwrap();

        assert_eq!(response["result"]["tools"], json!([]));
        let requests = server.await.unwrap();

        for (index, (headers, _)) in requests.iter().enumerate() {
            assert!(headers.contains("accept: application/json, text/event-stream"));
            assert!(headers.contains("authorization: Bearer fixture-token"));
            assert!(headers.contains("mcp-protocol-version: 2025-06-18"));
            assert_eq!(headers.contains("mcp-session-id: fixture-session"), index > 0);
        }

        assert_eq!(requests[0].1["method"], "initialize");
        assert_eq!(requests[1].1["method"], "notifications/initialized");
        assert_eq!(requests[2].1["method"], "tools/list");
    }

    #[tokio::test]
    async fn rejects_http_errors_and_invalid_response_types() {
        let responses = vec![
            http_response("400 Bad Request", "", "rejected"),
            http_response("200 OK", "Content-Type: text/plain\r\n", "{}"),
            http_response("200 OK", "Content-Type: application/json\r\n", "invalid"),
            http_response(
                "200 OK",
                "Content-Type: application/json\r\n",
                r#"{"jsonrpc":"2.0","id":99,"result":{}}"#,
            ),
            http_response("202 Accepted", "", ""),
            http_response("202 Accepted", "", "not empty"),
            http_response("200 OK", "Content-Type: application/json\r\n", "{}"),
        ];

        let count = responses.len();
        let (url, server) = http_fixture(responses).await;
        let client = Client::new();

        for index in 0..count {
            let request = if index < 5 {
                json!({"jsonrpc":"2.0", "id":1, "method":"tools/list"})
            } else {
                json!({"jsonrpc":"2.0", "method":"notifications/initialized"})
            };

            assert!(http(&client, &json!({"url":url, "request":request})).await.is_err());
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn reads_bounded_sse_until_the_matching_response() {
        use super::events;
        use futures_util::{StreamExt, stream};

        let text = concat!(
            "event: message\r\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":99,\"result\":{}}\r\n\r\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":\"é\"}\r\n\r\n"
        );

        let bytes = text.as_bytes().iter().map(|byte| Ok::<_, std::io::Error>(vec![*byte]));
        let stream = stream::iter(bytes).chain(stream::pending());
        let value =
            tokio::time::timeout(std::time::Duration::from_secs(1), events(stream, &json!(7)))
                .await
                .unwrap()
                .unwrap();

        assert_eq!(value["result"], "é");

        for bytes in [
            b"data: nope\n\n".to_vec(),
            b"data: \xff\n\n".to_vec(),
            b": no response\r\r".to_vec(),
            vec![b'x'; BODY_LIMIT + 1],
        ] {
            let stream = stream::iter(vec![Ok::<_, std::io::Error>(bytes)]);

            assert!(events(stream, &json!(7)).await.is_err());
        }

        let error = std::io::Error::other("fixture failure");

        let stream = stream::iter(vec![Err::<Vec<u8>, _>(error)]);

        assert!(events(stream, &json!(7)).await.is_err());
    }

    #[tokio::test]
    async fn bounds_http_bodies() {
        use futures_util::stream;

        assert_eq!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(b"{}".to_vec())])).await.unwrap(),
            "{}"
        );

        assert!(collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])])).await.is_err());
        assert!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]))
                .await
                .is_err()
        );
    }
}
