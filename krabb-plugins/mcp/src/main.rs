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
    io::{AsyncWriteExt, BufReader},
    process::Command,
    time::timeout,
};

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
        .spawn()
        .map_err(|error| denied(format!("MCP process failed to start: {error}")))?;

    let result = timeout(DEADLINE, async {
        let mut input = child.stdin.take().ok_or_else(|| denied("MCP process has no stdin"))?;
        let output = child.stdout.take().ok_or_else(|| denied("MCP process has no stdout"))?;
        jsonl::write(&mut input, &payload).await?;
        input.shutdown().await.map_err(|error| denied(format!("MCP input failed: {error}")))?;

        let mut output = BufReader::new(output);
        jsonl::read::<Value>(&mut output, FRAME)
            .await?
            .ok_or_else(|| denied("MCP process returned no response"))
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

async fn http(client: &Client, params: &Value) -> krabb_core::Result<Value> {
    let url = params["url"]
        .as_str()
        .filter(|url| safe_url(url))
        .ok_or_else(|| denied("http.url must use HTTPS or loopback HTTP"))?;

    let payload =
        params.get("request").cloned().ok_or_else(|| denied("http.request is required"))?;

    let mut request = client.post(url).json(&payload);

    if let Some(token) = params["token"].as_str().filter(|token| !token.trim().is_empty()) {
        request = request.bearer_auth(token);
    }

    let response = timeout(DEADLINE, request.send())
        .await
        .map_err(|_| denied("MCP HTTP request timed out"))?
        .map_err(|error| denied(format!("MCP HTTP request failed: {error}")))?;

    let status = response.status();
    let body = read(response).await?;
    let value = serde_json::from_str::<Value>(&body)
        .map_err(|error| denied(format!("MCP HTTP response failed: {error}")))?;

    response_body(status, value)
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

fn response_body(status: reqwest::StatusCode, value: Value) -> krabb_core::Result<Value> {
    if !status.is_success() {
        return Err(denied(format!("MCP HTTP request was rejected with {status}")));
    }

    Ok(value)
}

fn denied(message: impl Into<String>) -> krabb_core::Error {
    let message = message.into();
    krabb_core::Error::Denied(if message.ends_with('.') { message } else { format!("{message}.") })
}

#[cfg(test)]
mod tests {
    use super::{BODY_LIMIT, call, collect, denied, hello, http, response_body, safe_url, stdio};
    use krabb_core::types::{Capability, Request};
    use reqwest::Client;
    use serde_json::json;

    #[test]
    fn describes_mcp_capability() {
        let hello = hello();

        assert_eq!(hello.id, "mcp");
        assert_eq!(hello.capabilities, vec![Capability::Mcp, Capability::Resource]);
        assert!(denied("failed").to_string().ends_with('.'));
        assert_eq!(
            response_body(reqwest::StatusCode::OK, json!({"ok": true})).unwrap()["ok"],
            true
        );

        assert!(response_body(reqwest::StatusCode::BAD_REQUEST, json!({})).is_err());
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
    async fn executes_stdio_requests() {
        let value = stdio(&json!({
            "approved": true,
            "command": "sh",
            "args": ["-c", "read line; printf '%s\\n' \"$line\""],
            "request": {"jsonrpc": "2.0", "id": 1, "method": "initialize"}
        }))
        .await
        .unwrap();

        assert_eq!(value["method"], "initialize");
        let value = stdio(&json!({
            "approved": true,
            "command": "cat",
            "request": {"jsonrpc": "2.0", "id": 2, "method": "ping"}
        }))
        .await
        .unwrap();

        assert_eq!(value["method"], "ping");
        assert!(
            stdio(&json!({
                "approved": true,
                "command": "sh",
                "args": ["-c", "exit 0"],
                "request": {"jsonrpc": "2.0", "id": 3, "method": "empty"}
            }))
            .await
            .is_err()
        );
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];

            loop {
                let count = stream.read(&mut buffer).await.unwrap();

                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);

                let Some(headers_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };

                let headers_end = headers_end + 4;
                let headers = String::from_utf8_lossy(&request[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length: ")
                            .or_else(|| line.strip_prefix("Content-Length: "))
                    })
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap();

                if request.len() >= headers_end + content_length {
                    let body = &request[headers_end..headers_end + content_length];
                    let request: serde_json::Value = serde_json::from_slice(body).unwrap();

                    assert!(headers.starts_with("POST /mcp HTTP/1.1\r\n"));
                    assert!(
                        headers.lines().any(|line| line == "authorization: Bearer fixture-token")
                    );

                    assert_eq!(request["method"], "tools/list");

                    let response = serde_json::to_vec(&json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {"tools": [{"name": "fixture_tool"}]}
                    }))
                    .unwrap();
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    );

                    stream.write_all(headers.as_bytes()).await.unwrap();
                    stream.write_all(&response).await.unwrap();
                    return;
                }
            }
        });

        let response = call(
            &Client::new(),
            Request::call(
                1,
                "http",
                json!({
                    "url": format!("http://{address}/mcp"),
                    "token": "fixture-token",
                    "request": {"jsonrpc": "2.0", "id": 7, "method": "tools/list"}
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.result.unwrap()["result"]["tools"][0]["name"], "fixture_tool");
        server.await.unwrap();
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
