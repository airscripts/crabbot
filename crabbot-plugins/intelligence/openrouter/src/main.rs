#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::{Emitter, serve_events},
    types::{
        Capability, Content, Event, Hello, ModelReply, ModelRequest, Protocol, Request, Response,
        Role,
    },
};

use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::time::Duration;

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const TOOL_LIMIT: usize = 16;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenRouter client failed: {error}."))
        })?;

    serve_events(
        Hello {
            protocol: Protocol { major: 0, minor: 1 },
            id: "openrouter".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Model, Capability::Vision],
            commands: vec![],
        },
        move |request, emitter| {
            let client = client.clone();
            async move { generate(&client, request, emitter).await }
        },
    )
    .await
}

async fn generate(
    client: &reqwest::Client,
    request: Request,
    mut emitter: Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method != "generate" {
        return Ok(None);
    }

    let input: ModelRequest = serde_json::from_value(params)?;
    let key = credential()?;
    let model = if input.model.trim().is_empty() || input.model == "default" {
        std::env::var("CRABBOT_OPENROUTER_MODEL").map_err(|_| {
            crabbot_core::Error::Denied("CRABBOT_OPENROUTER_MODEL is not configured.".into())
        })?
    } else {
        input.model.clone()
    };

    let base = base_url()?;
    let request = ModelRequest { model, ..input };

    if request.stream {
        stream(client, id, request, &key, &base, &mut emitter).await
    } else {
        complete(client, id, request, &key, &base).await
    }
}

fn base_url() -> crabbot_core::Result<String> {
    let value = std::env::var("CRABBOT_OPENROUTER_BASE_URL")
        .unwrap_or_else(|_| "https://openrouter.ai/api/v1".into());

    let url = reqwest::Url::parse(&value)
        .map_err(|_| crabbot_core::Error::Denied("OpenRouter base URL is invalid.".into()))?;

    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));

    if url.scheme() != "https" && !local {
        return Err(crabbot_core::Error::Denied(
            "OpenRouter base URL must use HTTPS outside loopback.".into(),
        ));
    }

    Ok(value.trim_end_matches('/').into())
}

async fn complete(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let response = client
        .post(format!("{base}/chat/completions"))
        .headers(headers()?)
        .bearer_auth(key)
        .json(&json!({
            "model": input.model,
            "messages": messages(&input)?,
            "stream": false,
            "tools": tools(&input)
        }))
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenRouter request failed: {error}."))
        })?;

    let status = response.status();
    let body = collect(response.bytes_stream()).await?;
    response_body(id, status, serde_json::from_slice(&body)?)
}

async fn stream(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let response = client
        .post(format!("{base}/chat/completions"))
        .headers(headers()?)
        .bearer_auth(key)
        .json(&json!({
            "model": input.model,
            "messages": messages(&input)?,
            "stream": true,
            "tools": tools(&input)
        }))
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenRouter stream failed: {error}."))
        })?;

    if !response.status().is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "OpenRouter stream was rejected with {}.",
            response.status()
        )));
    }

    stream_body(id, response.bytes_stream(), emitter).await
}

fn headers() -> crabbot_core::Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();

    if let Some(value) =
        std::env::var("CRABBOT_OPENROUTER_REFERER").ok().filter(|value| !value.is_empty())
    {
        headers.insert(
            "HTTP-Referer",
            value.parse().map_err(|_| {
                crabbot_core::Error::Denied("OpenRouter referer is invalid.".into())
            })?,
        );
    }

    if let Some(value) =
        std::env::var("CRABBOT_OPENROUTER_TITLE").ok().filter(|value| !value.is_empty())
    {
        headers.insert(
            "X-Title",
            value
                .parse()
                .map_err(|_| crabbot_core::Error::Denied("OpenRouter title is invalid.".into()))?,
        );
    }

    Ok(headers)
}

async fn collect<S, C, E>(mut stream: S) -> crabbot_core::Result<Vec<u8>>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenRouter response failed: {error}."))
        })?;

        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied(
                "OpenRouter response exceeded the frame budget.".into(),
            ));
        }

        bytes.extend_from_slice(chunk.as_ref());
    }

    Ok(bytes)
}

fn messages(input: &ModelRequest) -> crabbot_core::Result<Vec<Value>> {
    input.messages.iter().map(|message| {
        let role = match message.role { Role::System => "system", Role::User => "user", Role::Assistant => "assistant", Role::Tool => "user" };
        let mut parts = Vec::new();

        if message.role == Role::Tool {
            parts.push(json!({"type":"text","text":"[Tool result]"}));
        }

        for item in &message.content {
            match item {
                Content::Text { text } => parts.push(json!({"type":"text","text":text})),

                Content::Image { uri, .. } if message.role != Role::System => {
                    parts.push(json!({"type":"image_url","image_url":{"url":image(uri)?}}));
                }

                Content::Image { alt, .. } => parts.push(json!({"type":"text","text":alt.as_deref().unwrap_or("[Image attachment.]")})),
                item => parts.push(json!({"type":"text","text":item.render()})),
            }
        }

        let content = if parts.iter().all(|part| part["type"] == "text") {
            Value::String(parts.iter().filter_map(|part| part["text"].as_str()).collect::<Vec<_>>().join("\n"))
        } else { Value::Array(parts) };

        Ok(json!({"role":role,"content":content}))
    }).collect()
}

fn image(uri: &str) -> crabbot_core::Result<&str> {
    if uri.starts_with("https://") || uri.starts_with("data:image/") {
        Ok(uri)
    } else {
        Err(crabbot_core::Error::Denied(
            "OpenRouter received an unsupported image reference.".into(),
        ))
    }
}

fn tools(input: &ModelRequest) -> Vec<Value> {
    input.tools.iter().take(TOOL_LIMIT).map(|tool| json!({"type":"function","function":{"name":tool.name,"description":tool.description,"parameters":tool.schema}})).collect()
}

async fn stream_body<S, C, E>(
    id: u64,
    mut stream: S,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut buffer = Vec::new();

    let mut text = String::new();
    let mut pending = String::new();
    let mut calls = BTreeMap::<usize, (String, String)>::new();
    let mut bytes: usize = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenRouter stream failed: {error}."))
        })?;

        bytes = bytes.saturating_add(chunk.as_ref().len());

        if bytes > BODY_LIMIT {
            return Err(crabbot_core::Error::Denied(
                "OpenRouter stream exceeded the frame budget.".into(),
            ));
        }

        buffer.extend_from_slice(chunk.as_ref());

        while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
            let line = buffer.drain(..=end).collect::<Vec<_>>();

            if stream_line(&line, &mut text, &mut pending, &mut calls)? {
                break;
            }

            if pending.len() >= 128 {
                emit(&mut pending, emitter).await?;
            }
        }

        if !pending.is_empty() {
            emit(&mut pending, emitter).await?;
        }
    }

    if !buffer.is_empty() {
        stream_line(&buffer, &mut text, &mut pending, &mut calls)?;
    }

    emit(&mut pending, emitter).await?;
    let events = calls
        .into_values()
        .map(|(name, args)| {
            let args = if args.is_empty() { json!({}) } else { serde_json::from_str(&args)? };

            Ok(Event::Tool { name, args, id: None, thought_signature: None })
        })
        .collect::<crabbot_core::Result<Vec<_>>>()?;

    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: "stream".into(),
            input: None,
            output: None,
            events,
        })?,
    )
}

fn stream_line(
    line: &[u8],
    text: &mut String,
    pending: &mut String,
    calls: &mut BTreeMap<usize, (String, String)>,
) -> crabbot_core::Result<bool> {
    let line = std::str::from_utf8(line)
        .map_err(|_| crabbot_core::Error::Denied("OpenRouter stream was not UTF-8.".into()))?;

    let Some(data) = line.trim().strip_prefix("data:") else { return Ok(false) };

    let data = data.trim();

    if data == "[DONE]" {
        return Ok(true);
    }

    let value: Value = serde_json::from_str(data)?;
    let delta = &value["choices"][0]["delta"];

    if let Some(value) = delta["content"].as_str() {
        text.push_str(value);
        pending.push_str(value);
    }

    if let Some(values) = delta["tool_calls"].as_array() {
        for value in values {
            let index = value["index"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value < TOOL_LIMIT)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "OpenRouter stream exceeded the tool-call limit.".into(),
                    )
                })?;

            let entry = calls.entry(index).or_insert_with(|| (String::new(), String::new()));

            if let Some(value) = value["function"]["name"].as_str() {
                entry.0.push_str(value);
            }

            if let Some(value) = value["function"]["arguments"].as_str() {
                entry.1.push_str(value);
            }
        }
    }

    Ok(false)
}

async fn emit(pending: &mut String, emitter: &mut Emitter) -> crabbot_core::Result<()> {
    if !pending.is_empty() {
        emitter.event(json!({"kind":"text","text":std::mem::take(pending)})).await?;
    }

    Ok(())
}

fn response_body(
    id: u64,
    status: reqwest::StatusCode,
    body: Value,
) -> crabbot_core::Result<Option<Response>> {
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "OpenRouter rejected the request: {}.",
            body["error"]["message"].as_str().unwrap_or("request failed")
        )));
    }

    let choice = body["choices"].as_array().and_then(|values| values.first()).ok_or_else(|| {
        crabbot_core::Error::Denied("OpenRouter response contained no choices.".into())
    })?;

    let text = choice["message"]["content"].as_str().unwrap_or_default().to_owned();
    let events: Vec<Event> = choice["message"]["tool_calls"]
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| {
                    let name = value["function"]["name"].as_str()?.to_owned();
                    let args = value["function"]["arguments"]
                        .as_str()
                        .and_then(|value| serde_json::from_str(value).ok())
                        .unwrap_or_else(|| value["function"]["arguments"].clone());
                    Some(Event::Tool { name, args, id: None, thought_signature: None })
                })
                .collect()
        })
        .unwrap_or_default();

    if text.is_empty() && events.is_empty() {
        return Err(crabbot_core::Error::Denied(
            "OpenRouter response contained no text or tools.".into(),
        ));
    }

    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: choice["finish_reason"].as_str().unwrap_or("stop").into(),
            input: body["usage"]["prompt_tokens"].as_u64(),
            output: body["usage"]["completion_tokens"].as_u64(),
            events,
        })?,
    )
}

fn response(id: u64, result: Value) -> crabbot_core::Result<Option<Response>> {
    let response = Response::ok(id, result);

    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "OpenRouter response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(Some(response))
}

fn credential() -> crabbot_core::Result<String> {
    if let Ok(value) = std::env::var("CRABBOT_OPENROUTER_KEY")
        && !value.trim().is_empty()
    {
        return Ok(value);
    }

    Err(crabbot_core::Error::Denied("CRABBOT_OPENROUTER_KEY is not configured.".into()))
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_LIMIT, base_url, collect, credential, generate, headers, image, messages, response,
        response_body, stream_body, stream_line, tools,
    };

    use crabbot_core::types::{Content, Message, ModelRequest, Request, Role, ToolSpec};
    use futures_util::stream;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the OpenRouter test listener: {error}."),
        }
    }

    fn model() -> ModelRequest {
        ModelRequest {
            model: "test/model".into(),
            workspace: None,
            messages: vec![
                Message {
                    id: "system".into(),
                    session: "session".into(),
                    role: Role::System,
                    sender: None,
                    content: vec![Content::Text { text: "rules".into() }],
                },
                Message {
                    id: "user".into(),
                    session: "session".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![
                        Content::Text { text: "hello".into() },
                        Content::Image { uri: "https://cdn.example/image.png".into(), alt: None },
                    ],
                },
            ],
            stream: false,
            tools: vec![ToolSpec {
                name: "read".into(),
                description: Some("Read a file.".into()),
                schema: json!({"type":"object"}),
            }],
        }
    }

    #[test]
    fn builds_messages_tools_and_images() {
        let input = model();

        let values = messages(&input).unwrap();

        assert_eq!(values[0], json!({"role":"system","content":"rules"}));
        assert_eq!(values[1]["content"][1]["type"], "image_url");
        assert_eq!(tools(&input).len(), 1);
        assert!(image("file:///outside").is_err());
        assert!(image("data:image/png;base64,abc").is_ok());
        let mut tool = input.clone();
        tool.messages.push(Message {
            id: "tool".into(),
            session: "session".into(),
            role: Role::Tool,
            sender: None,
            content: vec![Content::Text { text: "result".into() }],
        });

        assert_eq!(messages(&tool).unwrap().last().unwrap()["role"], "user");
    }

    #[tokio::test]
    async fn ignores_notes_and_unknown_calls_without_provider_access() {
        let (output, _) = tokio::sync::mpsc::channel(1);
        let emitter = crabbot_core::plugin::Emitter::new(output);

        assert!(
            generate(
                &reqwest::Client::new(),
                Request::Note { jsonrpc: "2.0".into(), method: "note".into(), params: json!({}) },
                emitter,
            )
            .await
            .unwrap()
            .is_none()
        );

        let (output, _) = tokio::sync::mpsc::channel(1);
        let emitter = crabbot_core::plugin::Emitter::new(output);

        assert!(
            generate(&reqwest::Client::new(), Request::call(1, "other", json!({})), emitter,)
                .await
                .unwrap()
                .is_none()
        );

        let (output, _) = tokio::sync::mpsc::channel(1);
        let emitter = crabbot_core::plugin::Emitter::new(output);

        assert!(
            generate(&reqwest::Client::new(), Request::call(1, "generate", json!({})), emitter,)
                .await
                .is_err()
        );
    }

    #[test]
    fn validates_provider_configuration_and_protocol_size() {
        assert!(base_url().is_ok());
        assert!(credential().is_err());
        assert!(headers().is_ok());
        assert!(response(1, json!({"ok": true})).unwrap().is_some());
        assert!(response_body(1, reqwest::StatusCode::OK, json!({"choices": []})).is_err());
    }

    #[test]
    fn parses_complete_and_tool_replies() {
        let response = response_body(
            1,
            reqwest::StatusCode::OK,
            json!({"choices":[{"message":{"content":"done","tool_calls":[{"function":{"name":"read","arguments":"{\"path\":\"note\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}),
        ).unwrap().unwrap();

        assert_eq!(response.result.as_ref().unwrap()["text"], "done");
        assert_eq!(response.result.as_ref().unwrap()["events"][0]["name"], "read");
        assert!(response_body(1, reqwest::StatusCode::UNAUTHORIZED, json!({})).is_err());
        assert!(response_body(1, reqwest::StatusCode::OK, json!({})).is_err());
        assert!(
            response_body(1, reqwest::StatusCode::OK, json!({"choices":[{"message":{}}]})).is_err()
        );
    }

    #[tokio::test]
    async fn bounds_and_parses_streams() {
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n".to_vec(),
            ),
            Ok(b"data: [DONE]\n".to_vec()),
        ]);

        let (output, mut events) = tokio::sync::mpsc::channel(4);
        let mut emitter = crabbot_core::plugin::Emitter::new(output);
        let response = stream_body(2, chunks, &mut emitter).await.unwrap().unwrap();

        assert_eq!(response.result.unwrap()["text"], "ok");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "ok");

        let mut text = String::new();
        let mut pending = String::new();
        let mut calls = std::collections::BTreeMap::new();

        assert!(stream_line(b"data: [DONE]\n", &mut text, &mut pending, &mut calls).unwrap());
        assert!(stream_line(b"data: nope\n", &mut text, &mut pending, &mut calls).is_err());
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]);

        assert!(collect(chunks).await.is_err());
        let mut text = String::new();
        let mut pending = String::new();
        let mut calls = std::collections::BTreeMap::new();

        assert!(stream_line(
            br#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"read","arguments":"{\"path\":\"note\"}"}}]}}]}"#,
            &mut text,
            &mut pending,
            &mut calls,
        )
        .is_ok());

        assert_eq!(calls[&0].0, "read");
        assert!(stream_line(b"invalid", &mut text, &mut pending, &mut calls).is_ok());
        assert!(stream_line(&[0xff], &mut text, &mut pending, &mut calls).is_err());
        assert!(stream_line(b"data: {", &mut text, &mut pending, &mut calls).is_err());
        assert!(
            stream_line(
                br#"data: {"choices":[{"delta":{"tool_calls":[{"index":16}]}}]}"#,
                &mut text,
                &mut pending,
                &mut calls,
            )
            .is_err()
        );

        let (output, _) = tokio::sync::mpsc::channel(1);
        let mut emitter = crabbot_core::plugin::Emitter::new(output);
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(b"data: nope\n".to_vec())]);

        assert!(stream_body(2, chunks, &mut emitter).await.is_err());
    }

    #[tokio::test]
    async fn completes_against_a_bounded_http_response() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"choices":[{"message":{"content":"done"},"finish_reason":"stop"}]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let result = super::complete(
            &reqwest::Client::new(),
            1,
            model(),
            "key",
            &format!("http://{address}"),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.result.unwrap()["text"], "done");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn streams_against_a_bounded_http_response() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body =
                b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n";
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let (output, mut events) = tokio::sync::mpsc::channel(4);
        let mut emitter = crabbot_core::plugin::Emitter::new(output);
        let mut input = model();
        input.stream = true;
        let response = super::stream(
            &reqwest::Client::new(),
            1,
            input,
            "key",
            &format!("http://{address}"),
            &mut emitter,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.result.unwrap()["text"], "ok");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_provider_failures_and_oversized_results() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let body = br#"{"error":{"message":"nope"}}"#;

                let header = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );

                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let client = reqwest::Client::new();
        let base = format!("http://{address}");

        assert!(super::complete(&client, 1, model(), "key", &base).await.is_err());

        let (output, _) = tokio::sync::mpsc::channel(1);
        let mut emitter = crabbot_core::plugin::Emitter::new(output);
        let mut input = model();
        input.stream = true;

        assert!(super::stream(&client, 2, input, "key", &base, &mut emitter).await.is_err());

        assert!(collect(stream::iter(vec![Err::<Vec<u8>, _>("broken")])).await.is_err());
        assert!(response(1, json!({"payload": "x".repeat(crabbot_core::jsonl::MAX)})).is_err());
        server.await.unwrap();
    }

    #[test]
    fn renders_non_image_content_and_rejects_invalid_configuration() {
        let mut input = model();
        input.messages[1].content = vec![
            Content::Image {
                uri: "https://cdn.example/image.png".into(),
                alt: Some("diagram".into()),
            },
            Content::ToolCall {
                name: "read".into(),
                args: json!({"path": "README.md"}),
                id: Some("call-1".into()),
                thought_signature: None,
            },
        ];

        let values = messages(&input).unwrap();

        assert_eq!(values[1]["content"][0]["type"], "image_url");
        assert_eq!(values[1]["content"][1]["text"], "[Tool call read]: {\"path\":\"README.md\"}");
        assert!(base_url().is_ok());
        assert!(credential().is_err());
    }
}
