#![forbid(unsafe_code)]

use crabbot_core::types::{Content, Event, ModelReply, ModelRequest, Request, Response, Role};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_events,
    types::{Capability, Hello, Protocol},
};

use std::collections::VecDeque;
#[cfg(not(test))]
use std::time::Duration;

use serde_json::{Value, json};
const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Gemini client failed: {error}.")))?;

    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "gemini".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Model, Capability::Vision],
            commands: Vec::new(),
        },
        move |request, _emitter| {
            let client = client.clone();
            async move { generate(&client, request).await }
        },
    )
    .await
}

async fn generate(
    client: &reqwest::Client,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method != "generate" {
        return Ok(None);
    }

    let input: ModelRequest = serde_json::from_value(params)?;
    let key = std::env::var("CRABBOT_GEMINI_KEY")
        .map_err(|_| crabbot_core::Error::Denied("CRABBOT_GEMINI_KEY is not configured.".into()))?;

    if key.trim().is_empty() {
        return Err(crabbot_core::Error::Denied("CRABBOT_GEMINI_KEY is not configured.".into()));
    }

    let base = std::env::var("CRABBOT_GEMINI_BASE_URL")
        .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".into());

    generate_configured(client, id, input, &key, &base).await
}

async fn generate_configured(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let base_url = reqwest::Url::parse(base)
        .map_err(|_| crabbot_core::Error::Denied("Gemini base URL is invalid.".into()))?;

    if base_url.scheme() != "https"
        && !matches!(base_url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
    {
        return Err(crabbot_core::Error::Denied(
            "Gemini base URL must use HTTPS outside loopback.".into(),
        ));
    }

    generate_at(client, id, input, key, base_url.as_str()).await
}

async fn generate_at(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let model = if input.model.trim().is_empty() || input.model == "default" {
        "gemini-2.0-flash".into()
    } else {
        input.model.clone()
    };

    let response = client
        .post(format!("{}/models/{}:generateContent", base.trim_end_matches('/'), model))
        .query(&[("key", key.to_owned())])
        .json(&request_body(&input)?)
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Gemini request failed: {error}.")))?;

    let status = response.status();
    let body = collect_response(response).await?;

    let value: Value = serde_json::from_slice(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("Gemini response was invalid: {error}."))
    })?;

    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Gemini request was rejected with {status}."
        )));
    }

    let (text, events) = parse_reply(&value);
    let stop = if events.is_empty() { "stop" } else { "tool" };

    Ok(Some(Response::ok(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: stop.into(),
            input: None,
            output: None,
            events,
        })?,
    )))
}

async fn collect_response(mut response: reqwest::Response) -> crabbot_core::Result<Vec<u8>> {
    let mut body = Vec::new();

    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Gemini response failed: {error}.")))?
    {
        if chunk.len() > BODY_LIMIT.saturating_sub(body.len()) {
            return Err(crabbot_core::Error::Denied(
                "Gemini response exceeded the protocol limit.".into(),
            ));
        }

        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

fn parse_reply(value: &Value) -> (String, Vec<Event>) {
    let mut text = String::new();
    let mut events = Vec::new();

    for part in value["candidates"][0]["content"]["parts"].as_array().into_iter().flatten() {
        if let Some(value) = part["text"].as_str() {
            text.push_str(value);
        }

        if let Some(call) = part.get("functionCall")
            && let Some(name) = call["name"].as_str()
        {
            events.push(Event::Tool { name: name.into(), args: call["args"].clone() });
        }
    }

    (text, events)
}

fn request_body(input: &ModelRequest) -> crabbot_core::Result<Value> {
    if requires_thought_signature(&input.model)
        && (!input.tools.is_empty()
            || input.messages.iter().any(|message| message.role == Role::Tool))
    {
        return Err(crabbot_core::Error::Denied(
            "Gemini models requiring thought signatures do not support tool workflows.".into(),
        ));
    }

    let mut contents = Vec::new();
    let mut system = Vec::new();
    let mut calls = VecDeque::new();

    for message in &input.messages {
        if message.role == Role::System {
            system.extend(message.content.iter().map(text_part));
            continue;
        }

        if message.role == Role::Assistant {
            let (parts, found) = assistant_parts(message);
            calls.extend(found);
            contents.push(json!({"role": "model", "parts": parts}));
            continue;
        }

        if message.role == Role::Tool {
            let index = message
                .sender
                .as_deref()
                .and_then(|sender| calls.iter().position(|call| call.name == sender))
                .or_else(|| message.sender.is_none().then_some(0));

            let matched = index.and_then(|index| calls.remove(index));
            let name = message
                .sender
                .clone()
                .or_else(|| matched.as_ref().map(|call| call.name.clone()))
                .unwrap_or_else(|| "tool".into());

            let mut response = json!({
                "name": name,
                "response": {"output": rendered_content(&message.content)},
            });

            if let Some(id) = matched.map(|call| call.id) {
                response["id"] = json!(id);
            }

            contents.push(json!({"role": "user", "parts": [{"functionResponse": response}]}));
            continue;
        }

        contents.push(json!({
            "role": "user",
            "parts": message.content.iter().map(text_part).collect::<Vec<_>>(),
        }));
    }

    let mut body = json!({"contents": contents});

    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts": system});
    }

    if !input.tools.is_empty() {
        body["tools"] = json!([{"functionDeclarations": input.tools.iter().map(|tool| json!({
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.schema,
        })).collect::<Vec<_>>()}]);
    }

    Ok(body)
}

fn requires_thought_signature(model: &str) -> bool {
    model.to_ascii_lowercase().starts_with("gemini-3")
}

#[derive(Debug)]
struct FunctionCall {
    id: String,
    name: String,
}

fn text_part(content: &Content) -> Value {
    match content {
        Content::Text { text } => json!({"text": text}),

        Content::Image { alt, .. } => {
            json!({"text": alt.as_deref().unwrap_or("[Image attachment.]" )})
        }

        content => json!({"text": content.render()}),
    }
}

fn rendered_content(content: &[Content]) -> String {
    content
        .iter()
        .map(|content| match content {
            Content::Image { alt, .. } => alt.as_deref().unwrap_or("[Image attachment.]").into(),
            content => content.render(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn assistant_parts(message: &crabbot_core::types::Message) -> (Vec<Value>, Vec<FunctionCall>) {
    let mut parts = Vec::new();

    let mut calls = Vec::new();
    let mut index = 0;

    for content in &message.content {
        let Content::Text { text: value } = content else {
            parts.push(text_part(content));
            continue;
        };

        let mut buffered = String::new();

        for line in value.split_inclusive('\n') {
            let candidate = line.strip_suffix('\n').unwrap_or(line);
            let Some((name, args)) = candidate.strip_prefix("[Tool call ").and_then(|value| {
                let (name, args) = value.split_once("]: ")?;
                Some((name, serde_json::from_str::<Value>(args).ok()?))
            }) else {
                buffered.push_str(line);
                continue;
            };

            if !buffered.is_empty() {
                parts.push(json!({"text": buffered}));
                buffered.clear();
            }

            let id = format!("{}-{index}", message.id);
            index += 1;
            parts.push(json!({"functionCall": {"id": id, "name": name, "args": args}}));
            calls.push(FunctionCall { id, name: name.into() });
        }

        if !buffered.is_empty() {
            parts.push(json!({"text": buffered}));
        }
    }

    (parts, calls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabbot_core::types::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the Gemini test listener: {error}."),
        }
    }

    #[test]
    fn builds_gemini_messages_and_tools() {
        let request = ModelRequest {
            model: "gemini-2.0-flash".into(),
            workspace: None,
            messages: vec![
                Message {
                    id: "system".into(),
                    session: "s".into(),
                    role: Role::System,
                    sender: None,
                    content: vec![Content::Text { text: "Be concise.".into() }],
                },
                Message {
                    id: "user".into(),
                    session: "s".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Image { uri: "file://image".into(), alt: None }],
                },
            ],
            stream: false,
            tools: vec![crabbot_core::types::ToolSpec {
                name: "read".into(),
                description: Some("Read a file.".into()),
                schema: json!({"type": "object"}),
            }],
        };

        let body = request_body(&request).unwrap();

        assert_eq!(body["systemInstruction"]["parts"][0]["text"], "Be concise.");
        assert_eq!(body["contents"][0]["role"], "user");
        assert_eq!(body["tools"][0]["functionDeclarations"][0]["name"], "read");

        let mut input = request;
        input.messages.push(Message {
            id: "assistant-tool".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::Text {
                text: "I will read it.\n[Tool call read]: {\"path\":\"README.md\"}".into(),
            }],
        });

        input.messages.push(Message {
            id: "tool".into(),
            session: "s".into(),
            role: Role::Tool,
            sender: None,
            content: vec![Content::Text { text: "content".into() }],
        });

        let body = request_body(&input).unwrap();

        assert_eq!(body["contents"][1]["role"], "model");
        assert_eq!(body["contents"][1]["parts"][0]["text"], "I will read it.\n");
        assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["name"], "read");
        assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["args"]["path"], "README.md");
        assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["id"], "assistant-tool-0");
        assert_eq!(body["contents"][2]["role"], "user");
        assert_eq!(body["contents"][2]["parts"][0]["functionResponse"]["name"], "read");
        assert_eq!(body["contents"][2]["parts"][0]["functionResponse"]["id"], "assistant-tool-0");
        assert_eq!(
            body["contents"][2]["parts"][0]["functionResponse"]["response"]["output"],
            "content"
        );

        input.messages.push(Message {
            id: "media".into(),
            session: "s".into(),
            role: Role::User,
            sender: None,
            content: vec![
                Content::Image { uri: "file://image".into(), alt: Some("alt".into()) },
                Content::File {
                    uri: "file://note".into(),
                    name: "note.txt".into(),
                    mime: Some("text/plain".into()),
                },
                Content::Audio { uri: "file://voice".into(), mime: None },
            ],
        });

        let body = request_body(&input).unwrap();

        assert_eq!(body["contents"][3]["parts"][0]["text"], "alt");
        assert_eq!(body["contents"][3]["parts"][1]["text"], "[File: note.txt]");
        assert_eq!(body["contents"][3]["parts"][2]["text"], "[Audio attachment.]");
    }

    #[test]
    fn preserves_large_function_call_arguments() {
        let args = json!({"patch": "x".repeat(4097)});
        let input = ModelRequest {
            model: "gemini-2.0-flash".into(),
            workspace: None,
            messages: vec![Message {
                id: "assistant-large".into(),
                session: "s".into(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: format!("[Tool call patch]: {args}") }],
            }],
            stream: false,
            tools: Vec::new(),
        };

        let body = request_body(&input).unwrap();

        assert_eq!(body["contents"][0]["parts"][0]["functionCall"]["args"], args);
        assert_eq!(body["contents"][0]["parts"][0]["functionCall"]["id"], "assistant-large-0");
    }

    #[test]
    fn rejects_gemini_three_tool_workflows_without_signatures() {
        let request = ModelRequest {
            model: "gemini-3-pro-preview".into(),
            workspace: None,
            messages: Vec::new(),
            stream: false,
            tools: vec![crabbot_core::types::ToolSpec {
                name: "read".into(),
                description: None,
                schema: json!({"type": "object"}),
            }],
        };

        let error = request_body(&request).unwrap_err();

        assert!(error.to_string().contains("thought signatures"));
    }

    #[tokio::test]
    async fn rejects_notes_and_unknown_calls_before_credentials() {
        let client = reqwest::Client::new();

        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "note".into(), params: json!({}) };

        assert!(generate(&client, note).await.unwrap().is_none());
        assert!(generate(&client, Request::call(1, "other", json!({}))).await.unwrap().is_none());
        assert!(generate(&client, Request::call(1, "generate", json!("bad"))).await.is_err());
    }

    #[tokio::test]
    async fn validates_gemini_configuration_before_network_access() {
        let client = reqwest::Client::new();
        let request = ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: Vec::new(),
            stream: false,
            tools: Vec::new(),
        };

        assert!(
            generate_configured(&client, 1, request.clone(), "key", "not a URL").await.is_err()
        );

        assert!(
            generate_configured(&client, 1, request.clone(), "key", "http://example.com")
                .await
                .is_err()
        );

        assert!(
            generate_configured(&client, 1, request, "key", "http://127.0.0.1:1").await.is_err()
        );
    }

    #[test]
    fn parses_text_and_function_calls() {
        let value = json!({
            "candidates": [{"content": {"parts": [
                {"text": "I will read it."},
                {"functionCall": {"name": "read", "args": {"path": "README.md"}}}
            ]}}]
        });

        let (text, events) = parse_reply(&value);

        assert_eq!(text, "I will read it.");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            Event::Tool { name: "read".into(), args: json!({"path": "README.md"}) }
        );

        assert_eq!(parse_reply(&json!({})), (String::new(), Vec::new()));
        assert_eq!(
            parse_reply(&json!({"candidates": [{"content": {"parts": [
                {"functionCall": {"args": {}}},
                {}
            ]}}]})),
            (String::new(), Vec::new())
        );
    }

    #[tokio::test]
    async fn sends_a_bounded_request_and_parses_status() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let client = reqwest::Client::new();
        let request = ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: Vec::new(),
            stream: false,
            tools: Vec::new(),
        };

        let response = generate_at(&client, 1, request, "key", &format!("http://{address}"))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.result.unwrap()["text"], "ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_an_oversized_response_before_parsing() {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = vec![b'x'; BODY_LIMIT + 1];
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let request = ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: Vec::new(),
            stream: false,
            tools: Vec::new(),
        };

        assert!(
            generate_at(&reqwest::Client::new(), 1, request, "key", &format!("http://{address}"),)
                .await
                .is_err()
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_failed_and_invalid_responses() {
        for (status, body) in [
            ("403 Forbidden", br#"{"error":"denied"}"#.as_slice()),
            ("200 OK", br#"not-json"#.as_slice()),
        ] {
            let Some(listener) = loopback_listener().await else {
                return;
            };

            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let header = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            });

            let request = ModelRequest {
                model: "test".into(),
                workspace: None,
                messages: Vec::new(),
                stream: false,
                tools: Vec::new(),
            };

            assert!(
                generate_at(
                    &reqwest::Client::new(),
                    1,
                    request,
                    "key",
                    &format!("http://{address}")
                )
                .await
                .is_err()
            );
            server.await.unwrap();
        }
    }
}
