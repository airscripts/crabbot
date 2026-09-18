#![forbid(unsafe_code)]

use crabbot_core::types::{Content, Event, ModelReply, ModelRequest, Request, Response, Role};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_events,
    types::{Capability, Hello, Protocol},
};
use serde_json::{Value, json};
#[cfg(not(test))]
use std::time::Duration;

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
    let base_url = reqwest::Url::parse(&base)
        .map_err(|_| crabbot_core::Error::Denied("Gemini base URL is invalid.".into()))?;
    if base_url.scheme() != "https"
        && !matches!(base_url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
    {
        return Err(crabbot_core::Error::Denied(
            "Gemini base URL must use HTTPS outside loopback.".into(),
        ));
    }

    generate_at(client, id, input, &key, base_url.as_str()).await
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
    let body = response.bytes().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("Gemini response failed: {error}."))
    })?;
    if body.len() > BODY_LIMIT {
        return Err(crabbot_core::Error::Denied(
            "Gemini response exceeded the protocol limit.".into(),
        ));
    }
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
    let mut contents = Vec::new();
    let mut system = Vec::new();
    for message in &input.messages {
        let parts = message
            .content
            .iter()
            .map(|content| match content {
                Content::Text { text } => json!({"text": text}),
                Content::Image { alt, .. } => {
                    json!({"text": alt.as_deref().unwrap_or("[Image attachment.]" )})
                }
                content => json!({"text": content.render()}),
            })
            .collect::<Vec<_>>();
        if message.role == Role::System {
            system.extend(parts);
        } else {
            contents.push(json!({
                "role": if message.role == Role::Assistant { "model" } else { "user" },
                "parts": parts,
            }));
        }
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
            id: "assistant".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::Text { text: "done".into() }],
        });

        input.messages.push(Message {
            id: "tool".into(),
            session: "s".into(),
            role: Role::Tool,
            sender: None,
            content: vec![Content::Image { uri: "file://image".into(), alt: Some("alt".into()) }],
        });

        let body = request_body(&input).unwrap();
        assert_eq!(body["contents"][1]["role"], "model");
        assert_eq!(body["contents"][2]["parts"][0]["text"], "alt");
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
