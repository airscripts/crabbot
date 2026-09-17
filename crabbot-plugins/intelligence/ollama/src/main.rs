#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::{Emitter, serve_events},
    types::{
        Capability, Content, Event, Hello, ModelReply, ModelRequest, Protocol, Request, Response,
    },
};
use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Ollama client failed: {error}.")))?;
    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "ollama".into(),
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
    let host =
        std::env::var("CRABBOT_OLLAMA_HOST").unwrap_or_else(|_| "http://127.0.0.1:11434".into());
    generate_at(client, id, input, &host, &mut emitter).await
}

async fn generate_at(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    host: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    if input.stream {
        stream_request(client, id, input, host, emitter).await
    } else {
        generate_request(client, id, input, host).await
    }
}

async fn generate_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    host: &str,
) -> crabbot_core::Result<Option<Response>> {
    request(client, id, input, host, false).await
}

async fn request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    host: &str,
    stream: bool,
) -> crabbot_core::Result<Option<Response>> {
    let mut post = client.post(format!("{host}/api/chat"));
    if let Some(key) = credential() {
        post = post.bearer_auth(key);
    }
    let messages = messages(&input)?;
    let response = post
        .json(&json!({"model": input.model, "messages": messages, "tools": tools(&input), "stream": stream}))
        .send()
        .await
        .map_err(|error| {
            let kind = if stream { "stream" } else { "request" };
            crabbot_core::Error::Denied(format!("Ollama {kind} failed: {error}."))
        })?;
    let status = response.status();
    let body = read(response, "Ollama").await?;
    if stream {
        return stream_body(id, status, &body);
    }
    let body: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("Ollama response failed: {error}."))
    })?;
    response_body(id, status, body)
}

async fn read(response: reqwest::Response, provider: &str) -> crabbot_core::Result<String> {
    collect(response.bytes_stream(), provider).await
}

async fn collect<S, C, E>(mut stream: S, provider: &str) -> crabbot_core::Result<String>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("{provider} response failed: {error}."))
        })?;
        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied(format!(
                "{provider} response exceeded the {BODY_LIMIT}-byte limit."
            )));
        }
        bytes.extend_from_slice(chunk.as_ref());
    }

    String::from_utf8(bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("{provider} response was not UTF-8: {error}."))
    })
}

#[derive(Default)]
struct ToolDelta {
    name: String,
    arguments: serde_json::Value,
}

async fn live_stream<S, C, E>(
    id: u64,
    mut stream: S,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut buffered = Vec::new();
    let mut bytes = 0_usize;
    let mut text = String::new();
    let mut pending = String::new();
    let mut tools = BTreeMap::<usize, ToolDelta>::new();
    let mut stop = "stop".to_string();
    let mut input_tokens = None;
    let mut output_tokens = None;
    let mut ticker = tokio::time::interval(Duration::from_millis(80));
    ticker.tick().await;

    'stream: loop {
        tokio::select! {
            chunk = stream.next() => {
                let Some(chunk) = chunk else { break };
                let chunk = chunk.map_err(|error| {
                    crabbot_core::Error::Denied(format!("Ollama stream failed: {error}."))
                })?;
                if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes) {
                    return Err(crabbot_core::Error::Denied(format!(
                        "Ollama stream exceeded the {BODY_LIMIT}-byte limit."
                    )));
                }
                bytes = bytes.saturating_add(chunk.as_ref().len());
                buffered.extend_from_slice(chunk.as_ref());
                while let Some(end) = buffered.iter().position(|byte| *byte == b'\n') {
                    let line = buffered.drain(..=end).collect::<Vec<_>>();
                    if ollama_line(
                        &line,
                        &mut text,
                        &mut pending,
                        &mut tools,
                        &mut stop,
                        &mut input_tokens,
                        &mut output_tokens,
                    )? {
                        break 'stream;
                    }
                    if pending.len() >= 128 {
                        emit(&mut pending, emitter).await?;
                    }
                }
            }
            _ = ticker.tick(), if !pending.is_empty() => emit(&mut pending, emitter).await?,
        }
    }
    if !buffered.is_empty() {
        ollama_line(
            &buffered,
            &mut text,
            &mut pending,
            &mut tools,
            &mut stop,
            &mut input_tokens,
            &mut output_tokens,
        )?;
    }
    emit(&mut pending, emitter).await?;

    let events = tools
        .into_values()
        .map(|tool| {
            if tool.name.is_empty() {
                return Err(crabbot_core::Error::Denied(
                    "Ollama stream returned a tool call without a name.".into(),
                ));
            }
            let arguments = if let Some(value) = tool.arguments.as_str() {
                serde_json::from_str(value).map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Ollama stream returned invalid tool arguments: {error}."
                    ))
                })?
            } else {
                tool.arguments
            };
            Ok(Event::Tool { name: tool.name, args: arguments })
        })
        .collect::<crabbot_core::Result<Vec<_>>>()?;

    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop,
            input: input_tokens,
            output: output_tokens,
            events,
        })?,
    )
}

fn ollama_line(
    line: &[u8],
    text: &mut String,
    pending: &mut String,
    tools: &mut BTreeMap<usize, ToolDelta>,
    stop: &mut String,
    input_tokens: &mut Option<u64>,
    output_tokens: &mut Option<u64>,
) -> crabbot_core::Result<bool> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok(false);
    }
    let line = std::str::from_utf8(line).map_err(|error| {
        crabbot_core::Error::Denied(format!("Ollama stream was not UTF-8: {error}."))
    })?;
    let value: serde_json::Value = serde_json::from_str(line.trim())?;
    if let Some(error) = value["error"].as_str() {
        return Err(crabbot_core::Error::Denied(format!("Ollama stream failed: {error}.")));
    }
    if let Some(part) = value["message"]["content"].as_str() {
        append_text(part, text, pending)?;
    }
    if let Some(calls) = value["message"]["tool_calls"].as_array() {
        if calls.len() > 16 {
            return Err(crabbot_core::Error::Limit(
                "Ollama stream exceeded the tool-call limit.".into(),
            ));
        }
        for (position, call) in calls.iter().enumerate() {
            let index = call["index"]
                .as_u64()
                .and_then(|index| usize::try_from(index).ok())
                .unwrap_or(position);
            if index >= 16 {
                return Err(crabbot_core::Error::Limit(
                    "Ollama stream exceeded the tool-call limit.".into(),
                ));
            }
            let tool = tools.entry(index).or_default();
            if let Some(name) = call["function"]["name"].as_str() {
                tool.name = name.into();
            }
            if !call["function"]["arguments"].is_null() {
                tool.arguments = call["function"]["arguments"].clone();
            }
        }
    }
    if let Some(reason) = value["done_reason"].as_str() {
        *stop = reason.into();
    }
    *input_tokens = value["prompt_eval_count"].as_u64().or(*input_tokens);
    *output_tokens = value["eval_count"].as_u64().or(*output_tokens);
    Ok(value["done"] == true)
}

fn append_text(part: &str, text: &mut String, pending: &mut String) -> crabbot_core::Result<()> {
    if part.len() > BODY_LIMIT.saturating_sub(text.len()) {
        return Err(crabbot_core::Error::Limit(
            "Ollama stream exceeded the response limit.".into(),
        ));
    }
    text.push_str(part);
    pending.push_str(part);
    Ok(())
}

async fn emit(pending: &mut String, emitter: &mut Emitter) -> crabbot_core::Result<()> {
    if !pending.is_empty() {
        emitter.event(json!({"kind": "text", "text": std::mem::take(pending)})).await?;
    }
    Ok(())
}

fn credential() -> Option<String> {
    std::env::var("CRABBOT_OLLAMA_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| keyring("ollama"))
        .or_else(|| {
            let path = std::env::var_os("CRABBOT_CREDENTIALS")?;
            crabbot_file::private(&path).ok()?;
            let text = std::fs::read_to_string(path).ok()?;
            let value = serde_json::from_str::<serde_json::Value>(&text).ok()?;
            value["CRABBOT_OLLAMA_API_KEY"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
}

fn keyring(name: &str) -> Option<String> {
    if std::env::var("CRABBOT_KEYRING").ok().as_deref() != Some("1") {
        return None;
    }
    keyring::Entry::new("dev.airscripts.crabbot", name)
        .ok()?
        .get_password()
        .ok()
        .filter(|value| !value.trim().is_empty())
}

async fn stream_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    host: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let mut post = client.post(format!("{host}/api/chat"));
    let messages = messages(&input)?;
    if let Some(key) = credential() {
        post = post.bearer_auth(key);
    }
    let response = post
        .json(&json!({"model": input.model, "messages": messages, "tools": tools(&input), "stream": true}))
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Ollama stream failed: {error}.")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Ollama stream was rejected with {status}."
        )));
    }
    live_stream(id, response.bytes_stream(), emitter).await
}

fn messages(input: &ModelRequest) -> crabbot_core::Result<Vec<serde_json::Value>> {
    input
        .messages
        .iter()
        .map(|message| {
            let role = match message.role {
                crabbot_core::types::Role::System => "system",
                crabbot_core::types::Role::User => "user",
                crabbot_core::types::Role::Assistant => "assistant",
                crabbot_core::types::Role::Tool => "tool",
            };
            let mut text = Vec::new();
            let mut images = Vec::new();
            for content in &message.content {
                match content {
                    Content::Text { text: value } => text.push(value.clone()),
                    Content::Image { uri, .. } => images.push(image_data(uri)?),
                    content => text.push(content.render()),
                }
            }
            let mut value = json!({"role": role, "content": text.join("\n")});
            if !images.is_empty() {
                value["images"] = json!(images);
            }
            Ok(value)
        })
        .collect()
}

fn image_data(uri: &str) -> crabbot_core::Result<&str> {
    let Some((mime, encoded)) =
        uri.strip_prefix("data:").and_then(|value| value.split_once(";base64,"))
    else {
        return Err(crabbot_core::Error::Denied(
            "Ollama received an unsupported image reference.".into(),
        ));
    };
    if !matches!(mime, "image/png" | "image/jpeg" | "image/gif" | "image/webp")
        || encoded.is_empty()
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(crabbot_core::Error::Denied(
            "Ollama received an invalid image data URL.".into(),
        ));
    }
    Ok(encoded)
}

fn tools(input: &ModelRequest) -> Vec<serde_json::Value> {
    input
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.schema,
                }
            })
        })
        .collect()
}

fn stream_body(
    id: u64,
    status: reqwest::StatusCode,
    body: &str,
) -> crabbot_core::Result<Option<Response>> {
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Ollama stream was rejected with {status}."
        )));
    }
    let mut text = String::new();
    let mut stop = "stream".to_string();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line.trim())?;
        if let Some(part) = value["message"]["content"].as_str() {
            text.push_str(part);
        }
        if let Some(reason) = value["done_reason"].as_str() {
            stop = reason.into();
        }
    }
    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop,
            input: None,
            output: None,
            events: Vec::new(),
        })?,
    )
}

fn response_body(
    id: u64,
    status: reqwest::StatusCode,
    body: serde_json::Value,
) -> crabbot_core::Result<Option<Response>> {
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Ollama rejected the request: {}.",
            body["error"].as_str().unwrap_or("request failed")
        )));
    }
    let text = body["message"]["content"].as_str().unwrap_or_default().to_string();
    let events = body["message"]["tool_calls"]
        .as_array()
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| {
                    let name = call["function"]["name"].as_str()?.to_owned();
                    Some(crabbot_core::types::Event::Tool {
                        name,
                        args: call["function"]["arguments"].clone(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if text.is_empty() && events.is_empty() {
        return Err(crabbot_core::Error::Denied(
            "Ollama response contained no text or tools.".into(),
        ));
    }
    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: body["done_reason"].as_str().unwrap_or("stop").into(),
            input: body["prompt_eval_count"].as_u64(),
            output: body["eval_count"].as_u64(),
            events,
        })?,
    )
}

fn response(id: u64, result: serde_json::Value) -> crabbot_core::Result<Option<Response>> {
    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "Ollama response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_LIMIT, Emitter, collect, generate, generate_at, generate_request, live_stream,
        messages, response_body, stream_body, tools,
    };
    use crabbot_core::types::{Content, Message, ModelRequest, Request, Role, ToolSpec};
    use serde_json::json;

    fn test_emitter() -> Emitter {
        let (output, _) = tokio::sync::mpsc::channel(8);
        Emitter::new(output)
    }

    fn model() -> ModelRequest {
        ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: vec![
                Message {
                    id: "1".into(),
                    session: "test".into(),
                    role: Role::System,
                    sender: None,
                    content: vec![Content::Text { text: "rules".into() }],
                },
                Message {
                    id: "2".into(),
                    session: "test".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![
                        Content::Text { text: "hello".into() },
                        Content::File {
                            uri: "file://note".into(),
                            name: "note".into(),
                            mime: None,
                        },
                        Content::Image { uri: "data:image/png;base64,aW1hZ2U=".into(), alt: None },
                    ],
                },
                Message {
                    id: "3".into(),
                    session: "test".into(),
                    role: Role::Assistant,
                    sender: None,
                    content: vec![Content::Text { text: "prior".into() }],
                },
                Message {
                    id: "4".into(),
                    session: "test".into(),
                    role: Role::Tool,
                    sender: None,
                    content: vec![Content::Text { text: "result".into() }],
                },
            ],
            stream: false,
            tools: vec![ToolSpec {
                name: "read".into(),
                description: Some("Read a file".into()),
                schema: json!({"type": "object"}),
            }],
        }
    }

    #[test]
    fn parses_success_and_failure_responses() {
        let result = response_body(3, reqwest::StatusCode::OK, json!({"message":{"content":"done"},"done_reason":"stop","prompt_eval_count":2,"eval_count":4})).unwrap().unwrap();
        assert_eq!(result.result.unwrap()["text"], "done");
        assert!(response_body(3, reqwest::StatusCode::BAD_REQUEST, json!({"error":"no"})).is_err());
        assert!(response_body(3, reqwest::StatusCode::OK, json!({})).is_err());
        let tool = response_body(
            3,
            reqwest::StatusCode::OK,
            json!({"message":{"tool_calls":[{"function":{"name":"read","arguments":{"path":"note"}}}]}}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tool.result.unwrap()["events"][0]["name"], "read");
    }

    #[test]
    fn builds_messages_for_each_role() {
        let messages = messages(&model()).unwrap();
        assert_eq!(messages[0], json!({"role": "system", "content": "rules"}));
        assert_eq!(
            messages[1],
            json!({
                "role": "user",
                "content": "hello\n[File: note]",
                "images": ["aW1hZ2U="]
            })
        );
        assert_eq!(messages[2], json!({"role": "assistant", "content": "prior"}));
        assert_eq!(messages[3], json!({"role": "tool", "content": "result"}));
        assert_eq!(tools(&model())[0]["function"]["name"], "read");
    }

    #[test]
    fn rejects_image_paths() {
        let mut input = model();
        input.messages[1].content[2] =
            Content::Image { uri: "file:///private/image.png".into(), alt: None };
        assert!(messages(&input).is_err());
    }

    #[tokio::test]
    async fn bounds_provider_bodies() {
        use futures_util::stream;

        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(b"ok".to_vec())]);
        assert_eq!(collect(chunks, "Ollama").await.unwrap(), "ok");
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])]);
        assert!(collect(chunks, "Ollama").await.is_err());
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]);
        assert!(collect(chunks, "Ollama").await.is_err());
    }

    #[test]
    fn parses_stream_events() {
        let result = stream_body(
            4,
            reqwest::StatusCode::OK,
            "{\"message\":{\"content\":\"hel\"},\"done\":false}\n{\"message\":{\"content\":\"lo\"},\"done_reason\":\"stop\"}\n",
        )
        .unwrap()
        .unwrap();
        let reply = result.result.unwrap();
        assert_eq!(reply["text"], "hello");
        assert_eq!(reply["stop"], "stop");
        assert!(stream_body(4, reqwest::StatusCode::BAD_REQUEST, "").is_err());
        assert!(stream_body(4, reqwest::StatusCode::OK, "invalid\n").is_err());
    }

    #[tokio::test]
    async fn validates_requests_and_network_failures() {
        let client = reqwest::Client::new();
        assert!(
            generate(&client, Request::call(1, "unknown", json!({})), test_emitter())
                .await
                .unwrap()
                .is_none()
        );
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "generate".into(), params: json!({}) };
        assert!(generate(&client, note, test_emitter()).await.unwrap().is_none());
        assert!(generate_request(&client, 1, model(), "http://127.0.0.1:1").await.is_err());
        let mut emitter = test_emitter();
        assert!(
            generate_at(&client, 1, model(), "http://127.0.0.1:1", &mut emitter).await.is_err()
        );
        let mut stream = model();
        stream.stream = true;
        assert!(generate_at(&client, 1, stream, "http://127.0.0.1:1", &mut emitter).await.is_err());
        assert!(
            generate(&client, Request::call(1, "generate", json!({})), test_emitter())
                .await
                .is_err()
        );
        assert!(response_body(3, reqwest::StatusCode::OK, json!({"message":{}})).is_err());
        assert!(response_body(3, reqwest::StatusCode::BAD_REQUEST, json!({})).is_err());
    }

    #[test]
    fn parses_stream_chunks() {
        let result = stream_body(
            1,
            reqwest::StatusCode::OK,
            "{\"message\":{\"content\":\"hello\"},\"done\":false}\n{\"message\":{},\"done\":true,\"done_reason\":\"stop\"}\n",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.result.unwrap()["text"], "hello");
    }

    #[tokio::test]
    async fn streams_text_and_tool_calls() {
        use futures_util::stream;

        let (output, mut events) = tokio::sync::mpsc::channel(8);
        let mut emitter = Emitter::new(output);
        let body = concat!(
            "{\"message\":{\"content\":\"Ready.\"},\"done\":false}\n",
            "{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"read\",\"arguments\":{\"path\":\"README.md\"}}}]},\"done\":true,\"done_reason\":\"stop\",\"prompt_eval_count\":4,\"eval_count\":2}\n",
        );
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(body.as_bytes().to_vec())]);
        let response = live_stream(5, chunks, &mut emitter).await.unwrap().unwrap();

        assert_eq!(response.result.as_ref().unwrap()["text"], "Ready.");
        assert_eq!(response.result.as_ref().unwrap()["events"][0]["name"], "read");
        assert_eq!(response.result.as_ref().unwrap()["events"][0]["args"]["path"], "README.md");
        assert_eq!(response.result.as_ref().unwrap()["input"], 4);
        let event: crabbot_core::types::Request =
            serde_json::from_value(events.try_recv().unwrap()).unwrap();
        assert!(matches!(
            event,
            crabbot_core::types::Request::Note { params, .. }
                if params["event"]["text"] == "Ready."
        ));
    }
}
