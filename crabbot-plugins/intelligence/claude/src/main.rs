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
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Anthropic client failed: {error}."))
        })?;
    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "claude".into(),
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
    let primary = std::env::var("CRABBOT_CLAUDE_KEY").ok();
    let key = key(primary.as_deref())
        .or_else(|_| {
            keyring("claude").ok_or_else(|| {
                crabbot_core::Error::Denied("CRABBOT_CLAUDE_KEY is not configured.".into())
            })
        })
        .or_else(|_| file_key())?;
    let base = std::env::var("CRABBOT_CLAUDE_BASE_URL")
        .unwrap_or_else(|_| "https://api.anthropic.com".into());
    generate_at(client, id, input, &key, &base, &mut emitter).await
}

async fn generate_at(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    if input.stream {
        stream_request(client, id, input, key, base, emitter).await
    } else {
        generate_request(client, id, input, key, base).await
    }
}

fn key(primary: Option<&str>) -> crabbot_core::Result<String> {
    [primary]
        .into_iter()
        .flatten()
        .find(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| crabbot_core::Error::Denied("CRABBOT_CLAUDE_KEY is not configured.".into()))
}

fn file_key() -> crabbot_core::Result<String> {
    let Some(path) = std::env::var_os("CRABBOT_CREDENTIALS") else {
        return Err(crabbot_core::Error::Denied("CRABBOT_CLAUDE_KEY is not configured.".into()));
    };
    crabbot_file::private(&path)?;
    let text = std::fs::read_to_string(path).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials could not be read: {error}."))
    })?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials are invalid: {error}."))
    })?;
    key(value["CRABBOT_CLAUDE_KEY"].as_str())
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

async fn generate_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    request(client, id, input, key, base, false).await
}

async fn stream_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let (system, messages) = messages(&input)?;
    let response = client
        .post(format!("{base}/v1/messages"))
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .json(&json!({"model": input.model, "max_tokens": 4096, "system": system, "messages": messages, "tools": tools(&input), "stream": true}))
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Anthropic stream failed: {error}."))
        })?;
    let status = response.status();
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Anthropic stream was rejected with {status}."
        )));
    }
    live_stream(id, response.bytes_stream(), emitter).await
}

async fn request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    stream: bool,
) -> crabbot_core::Result<Option<Response>> {
    let (system, messages) = messages(&input)?;
    let response = client
        .post(format!("{base}/v1/messages"))
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .json(&json!({"model": input.model, "max_tokens": 4096, "system": system, "messages": messages, "tools": tools(&input), "stream": stream}))
        .send()
        .await
        .map_err(|error| {
            let kind = if stream { "stream" } else { "request" };
            crabbot_core::Error::Denied(format!("Anthropic {kind} failed: {error}."))
        })?;
    let status = response.status();
    let body = read(response, "Anthropic").await?;
    if stream {
        return stream_body(id, status, &body);
    }
    let body: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("Anthropic response failed: {error}."))
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
    arguments: String,
    input: serde_json::Value,
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
    let mut stop = "end_turn".to_string();
    let mut ticker = tokio::time::interval(Duration::from_millis(80));
    ticker.tick().await;

    loop {
        tokio::select! {
            chunk = stream.next() => {
                let Some(chunk) = chunk else { break };
                let chunk = chunk.map_err(|error| {
                    crabbot_core::Error::Denied(format!("Anthropic stream failed: {error}."))
                })?;
                if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes) {
                    return Err(crabbot_core::Error::Denied(format!(
                        "Anthropic stream exceeded the {BODY_LIMIT}-byte limit."
                    )));
                }
                bytes = bytes.saturating_add(chunk.as_ref().len());
                buffered.extend_from_slice(chunk.as_ref());
                while let Some(end) = buffered.iter().position(|byte| *byte == b'\n') {
                    let line = buffered.drain(..=end).collect::<Vec<_>>();
                    if anthropic_line(&line, &mut text, &mut pending, &mut tools, &mut stop)? {
                        break;
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
        anthropic_line(&buffered, &mut text, &mut pending, &mut tools, &mut stop)?;
    }
    emit(&mut pending, emitter).await?;

    let events = tools
        .into_values()
        .map(|tool| {
            if tool.name.is_empty() {
                return Err(crabbot_core::Error::Denied(
                    "Anthropic stream returned a tool call without a name.".into(),
                ));
            }
            let args = if tool.arguments.is_empty() {
                tool.input
            } else {
                serde_json::from_str(&tool.arguments).map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Anthropic stream returned invalid tool arguments: {error}."
                    ))
                })?
            };
            Ok(Event::Tool { name: tool.name, args })
        })
        .collect::<crabbot_core::Result<Vec<_>>>()?;

    response(
        id,
        serde_json::to_value(ModelReply { text, stop, input: None, output: None, events })?,
    )
}

fn anthropic_line(
    line: &[u8],
    text: &mut String,
    pending: &mut String,
    tools: &mut BTreeMap<usize, ToolDelta>,
    stop: &mut String,
) -> crabbot_core::Result<bool> {
    let line = std::str::from_utf8(line).map_err(|error| {
        crabbot_core::Error::Denied(format!("Anthropic stream was not UTF-8: {error}."))
    })?;
    let Some(data) = line.trim().strip_prefix("data:") else {
        return Ok(false);
    };
    let value: serde_json::Value = serde_json::from_str(data.trim())?;
    match value["type"].as_str().unwrap_or_default() {
        "error" => {
            return Err(crabbot_core::Error::Denied(
                "Anthropic stream returned an error event.".into(),
            ));
        }
        "content_block_start" => {
            if value["content_block"]["type"] == "tool_use" {
                let index = tool_index(&value["index"])?;
                let tool = tools.entry(index).or_default();
                tool.name = value["content_block"]["name"].as_str().unwrap_or_default().into();
                tool.input = value["content_block"]["input"].clone();
            }
        }
        "content_block_delta" => {
            if let Some(part) = value["delta"]["text"].as_str() {
                append_text(part, text, pending)?;
            }
            if let Some(part) = value["delta"]["partial_json"].as_str() {
                let index = tool_index(&value["index"])?;
                let tool = tools.entry(index).or_default();
                if part.len() > BODY_LIMIT.saturating_sub(tool.arguments.len()) {
                    return Err(crabbot_core::Error::Limit(
                        "Anthropic tool arguments exceeded the response limit.".into(),
                    ));
                }
                tool.arguments.push_str(part);
            }
        }
        "message_delta" => {
            if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                *stop = reason.into();
            }
        }
        _ => {}
    }
    Ok(false)
}

fn tool_index(value: &serde_json::Value) -> crabbot_core::Result<usize> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|index| *index < 16)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("Anthropic stream exceeded the tool-call limit.".into())
        })
}

fn append_text(part: &str, text: &mut String, pending: &mut String) -> crabbot_core::Result<()> {
    if part.len() > BODY_LIMIT.saturating_sub(text.len()) {
        return Err(crabbot_core::Error::Limit(
            "Anthropic stream exceeded the response limit.".into(),
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

fn messages(input: &ModelRequest) -> crabbot_core::Result<(String, Vec<serde_json::Value>)> {
    let mut system = Vec::new();
    let mut messages = Vec::new();
    for message in &input.messages {
        let mut blocks = Vec::new();
        for content in &message.content {
            match content {
                Content::Text { text } => blocks.push(json!({"type": "text", "text": text})),
                Content::Image { uri, alt }
                    if message.role != crabbot_core::types::Role::System =>
                {
                    blocks.push(image_block(uri)?);
                    if let Some(alt) = alt {
                        blocks.push(json!({"type": "text", "text": alt}));
                    }
                }
                Content::Image { alt, .. } => blocks.push(json!({
                    "type": "text",
                    "text": alt.as_deref().unwrap_or("[Image attachment.]"),
                })),
                content => blocks.push(json!({"type": "text", "text": content.render()})),
            }
        }
        let has_image = blocks.iter().any(|block| block["type"] == "image");
        let text =
            blocks.iter().filter_map(|block| block["text"].as_str()).collect::<Vec<_>>().join("\n");
        match message.role {
            crabbot_core::types::Role::System => system.push(text),
            crabbot_core::types::Role::User => messages.push(json!({
                "role": "user",
                "content": if has_image { json!(blocks) } else { json!(text) }
            })),
            crabbot_core::types::Role::Assistant => messages.push(json!({
                "role": "assistant",
                "content": if has_image { json!(blocks) } else { json!(text) }
            })),
            crabbot_core::types::Role::Tool => {
                let prefix = format!(
                    "[Tool result{}]",
                    message.sender.as_deref().map_or(String::new(), |name| format!(" from {name}"))
                );
                let content = if has_image {
                    let mut tool_blocks = vec![json!({"type": "text", "text": prefix})];
                    tool_blocks.extend(blocks);
                    json!(tool_blocks)
                } else {
                    json!(format!("{prefix}\n{text}"))
                };
                messages.push(json!({"role": "user", "content": content}));
            }
        }
    }
    Ok((system.join("\n"), messages))
}

fn image_block(uri: &str) -> crabbot_core::Result<serde_json::Value> {
    if uri.starts_with("https://") {
        return Ok(json!({
            "type": "image",
            "source": {"type": "url", "url": uri}
        }));
    }
    let Some((mime, data)) =
        uri.strip_prefix("data:").and_then(|value| value.split_once(";base64,"))
    else {
        return Err(crabbot_core::Error::Denied(
            "Anthropic received an unsupported image reference.".into(),
        ));
    };
    if !matches!(mime, "image/png" | "image/jpeg" | "image/gif" | "image/webp")
        || data.is_empty()
        || !data
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(crabbot_core::Error::Denied(
            "Anthropic received an invalid image data URL.".into(),
        ));
    }
    Ok(json!({
        "type": "image",
        "source": {"type": "base64", "media_type": mime, "data": data}
    }))
}

fn tools(input: &ModelRequest) -> Vec<serde_json::Value> {
    input
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.schema,
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
            "Anthropic stream was rejected with {status}."
        )));
    }
    let mut text = String::new();
    let mut stop = "stream".to_string();
    for line in body.lines() {
        let Some(value) = line.trim().strip_prefix("data:") else {
            continue;
        };
        let value: serde_json::Value = serde_json::from_str(value.trim())?;
        if let Some(part) = value["delta"]["text"].as_str() {
            text.push_str(part);
        }
        if let Some(reason) = value["delta"]["stop_reason"].as_str() {
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
            "Anthropic rejected the request: {}.",
            body["error"]["message"].as_str().unwrap_or("request failed")
        )));
    }
    let blocks = body["content"].as_array().ok_or_else(|| {
        crabbot_core::Error::Denied("Anthropic response contained no content.".into())
    })?;
    let text = blocks.iter().filter_map(|content| content["text"].as_str()).collect::<String>();
    let events = blocks
        .iter()
        .filter(|content| content["type"] == "tool_use")
        .filter_map(|content| {
            Some(crabbot_core::types::Event::Tool {
                name: content["name"].as_str()?.into(),
                args: content["input"].clone(),
            })
        })
        .collect::<Vec<_>>();
    if text.is_empty() && events.is_empty() {
        return Err(crabbot_core::Error::Denied(
            "Anthropic response contained no text or tools.".into(),
        ));
    }
    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: body["stop_reason"].as_str().unwrap_or("stop").into(),
            input: body["usage"]["input_tokens"].as_u64(),
            output: body["usage"]["output_tokens"].as_u64(),
            events,
        })?,
    )
}

fn response(id: u64, result: serde_json::Value) -> crabbot_core::Result<Option<Response>> {
    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "Anthropic response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_LIMIT, Emitter, anthropic_line, append_text, collect, generate, generate_at,
        generate_request, image_block, key, keyring, live_stream, messages, response,
        response_body, stream_body, tool_index, tools,
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
                    id: "system".into(),
                    session: "test".into(),
                    role: Role::System,
                    sender: None,
                    content: vec![Content::Text { text: "rules".into() }],
                },
                Message {
                    id: "user".into(),
                    session: "test".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: "hello".into() }],
                },
                Message {
                    id: "assistant".into(),
                    session: "test".into(),
                    role: Role::Assistant,
                    sender: None,
                    content: vec![Content::Text { text: "prior".into() }],
                },
                Message {
                    id: "tool".into(),
                    session: "test".into(),
                    role: Role::Tool,
                    sender: None,
                    content: vec![
                        Content::Text { text: "result".into() },
                        Content::Image { uri: "data:image/png;base64,aW1hZ2U=".into(), alt: None },
                    ],
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
        let result = response_body(3, reqwest::StatusCode::OK, json!({"content":[{"text":"done"}],"stop_reason":"end","usage":{"input_tokens":2,"output_tokens":4}})).unwrap().unwrap();
        assert_eq!(result.result.unwrap()["text"], "done");
        assert!(
            response_body(3, reqwest::StatusCode::BAD_REQUEST, json!({"error":{"message":"no"}}))
                .is_err()
        );
        assert!(response_body(3, reqwest::StatusCode::OK, json!({})).is_err());
        let tool = response_body(
            3,
            reqwest::StatusCode::OK,
            json!({"content":[{"type":"tool_use","name":"read","input":{"path":"note"}}]}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tool.result.unwrap()["events"][0]["name"], "read");
    }

    #[test]
    fn builds_messages_and_system_prompt() {
        let (system, messages) = messages(&model()).unwrap();
        assert_eq!(system, "rules");
        assert_eq!(messages[0], json!({"role": "user", "content": "hello"}));
        assert_eq!(messages[1], json!({"role": "assistant", "content": "prior"}));
        assert_eq!(
            messages[2],
            json!({
                "role": "user",
                "content": [
                    {"type": "text", "text": "[Tool result]"},
                    {"type": "text", "text": "result"},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": "aW1hZ2U="
                        }
                    }
                ]
            })
        );
        assert_eq!(tools(&model())[0]["name"], "read");
    }

    #[test]
    fn builds_image_content_blocks_and_rejects_local_paths() {
        let mut input = model();
        input.messages[1]
            .content
            .push(Content::Image { uri: "data:image/jpeg;base64,aW1hZ2U=".into(), alt: None });
        let (_, values) = messages(&input).unwrap();
        assert_eq!(
            values[0]["content"],
            json!([
                {"type": "text", "text": "hello"},
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": "aW1hZ2U="
                }}
            ])
        );
        input.messages[1]
            .content
            .push(Content::Image { uri: "file:///private/image.png".into(), alt: None });
        assert!(messages(&input).is_err());
        assert!(image_block("https://example.com/image.png").is_ok());
        assert!(image_block("data:image/bmp;base64,abc").is_err());
        assert!(image_block("data:image/png;base64,").is_err());
        assert!(image_block("data:image/png;base64,not ok").is_err());
    }

    #[test]
    fn validates_api_keys() {
        assert_eq!(key(Some("primary")).unwrap(), "primary");
        assert!(key(Some(" ")).is_err());
        assert!(key(None).is_err());
        assert!(keyring("claude").is_none());
        assert!(tool_index(&json!(16)).is_err());
        let mut text = String::new();
        let mut pending = String::new();
        assert!(append_text(&"x".repeat(BODY_LIMIT + 1), &mut text, &mut pending).is_err());
    }

    #[test]
    fn covers_stream_line_error_and_tool_limit_paths() {
        let mut text = String::new();
        let mut pending = String::new();
        let mut tools = std::collections::BTreeMap::new();
        let mut stop = String::new();
        assert!(anthropic_line(&[0xff], &mut text, &mut pending, &mut tools, &mut stop).is_err());

        assert!(
            anthropic_line(
                br#"data: {"type":"error"}"#,
                &mut text,
                &mut pending,
                &mut tools,
                &mut stop,
            )
            .is_err()
        );

        assert!(
            anthropic_line(
                br#"data: {"type":"content_block_delta","index":16,"delta":{"partial_json":"{}"}}"#,
                &mut text,
                &mut pending,
                &mut tools,
                &mut stop,
            )
            .is_err()
        );

        assert!(
            anthropic_line(
                br#"data: {"type":"message_delta","delta":{"stop_reason":"stop"}}"#,
                &mut text,
                &mut pending,
                &mut tools,
                &mut stop,
            )
            .is_ok()
        );

        assert_eq!(stop, "stop");
    }

    #[tokio::test]
    async fn covers_tool_defaults_and_frame_bounds() {
        let (output, _) = tokio::sync::mpsc::channel(1);
        let mut emitter = Emitter::new(output);

        let result = live_stream(
            1,
            futures_util::stream::iter(vec![Ok::<_, std::io::Error>(
                br#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","input":{}}}
"#.to_vec(),
            )]),
            &mut emitter,
        )
        .await;

        assert!(result.is_err());
        assert!(response(1, json!({"large": "x".repeat(crabbot_core::jsonl::MAX)})).is_err());
    }

    #[tokio::test]
    async fn bounds_provider_bodies() {
        use futures_util::stream;

        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(b"ok".to_vec())]);
        assert_eq!(collect(chunks, "Anthropic").await.unwrap(), "ok");
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])]);
        assert!(collect(chunks, "Anthropic").await.is_err());
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]);
        assert!(collect(chunks, "Anthropic").await.is_err());
    }

    #[test]
    fn parses_stream_events() {
        let result = stream_body(
            4,
            reqwest::StatusCode::OK,
            "event: content_block_delta\ndata: {\"delta\":{\"text\":\"hel\"}}\n\ndata: {\"delta\":{\"text\":\"lo\",\"stop_reason\":\"end_turn\"}}\n",
        )
        .unwrap()
        .unwrap();
        let reply = result.result.unwrap();
        assert_eq!(reply["text"], "hello");
        assert_eq!(reply["stop"], "end_turn");
        assert!(stream_body(4, reqwest::StatusCode::BAD_REQUEST, "").is_err());
        assert!(stream_body(4, reqwest::StatusCode::OK, "data: invalid\n").is_err());
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
        assert!(
            generate_request(&client, 1, model(), "secret", "http://127.0.0.1:1").await.is_err()
        );
        let mut emitter = test_emitter();
        assert!(
            generate_at(&client, 1, model(), "secret", "http://127.0.0.1:1", &mut emitter)
                .await
                .is_err()
        );
        let mut stream = model();
        stream.stream = true;
        assert!(
            generate_at(&client, 1, stream, "secret", "http://127.0.0.1:1", &mut emitter)
                .await
                .is_err()
        );
        assert!(
            generate(&client, Request::call(1, "generate", json!({})), test_emitter())
                .await
                .is_err()
        );
        assert!(response_body(3, reqwest::StatusCode::OK, json!({"content":[]})).is_err());
        assert!(response_body(3, reqwest::StatusCode::BAD_REQUEST, json!({})).is_err());
    }

    #[test]
    fn parses_stream_chunks() {
        let result = stream_body(
            1,
            reqwest::StatusCode::OK,
            "data: {\"delta\":{\"text\":\"hello\"}}\n\ndata: {\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
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
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"name\":\"read\",\"input\":{}}}\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Ready.\"}}\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n",
        );
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(body.as_bytes().to_vec())]);
        let response = live_stream(5, chunks, &mut emitter).await.unwrap().unwrap();

        assert_eq!(response.result.as_ref().unwrap()["text"], "Ready.");
        assert_eq!(response.result.as_ref().unwrap()["events"][0]["name"], "read");
        assert_eq!(response.result.as_ref().unwrap()["events"][0]["args"]["path"], "README.md");
        let event: crabbot_core::types::Request =
            serde_json::from_value(events.try_recv().unwrap()).unwrap();
        assert!(matches!(
            event,
            crabbot_core::types::Request::Note { params, .. }
                if params["event"]["text"] == "Ready."
        ));
    }
}
