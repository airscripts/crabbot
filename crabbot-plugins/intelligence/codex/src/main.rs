#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::{Emitter, serve_events},
    types::{
        Capability, CommandSpec, Content, Event, Hello, ModelReply, ModelRequest, Protocol,
        Request, Response,
    },
};
use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

mod codex;

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("OpenAI client failed: {error}.")))?;
    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "codex".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Model, Capability::Vision],
            commands: vec![CommandSpec {
                name: "codex".into(),
                description: "Manage Codex ChatGPT sign-in.".into(),
                interactive: false,
            }],
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
    emitter: Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    if method == "command" {
        let text = codex::command(&params, emitter).await?;
        return Ok(Some(Response::ok(id, json!({"text": text}))));
    }
    if method != "generate" {
        return Ok(None);
    }
    let input: ModelRequest = serde_json::from_value(params)?;
    let Some(key) = credential()? else {
        return codex::generate(id, input, emitter).await;
    };
    let base = std::env::var("CRABBOT_CODEX_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".into());
    generate_at(client, id, input, &key, &base, emitter).await
}

async fn generate_at(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    mut emitter: Emitter,
) -> crabbot_core::Result<Option<Response>> {
    if input.stream {
        stream_request(client, id, input, key, base, &mut emitter).await
    } else {
        generate_request(client, id, input, key, base).await
    }
}

async fn generate_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let messages = messages(&input)?;
    let response = client
        .post(format!("{base}/chat/completions"))
        .bearer_auth(key)
        .json(&json!({"model": input.model, "messages": messages, "stream": false, "tools": tools(&input)}))
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("OpenAI request failed: {error}."))
        })?;
    let status = response.status();
    let body = read(response, "OpenAI").await?;
    let body: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
        crabbot_core::Error::Denied(format!("OpenAI response failed: {error}."))
    })?;
    response_body(id, status, body)
}

async fn stream_request(
    client: &reqwest::Client,
    id: u64,
    input: ModelRequest,
    key: &str,
    base: &str,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let messages = messages(&input)?;
    let response = client
        .post(format!("{base}/chat/completions"))
        .bearer_auth(key)
        .json(&json!({"model": input.model, "messages": messages, "stream": true, "tools": tools(&input)}))
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("OpenAI stream failed: {error}.")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "OpenAI stream was rejected with {status}."
        )));
    }
    stream_body(id, status, response.bytes_stream(), emitter).await
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

fn messages(input: &ModelRequest) -> crabbot_core::Result<Vec<serde_json::Value>> {
    input
        .messages
        .iter()
        .map(|message| {
            let role = match message.role {
                crabbot_core::types::Role::System => "system",
                crabbot_core::types::Role::User => "user",
                crabbot_core::types::Role::Assistant => "assistant",
                crabbot_core::types::Role::Tool => "user",
            };
            let mut parts = Vec::new();
            if message.role == crabbot_core::types::Role::Tool {
                parts.push(json!({
                    "type": "text",
                    "text": format!(
                        "[Tool result{}]",
                        message.sender.as_deref().map_or(String::new(), |name| format!(" from {name}"))
                    )
                }));
            }
            for content in &message.content {
                match content {
                    Content::Text { text } => parts.push(json!({"type": "text", "text": text})),
                    Content::Image { uri, .. }
                        if message.role != crabbot_core::types::Role::System =>
                    {
                        parts.push(json!({
                            "type": "image_url",
                            "image_url": {"url": image_url(uri)?, "detail": "auto"}
                        }));
                    }
                    Content::Image { alt, .. } => parts.push(json!({
                        "type": "text",
                        "text": alt.as_deref().unwrap_or("[Image attachment.]"),
                    })),
                    content => parts.push(json!({"type": "text", "text": content.render()})),
                }
            }
            let content = if parts.iter().all(|part| part["type"] == "text") {
                serde_json::Value::String(
                    parts
                        .iter()
                        .filter_map(|part| part["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            } else {
                serde_json::Value::Array(parts)
            };
            Ok(json!({"role": role, "content": content}))
        })
        .collect()
}

fn image_url(uri: &str) -> crabbot_core::Result<&str> {
    if uri.starts_with("https://") {
        return Ok(uri);
    }
    let Some((mime, encoded)) =
        uri.strip_prefix("data:").and_then(|value| value.split_once(";base64,"))
    else {
        return Err(crabbot_core::Error::Denied(
            "OpenAI received an unsupported image reference.".into(),
        ));
    };
    if !matches!(mime, "image/png" | "image/jpeg" | "image/gif" | "image/webp")
        || encoded.is_empty()
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(crabbot_core::Error::Denied(
            "OpenAI received an invalid image data URL.".into(),
        ));
    }
    Ok(uri)
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

#[derive(Default)]
struct ToolDelta {
    name: String,
    arguments: String,
}

async fn stream_body<S, C, E>(
    id: u64,
    status: reqwest::StatusCode,
    mut stream: S,
    emitter: &mut Emitter,
) -> crabbot_core::Result<Option<Response>>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "OpenAI stream was rejected with {status}."
        )));
    }
    let mut buffered = Vec::new();
    let mut bytes = 0_usize;
    let mut text = String::new();
    let mut pending = String::new();
    let mut tools = BTreeMap::<usize, ToolDelta>::new();
    let mut done = false;
    let mut ticker = tokio::time::interval(Duration::from_millis(80));
    ticker.tick().await;

    loop {
        tokio::select! {
            chunk = stream.next() => {
                let Some(chunk) = chunk else { break };
                let chunk = chunk.map_err(|error| {
                    crabbot_core::Error::Denied(format!("OpenAI stream failed: {error}."))
                })?;
                if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes) {
                    return Err(crabbot_core::Error::Denied(format!(
                        "OpenAI stream exceeded the {BODY_LIMIT}-byte limit."
                    )));
                }
                bytes = bytes.saturating_add(chunk.as_ref().len());
                buffered.extend_from_slice(chunk.as_ref());
                while let Some(end) = buffered.iter().position(|byte| *byte == b'\n') {
                    let line = buffered.drain(..=end).collect::<Vec<_>>();
                    if stream_line(&line, &mut text, &mut pending, &mut tools)? {
                        done = true;
                        break;
                    }
                    if pending.len() >= 128 {
                        emit(&mut pending, emitter).await?;
                    }
                }
                if done {
                    break;
                }
            }
            _ = ticker.tick(), if !pending.is_empty() => emit(&mut pending, emitter).await?,
        }
    }
    if !buffered.is_empty() {
        stream_line(&buffered, &mut text, &mut pending, &mut tools)?;
    }
    emit(&mut pending, emitter).await?;

    let events = tools
        .into_values()
        .map(|tool| {
            if tool.name.is_empty() {
                return Err(crabbot_core::Error::Denied(
                    "OpenAI stream returned a tool call without a name.".into(),
                ));
            }
            let args = if tool.arguments.is_empty() {
                json!({})
            } else {
                serde_json::from_str(&tool.arguments).map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "OpenAI stream returned invalid tool arguments: {error}."
                    ))
                })?
            };
            Ok(Event::Tool { name: tool.name, args })
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
    tools: &mut BTreeMap<usize, ToolDelta>,
) -> crabbot_core::Result<bool> {
    let line = std::str::from_utf8(line).map_err(|error| {
        crabbot_core::Error::Denied(format!("OpenAI stream was not UTF-8: {error}."))
    })?;
    let Some(data) = line.trim().strip_prefix("data:") else {
        return Ok(false);
    };
    let data = data.trim();
    if data == "[DONE]" {
        return Ok(true);
    }
    let value: serde_json::Value = serde_json::from_str(data)?;
    let delta = &value["choices"][0]["delta"];
    if let Some(part) = delta["content"].as_str() {
        text.push_str(part);
        pending.push_str(part);
    }
    if let Some(calls) = delta["tool_calls"].as_array() {
        for call in calls {
            let index = call["index"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|index| *index < 16)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "OpenAI stream exceeded the tool-call limit.".into(),
                    )
                })?;
            let tool = tools.entry(index).or_default();
            if let Some(name) = call["function"]["name"].as_str() {
                tool.name.push_str(name);
            }
            if let Some(arguments) = call["function"]["arguments"].as_str() {
                tool.arguments.push_str(arguments);
            }
        }
    }
    Ok(false)
}

async fn emit(pending: &mut String, emitter: &mut Emitter) -> crabbot_core::Result<()> {
    if !pending.is_empty() {
        emitter.event(json!({"kind": "text", "text": std::mem::take(pending)})).await?;
    }
    Ok(())
}

fn response_body(
    id: u64,
    status: reqwest::StatusCode,
    body: serde_json::Value,
) -> crabbot_core::Result<Option<Response>> {
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied(format!(
            "OpenAI rejected the request: {}.",
            body["error"]["message"].as_str().unwrap_or("request failed")
        )));
    }
    let choice =
        body["choices"].as_array().and_then(|choices| choices.first()).ok_or_else(|| {
            crabbot_core::Error::Denied("OpenAI response contained no choices.".into())
        })?;
    let text = choice["message"]["content"].as_str().unwrap_or_default().to_string();
    let events = choice["message"]["tool_calls"]
        .as_array()
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| {
                    let name = call["function"]["name"].as_str()?.to_owned();
                    let args = call["function"]["arguments"]
                        .as_str()
                        .and_then(|value| serde_json::from_str(value).ok())
                        .unwrap_or_else(|| call["function"]["arguments"].clone());
                    Some(crabbot_core::types::Event::Tool { name, args })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if text.is_empty() && events.is_empty() {
        return Err(crabbot_core::Error::Denied(
            "OpenAI response contained no text or tools.".into(),
        ));
    }
    let usage = &body["usage"];
    response(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: choice["finish_reason"].as_str().unwrap_or("stop").into(),
            input: usage["prompt_tokens"].as_u64(),
            output: usage["completion_tokens"].as_u64(),
            events,
        })?,
    )
}

fn response(id: u64, result: serde_json::Value) -> crabbot_core::Result<Option<Response>> {
    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "OpenAI response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

fn credential() -> crabbot_core::Result<Option<String>> {
    if let Ok(value) = std::env::var("CRABBOT_CODEX_KEY")
        && !value.trim().is_empty()
    {
        return Ok(Some(value));
    }

    if let Some(value) = keyring("codex") {
        return Ok(Some(value));
    }

    if let Some(path) = credentials_path()
        && path.is_file()
    {
        crabbot_file::private(&path)?;
        if let Some(key) = credential_file(&path)? {
            return Ok(Some(key));
        }
    }

    Ok(None)
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

fn credential_file(path: &std::path::Path) -> crabbot_core::Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    if crabbot_file::private(path).is_err() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(path).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials could not be read: {error}."))
    })?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials are invalid: {error}."))
    })?;
    Ok(value["CRABBOT_CODEX_KEY"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned))
}

fn credentials_path() -> Option<std::path::PathBuf> {
    std::env::var_os("CRABBOT_CREDENTIALS").map(std::path::PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_LIMIT, Emitter, collect, credential, credential_file, credentials_path, generate,
        generate_at, generate_request, keyring, messages, response_body, stream_body,
    };
    use crabbot_core::types::{Content, ModelRequest, Request, Role, ToolSpec};
    use serde_json::json;

    fn quiet_emitter() -> (Emitter, tokio::sync::mpsc::Receiver<serde_json::Value>) {
        let (output, receiver) = tokio::sync::mpsc::channel(8);
        (Emitter::new(output), receiver)
    }

    fn model() -> ModelRequest {
        ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: vec![
                crabbot_core::types::Message {
                    id: "1".into(),
                    session: "test".into(),
                    role: Role::System,
                    sender: None,
                    content: vec![Content::Text { text: "rules".into() }],
                },
                crabbot_core::types::Message {
                    id: "2".into(),
                    session: "test".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![
                        Content::Text { text: "hello".into() },
                        Content::Audio { uri: "file://voice".into(), mime: None },
                    ],
                },
                crabbot_core::types::Message {
                    id: "3".into(),
                    session: "test".into(),
                    role: Role::Assistant,
                    sender: None,
                    content: vec![Content::Text { text: "prior".into() }],
                },
                crabbot_core::types::Message {
                    id: "4".into(),
                    session: "test".into(),
                    role: Role::Tool,
                    sender: None,
                    content: vec![Content::Text { text: "result".into() }],
                },
            ],
            stream: false,
            tools: Vec::new(),
        }
    }

    #[test]
    fn reads_only_explicit_codex_credentials() {
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("crabbot-codex-auth-{}.json", std::process::id()));
        std::fs::write(&path, r#"{"CRABBOT_CODEX_KEY":"key"}"#).unwrap();
        #[cfg(unix)]
        {
            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o600);
            std::fs::set_permissions(&path, permissions).unwrap();
        }
        assert_eq!(credential_file(&path).unwrap(), Some("key".into()));
        std::fs::write(&path, r#"{"access_token":"secret"}"#).unwrap();
        assert_eq!(credential_file(&path).unwrap(), None);
        std::fs::write(&path, "broken").unwrap();
        assert!(credential_file(&path).is_err());
        std::fs::write(&path, r#"{"CRABBOT_CODEX_KEY":" "}"#).unwrap();
        assert_eq!(credential_file(&path).unwrap(), None);
        #[cfg(unix)]
        {
            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o644);
            std::fs::set_permissions(&path, permissions).unwrap();
            std::fs::write(&path, r#"{"CRABBOT_CODEX_KEY":"key"}"#).unwrap();
            assert_eq!(credential_file(&path).unwrap(), Some("key".into()));
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
        assert_eq!(credential_file(&path.with_file_name("missing-auth.json")).unwrap(), None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checks_optional_credential_sources_without_fallbacks() {
        assert!(keyring("codex").is_none());
        assert!(credentials_path().is_none());
        assert!(credential().unwrap().is_none());
    }

    #[tokio::test]
    async fn bounds_provider_bodies() {
        use futures_util::stream;

        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(b"ok".to_vec())]);
        assert_eq!(collect(chunks, "OpenAI").await.unwrap(), "ok");
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])]);
        assert!(collect(chunks, "OpenAI").await.is_err());
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]);
        assert!(collect(chunks, "OpenAI").await.is_err());
    }

    #[test]
    fn builds_messages_for_each_role() {
        let messages = messages(&model()).unwrap();
        assert_eq!(messages[0], json!({"role": "system", "content": "rules"}));
        assert_eq!(messages[1], json!({"role": "user", "content": "hello\n[Audio attachment.]"}));
        assert_eq!(messages[2], json!({"role": "assistant", "content": "prior"}));
        assert_eq!(messages[3], json!({"role": "user", "content": "[Tool result]\nresult"}));
    }

    #[test]
    fn encodes_images_and_rejects_local_paths() {
        let mut input = model();
        input.messages[1]
            .content
            .push(Content::Image { uri: "data:image/png;base64,aW1hZ2U=".into(), alt: None });
        let values = messages(&input).unwrap();
        assert_eq!(
            values[1]["content"],
            json!([
                {"type": "text", "text": "hello"},
                {"type": "text", "text": "[Audio attachment.]"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "data:image/png;base64,aW1hZ2U=",
                        "detail": "auto"
                    }
                }
            ])
        );
        input.messages[1].content[2] =
            Content::Image { uri: "file:///private/image.png".into(), alt: None };
        assert!(messages(&input).is_err());
    }

    #[tokio::test]
    async fn generates_from_a_compatible_response() {
        let result = response_body(
            7,
            reqwest::StatusCode::OK,
            json!({"choices":[{"message":{"content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.result.unwrap()["text"], "done");
    }

    #[test]
    fn parses_tool_calls() {
        let result = response_body(
            7,
            reqwest::StatusCode::OK,
            json!({
                "choices": [{"message": {"content": null, "tool_calls": [{
                    "function": {"name": "read", "arguments": "{\\\"path\\\":\\\"note\\\"}"}
                }]}, "finish_reason": "tool_calls"}]
            }),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.result.unwrap()["events"][0]["name"], "read");
    }

    #[tokio::test]
    async fn rejects_provider_and_request_errors() {
        let client = reqwest::Client::new();
        assert!(
            response_body(1, reqwest::StatusCode::UNAUTHORIZED, json!({"error":{"message":"no"}}))
                .is_err()
        );
        assert!(response_body(1, reqwest::StatusCode::OK, json!({})).is_err());
        assert!(
            generate_request(&client, 1, model(), "secret", "http://127.0.0.1:1/v1").await.is_err()
        );
        assert!(
            generate(&client, Request::call(1, "unknown", json!({})), quiet_emitter().0)
                .await
                .unwrap()
                .is_none()
        );
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "generate".into(), params: json!({}) };
        assert!(generate(&client, note, quiet_emitter().0).await.unwrap().is_none());
        assert!(
            generate(&client, Request::call(1, "generate", json!({})), quiet_emitter().0)
                .await
                .is_err()
        );
        assert!(
            generate_at(&client, 1, model(), "secret", "http://127.0.0.1:1/v1", quiet_emitter().0,)
                .await
                .is_err()
        );
        let mut stream = model();
        stream.stream = true;
        assert!(
            generate_at(&client, 1, stream, "secret", "http://127.0.0.1:1/v1", quiet_emitter().0,)
                .await
                .is_err()
        );
        let mut stream = model();
        stream.stream = true;
        stream.tools.push(ToolSpec {
            name: "read".into(),
            description: None,
            schema: json!({"type": "object"}),
        });
        assert!(
            generate_at(&client, 1, stream, "secret", "http://127.0.0.1:1/v1", quiet_emitter().0,)
                .await
                .is_err()
        );
        assert!(response_body(1, reqwest::StatusCode::OK, json!({"choices":[]})).is_err());
        assert!(
            response_body(1, reqwest::StatusCode::OK, json!({"choices":[{"message":{}}]}),)
                .is_err()
        );
        assert!(
            response_body(
                1,
                reqwest::StatusCode::OK,
                json!({"choices":[{"message":{"content":"done"}}]})
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn parses_stream_chunks() {
        use futures_util::stream;

        let (mut emitter, mut events) = quiet_emitter();
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(
                b"event: message\ndata: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n"
                    .to_vec(),
            ),
            Ok(b"data: [DONE]\n".to_vec()),
        ]);
        let result =
            stream_body(2, reqwest::StatusCode::OK, chunks, &mut emitter).await.unwrap().unwrap();
        assert_eq!(result.result.unwrap()["text"], "ok");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "ok");

        let (mut emitter, _) = quiet_emitter();
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(Vec::new())]);
        assert!(
            stream_body(2, reqwest::StatusCode::BAD_REQUEST, chunks, &mut emitter).await.is_err()
        );
        let (mut emitter, _) = quiet_emitter();
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>(b"data: nope\n".to_vec())]);
        assert!(stream_body(2, reqwest::StatusCode::OK, chunks, &mut emitter).await.is_err());
    }

    #[tokio::test]
    async fn parses_streamed_tool_calls() {
        use futures_util::stream;

        let (mut emitter, _) = quiet_emitter();
        let first = json!({"choices": [{"delta": {"tool_calls": [{
            "index": 0,
            "function": {"name": "read", "arguments": "{\"path\":"}
        }]}}]});
        let second = json!({"choices": [{"delta": {"tool_calls": [{
            "index": 0,
            "function": {"arguments": "\"note\"}"}
        }]}}]});
        let chunks = stream::iter(vec![
            Ok::<_, std::io::Error>(format!("data: {first}\n").into_bytes()),
            Ok(format!("data: {second}\ndata: [DONE]\n").into_bytes()),
        ]);
        let result =
            stream_body(3, reqwest::StatusCode::OK, chunks, &mut emitter).await.unwrap().unwrap();
        assert_eq!(result.result.unwrap()["events"][0]["name"], "read");
    }
}
