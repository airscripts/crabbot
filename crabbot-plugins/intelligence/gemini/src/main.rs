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
const DEFAULT_MODEL: &str = "gemini-3.8-flash";
const LEGACY_MODEL: &str = "gemini-2.5-flash";
const HOST_DEFAULT_MODEL: &str = "gpt-6-luna";

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
    let model = effective_model(&input);

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
            events.push(Event::Tool {
                name: name.into(),
                args: call["args"].clone(),
                id: call["id"].as_str().map(str::to_owned),
                thought_signature: part["thoughtSignature"].as_str().map(str::to_owned),
            });
        }
    }

    (text, events)
}

fn request_body(input: &ModelRequest) -> crabbot_core::Result<Value> {
    let model = effective_model(input);

    let require_signature = requires_thought_signature(&model);

    let mut contents = Vec::new();
    let mut system = Vec::new();
    let mut calls = VecDeque::new();
    let mut tool_parts = None;

    let flush_tool_parts = |contents: &mut Vec<Value>, tool_parts: &mut Option<Vec<Value>>| {
        if let Some(parts) = tool_parts.take() {
            contents.push(json!({"role": "user", "parts": parts}));
        }
    };

    for message in &input.messages {
        if message.role == Role::System {
            flush_tool_parts(&mut contents, &mut tool_parts);
            system.extend(message.content.iter().flat_map(content_parts));
            continue;
        }

        if message.role == Role::Assistant {
            flush_tool_parts(&mut contents, &mut tool_parts);
            let (parts, found) = assistant_parts(message, require_signature)?;
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

            if matched.as_ref().is_some_and(|call| call.legacy) {
                tool_parts.get_or_insert_with(Vec::new).push(json!({
                    "text": format!("[Tool result {name}]: {}", rendered_content(&message.content)),
                }));

                continue;
            }

            let mut response = json!({
                "name": name,
                "response": {"output": rendered_content(&message.content)},
            });

            if let Some(id) = matched.map(|call| call.id) {
                response["id"] = json!(id);
            }

            tool_parts.get_or_insert_with(Vec::new).push(json!({"functionResponse": response}));
            continue;
        }

        flush_tool_parts(&mut contents, &mut tool_parts);
        contents.push(json!({
            "role": "user",
            "parts": message.content.iter().flat_map(content_parts).collect::<Vec<_>>(),
        }));
    }

    flush_tool_parts(&mut contents, &mut tool_parts);

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
    // Compatibility layer for Gemini 2.x transcripts. Remove this branch when
    // Google no longer supports those models and all persisted histories have
    // migrated to Gemini 3.x thought signatures.
    model.to_ascii_lowercase().starts_with("gemini-3")
}

fn effective_model(input: &ModelRequest) -> String {
    if input.model.trim().is_empty()
        || input.model == "default"
        || input.model == HOST_DEFAULT_MODEL
    {
        if has_legacy_tool_history(input) {
            return LEGACY_MODEL.into();
        }

        return DEFAULT_MODEL.into();
    }

    input.model.clone()
}

fn has_legacy_tool_history(input: &ModelRequest) -> bool {
    input.messages.iter().any(|message| {
        if message.role != Role::Assistant {
            return false;
        }

        let mut saw_tool_call = false;

        for content in &message.content {
            match content {
                Content::ToolCall { thought_signature, .. } => {
                    if thought_signature.is_none() && !saw_tool_call {
                        return true;
                    }

                    saw_tool_call = true;
                }

                Content::Text { text } => {
                    for line in text.split_inclusive('\n') {
                        let candidate = line.strip_suffix('\n').unwrap_or(line);
                        let is_tool_call = candidate
                            .strip_prefix("[Tool call ")
                            .and_then(|value| {
                                let (_, args) = value.split_once("]: ")?;
                                serde_json::from_str::<Value>(args).ok()
                            })
                            .is_some();

                        if is_tool_call {
                            if !saw_tool_call {
                                return true;
                            }

                            saw_tool_call = true;
                        }
                    }
                }

                _ => {}
            }
        }

        false
    })
}

#[derive(Debug)]
struct FunctionCall {
    id: String,
    name: String,
    legacy: bool,
}

fn content_parts(content: &Content) -> Vec<Value> {
    match content {
        Content::Text { text } => vec![json!({"text": text})],

        Content::Image { uri, alt } => image_part(uri).map_or_else(
            || vec![json!({"text": alt.as_deref().unwrap_or("[Image attachment.]" )})],
            |image| {
                let mut parts = vec![image];

                if let Some(alt) = alt {
                    parts.push(json!({"text": alt}));
                }

                parts
            },
        ),

        content => vec![json!({"text": content.render()})],
    }
}

fn image_part(uri: &str) -> Option<Value> {
    let (mime, data) = uri.strip_prefix("data:")?.split_once(";base64,")?;

    if !matches!(mime, "image/png" | "image/jpeg" | "image/gif" | "image/webp")
        || data.is_empty()
        || !data
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return None;
    }

    Some(json!({"inlineData": {"mimeType": mime, "data": data}}))
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

fn assistant_parts(
    message: &crabbot_core::types::Message,
    require_signature: bool,
) -> crabbot_core::Result<(Vec<Value>, Vec<FunctionCall>)> {
    let mut parts = Vec::new();

    let mut calls = Vec::new();
    let mut index = 0;
    let mut saw_tool_call = false;
    let mut legacy_turn = false;

    for content in &message.content {
        if let Content::ToolCall { name, args, id, thought_signature } = content {
            let legacy = require_signature
                && (legacy_turn || (!saw_tool_call && thought_signature.is_none()));

            let id = id.clone().unwrap_or_else(|| format!("{}-{index}", message.id));
            index += 1;

            if legacy {
                parts.push(json!({"text": content.render()}));
                calls.push(FunctionCall { id, name: name.clone(), legacy });
                legacy_turn = true;
                saw_tool_call = true;
                continue;
            }

            let call = json!({"id": id, "name": name, "args": args});
            let mut part = json!({"functionCall": call});

            if require_signature && let Some(signature) = thought_signature {
                part["thoughtSignature"] = json!(signature);
            }

            parts.push(part);
            calls.push(FunctionCall { id, name: name.clone(), legacy });
            saw_tool_call = true;
            continue;
        }

        let Content::Text { text: value } = content else {
            parts.extend(content_parts(content));
            continue;
        };

        // TODO(remove-gemini-2-compat): delete marker parsing once Gemini 2.x
        // support and its persisted transcripts are no longer supported.
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

            if require_signature {
                let id = format!("{}-{index}", message.id);
                index += 1;

                if !buffered.is_empty() {
                    parts.push(json!({"text": buffered}));
                    buffered.clear();
                }

                parts.push(json!({"text": candidate}));
                calls.push(FunctionCall { id, name: name.into(), legacy: true });
                legacy_turn = true;
                saw_tool_call = true;

                if line.ends_with('\n') {
                    parts.push(json!({"text": "\n"}));
                }

                continue;
            }

            if !buffered.is_empty() {
                parts.push(json!({"text": buffered}));
                buffered.clear();
            }

            let id = format!("{}-{index}", message.id);
            index += 1;

            parts.push(json!({"functionCall": {"id": id, "name": name, "args": args}}));
            calls.push(FunctionCall { id, name: name.into(), legacy: false });
            saw_tool_call = true;
        }

        if !buffered.is_empty() {
            parts.push(json!({"text": buffered}));
        }
    }

    Ok((parts, calls))
}

#[cfg(test)]
mod tests;
