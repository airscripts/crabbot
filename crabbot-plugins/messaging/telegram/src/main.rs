#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol, Request, Response},
};

use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const ID_LIMIT: usize = 256;
const TEXT_LIMIT: usize = 256 * 1024;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = client()?;

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "telegram".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: vec![],
        },
        move |request| {
            let client = client.clone();
            async move { call(&client, request).await }
        },
    )
    .await
}

fn client() -> crabbot_core::Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(40))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Telegram client failed: {error}.")))
}

async fn call(
    client: &reqwest::Client,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let token = credential()?;
    call_with(client, id, method, params, &token, &format!("https://api.telegram.org/bot{token}"))
        .await
}

fn credential() -> crabbot_core::Result<String> {
    for name in ["CRABBOT_TELEGRAM_TOKEN"] {
        if let Ok(value) = std::env::var(name)
            && !value.trim().is_empty()
        {
            return Ok(value);
        }
    }

    if let Some(value) = keyring("telegram") {
        return Ok(value);
    }

    let Some(path) = std::env::var_os("CRABBOT_CREDENTIALS") else {
        return Err(crabbot_core::Error::Denied(
            "CRABBOT_TELEGRAM_TOKEN is not configured.".into(),
        ));
    };

    crabbot_file::private(&path)?;
    let text = std::fs::read_to_string(path).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials could not be read: {error}."))
    })?;

    let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Credentials are invalid: {error}."))
    })?;

    value["CRABBOT_TELEGRAM_TOKEN"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_TELEGRAM_TOKEN is not configured.".into())
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

async fn call_with(
    client: &reqwest::Client,
    id: u64,
    method: String,
    params: serde_json::Value,
    _token: &str,
    base: &str,
) -> crabbot_core::Result<Option<Response>> {
    let Some(operation) = operation(&method, &params)? else {
        return Ok(None);
    };

    let result = match operation {
        Operation::Info => send(client, &format!("{base}/getMe"), json!({})).await?,

        Operation::Poll(offset) => {
            let updates = send(
                client,
                &format!("{base}/getUpdates"),
                json!({"offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}),
            )
            .await?;
            normalize(updates)
        }

        Operation::Send { chat, text, thread } => {
            let mut result = serde_json::Value::Null;

            for part in chunks(&text, 4_096) {
                let mut body = json!({"chat_id": chat, "text": part});

                if let Some(thread) = &thread {
                    body["message_thread_id"] = json!(thread.parse::<i64>().map_err(|_| {
                        crabbot_core::Error::Denied("Telegram thread ID was invalid.".into())
                    })?);
                }

                result = send(client, &format!("{base}/sendMessage"), body).await?;
            }

            result
        }

        Operation::Edit { chat, message, text, thread } => {
            let mut results = Vec::new();

            for (url, body) in edit_requests(base, chat, message, &text, thread.as_deref())? {
                results.push(send(client, &url, body).await?);
            }

            let edited = results.remove(0);
            json!({"edited": edited, "sent": results})
        }

        Operation::Approval { chat, text, approve, deny, thread } => {
            let (url, body) =
                approval_request(base, chat, &text, &approve, &deny, thread.as_deref())?;
            send(client, &url, body).await?
        }

        Operation::Callback { id, text } => {
            let mut body = json!({"callback_query_id": id});

            if let Some(text) = text {
                body["text"] = json!(text);
            }

            send(client, &format!("{base}/answerCallbackQuery"), body).await?
        }

        Operation::Media(uri) => media(client, &uri, _token, base).await?,
    };

    let response = Response::ok(id, result);

    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "Telegram response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(Some(response))
}

enum Operation {
    Info,
    Poll(i64),
    Send { chat: i64, text: String, thread: Option<String> },
    Edit { chat: i64, message: i64, text: String, thread: Option<String> },
    Approval { chat: i64, text: String, approve: String, deny: String, thread: Option<String> },
    Callback { id: String, text: Option<String> },
    Media(String),
}

fn edit_request(base: &str, chat: i64, message: i64, text: &str) -> (String, serde_json::Value) {
    (
        format!("{base}/editMessageText"),
        json!({"chat_id": chat, "message_id": message, "text": text}),
    )
}

fn edit_requests(
    base: &str,
    chat: i64,
    message: i64,
    text: &str,
    thread: Option<&str>,
) -> crabbot_core::Result<Vec<(String, serde_json::Value)>> {
    let mut parts = chunks(text, 4_096).into_iter();

    let first = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("edit.text is required.".into()))?;

    let mut requests = vec![edit_request(base, chat, message, &first)];

    for part in parts {
        let mut body = json!({"chat_id": chat, "text": part});

        if let Some(thread) = thread {
            body["message_thread_id"] = json!(thread.parse::<i64>().map_err(|_| {
                crabbot_core::Error::Denied("Telegram thread ID was invalid.".into())
            })?);
        }

        requests.push((format!("{base}/sendMessage"), body));
    }

    Ok(requests)
}

fn approval_request(
    base: &str,
    chat: i64,
    text: &str,
    approve: &str,
    deny: &str,
    thread: Option<&str>,
) -> crabbot_core::Result<(String, serde_json::Value)> {
    if text.is_empty() || text.len() > TEXT_LIMIT {
        return Err(crabbot_core::Error::Denied(
            "approval.text is empty or exceeds the Telegram limit.".into(),
        ));
    }

    if !callback_data(approve) || !callback_data(deny) || approve == deny {
        return Err(crabbot_core::Error::Denied("Approval callback data is invalid.".into()));
    }

    let mut body = json!({
        "chat_id": chat,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "Approve", "callback_data": approve},
                {"text": "Deny", "callback_data": deny}
            ]]
        }
    });

    if let Some(thread) = thread {
        body["message_thread_id"] = json!(thread.parse::<i64>().map_err(|_| {
            crabbot_core::Error::Denied("Telegram thread ID was invalid.".into())
        })?);
    }

    Ok((format!("{base}/sendMessage"), body))
}

fn callback_data(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64
}

fn chunks(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut units: usize = 0;

    for character in text.chars() {
        let width = character.len_utf16();

        if !current.is_empty() && units.saturating_add(width) > limit {
            chunks.push(std::mem::take(&mut current));
            units = 0;
        }

        current.push(character);
        units = units.saturating_add(width);
    }

    if !current.is_empty() || chunks.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn operation(method: &str, params: &serde_json::Value) -> crabbot_core::Result<Option<Operation>> {
    match method {
        "info" => Ok(Some(Operation::Info)),
        "poll" => Ok(Some(Operation::Poll(params["offset"].as_i64().unwrap_or(0)))),
        "send" => Ok(Some(Operation::Send {
            chat: params["chat"]
                .as_i64()
                .ok_or_else(|| crabbot_core::Error::Denied("send.chat is required.".into()))?,
            text: params["text"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("send.text is required.".into()))?
                .into(),
            thread: params["thread"].as_str().map(str::to_owned),
        })),

        "edit" => {
            let text = params["text"]
                .as_str()
                .filter(|text| !text.is_empty() && text.len() <= TEXT_LIMIT)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "edit.text is empty or exceeds the Telegram limit.".into(),
                    )
                })?;

            Ok(Some(Operation::Edit {
                chat: params["chat"]
                    .as_i64()
                    .ok_or_else(|| crabbot_core::Error::Denied("edit.chat is required.".into()))?,
                message: params["message"].as_i64().filter(|message| *message > 0).ok_or_else(
                    || crabbot_core::Error::Denied("edit.message must be positive.".into()),
                )?,
                text: text.into(),
                thread: params["thread"].as_str().map(str::to_owned),
            }))
        }

        "approval" => {
            let text = params["text"]
                .as_str()
                .filter(|text| !text.is_empty() && text.len() <= TEXT_LIMIT)
                .ok_or_else(|| {
                    crabbot_core::Error::Denied(
                        "approval.text is empty or exceeds the Telegram limit.".into(),
                    )
                })?;

            Ok(Some(Operation::Approval {
                chat: params["chat"].as_i64().ok_or_else(|| {
                    crabbot_core::Error::Denied("approval.chat is required.".into())
                })?,

                text: text.into(),
                approve: params["approve"]
                    .as_str()
                    .filter(|value| callback_data(value))
                    .ok_or_else(|| {
                        crabbot_core::Error::Denied("approval.approve is invalid.".into())
                    })?
                    .into(),
                deny: params["deny"]
                    .as_str()
                    .filter(|value| callback_data(value))
                    .ok_or_else(|| crabbot_core::Error::Denied("approval.deny is invalid.".into()))?
                    .into(),
                thread: params["thread"].as_str().map(str::to_owned),
            }))
        }

        "callback" => {
            let id = params["id"]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= ID_LIMIT)
                .ok_or_else(|| crabbot_core::Error::Denied("callback.id is required.".into()))?;

            let text =
                params["text"].as_str().filter(|value| value.len() <= 200).map(str::to_owned);

            Ok(Some(Operation::Callback { id: id.into(), text }))
        }

        "media" => Ok(Some(Operation::Media(
            params["uri"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("media.uri is required.".into()))?
                .into(),
        ))),
        _ => Ok(None),
    }
}

async fn media(
    client: &reqwest::Client,
    uri: &str,
    token: &str,
    base: &str,
) -> crabbot_core::Result<serde_json::Value> {
    let root = media_root()?;
    media_at(client, uri, token, base, &root).await
}

async fn media_at(
    client: &reqwest::Client,
    uri: &str,
    token: &str,
    base: &str,
    root: &std::path::Path,
) -> crabbot_core::Result<serde_json::Value> {
    let file_id = uri
        .strip_prefix("telegram://file/")
        .filter(|value| {
            !value.is_empty() && value.len() <= 1024 && value.bytes().all(valid_file_id)
        })
        .ok_or_else(|| crabbot_core::Error::Denied("Telegram media URI is invalid.".into()))?;

    let file = send(client, &format!("{base}/getFile"), json!({"file_id": file_id})).await?;
    let path = file["file_path"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 1024 && !value.contains(".."))
        .ok_or_else(|| crabbot_core::Error::Denied("Telegram media path is invalid.".into()))?;

    let api = base.split_once("/bot").map_or(base, |(value, _)| value);
    let response = client
        .get(format!("{api}/file/bot{token}/{path}"))
        .send()
        .await
        .map_err(|_| crabbot_core::Error::Denied("Telegram media download failed.".into()))?;

    let status = response.status();
    let bytes = collect(response.bytes_stream()).await?;

    if !status.is_success() {
        return Err(crabbot_core::Error::Denied("Telegram media download failed.".into()));
    }

    cleanup(root);
    std::fs::create_dir_all(root).map_err(|error| {
        crabbot_core::Error::Denied(format!("Telegram media directory failed: {error}."))
    })?;

    let name = format!("{file_id}.bin");
    let destination = root.join(name);
    crabbot_file::save(&destination, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("Telegram media storage failed: {error}."))
    })?;

    Ok(json!({"uri": format!("file://{}", destination.display())}))
}

fn valid_file_id(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn media_root() -> crabbot_core::Result<PathBuf> {
    let root = std::env::var_os("CRABBOT_MEDIA")
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME")
                .map(|value| PathBuf::from(value).join("media").into_os_string())
        })
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_MEDIA or CRABBOT_HOME is required.".into())
        })?;

    Ok(PathBuf::from(root))
}

fn cleanup(root: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    let now = SystemTime::now();

    for entry in entries.flatten().take(256) {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };

        if !file_type.is_file() {
            continue;
        }

        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > Duration::from_secs(24 * 60 * 60));

        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

async fn send(
    client: &reqwest::Client,
    url: &str,
    body: serde_json::Value,
) -> crabbot_core::Result<serde_json::Value> {
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|_| crabbot_core::Error::Denied("Telegram request failed.".into()))?;

    let status = response.status();
    let body = read(response).await?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .map_err(|_| crabbot_core::Error::Denied("Telegram response failed.".into()))?;

    response_body(status, value)
}

async fn read(response: reqwest::Response) -> crabbot_core::Result<String> {
    collect(response.bytes_stream()).await
}

async fn collect<S, C, E>(mut stream: S) -> crabbot_core::Result<String>
where
    S: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("Telegram response failed: {error}."))
        })?;

        if chunk.as_ref().len() > BODY_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied("Telegram response was too large.".into()));
        }

        bytes.extend_from_slice(chunk.as_ref());
    }

    String::from_utf8(bytes)
        .map_err(|_| crabbot_core::Error::Denied("Telegram response failed.".into()))
}

fn response_body(
    status: reqwest::StatusCode,
    value: serde_json::Value,
) -> crabbot_core::Result<serde_json::Value> {
    if !status.is_success() || value["ok"] != true {
        return Err(crabbot_core::Error::Denied(format!(
            "Telegram rejected the request: {}.",
            value["description"].as_str().unwrap_or("unknown error")
        )));
    }

    Ok(value["result"].clone())
}

fn normalize(value: serde_json::Value) -> serde_json::Value {
    let events =
        value.as_array().into_iter().flatten().filter_map(normalize_update).collect::<Vec<_>>();

    json!({"events": events})
}

fn normalize_update(update: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(callback) = update.get("callback_query") {
        let message = callback.get("message")?;
        let chat = message["chat"]["id"].as_i64()?;
        let kind = message["chat"]["type"].as_str().unwrap_or("unknown");
        return Some(json!({
            "id": update["update_id"],
            "kind": "callback",
            "chat": chat,
            "private": kind == "private",
            "thread": message["message_thread_id"],
            "topic": message["message_thread_id"],
            "sender": callback["from"]["id"],
            "roles": [],
            "text": "",
            "content": [],
            "callback_id": callback["id"],
            "data": callback["data"],
        }));
    }

    let message = update.get("message")?;
    let chat = message["chat"]["id"].as_i64()?;
    let kind = message["chat"]["type"].as_str().unwrap_or("unknown");
    let mut content = Vec::new();
    let photo = message["photo"].as_array().and_then(|files| files.last());

    if let Some(text) = message["text"].as_str().filter(|text| !text.is_empty()) {
        content.push(json!({"kind": "text", "text": text}));
    }

    if photo.is_none()
        && let Some(caption) = message["caption"].as_str().filter(|text| !text.is_empty())
    {
        content.push(json!({"kind": "text", "text": caption}));
    }

    if let Some(file) = photo
        && let Some(id) = file["file_id"].as_str()
    {
        content.push(json!({"kind": "image", "uri": format!("telegram://file/{id}"), "alt": message["caption"].as_str()}));
    }

    if let Some(file) = message["document"]["file_id"].as_str() {
        content.push(json!({"kind": "file", "uri": format!("telegram://file/{file}"), "name": message["document"]["file_name"].as_str().unwrap_or("document"), "mime": message["document"]["mime_type"].as_str()}));
    }

    if let Some(file) = message["voice"]["file_id"].as_str() {
        content.push(json!({"kind": "audio", "uri": format!("telegram://file/{file}"), "mime": message["voice"]["mime_type"].as_str()}));
    }

    let text = message["text"].as_str().or_else(|| message["caption"].as_str());
    let role = &message["from"]["role"];
    let roles = role
        .as_array()
        .cloned()
        .unwrap_or_else(|| role.is_null().then(Vec::new).unwrap_or_else(|| vec![role.clone()]));

    Some(json!({
        "id": update["update_id"],
        "chat": chat,
        "kind": kind,
        "private": kind == "private",
        "thread": message["message_thread_id"],
        "topic": message["message_thread_id"],
        "sender": message["from"]["id"],
        "roles": roles,
        "text": text,
        "content": content,
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_LIMIT, Operation, approval_request, call, call_with, cleanup, client, collect,
        credential, keyring, media_at, media_root, normalize, operation, response_body, send,
        valid_file_id,
    };

    use crabbot_core::types::Request;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn normalizes_messages_and_skips_other_updates() {
        let result = normalize(json!([
            {"update_id": 3, "message": {"chat": {"id": 8}, "from": {"id": 9, "role": "admin"}, "text": "hello"}},
            {"update_id": 4, "edited_message": {"chat": {"id": 8}}}
        ]));

        assert_eq!(result["events"].as_array().unwrap().len(), 1);
        assert_eq!(result["events"][0]["chat"], 8);
        assert_eq!(result["events"][0]["text"], "hello");
        assert_eq!(result["events"][0]["content"][0]["kind"], "text");
        assert_eq!(result["events"][0]["roles"], json!(["admin"]));
        assert!(result["events"][0]["role"].is_null());
        let unsupported = normalize(json!([{
            "update_id": 8,
            "message": {"chat": {"id": 8, "type": "private"}, "sticker": {"file_id": "sticker"}}
        }]));

        assert_eq!(unsupported["events"][0]["id"], 8);
        assert!(unsupported["events"][0]["content"].as_array().unwrap().is_empty());
        let media = normalize(
            json!([{"update_id": 5, "message": {"chat": {"id": 8, "type": "private"}, "document": {"file_id": "file", "file_name": "note.txt", "mime_type": "text/plain"}}}]),
        );

        assert_eq!(media["events"][0]["private"], true);
        assert_eq!(media["events"][0]["content"][0]["kind"], "file");
        let photo = normalize(
            json!([{"update_id": 6, "message": {"chat": {"id": 8, "type": "group"}, "text": "hello", "caption": "look", "photo": [{"file_id": "image"}]}}]),
        );

        assert_eq!(photo["events"][0]["private"], false);
        assert_eq!(photo["events"][0]["content"][0]["kind"], "text");
        assert_eq!(photo["events"][0]["content"][1]["kind"], "image");
        assert_eq!(photo["events"][0]["content"].as_array().unwrap().len(), 2);
        let caption = normalize(
            json!([{"update_id": 9, "message": {"chat": {"id": 8, "type": "group"}, "caption": "@crabbot", "photo": [{"file_id": "image"}]}}]),
        );

        assert_eq!(caption["events"][0]["text"], "@crabbot");
        let topic = normalize(
            json!([{"update_id": 7, "message": {"chat": {"id": 8, "type": "supergroup"}, "message_thread_id": 4, "from": {"id": 9}, "text": "topic"}}]),
        );

        assert_eq!(topic["events"][0]["topic"], 4);
        let callback = normalize(json!([{
            "update_id": 10,
            "callback_query": {
                "id": "callback-1",
                "data": "a0123456789abcdef.0123456789abcdef0123456789abcdef",
                "from": {"id": 9},
                "message": {
                    "message_thread_id": 4,
                    "chat": {"id": 8, "type": "supergroup"}
                }
            }
        }]));

        assert_eq!(callback["events"][0]["kind"], "callback");
        assert_eq!(callback["events"][0]["id"], 10);
        assert_eq!(callback["events"][0]["callback_id"], "callback-1");
        assert_eq!(callback["events"][0]["sender"], 9);
        assert_eq!(callback["events"][0]["chat"], 8);
        assert_eq!(callback["events"][0]["thread"], 4);
        assert_eq!(
            callback["events"][0]["data"],
            "a0123456789abcdef.0123456789abcdef0123456789abcdef"
        );
        let voice = normalize(
            json!([{"update_id": 6, "message": {"chat": {"id": 8}, "voice": {"file_id": "voice", "mime_type": "audio/ogg"}}}]),
        );

        assert_eq!(voice["events"][0]["content"][0]["kind"], "audio");
        assert_eq!(normalize(json!({}))["events"].as_array().unwrap().len(), 0);
        assert_eq!(
            normalize(json!([{"message": {"chat": {"id": "wrong"}}}, {"message": {}}]))["events"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn validates_media_requests() {
        assert!(matches!(
            operation("media", &json!({"uri": "telegram://file/abc_1"})).unwrap(),
            Some(Operation::Media(uri)) if uri == "telegram://file/abc_1"
        ));
        assert!(operation("media", &json!({})).is_err());
        assert!(operation("media", &json!({"uri": "file://outside"})).is_ok());
        assert!(valid_file_id(b'a'));
        assert!(valid_file_id(b'_'));
        assert!(!valid_file_id(b'/'));
    }

    #[test]
    fn reports_missing_credentials_and_media_root() {
        assert!(client().is_ok());

        if std::env::var_os("CRABBOT_KEYRING").is_none() {
            assert!(keyring("telegram").is_none());
        }

        if std::env::var_os("CRABBOT_TELEGRAM_TOKEN").is_none()
            && std::env::var_os("CRABBOT_CREDENTIALS").is_none()
        {
            assert!(credential().is_err());
        }

        if std::env::var_os("CRABBOT_MEDIA").is_none() && std::env::var_os("CRABBOT_HOME").is_none()
        {
            assert!(media_root().is_err());
        }
    }

    #[tokio::test]
    async fn downloads_media_into_private_cache() {
        let listener = match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener failed: {error}"),
        };

        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let body = if index == 0 {
                    br#"{"ok":true,"result":{"file_path":"photos/p.bin"}}"#.to_vec()
                } else {
                    b"abc".to_vec()
                };

                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });

        let root =
            std::env::temp_dir().join(format!("crabbot-telegram-media-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let client =
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();

        let base = format!("http://127.0.0.1:{port}/botTOKEN");
        let value =
            media_at(&client, "telegram://file/abc_1", "TOKEN", &base, &root).await.unwrap();

        let path = value["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"abc");
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejects_invalid_media_and_downloads() {
        let client = reqwest::Client::new();
        let root = std::env::temp_dir()
            .join(format!("crabbot-telegram-media-invalid-{}", std::process::id()));

        assert!(
            media_at(&client, "file://outside", "TOKEN", "http://127.0.0.1:1", &root)
                .await
                .is_err()
        );

        let listener = match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener failed: {error}"),
        };

        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for body in [
                br#"{"ok":true,"result":{"file_path":"../outside"}}"#.to_vec(),
                br#"{"ok":true,"result":{"file_path":"photos/p.bin"}}"#.to_vec(),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });

        let base = format!("http://127.0.0.1:{port}/botTOKEN");
        assert!(media_at(&client, "telegram://file/abc", "TOKEN", &base, &root).await.is_err());
        assert!(media_at(&client, "telegram://file/abc", "TOKEN", &base, &root).await.is_err());
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn cleans_only_old_files() {
        let root =
            std::env::temp_dir().join(format!("crabbot-telegram-cleanup-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("fresh"), "ok").unwrap();
        cleanup(&root);
        assert!(root.join("fresh").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn parses_success_and_failure_responses() {
        assert_eq!(
            response_body(reqwest::StatusCode::OK, json!({"ok":true,"result":{"id":1}})).unwrap()["id"],
            1
        );
        assert!(
            response_body(reqwest::StatusCode::BAD_REQUEST, json!({"ok":false,"description":"no"}))
                .is_err()
        );
        assert!(
            response_body(reqwest::StatusCode::OK, json!({"ok":true})).unwrap()["missing"]
                .is_null()
        );
    }

    #[test]
    fn prepares_channel_operations() {
        assert!(matches!(operation("info", &json!({})).unwrap(), Some(Operation::Info)));
        assert!(matches!(
            operation("poll", &json!({"offset": 3})).unwrap(),
            Some(Operation::Poll(3))
        ));
        assert!(matches!(
            operation("send", &json!({"chat": 8, "text": "hello"})).unwrap(),
            Some(Operation::Send { chat: 8, text, thread: None }) if text == "hello"
        ));
        assert!(matches!(
            operation("send", &json!({"chat": 8, "text": "hello", "thread": "4"})).unwrap(),
            Some(Operation::Send { chat: 8, text, thread: Some(thread) })

                if text == "hello" && thread == "4"
        ));
        assert!(matches!(
            operation("edit", &json!({"chat": 8, "message": 12, "text": "hello"})).unwrap(),
            Some(Operation::Edit { chat: 8, message: 12, text, thread: None }) if text == "hello"
        ));
        assert!(operation("send", &json!({})).is_err());
        assert!(operation("send", &json!({"chat": 8})).is_err());
        assert!(matches!(
            operation(
                "approval",
                &json!({"chat": 8, "text": "Approve write?", "approve": "allow", "deny": "deny"})
            )
            .unwrap(),
            Some(Operation::Approval { chat: 8, text, approve, deny, thread: None })

                if text == "Approve write?" && approve == "allow" && deny == "deny"
        ));
        assert!(matches!(
            operation("callback", &json!({"id": "callback-1", "text": "Denied."})).unwrap(),
            Some(Operation::Callback { id, text: Some(text) })

                if id == "callback-1" && text == "Denied."
        ));
        assert!(
            operation(
                "approval",
                &json!({"chat": 8, "text": "Approve?", "approve": "x".repeat(65), "deny": "deny"})
            )
            .is_err()
        );
        assert!(operation("callback", &json!({})).is_err());
        assert!(operation("edit", &json!({"chat": 8, "message": 12, "text": ""})).is_err());
        assert!(operation("edit", &json!({"chat": 8, "message": 0, "text": "hello"})).is_err());
        assert!(
            operation("edit", &json!({"chat": 8, "message": 12, "text": "x".repeat(4_097)}))
                .is_ok()
        );
        assert!(operation("unknown", &json!({})).unwrap().is_none());
    }

    #[test]
    fn prepares_message_edits() {
        let (url, body) = super::edit_request("https://telegram.test/bot", 8, 12, "hello");
        assert_eq!(url, "https://telegram.test/bot/editMessageText");
        assert_eq!(body, json!({"chat_id": 8, "message_id": 12, "text": "hello"}));

        let requests =
            super::edit_requests("https://telegram.test/bot", 8, 12, &"x".repeat(4_097), Some("4"))
                .unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1["text"].as_str().unwrap().len(), 4_096);
        assert!(requests[0].1["message_thread_id"].is_null());
        assert_eq!(requests[1].0, "https://telegram.test/bot/sendMessage");
        assert_eq!(requests[1].1["text"], "x");
        assert_eq!(requests[1].1["message_thread_id"], 4);
        assert!(super::edit_requests("https://telegram.test/bot", 8, 12, "", None).is_err());
        assert!(
            super::edit_requests(
                "https://telegram.test/bot",
                8,
                12,
                &"x".repeat(4097),
                Some("bad")
            )
            .is_err()
        );
    }

    #[test]
    fn prepares_inline_approval_controls() {
        let (url, body) = approval_request(
            "https://telegram.test/bot",
            8,
            "Approve write?",
            "allow-token",
            "deny-token",
            Some("4"),
        )
        .unwrap();

        assert_eq!(url, "https://telegram.test/bot/sendMessage");
        assert_eq!(body["chat_id"], 8);
        assert_eq!(body["message_thread_id"], 4);
        assert_eq!(body["reply_markup"]["inline_keyboard"][0][0]["text"], "Approve");
        assert_eq!(body["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "allow-token");
        assert_eq!(body["reply_markup"]["inline_keyboard"][0][1]["callback_data"], "deny-token");
        assert!(
            approval_request(
                "https://telegram.test/bot",
                8,
                "Approve?",
                &"x".repeat(65),
                "deny",
                None,
            )
            .is_err()
        );
        assert!(
            approval_request("https://telegram.test/bot", 8, "Approve?", "same", "same", None,)
                .is_err()
        );
        assert!(
            approval_request(
                "https://telegram.test/bot",
                8,
                "Approve?",
                "allow",
                "deny",
                Some("invalid"),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn validates_channel_calls_and_transport_errors() {
        let client = reqwest::Client::new();
        assert!(
            call_with(&client, 1, "unknown".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            call_with(&client, 1, "info".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .is_err()
        );
        assert!(
            call_with(
                &client,
                1,
                "poll".into(),
                json!({"offset": 3}),
                "secret",
                "http://127.0.0.1:1"
            )
            .await
            .is_err()
        );
        assert!(
            call_with(&client, 1, "send".into(), json!({}), "secret", "http://127.0.0.1:1")
                .await
                .is_err()
        );
        assert!(
            call_with(
                &client,
                1,
                "send".into(),
                json!({"chat": 3}),
                "secret",
                "http://127.0.0.1:1"
            )
            .await
            .is_err()
        );
        assert!(
            call_with(
                &client,
                1,
                "media".into(),
                json!({"uri": "telegram://file/abc"}),
                "secret",
                "http://127.0.0.1:1"
            )
            .await
            .is_err()
        );
        assert!(send(&client, "http://127.0.0.1:1", json!({})).await.is_err());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "poll".into(), params: json!({}) };

        assert!(call(&client, note).await.unwrap().is_none());
        let request = Request::Call {
            jsonrpc: "2.0".into(),
            id: 2,
            method: "info".into(),
            params: json!({}),
        };

        assert!(call(&client, request).await.is_err());
    }

    #[tokio::test]
    async fn bounds_channel_bodies() {
        use futures_util::stream;

        assert_eq!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(b"ok".to_vec())])).await.unwrap(),
            "ok"
        );
        assert!(collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![0xff])])).await.is_err());
        assert!(
            collect(stream::iter(vec![Ok::<_, std::io::Error>(vec![b'x'; BODY_LIMIT + 1])]))
                .await
                .is_err()
        );
    }

    #[test]
    fn chunks_long_messages() {
        let parts = super::chunks(&"x".repeat(9), 4);
        assert_eq!(parts, vec!["xxxx", "xxxx", "x"]);
        assert_eq!(super::chunks("😀😀😀", 4), vec!["😀😀", "😀"]);
        assert_eq!(super::chunks("", 4), vec![String::new()]);
    }
}
