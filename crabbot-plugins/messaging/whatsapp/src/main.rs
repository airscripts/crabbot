#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Content, Hello, Protocol, Request, Response},
};
use futures_util::StreamExt;
use ring::hmac;
use serde_json::{Value, json};
use std::{collections::VecDeque, fs, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
    time::Duration,
};

const BODY_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const MEDIA_LIMIT: usize = 4 * 1024 * 1024;
const QUEUE_LIMIT: usize = 256;

#[derive(Clone)]
struct App {
    client: reqwest::Client,
    queue: Arc<Mutex<VecDeque<Value>>>,
}

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("WhatsApp client failed: {error}."))
        })?;
    let app = App { client, queue: Arc::new(Mutex::new(VecDeque::new())) };
    let listener = TcpListener::bind(listen()).await.map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp webhook listener failed: {error}."))
    })?;
    let webhook = app.clone();
    tokio::spawn(async move { webhook_loop(listener, webhook).await });
    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "whatsapp".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: vec![],
        },
        move |request| {
            let app = app.clone();
            async move { call(&app, request).await }
        },
    )
    .await
}

async fn call(app: &App, request: Request) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    let result = match method.as_str() {
        "poll" => {
            let mut queue = app.queue.lock().await;
            let count = queue.len().min(32);
            let events = queue.drain(..count).collect::<Vec<_>>();
            json!({"events": events})
        }
        "send" => send(app, &params).await?,
        "media" => {
            media(
                app,
                params["uri"]
                    .as_str()
                    .ok_or_else(|| crabbot_core::Error::Denied("media.uri is required.".into()))?,
            )
            .await?
        }
        _ => return Ok(None),
    };
    Ok(Some(Response::ok(id, result)))
}

async fn webhook_loop(listener: TcpListener, app: App) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let app = app.clone();
        tokio::spawn(async move {
            let secret = app_secret().ok();
            let verify = verify_token().ok();
            let response =
                handle_http_with(&mut stream, &app, secret.as_deref(), verify.as_deref())
                    .await
                    .unwrap_or_else(|error| (400, error.to_string()));
            let body = response.1.as_bytes();
            let header = format!(
                "HTTP/1.1 {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.0,
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(body).await;
        });
    }
}

async fn handle_http_with(
    stream: &mut tokio::net::TcpStream,
    app: &App,
    secret: Option<&str>,
    verify: Option<&str>,
) -> Result<(u16, String), String> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end;
    loop {
        let count = stream.read(&mut buffer).await.map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("Webhook request was incomplete.".into());
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > BODY_LIMIT {
            return Err("Webhook request was too large.".into());
        }
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            header_end = position + 4;
            break;
        }
    }
    let header =
        std::str::from_utf8(&bytes[..header_end]).map_err(|_| "Webhook headers were invalid.")?;
    let mut lines = header.split("\r\n");
    let request = lines.next().ok_or("Webhook request was invalid.")?.to_owned();
    let mut length = 0_usize;
    let mut signature = None;
    for line in lines {
        if let Some(value) = line.strip_prefix("Content-Length:") {
            length = value.trim().parse().map_err(|_| "Webhook length was invalid.")?;
        }
        if let Some(value) = line.strip_prefix("X-Hub-Signature-256:") {
            signature = Some(value.trim().to_owned());
        }
    }
    if length > BODY_LIMIT {
        return Err("Webhook body was too large.".into());
    }
    while bytes.len() < header_end + length {
        let count = stream.read(&mut buffer).await.map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("Webhook body was incomplete.".into());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let body = &bytes[header_end..header_end + length];
    if request.starts_with("GET ") {
        return challenge_with(&request, verify.ok_or("Webhook verification token is missing.")?);
    }
    if !request.starts_with("POST ") {
        return Err("Webhook method is not supported.".into());
    }
    verify_with(body, signature.as_deref(), secret.ok_or("Webhook secret is missing.")?)?;
    let value: Value = serde_json::from_slice(body).map_err(|_| "Webhook JSON was invalid.")?;
    let events = normalize(&value);
    let mut queue = app.queue.lock().await;
    if queue.len() + events.len() > QUEUE_LIMIT {
        return Ok((503, "Webhook queue is full.".into()));
    }
    queue.extend(events);
    Ok((200, "OK".into()))
}

fn challenge_with(request: &str, expected: &str) -> Result<(u16, String), String> {
    let path = request.split_whitespace().nth(1).ok_or("Webhook path was invalid.")?;
    let query = path.split_once('?').map(|(_, value)| value).unwrap_or_default();
    let mut mode = None;
    let mut token = None;
    let mut challenge = None;
    for item in query.split('&') {
        let Some((key, value)) = item.split_once('=') else { continue };
        match key {
            "hub.mode" => mode = Some(value),
            "hub.verify_token" => token = Some(value),
            "hub.challenge" => challenge = Some(value),
            _ => {}
        }
    }
    if mode == Some("subscribe") && token == Some(expected) {
        return Ok((200, challenge.unwrap_or_default().into()));
    }
    Err("Webhook verification failed.".into())
}

fn verify_with(body: &[u8], signature: Option<&str>, secret: &str) -> Result<(), String> {
    let signature = signature.ok_or("Webhook signature is missing.")?;
    let encoded = signature.strip_prefix("sha256=").ok_or("Webhook signature is invalid.")?;
    let expected = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()), body);
    let expected = hex(expected.as_ref());
    if !constant_time(expected.as_bytes(), encoded.as_bytes()) {
        return Err("Webhook signature is invalid.".into());
    }
    Ok(())
}

fn constant_time(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).fold(0_u8, |value, (left, right)| value | (left ^ right)) == 0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize(value: &Value) -> Vec<Value> {
    let mut events = Vec::new();
    let Some(entries) = value["entry"].as_array() else { return events };
    for entry in entries {
        let Some(changes) = entry["changes"].as_array() else { continue };
        for change in changes {
            let Some(messages) = change["value"]["messages"].as_array() else { continue };
            for message in messages {
                let Some(id) = message["id"].as_str().filter(|value| !value.is_empty()) else {
                    continue;
                };
                let Some(sender) = message["from"].as_str().filter(|value| !value.is_empty())
                else {
                    continue;
                };
                let mut content = Vec::new();
                let mut text = String::new();
                match message["type"].as_str().unwrap_or_default() {
                    "text" => text = message["text"]["body"].as_str().unwrap_or_default().into(),
                    "image" => {
                        let Some(uri) = media_ref(&message["image"]["id"]) else { continue };
                        content.push(json!({"kind":"image","uri":uri,"alt":message["image"]["caption"].as_str()}));
                    }
                    "audio" => {
                        let Some(uri) = media_ref(&message["audio"]["id"]) else { continue };
                        content.push(json!({"kind":"audio","uri":uri,"mime":message["audio"]["mime_type"].as_str()}));
                    }
                    "document" => {
                        let Some(uri) = media_ref(&message["document"]["id"]) else { continue };
                        content.push(json!({"kind":"file","uri":uri,"name":message["document"]["filename"].as_str().unwrap_or("attachment"),"mime":message["document"]["mime_type"].as_str()}));
                        text = message["document"]["caption"].as_str().unwrap_or_default().into();
                    }
                    _ => continue,
                }
                if !text.is_empty() {
                    content.insert(0, json!({"kind":"text","text":text}));
                }
                if content.is_empty() {
                    continue;
                }
                events.push(json!({"id":id,"chat":sender,"private":true,"sender":sender,"text":text,"content":content}));
            }
        }
    }
    events
}

fn media_ref(value: &Value) -> Option<String> {
    let id = value.as_str().filter(|value| {
        !value.is_empty()
            && value.len() <= 256
            && value
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || value == b'-' || value == b'_')
    })?;
    Some(format!("whatsapp://media/{id}"))
}

async fn send(app: &App, params: &Value) -> crabbot_core::Result<Value> {
    let chat = params["chat"]
        .as_str()
        .filter(|value| value.len() <= 64)
        .ok_or_else(|| crabbot_core::Error::Denied("send.chat is invalid.".into()))?;
    let text = params["text"].as_str().unwrap_or_default();
    let token = token()?;
    let base = graph_url()?;
    let mut result = Value::Null;
    for part in chunks(text, 4_000) {
        if part.is_empty() {
            continue;
        }
        result = graph(
            app,
            &format!("{base}/{}/messages", phone()?),
            &token,
            json!({"messaging_product":"whatsapp","to":chat,"type":"text","text":{"body":part}}),
        )
        .await?;
    }
    if let Some(values) = params["content"].as_array() {
        for value in values {
            let item: Content = serde_json::from_value(value.clone())
                .map_err(|_| crabbot_core::Error::Denied("send.content is invalid.".into()))?;
            result = match item {
                Content::Text { text } => {
                    graph(
                        app,
                        &format!("{base}/{}/messages", phone()?),
                        &token,
                        json!({"messaging_product":"whatsapp","to":chat,"type":"text","text":{"body":text}}),
                    )
                    .await?
                }
                Content::Image { uri, alt } => {
                    send_media(app, &base, &token, chat, &uri, "image", None, alt.as_deref()).await?
                }
                Content::Audio { uri, mime } => {
                    send_media(app, &base, &token, chat, &uri, "audio", mime.as_deref(), None).await?
                }
                Content::File { uri, name, mime } => {
                    send_media(app, &base, &token, chat, &uri, "document", mime.as_deref(), Some(&name)).await?
                }
            }
        }
    }
    if result.is_null() {
        return Err(crabbot_core::Error::Denied("send.text is required.".into()));
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
async fn send_media(
    app: &App,
    base: &str,
    token: &str,
    chat: &str,
    uri: &str,
    kind: &str,
    mime: Option<&str>,
    name: Option<&str>,
) -> crabbot_core::Result<Value> {
    let root = media_root()?;
    let phone = phone()?;
    send_media_at(app, base, token, chat, uri, kind, mime, name, &phone, &root).await
}

#[allow(clippy::too_many_arguments)]
async fn send_media_at(
    app: &App,
    base: &str,
    token: &str,
    chat: &str,
    uri: &str,
    kind: &str,
    mime: Option<&str>,
    name: Option<&str>,
    phone: &str,
    root: &std::path::Path,
) -> crabbot_core::Result<Value> {
    let path = local_media_at(uri, root)?;
    let bytes = fs::read(&path).map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp attachment could not be read: {error}."))
    })?;
    if bytes.len() > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("WhatsApp attachment is too large.".into()));
    }
    let filename = name
        .and_then(|value| {
            PathBuf::from(value).file_name().and_then(|value| value.to_str()).map(str::to_owned)
        })
        .unwrap_or_else(|| "attachment.bin".into());
    let mut part = reqwest::multipart::Part::bytes(bytes).file_name(filename.clone());
    if let Some(mime) = mime {
        part = part.mime_str(mime).map_err(|_| {
            crabbot_core::Error::Denied("WhatsApp attachment MIME type is invalid.".into())
        })?;
    }
    let upload = app
        .client
        .post(format!("{base}/{phone}/media"))
        .bearer_auth(token)
        .multipart(
            reqwest::multipart::Form::new()
                .text("messaging_product", "whatsapp")
                .part("file", part),
        )
        .send()
        .await
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("WhatsApp upload failed: {error}."))
        })?;
    let media: Value = upload.json().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp upload response failed: {error}."))
    })?;
    let id = media["id"].as_str().ok_or_else(|| {
        crabbot_core::Error::Denied("WhatsApp upload returned no media ID.".into())
    })?;
    let body = match kind {
        "image" => {
            json!({"messaging_product":"whatsapp","to":chat,"type":"image","image":{"id":id}})
        }
        "audio" => {
            json!({"messaging_product":"whatsapp","to":chat,"type":"audio","audio":{"id":id}})
        }
        "document" => {
            json!({"messaging_product":"whatsapp","to":chat,"type":"document","document":{"id":id,"filename":filename}})
        }
        _ => {
            return Err(crabbot_core::Error::Denied("WhatsApp attachment type is invalid.".into()));
        }
    };
    graph(app, &format!("{base}/{phone}/messages"), token, body).await
}

async fn graph(app: &App, url: &str, token: &str, body: Value) -> crabbot_core::Result<Value> {
    let response =
        app.client.post(url).bearer_auth(token).json(&body).send().await.map_err(|error| {
            crabbot_core::Error::Denied(format!("WhatsApp request failed: {error}."))
        })?;
    let status = response.status();
    let body: Value = response.json().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp response failed: {error}."))
    })?;
    if !status.is_success() {
        return Err(crabbot_core::Error::Denied("WhatsApp rejected the request.".into()));
    }
    Ok(body)
}

async fn media(app: &App, uri: &str) -> crabbot_core::Result<Value> {
    let id = uri
        .strip_prefix("whatsapp://media/")
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 256
                && value
                    .bytes()
                    .all(|value| value.is_ascii_alphanumeric() || value == b'-' || value == b'_')
        })
        .ok_or_else(|| crabbot_core::Error::Denied("WhatsApp media URI is invalid.".into()))?;
    let token = token()?;
    let base = graph_url()?;
    let root = media_root()?;
    media_at(app, id, &token, &base, &root, allowed_url).await
}

async fn media_at(
    app: &App,
    id: &str,
    token: &str,
    base: &str,
    root: &std::path::Path,
    allow: fn(&str) -> bool,
) -> crabbot_core::Result<Value> {
    let lookup = app.client.get(format!("{base}/{id}")).bearer_auth(token).send().await.map_err(
        |error| crabbot_core::Error::Denied(format!("WhatsApp media lookup failed: {error}.")),
    )?;
    if !lookup.status().is_success() {
        return Err(crabbot_core::Error::Denied("WhatsApp media lookup failed.".into()));
    }
    let response: Value = lookup.json().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp media response failed: {error}."))
    })?;
    let url = response["url"]
        .as_str()
        .filter(|value| allow(value))
        .ok_or_else(|| crabbot_core::Error::Denied("WhatsApp media URL is invalid.".into()))?;
    let response = app.client.get(url).bearer_auth(token).send().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp media download failed: {error}."))
    })?;
    if !response.status().is_success() {
        return Err(crabbot_core::Error::Denied("WhatsApp media download failed.".into()));
    }
    let bytes = collect(response.bytes_stream()).await?;
    fs::create_dir_all(root).map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp media directory failed: {error}."))
    })?;
    let path = root.join(format!("{id}.bin"));
    crabbot_file::save(&path, bytes).map_err(|error| {
        crabbot_core::Error::Denied(format!("WhatsApp media storage failed: {error}."))
    })?;
    Ok(json!({"uri":format!("file://{}", path.display())}))
}

async fn collect<S, C>(mut stream: S) -> crabbot_core::Result<Vec<u8>>
where
    S: futures_util::Stream<Item = Result<C, reqwest::Error>> + Unpin,
    C: AsRef<[u8]>,
{
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            crabbot_core::Error::Denied(format!("WhatsApp media download failed: {error}."))
        })?;
        if chunk.as_ref().len() > MEDIA_LIMIT.saturating_sub(bytes.len()) {
            return Err(crabbot_core::Error::Denied("WhatsApp media is too large.".into()));
        }
        bytes.extend_from_slice(chunk.as_ref());
    }
    Ok(bytes)
}

fn local_media_at(uri: &str, root: &std::path::Path) -> crabbot_core::Result<PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("WhatsApp attachment URI is invalid.".into()))?;
    let root = fs::canonicalize(root)
        .map_err(|_| crabbot_core::Error::Denied("WhatsApp media root is unavailable.".into()))?;
    let path = fs::canonicalize(path)
        .map_err(|_| crabbot_core::Error::Denied("WhatsApp attachment is unavailable.".into()))?;
    if !path.starts_with(root) {
        return Err(crabbot_core::Error::Denied(
            "WhatsApp attachment leaves the media root.".into(),
        ));
    }
    Ok(path)
}

fn chunks(value: &str, limit: usize) -> Vec<String> {
    let mut output = Vec::new();
    let mut current = String::new();
    for value in value.chars() {
        current.push(value);
        if current.chars().count() == limit {
            output.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() || output.is_empty() {
        output.push(current);
    }
    output
}
fn listen() -> String {
    std::env::var("CRABBOT_WHATSAPP_LISTEN").unwrap_or_else(|_| "127.0.0.1:8787".into())
}
fn token() -> crabbot_core::Result<String> {
    env("CRABBOT_WHATSAPP_TOKEN")
}
fn app_secret() -> Result<String, String> {
    env("CRABBOT_WHATSAPP_APP_SECRET").map_err(|error| error.to_string())
}
fn verify_token() -> Result<String, String> {
    env("CRABBOT_WHATSAPP_VERIFY").map_err(|error| error.to_string())
}
fn phone() -> crabbot_core::Result<String> {
    env("CRABBOT_WHATSAPP_PHONE")
}
fn graph_url() -> crabbot_core::Result<String> {
    env("CRABBOT_WHATSAPP_GRAPH_URL")
}
fn env(name: &str) -> crabbot_core::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied(format!("{name} is not configured.")))
}
fn allowed_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some_and(|host| {
                host == "fbcdn.net"
                    || host.ends_with(".fbcdn.net")
                    || host == "fbsbx.com"
                    || host.ends_with(".fbsbx.com")
            })
    })
}
fn media_root() -> crabbot_core::Result<PathBuf> {
    std::env::var_os("CRABBOT_MEDIA")
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME")
                .map(|value| PathBuf::from(value).join("media").into_os_string())
        })
        .map(PathBuf::from)
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_MEDIA or CRABBOT_HOME is required.".into())
        })
}

#[cfg(test)]
mod tests {
    use super::{
        App, BODY_LIMIT, MEDIA_LIMIT, Request, allowed_url, call, challenge_with, chunks, collect,
        constant_time, graph, handle_http_with, media_at, media_ref, normalize, send_media_at,
        verify_with,
    };
    use futures_util::stream;
    use ring::hmac;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the WhatsApp test listener: {error}."),
        }
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .pool_max_idle_per_host(0)
            .build()
            .expect("the WhatsApp test client should build")
    }

    #[test]
    fn normalizes_supported_messages() {
        let value = json!({"entry":[{"changes":[{"value":{"messages":[{"id":"m1","from":"1","type":"text","text":{"body":"hello"}},{"id":"m2","from":"1","type":"audio","audio":{"id":"a1","mime_type":"audio/ogg"}},{"id":"m3","from":"1","type":"image","image":{"id":"i1","caption":"diagram"}},{"id":"m4","from":"1","type":"document","document":{"id":"d1","filename":"note.txt","mime_type":"text/plain","caption":"note"}}]}}]}]});
        let events = normalize(&value);
        assert_eq!(events.len(), 4);
        assert_eq!(events[1]["content"][0]["kind"], "audio");
        assert_eq!(events[2]["content"][0]["kind"], "image");
        assert_eq!(events[3]["content"][0]["kind"], "text");
    }

    #[test]
    fn compares_signatures_without_early_success() {
        assert!(constant_time(b"abc", b"abc"));
        assert!(!constant_time(b"abc", b"abd"));
        assert!(!constant_time(b"abc", b"ab"));
    }

    #[test]
    fn validates_media_urls_and_references() {
        assert!(allowed_url("https://lookaside.fbsbx.com/media"));
        assert!(allowed_url("https://cdn.fbcdn.net/media"));
        assert!(!allowed_url("https://evilfbsbx.com/media"));
        assert!(!allowed_url("http://lookaside.fbsbx.com/media"));
        assert_eq!(media_ref(&json!("media-1")).as_deref(), Some("whatsapp://media/media-1"));
        assert!(media_ref(&json!("../outside")).is_none());
        assert_eq!(chunks("abcdef", 2), vec!["ab", "cd", "ef"]);
    }

    #[test]
    fn validates_webhook_challenges_and_signatures() {
        assert_eq!(
            challenge_with(
                "GET /hook?hub.mode=subscribe&hub.verify_token=secret&hub.challenge=42 HTTP/1.1",
                "secret"
            )
            .unwrap(),
            (200, "42".into())
        );
        assert!(
            challenge_with("GET /hook?hub.mode=subscribe&hub.verify_token=bad", "secret").is_err()
        );
        let body = b"payload";
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"secret");
        let signature = format!("sha256={}", super::hex(hmac::sign(&key, body).as_ref()));
        assert!(verify_with(body, Some(&signature), "secret").is_ok());
        assert!(verify_with(body, Some("sha256=bad"), "secret").is_err());
        assert!(verify_with(body, None, "secret").is_err());
    }

    #[test]
    fn ignores_unsupported_messages() {
        assert!(normalize(&json!({"entry": [{"changes": [{"value": {"messages": [{"id": "x", "from": "1", "type": "sticker"}]}}]}]})).is_empty());
        assert!(normalize(&json!({})).is_empty());
        assert_eq!(chunks("", 2), vec![String::new()]);
    }

    #[tokio::test]
    async fn bounds_media_downloads() {
        let small = stream::iter(vec![Ok::<_, reqwest::Error>(b"ok".to_vec())]);
        assert_eq!(collect(small).await.unwrap(), b"ok");
        let large = stream::iter(vec![Ok::<_, reqwest::Error>(vec![b'x'; MEDIA_LIMIT + 1])]);
        assert!(collect(large).await.is_err());
        let body = stream::iter(vec![Ok::<_, reqwest::Error>(vec![b'x'; BODY_LIMIT + 1])]);
        assert!(collect(body).await.is_err());
    }

    #[tokio::test]
    async fn accepts_authenticated_webhooks_and_rejects_bad_methods() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        let body = br#"{"entry":[{"changes":[{"value":{"messages":[{"id":"m","from":"1","type":"text","text":{"body":"hi"}}]}}]}]}"#;
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"secret");
        let signature = format!("sha256={}", super::hex(hmac::sign(&key, body).as_ref()));
        let server_app = app.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_http_with(&mut stream, &server_app, Some("secret"), Some("verify"))
                .await
                .unwrap()
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST /webhook HTTP/1.1\r\nContent-Length: {}\r\nX-Hub-Signature-256: {}\r\n\r\n",
            body.len(),
            signature
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        assert_eq!(server.await.unwrap(), (200, "OK".into()));
        assert_eq!(app.queue.lock().await.len(), 1);

        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let app = app.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_http_with(&mut stream, &app, Some("secret"), Some("verify")).await
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(b"PUT / HTTP/1.1\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn polls_and_reports_unknown_channel_methods() {
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from(
                [json!({"id":"m1"})],
            ))),
        };
        let poll = call(&app, Request::call(1, "poll", json!({}))).await.unwrap().unwrap();
        assert_eq!(poll.result.unwrap()["events"][0]["id"], "m1");
        assert!(call(&app, Request::call(2, "unknown", json!({}))).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn posts_graph_requests_and_rejects_provider_errors() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"messages":[{"id":"1"}]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        let value = graph(&app, &format!("http://{address}"), "token", json!({"type":"text"}))
            .await
            .unwrap();
        assert_eq!(value["messages"][0]["id"], "1");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_invalid_send_and_media_requests_before_network_access() {
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        assert!(call(&app, Request::call(1, "send", json!({}))).await.is_err());
        assert!(call(&app, Request::call(2, "send", json!({"chat":"1"}))).await.is_err());
        assert!(call(&app, Request::call(3, "media", json!({"uri":"file://bad"}))).await.is_err());
        assert!(call(&app, Request::call(4, "media", json!({}))).await.is_err());
    }

    #[test]
    fn rejects_malformed_challenges_and_urls() {
        assert!(challenge_with("GET / HTTP/1.1", "secret").is_err());
        assert!(
            challenge_with("GET /?hub.mode=subscribe&hub.verify_token=secret", "secret").is_ok()
        );
        assert!(verify_with(b"body", Some("md5=bad"), "secret").is_err());
        assert!(!allowed_url("https://example.com/media"));
        assert!(media_ref(&json!("bad id")).is_none());
    }

    #[tokio::test]
    async fn rejects_failed_graph_responses() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = br#"{"error":"nope"}"#;
            let header = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        assert!(graph(&app, &format!("http://{address}"), "token", json!({})).await.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn uploads_a_local_attachment_and_sends_it() {
        let root =
            std::env::temp_dir().join(format!("crabbot-whatsapp-media-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("note.txt");
        std::fs::write(&path, b"note").unwrap();
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for body in
                [br#"{"id":"media-1"}"#.as_slice(), br#"{"messages":[{"id":"sent"}]}"#.as_slice()]
            {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 8192];
                let _ = stream.read(&mut request).await.unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        let value = send_media_at(
            &app,
            &format!("http://{address}"),
            "token",
            "123",
            &format!("file://{}", path.display()),
            "document",
            Some("text/plain"),
            Some("note.txt"),
            "phone",
            &root,
        )
        .await
        .unwrap();
        assert_eq!(value["messages"][0]["id"], "sent");
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn downloads_media_into_the_private_root() {
        let root =
            std::env::temp_dir().join(format!("crabbot-whatsapp-download-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut response, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = response.read(&mut request).await.unwrap();
            let body = format!(r#"{{"url":"http://{address}/file"}}"#);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            response.write_all(header.as_bytes()).await.unwrap();
            response.write_all(body.as_bytes()).await.unwrap();
            drop(response);
            let (mut response, _) = listener.accept().await.unwrap();
            let body = b"voice";
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            response.write_all(header.as_bytes()).await.unwrap();
            response.write_all(body).await.unwrap();
            drop(response);
        });
        let app = App {
            client: test_client(),
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        };
        let value =
            media_at(&app, "media-1", "token", &format!("http://{address}"), &root, |_| true)
                .await
                .unwrap();
        let path = value["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"voice");
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
