#![forbid(unsafe_code)]

#[cfg(not(test))]
use crabbot_core::types::{Request, Response};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol},
};
#[cfg(not(test))]
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
#[cfg(not(test))]
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::{fs, io::AsyncWriteExt, net::TcpStream, sync::Mutex};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
#[cfg(not(test))]
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct App {
    client: reqwest::Client,
    #[cfg_attr(test, allow(dead_code))]
    socket: Arc<Mutex<Option<Socket>>>,
    attachments: Arc<Mutex<BTreeMap<String, Attachment>>>,
}

#[derive(Clone)]
struct Attachment {
    url: String,
    name: String,
    mime: Option<String>,
    size: Option<usize>,
}

const MEDIA_LIMIT: usize = 4 * 1024 * 1024;

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .map_err(|error| crabbot_core::Error::Denied(format!("Slack client failed: {error}.")))?;
    let app = Arc::new(App {
        client,
        socket: Arc::new(Mutex::new(None)),
        attachments: Arc::new(Mutex::new(BTreeMap::new())),
    });
    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "slack".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: Vec::new(),
        },
        move |request| {
            let app = Arc::clone(&app);
            async move { call(&app, request).await }
        },
    )
    .await
}

#[cfg(not(test))]
async fn call(app: &App, request: Request) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    let token = std::env::var("CRABBOT_SLACK_BOT_TOKEN").map_err(|_| {
        crabbot_core::Error::Denied("CRABBOT_SLACK_BOT_TOKEN is not configured.".into())
    })?;
    let result = match method.as_str() {
        "poll" => poll(app, &token).await?,
        "send" => send(&app.client, &token, &params).await?,
        "media" => media(app, &token, &params).await?,
        "info" => json!({"mode": "web_api", "socket_mode": true}),
        _ => return Ok(None),
    };
    Ok(Some(Response::ok(id, result)))
}

#[cfg(not(test))]
async fn api(
    client: &reqwest::Client,
    token: &str,
    method: &str,
    body: Value,
) -> crabbot_core::Result<Value> {
    api_at(client, token, method, body, "https://slack.com/api").await
}

async fn api_at(
    client: &reqwest::Client,
    token: &str,
    method: &str,
    body: Value,
    base: &str,
) -> crabbot_core::Result<Value> {
    let response = client
        .post(format!("{base}/{method}"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Slack request failed: {error}.")))?;
    let value: Value = response.json().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack response was invalid: {error}."))
    })?;
    if value["ok"] != true {
        return Err(crabbot_core::Error::Denied(
            value["error"].as_str().unwrap_or("Slack rejected the request.").into(),
        ));
    }
    Ok(value)
}

#[cfg(not(test))]
async fn poll(app: &App, token: &str) -> crabbot_core::Result<Value> {
    if let Ok(app_token) = std::env::var("CRABBOT_SLACK_APP_TOKEN")
        && !app_token.trim().is_empty()
        && let Some(events) = socket_poll(app, &app_token).await?
    {
        return Ok(json!({"events": events}));
    }
    let channels = std::env::var("CRABBOT_SLACK_CHANNELS").unwrap_or_default();
    poll_at(app, token, &channels, "https://slack.com/api").await
}

async fn poll_at(
    app: &App,
    token: &str,
    channels: &str,
    base: &str,
) -> crabbot_core::Result<Value> {
    let mut events = Vec::new();
    for channel in channels.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let value = api_at(
            &app.client,
            token,
            "conversations.history",
            json!({"channel": channel, "limit": 32}),
            base,
        )
        .await?;
        for message in value["messages"].as_array().into_iter().flatten() {
            let Some(timestamp) = message["ts"].as_str() else { continue };
            let text = message["text"].as_str().unwrap_or_default();
            if message["subtype"].is_string() {
                continue;
            }
            let content = remember_files(message, &app.attachments).await;
            if text.is_empty() && content.is_empty() {
                continue;
            }
            events.push(json!({
                "id": timestamp,
                "chat": channel,
                "sender": message["user"].as_str().unwrap_or_default(),
                "private": false,
                "text": text,
                "thread": message["thread_ts"].as_str(),
                "content": content,
            }));
        }
    }
    Ok(json!({"events": events}))
}

#[cfg(not(test))]
async fn socket_poll(app: &App, token: &str) -> crabbot_core::Result<Option<Vec<Value>>> {
    let mut socket = app.socket.lock().await;
    if socket.is_none() {
        let value = api(&app.client, token, "apps.connections.open", json!({})).await?;
        let url = value["url"].as_str().ok_or_else(|| {
            crabbot_core::Error::Denied("Slack Socket Mode did not return a URL.".into())
        })?;
        *socket = Some(
            connect_async(url)
                .await
                .map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Slack Socket Mode connection failed: {error}."
                    ))
                })?
                .0,
        );
    }
    let stream = socket.as_mut().expect("socket was initialized");
    let Some(message) = tokio::time::timeout(Duration::from_secs(25), stream.next())
        .await
        .map_err(|_| crabbot_core::Error::Denied("Slack Socket Mode polling timed out.".into()))?
        .transpose()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Slack Socket Mode failed: {error}."))
        })?
    else {
        return Ok(Some(Vec::new()));
    };
    let Message::Text(text) = message else {
        return Ok(Some(Vec::new()));
    };
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack Socket Mode event was invalid: {error}."))
    })?;
    if let Some(envelope) = value["envelope_id"].as_str() {
        stream
            .send(Message::Text(json!({"envelope_id": envelope}).to_string().into()))
            .await
            .map_err(|error| {
                crabbot_core::Error::Denied(format!(
                    "Slack Socket Mode acknowledgement failed: {error}."
                ))
            })?;
    }
    let event = &value["payload"]["event"];
    if event["type"] != "message" || event["subtype"].is_string() {
        return Ok(Some(Vec::new()));
    }
    let Some(chat) = event["channel"].as_str() else {
        return Ok(Some(Vec::new()));
    };
    let text = event["text"].as_str().unwrap_or_default();
    let content = remember_files(event, &app.attachments).await;
    if text.is_empty() && content.is_empty() {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(vec![json!({
        "id": event["ts"].as_str().unwrap_or_default(),
        "chat": chat,
        "sender": event["user"].as_str().unwrap_or_default(),
        "private": chat.starts_with('D'),
        "text": text,
        "thread": event["thread_ts"].as_str(),
        "content": content,
    })]))
}

async fn remember_files(
    value: &Value,
    attachments: &Arc<Mutex<BTreeMap<String, Attachment>>>,
) -> Vec<Value> {
    let mut content = Vec::new();
    if let Some(text) = value["text"].as_str().filter(|text| !text.is_empty()) {
        content.push(json!({"kind": "text", "text": text}));
    }
    let Some(files) = value["files"].as_array() else { return content };
    let mut stored = attachments.lock().await;
    for file in files.iter().take(8) {
        let Some(id) = file["id"].as_str().filter(|value| safe_id(value)) else { continue };
        let Some(url) =
            file["url_private_download"].as_str().or_else(|| file["url_private"].as_str())
        else {
            continue;
        };
        if !allowed_url(url) {
            continue;
        }
        let name = file["name"].as_str().unwrap_or("attachment").to_owned();
        let mime = file["mimetype"].as_str().map(str::to_owned);
        let size = file["size"].as_u64().and_then(|value| usize::try_from(value).ok());
        stored.insert(
            id.to_owned(),
            Attachment { url: url.to_owned(), name: name.clone(), mime: mime.clone(), size },
        );
        let uri = format!("slack://file/{id}");
        let kind = if mime.as_deref().is_some_and(|value| value.starts_with("audio/")) {
            "audio"
        } else if mime.as_deref().is_some_and(|value| value.starts_with("image/")) {
            "image"
        } else {
            "file"
        };
        if kind == "image" {
            content.push(json!({"kind": kind, "uri": uri, "alt": name}));
        } else if kind == "audio" {
            content.push(json!({"kind": kind, "uri": uri, "mime": mime}));
        } else {
            content.push(json!({"kind": kind, "uri": uri, "name": name, "mime": mime}));
        }
    }
    while stored.len() > 256 {
        let Some(key) = stored.keys().next().cloned() else { break };
        stored.remove(&key);
    }
    content
}

async fn media(app: &App, token: &str, params: &Value) -> crabbot_core::Result<Value> {
    let uri = params["uri"]
        .as_str()
        .and_then(|value| value.strip_prefix("slack://file/"))
        .filter(|value| safe_id(value))
        .ok_or_else(|| crabbot_core::Error::Denied("Slack media URI is invalid.".into()))?;
    let attachment =
        app.attachments.lock().await.get(uri).cloned().ok_or_else(|| {
            crabbot_core::Error::Denied("Slack attachment is unavailable.".into())
        })?;
    if attachment.size.is_some_and(|size| size > MEDIA_LIMIT) {
        return Err(crabbot_core::Error::Denied("Slack attachment is too large.".into()));
    }
    let response =
        app.client.get(&attachment.url).bearer_auth(token).send().await.map_err(|error| {
            crabbot_core::Error::Denied(format!("Slack media failed: {error}."))
        })?;
    if !response.status().is_success() {
        return Err(crabbot_core::Error::Denied("Slack media download was rejected.".into()));
    }
    let bytes = response.bytes().await.map_err(|error| {
        crabbot_core::Error::Denied(format!("Slack media response failed: {error}."))
    })?;
    if bytes.len() > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Slack attachment is too large.".into()));
    }
    let root = std::env::var_os("CRABBOT_MEDIA")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("CRABBOT_MEDIA is not configured.".into()))?;
    fs::create_dir_all(&root).await?;
    let name = safe_name(&attachment.name).unwrap_or_else(|| "attachment.bin".into());
    let path = root.join(format!("slack-{uri}-{name}"));
    if fs::symlink_metadata(&path).await.is_ok() {
        return Err(crabbot_core::Error::Denied("Slack media destination already exists.".into()));
    }
    let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&path).await?;
    file.write_all(&bytes).await?;
    Ok(json!({"uri": format!("file://{}", path.display()), "mime": attachment.mime}))
}

fn allowed_url(value: &str) -> bool {
    reqwest::Url::parse(value).ok().is_some_and(|url| {
        url.scheme() == "https"
            && matches!(url.host_str(), Some("files.slack.com" | "slack-files.com"))
    })
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn safe_name(value: &str) -> Option<String> {
    let value = PathBuf::from(value).file_name()?.to_str()?.to_owned();
    (!value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte != b'/' && byte != b'\\'))
    .then_some(value)
}

#[cfg(not(test))]
async fn send(
    client: &reqwest::Client,
    token: &str,
    params: &Value,
) -> crabbot_core::Result<Value> {
    send_at(client, token, params, "https://slack.com/api").await
}

async fn send_at(
    client: &reqwest::Client,
    token: &str,
    params: &Value,
    base: &str,
) -> crabbot_core::Result<Value> {
    let channel = params["chat"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Slack send requires a chat.".into()))?;
    let text = params["text"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Slack send requires text.".into()))?;
    api_at(
        client,
        token,
        "chat.postMessage",
        json!({"channel": channel, "text": text, "thread_ts": params["thread"]}),
        base,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the Slack test listener: {error}."),
        }
    }

    #[tokio::test]
    async fn normalizes_text_and_rich_files() {
        let attachments = Arc::new(Mutex::new(BTreeMap::new()));
        let value = json!({
            "text": "hello",
            "files": [
                {"id": "F1", "name": "diagram.png", "mimetype": "image/png", "url_private_download": "https://files.slack.com/files-pri/F1/download"},
                {"id": "F2", "name": "voice.ogg", "mimetype": "audio/ogg", "url_private": "https://slack-files.com/F2"},
                {"id": "F3", "name": "note.txt", "mimetype": "text/plain", "url_private": "https://files.slack.com/files-pri/F3/download"}
            ]
        });
        let content = remember_files(&value, &attachments).await;
        assert_eq!(content[0]["kind"], "text");
        assert_eq!(content[1]["kind"], "image");
        assert_eq!(content[2]["kind"], "audio");
        assert_eq!(content[3]["kind"], "file");
        assert_eq!(attachments.lock().await.len(), 3);
    }

    #[test]
    fn validates_urls_and_names() {
        assert!(allowed_url("https://files.slack.com/files-pri/F1/download"));
        assert!(allowed_url("https://slack-files.com/F2"));
        assert!(!allowed_url("https://example.com/file"));
        assert_eq!(safe_name("../note.txt").as_deref(), Some("note.txt"));
        assert!(safe_name("/").is_none());
    }

    #[tokio::test]
    async fn calls_a_bounded_api_response() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"messages":[]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let value = api_at(
            &reqwest::Client::new(),
            "xoxb-test",
            "conversations.history",
            json!({"channel":"C1"}),
            &format!("http://{address}"),
        )
        .await
        .unwrap();
        assert_eq!(value["ok"], true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sends_a_message_through_the_normalized_contract() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"ts":"1"}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let value = send_at(
            &reqwest::Client::new(),
            "xoxb-test",
            &json!({"chat":"C1","text":"hello"}),
            &format!("http://{address}"),
        )
        .await
        .unwrap();
        assert_eq!(value["ts"], "1");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn polls_history_and_rejects_api_errors() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"ok":true,"messages":[{"ts":"1","user":"U1","text":"hello"}]}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let value = poll_at(&app, "token", "C1", &format!("http://{address}")).await.unwrap();
        assert_eq!(value["events"][0]["text"], "hello");
        server.await.unwrap();

        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = br#"{"ok":false,"error":"invalid_auth"}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        assert!(
            api_at(
                &reqwest::Client::new(),
                "token",
                "auth.test",
                json!({}),
                &format!("http://{address}")
            )
            .await
            .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_invalid_media_and_send_payloads() {
        let app = App {
            client: reqwest::Client::new(),
            socket: Arc::new(Mutex::new(None)),
            attachments: Arc::new(Mutex::new(BTreeMap::new())),
        };
        assert!(media(&app, "token", &json!({})).await.is_err());
        assert!(
            send_at(&reqwest::Client::new(), "token", &json!({}), "http://unused").await.is_err()
        );
        assert!(
            remember_files(
                &json!({"files":[{"id":"bad id","url_private":"http://evil"}]}),
                &app.attachments
            )
            .await
            .is_empty()
        );
    }
}
