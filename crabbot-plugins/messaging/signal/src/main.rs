#![forbid(unsafe_code)]

#[cfg(not(test))]
use crabbot_core::types::{Request, Response};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

const OUTPUT_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const MEDIA_LIMIT: usize = 4 * 1024 * 1024;

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "signal".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Channel],
            commands: Vec::new(),
        },
        |request| async move { call(request).await },
    )
    .await
}

#[cfg(not(test))]
async fn call(request: Request) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    let account = std::env::var("CRABBOT_SIGNAL_ACCOUNT").map_err(|_| {
        crabbot_core::Error::Denied("CRABBOT_SIGNAL_ACCOUNT is not configured.".into())
    })?;
    if account.trim().is_empty() {
        return Err(crabbot_core::Error::Denied(
            "CRABBOT_SIGNAL_ACCOUNT is not configured.".into(),
        ));
    }
    let result = match method.as_str() {
        "poll" => poll(&account).await?,
        "send" => send(&account, &params).await?,
        "media" => media(&params).await?,
        "info" => json!({"account": account}),
        _ => return Ok(None),
    };
    Ok(Some(Response::ok(id, result)))
}

async fn run_with(command: &str, args: &[String]) -> crabbot_core::Result<Value> {
    let output =
        tokio::time::timeout(Duration::from_secs(45), Command::new(command).args(args).output())
            .await
            .map_err(|_| crabbot_core::Error::Denied("signal-cli timed out.".into()))?
            .map_err(|error| crabbot_core::Error::Denied(format!("signal-cli failed: {error}.")))?;
    if output.stdout.len() > OUTPUT_LIMIT || output.stderr.len() > OUTPUT_LIMIT {
        return Err(crabbot_core::Error::Denied("signal-cli output exceeded the limit.".into()));
    }
    if !output.status.success() {
        return Err(crabbot_core::Error::Denied(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|_| crabbot_core::Error::Denied("signal-cli output was invalid UTF-8.".into()))?;
    Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({"output": text})))
}

#[cfg(not(test))]
async fn poll(account: &str) -> crabbot_core::Result<Value> {
    let command = std::env::var("CRABBOT_SIGNAL_COMMAND").unwrap_or_else(|_| "signal-cli".into());
    poll_with(account, &command, &[]).await
}

async fn poll_with(account: &str, command: &str, prefix: &[String]) -> crabbot_core::Result<Value> {
    let mut args = prefix.to_vec();
    args.extend(["-a".into(), account.into(), "receive".into(), "--json".into()]);
    let value = run_with(command, &args).await?;
    let events = value.as_array().into_iter().flatten().filter_map(normalize).collect::<Vec<_>>();
    Ok(json!({"events": events}))
}

fn normalize(value: &Value) -> Option<Value> {
    let envelope = value.get("envelope")?;
    let data = envelope.get("dataMessage")?;
    let source = envelope["sourceNumber"].as_str().or_else(|| envelope["source"].as_str())?;
    let timestamp = envelope["timestamp"].as_i64().unwrap_or_default();
    let group = data["groupInfo"]["groupId"].as_str().unwrap_or(source);
    let text = data["message"].as_str().unwrap_or_default();
    if text.is_empty() && data["attachments"].as_array().is_none() {
        return None;
    }
    let content = content(data);
    Some(json!({
        "id": timestamp.to_string(),
        "chat": group,
        "sender": source,
        "private": data["groupInfo"].is_null(),
        "text": text,
        "content": content,
    }))
}

fn content(data: &Value) -> Vec<Value> {
    let mut items = Vec::new();
    if let Some(text) = data["message"].as_str().filter(|text| !text.is_empty()) {
        items.push(json!({"kind": "text", "text": text}));
    }
    for attachment in data["attachments"].as_array().into_iter().flatten().take(8) {
        let id = attachment["id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| attachment["id"].as_i64().map(|value| value.to_string()));
        let Some(id) = id.filter(|id| !id.is_empty() && id.len() <= 128) else { continue };
        let uri = attachment["storedFilename"]
            .as_str()
            .map(|path| format!("file://{path}"))
            .unwrap_or_else(|| format!("signal://attachment/{id}"));
        let mime = attachment["contentType"].as_str();
        let name = attachment["filename"].as_str().unwrap_or("attachment");
        let kind = if mime.is_some_and(|value| value.starts_with("audio/")) {
            "audio"
        } else if mime.is_some_and(|value| value.starts_with("image/")) {
            "image"
        } else {
            "file"
        };
        if kind == "image" {
            items.push(json!({"kind": kind, "uri": uri, "alt": name}));
        } else if kind == "audio" {
            items.push(json!({"kind": kind, "uri": uri, "mime": mime}));
        } else {
            items.push(json!({"kind": kind, "uri": uri, "name": name, "mime": mime}));
        }
    }
    items
}

#[cfg(not(test))]
async fn send(account: &str, params: &Value) -> crabbot_core::Result<Value> {
    let command = std::env::var("CRABBOT_SIGNAL_COMMAND").unwrap_or_else(|_| "signal-cli".into());
    send_with(account, params, &command, &[]).await
}

async fn send_with(
    account: &str,
    params: &Value,
    command: &str,
    prefix: &[String],
) -> crabbot_core::Result<Value> {
    let chat = params["chat"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Signal send requires a chat.".into()))?;
    let text = params["text"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("Signal send requires text.".into()))?;
    let mut args = prefix.to_vec();
    args.extend(["-a".into(), account.into(), "send".into(), "-m".into(), text.into()]);
    if let Some(values) = params["content"].as_array() {
        for value in values.iter().take(8) {
            let Some(uri) = value["uri"].as_str() else { continue };
            let path = local_media(uri)?;
            args.extend(["-a".into(), path.display().to_string()]);
        }
    }
    args.push(chat.into());
    run_with(command, &args).await
}

#[cfg(not(test))]
async fn media(params: &Value) -> crabbot_core::Result<Value> {
    let uri = params["uri"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("media.uri is required.".into()))?;
    let root = media_root()?;
    media_at(uri, &root).await
}

async fn media_at(uri: &str, root: &Path) -> crabbot_core::Result<Value> {
    let path = local_media_at(uri, root)?;
    let metadata = tokio::fs::metadata(&path).await?;
    if metadata.len() as usize > MEDIA_LIMIT {
        return Err(crabbot_core::Error::Denied("Signal media is too large.".into()));
    }
    Ok(json!({"uri": format!("file://{}", path.display())}))
}

fn local_media(uri: &str) -> crabbot_core::Result<std::path::PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .ok_or_else(|| crabbot_core::Error::Denied("Signal media must be a local file.".into()))?;
    let path = Path::new(path);
    if path.components().any(|component| component == std::path::Component::ParentDir) {
        return Err(crabbot_core::Error::Denied("Signal media path is invalid.".into()));
    }
    let root = std::env::var_os("CRABBOT_MEDIA")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("CRABBOT_ROOT").map(|value| PathBuf::from(value).join("media"))
        })
        .ok_or_else(|| crabbot_core::Error::Denied("CRABBOT_MEDIA is not configured.".into()))?;
    local_media_at(uri, &root)
}

#[cfg(not(test))]
fn media_root() -> crabbot_core::Result<PathBuf> {
    std::env::var_os("CRABBOT_MEDIA")
        .or_else(|| {
            std::env::var_os("CRABBOT_ROOT")
                .map(|value| PathBuf::from(value).join("media").into_os_string())
        })
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("CRABBOT_MEDIA is not configured.".into()))
}

fn local_media_at(uri: &str, root: &Path) -> crabbot_core::Result<std::path::PathBuf> {
    let path = uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .ok_or_else(|| crabbot_core::Error::Denied("Signal media must be a local file.".into()))?;
    let root = std::fs::canonicalize(root)
        .map_err(|_| crabbot_core::Error::Denied("Signal media root is unavailable.".into()))?;
    let path = std::fs::canonicalize(path)
        .map_err(|_| crabbot_core::Error::Denied("Signal media file is unavailable.".into()))?;
    if !path.starts_with(&root) {
        return Err(crabbot_core::Error::Denied("Signal media leaves the media root.".into()));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_text_images_audio_and_files() {
        let value = json!({
            "envelope": {
                "sourceNumber": "+1",
                "timestamp": 42,
                "dataMessage": {
                    "message": "hello",
                    "attachments": [
                        {"id": "img", "contentType": "image/png", "filename": "diagram.png"},
                        {"id": "voice", "contentType": "audio/ogg", "filename": "voice.ogg"},
                        {"id": "note", "contentType": "text/plain", "filename": "note.txt"}
                    ]
                }
            }
        });
        let event = normalize(&value).unwrap();
        assert_eq!(event["content"][0]["kind"], "text");
        assert_eq!(event["content"][1]["kind"], "image");
        assert_eq!(event["content"][2]["kind"], "audio");
        assert_eq!(event["content"][3]["kind"], "file");
        let group = json!({
            "envelope": {
                "source": "+2",
                "timestamp": 43,
                "dataMessage": {
                    "groupInfo": {"groupId": "group"},
                    "attachments": [{"id": 7, "contentType": "application/octet-stream"}]
                }
            }
        });
        let group = normalize(&group).unwrap();
        assert_eq!(group["chat"], "group");
        assert_eq!(group["content"][0]["kind"], "file");
        assert!(normalize(&json!({"envelope":{"dataMessage":{}}})).is_none());
    }

    #[test]
    fn rejects_unsafe_media_paths() {
        assert!(local_media("file://../outside.txt").is_err());
        assert!(local_media("signal://attachment/1").is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounds_signal_cli_output() {
        let value = run_with("printf", &["%s".into(), "[{\"ok\":true}]".into()]).await.unwrap();
        assert_eq!(value[0]["ok"], true);
        let value = run_with("printf", &["%s".into(), "plain".into()]).await.unwrap();
        assert_eq!(value["output"], "plain");
        assert!(run_with("sh", &["-c".into(), "exit 1".into()]).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn polls_and_sends_through_a_configured_cli() {
        let poll = poll_with(
            "+1000",
            "sh",
            &["-c".into(), "printf '%s' '[{\"envelope\":{\"sourceNumber\":\"+1\",\"timestamp\":7,\"dataMessage\":{\"message\":\"hello\"}}}]'".into()],
        )
        .await
        .unwrap();
        assert_eq!(poll["events"][0]["text"], "hello");

        let sent = send_with(
            "+1000",
            &json!({"chat":"+2000","text":"hello"}),
            "sh",
            &["-c".into(), "printf '%s' '{\"sent\":true}'".into()],
        )
        .await
        .unwrap();
        assert_eq!(sent["sent"], true);
    }

    #[tokio::test]
    async fn resolves_media_inside_the_configured_root() {
        let root =
            std::env::temp_dir().join(format!("crabbot-signal-media-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("voice.ogg");
        std::fs::write(&path, b"voice").unwrap();
        let value = media_at(&format!("file://{}", path.display()), &root).await.unwrap();
        assert!(value["uri"].as_str().unwrap().ends_with("voice.ogg"));
        assert!(media_at("file://missing", &root).await.is_err());
        assert!(media_at("signal://attachment/1", &root).await.is_err());

        let outside =
            std::env::temp_dir().join(format!("crabbot-signal-outside-{}", std::process::id()));

        std::fs::write(&outside, b"outside").unwrap();
        assert!(media_at(&format!("file://{}", outside.display()), &root).await.is_err());
        let _ = std::fs::remove_file(outside);
        let _ = std::fs::remove_dir_all(root);
    }
}
