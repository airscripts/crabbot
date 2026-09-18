#![forbid(unsafe_code)]

use crabbot_core::{
    plugin::{Emitter, serve_events},
    types::{
        Capability, CommandSpec, Content, Hello, Message, ModelReply, ModelRequest, Protocol,
        Request, Response, Role,
    },
};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::Command,
    time::timeout,
};

const LINE_LIMIT: usize = crabbot_core::jsonl::MAX / 2;
const OUTPUT_LIMIT: usize = 512 * 1024;

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    serve_events(
        Hello {
            protocol: Protocol::CURRENT,
            id: "pi".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Agent],
            commands: vec![CommandSpec {
                name: "code".into(),
                description: "Run a Crabbot-managed Pi coding session.".into(),
                interactive: false,
            }],
        },
        |request, emitter| async move { call(request, emitter).await },
    )
    .await
}

async fn call(request: Request, mut emitter: Emitter) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    if method != "command" || params["name"].as_str() != Some("code") {
        return Ok(None);
    }
    let args = params["args"].as_array().cloned().unwrap_or_default();
    let request = SessionRequest::parse(&args)?;
    let text = run(request, &mut emitter).await?;
    Ok(Some(Response::ok(id, json!({"text": text}))))
}

struct SessionRequest {
    prompt: String,
    session: Option<String>,
    workspace: PathBuf,
}

impl SessionRequest {
    fn parse(args: &[Value]) -> crabbot_core::Result<Self> {
        let mut session = None;
        let mut workspace = std::env::var_os("CRABBOT_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut prompt = Vec::new();
        let mut index = 0;
        while index < args.len() {
            let value = args[index].as_str().unwrap_or_default();
            match value {
                "--session" => {
                    index += 1;
                    session = args.get(index).and_then(Value::as_str).map(str::to_owned);
                }
                "--workspace" => {
                    index += 1;
                    workspace =
                        args.get(index).and_then(Value::as_str).map(PathBuf::from).ok_or_else(
                            || crabbot_core::Error::Denied("A workspace is required.".into()),
                        )?;
                }
                value if !value.starts_with('-') => prompt.push(value.to_owned()),
                value => {
                    return Err(crabbot_core::Error::Denied(format!(
                        "Unknown code option: {value}."
                    )));
                }
            }
            index += 1;
        }
        let prompt = prompt.join(" ").trim().to_owned();
        if prompt.is_empty() {
            return Err(crabbot_core::Error::Denied("A coding prompt is required.".into()));
        }
        Ok(Self { prompt, session, workspace })
    }
}

async fn run(request: SessionRequest, emitter: &mut Emitter) -> crabbot_core::Result<String> {
    let model = emitter
        .call(
            "host/model",
            serde_json::to_value(ModelRequest {
                model: std::env::var("CRABBOT_MODEL").unwrap_or_else(|_| "default".into()),
                workspace: Some(request.workspace.display().to_string()),
                messages: vec![Message {
                    id: "code-context".into(),
                    session: request.session.clone().unwrap_or_else(|| "code".into()),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: request.prompt.clone() }],
                }],
                stream: false,
                tools: Vec::new(),
            })?,
        )
        .await?;
    let context = model
        .result
        .ok_or_else(|| {
            crabbot_core::Error::Denied(
                "The configured intelligence provider returned no response.".into(),
            )
        })
        .and_then(|value| {
            serde_json::from_value::<ModelReply>(value).map_err(|error| {
                crabbot_core::Error::Protocol(format!(
                    "The intelligence response was invalid: {error}."
                ))
            })
        })?;
    let prompt = format!(
        "Use this Crabbot intelligence context while working on the coding task:\n\n{}\n\nCoding task:\n{}",
        context.text, request.prompt
    );
    let command = std::env::var("CRABBOT_PI_COMMAND").unwrap_or_else(|_| "pi".into());
    let session_root = std::env::var_os("CRABBOT_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("agent-sessions");
    run_pi(request, emitter, prompt, command, session_root).await
}

async fn run_pi(
    request: SessionRequest,
    emitter: &mut Emitter,
    prompt: String,
    command: String,
    session_root: PathBuf,
) -> crabbot_core::Result<String> {
    if let Some(session) = request.session.as_deref() {
        if !safe(session) {
            return Err(crabbot_core::Error::Denied("The coding session ID is invalid.".into()));
        }
        tokio::fs::create_dir_all(session_root.join(session)).await?;
    }
    tokio::fs::create_dir_all(&session_root).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|error| {
        crabbot_core::Error::Denied(format!("Crabbot could not bind the Pi tool bridge: {error}."))
    })?;
    let address = listener.local_addr()?;
    let token = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let mut args = vec!["--mode".into(), "rpc".into(), "--no-builtin-tools".into()];
    if let Some(session) = request.session.as_deref() {
        args.extend(["--session-dir".into(), session_root.join(session).display().to_string()]);
    } else {
        args.push("--no-session".into());
    }
    let extension = session_root.join(format!("crabbot-pi-{}.ts", std::process::id()));
    tokio::fs::create_dir_all(&session_root).await?;
    tokio::fs::write(&extension, extension_source(address.port(), &token)).await?;
    args.extend(["--extension".into(), extension.display().to_string()]);
    let bridge = tokio::spawn(tool_bridge(listener, emitter.clone(), token.clone()));
    let mut child = match Command::new(command)
        .args(args)
        .current_dir(&request.workspace)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            bridge.abort();
            let _ = tokio::fs::remove_file(&extension).await;
            return Err(crabbot_core::Error::Denied(format!("Pi could not start: {error}.")));
        }
    };
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| crabbot_core::Error::Protocol("Pi stdin was unavailable.".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| crabbot_core::Error::Protocol("Pi stdout was unavailable.".into()))?;
    let command =
        serde_json::to_vec(&json!({"id": "crabbot", "type": "prompt", "message": prompt}))?;
    input.write_all(&command).await?;
    input.write_all(b"\n").await?;
    input.flush().await?;
    drop(input);
    let mut lines = BufReader::new(stdout).lines();
    let mut output = String::new();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        while let Some(line) = lines.next_line().await? {
            if line.len() > LINE_LIMIT {
                return Err(crabbot_core::Error::Limit("Pi emitted an oversized RPC line.".into()));
            }
            let value: Value = serde_json::from_str(&line).map_err(|error| {
                crabbot_core::Error::Protocol(format!("Pi emitted invalid RPC: {error}."))
            })?;
            if value["type"] == "response" && value["success"] == false {
                return Err(crabbot_core::Error::Denied(
                    value["error"].as_str().unwrap_or("Pi rejected the coding prompt.").into(),
                ));
            }
            let text = value["text"]
                .as_str()
                .or_else(|| value["assistantMessageEvent"]["delta"].as_str())
                .unwrap_or_default();
            if !text.is_empty() && output.len().saturating_add(text.len()) <= OUTPUT_LIMIT {
                output.push_str(text);
                emitter.event(json!({"kind": "text", "text": text})).await?;
            }
            if value["type"] == "message_end"
                && output.is_empty()
                && let Some(text) = full_message_text(&value["message"])
            {
                let remaining = OUTPUT_LIMIT.saturating_sub(output.len());
                let text = text.chars().take(remaining).collect::<String>();
                if !text.is_empty() {
                    output.push_str(&text);
                    emitter.event(json!({"kind": "text", "text": text})).await?;
                }
            }
            if matches!(value["type"].as_str(), Some("agent_end" | "session_end")) {
                break;
            }
        }
        Ok::<_, crabbot_core::Error>(())
    })
    .await;
    let _ = child.kill().await;
    bridge.abort();
    let _ = tokio::fs::remove_file(extension).await;
    result.map_err(|_| crabbot_core::Error::Protocol("Pi coding session timed out.".into()))??;
    if output.is_empty() {
        return Err(crabbot_core::Error::Denied("Pi returned no coding response.".into()));
    }
    Ok(output)
}

fn full_message_text(message: &Value) -> Option<String> {
    let mut text = String::new();
    for item in message["content"].as_array()?.iter() {
        if let Some(value) = item["text"].as_str() {
            text.push_str(value);
        }
    }
    (!text.is_empty()).then_some(text)
}

async fn tool_bridge(listener: TcpListener, emitter: Emitter, token: String) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { break };
        let emitter = emitter.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let _ = handle_tool_call(stream, emitter, &token).await;
        });
    }
}

async fn handle_tool_call(
    mut stream: TcpStream,
    emitter: Emitter,
    token: &str,
) -> crabbot_core::Result<()> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let count = timeout(Duration::from_secs(5), stream.read(&mut buffer))
            .await
            .map_err(|_| crabbot_core::Error::Protocol("Pi tool request timed out.".into()))??;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > LINE_LIMIT {
            return Err(crabbot_core::Error::Limit("Pi tool request was too large.".into()));
        }
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let length = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .ok_or_else(|| {
            crabbot_core::Error::Protocol("Pi tool request had no content length.".into())
        })?;
    if length > LINE_LIMIT {
        return Err(crabbot_core::Error::Limit("Pi tool request was too large.".into()));
    }
    while bytes.len() < header_end + length {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            return Err(crabbot_core::Error::Protocol("Pi tool request was incomplete.".into()));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let request: Value = serde_json::from_slice(&bytes[header_end..header_end + length])?;
    let result = if request["token"].as_str() != Some(token) {
        json!({"error": "Pi tool authorization failed."})
    } else {
        let response = emitter
            .call("host/tool", json!({"name": request["name"], "args": request["args"]}))
            .await?;
        response
            .result
            .unwrap_or_else(|| json!({"error": response.error.map(|error| error.message)}))
    };
    let body = serde_json::to_vec(&result)?;
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

fn extension_source(port: u16, token: &str) -> String {
    format!(
        r#"const endpoint = "http://127.0.0.1:{port}";
const token = "{token}";
const schema = {{ type: "object", additionalProperties: true }};
async function call(name, args) {{
  const response = await fetch(endpoint, {{ method: "POST", headers: {{ "content-type": "application/json" }}, body: JSON.stringify({{ token, name, args }}) }});
  const value = await response.json();
  if (value.error) throw new Error(value.error.message || value.error);
  return {{ content: [{{ type: "text", text: value.output || value.text || JSON.stringify(value) }}], details: {{}} }};
}}
export default function (pi) {{
  for (const name of ["read", "write", "patch", "search", "git", "shell", "list"]) {{
    pi.registerTool({{ name, label: name + " (Crabbot)", description: "Run the Crabbot " + name + " tool inside the approved workspace.", parameters: schema, execute: async (_id, params) => call(name, params) }});
  }}
}}
"#,
        port = port,
        token = token,
    )
}

fn safe(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_listener() -> Option<tokio::net::TcpListener> {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("Could not bind the Pi test listener: {error}."),
        }
    }

    fn restricted_network(error: &crabbot_core::Error) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        message.contains("operation not permitted") || message.contains("permission denied")
    }

    #[test]
    fn parses_session_arguments() {
        let request = SessionRequest::parse(&[
            json!("--session"),
            json!("feature_one"),
            json!("--workspace"),
            json!("/tmp/workspace"),
            json!("write"),
            json!("tests"),
        ])
        .unwrap();

        assert_eq!(request.session.as_deref(), Some("feature_one"));
        assert_eq!(request.workspace, PathBuf::from("/tmp/workspace"));
        assert_eq!(request.prompt, "write tests");
    }

    #[test]
    fn rejects_unsafe_session_arguments() {
        assert!(safe("feature_one"));
        assert!(!safe("../feature"));
        assert!(SessionRequest::parse(&[json!("--unknown")]).is_err());
    }

    #[test]
    fn extracts_pi_message_text() {
        let delta = json!({
            "type": "message_update",
            "assistantMessageEvent": {"type": "text_delta", "delta": "hello"}
        });
        assert_eq!(delta["assistantMessageEvent"]["delta"], "hello");

        let message = json!({"content": [{"type": "text", "text": "done"}]});
        assert_eq!(full_message_text(&message).as_deref(), Some("done"));
    }

    #[test]
    fn generates_an_authenticated_extension() {
        let source = extension_source(1234, "token");
        assert!(source.contains("http://127.0.0.1:1234"));
        assert!(source.contains("const token = \"token\""));
        assert!(source.contains("registerTool"));
    }

    #[tokio::test]
    async fn rejects_unauthorized_tool_bridge_requests() {
        let Some(listener) = loopback_listener().await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let (sender, _) = tokio::sync::mpsc::channel(2);
        let emitter = Emitter::new(sender);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_tool_call(stream, emitter, "expected").await
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let body = br#"{"token":"wrong","name":"read","args":{}}"#;
        let request = format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).contains("authorization failed"));
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_a_pi_turn_with_streamed_output() {
        use std::os::unix::fs::PermissionsExt;

        let nonce =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("crabbot-pi-test-{nonce}"));
        let command = root.join("pi");
        let sessions = root.join("sessions");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(
            &command,
            "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"message_update\",\"assistantMessageEvent\":{\"delta\":\"done\"}}'\nprintf '%s\\n' '{\"type\":\"agent_end\"}'\n",
        )
        .await
        .unwrap();
        tokio::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o700)).await.unwrap();
        let (output, mut events) = tokio::sync::mpsc::channel(4);
        let mut emitter = Emitter::new(output);
        let request = SessionRequest {
            prompt: "write tests".into(),
            session: Some("feature".into()),
            workspace: root.clone(),
        };
        let text = match run_pi(
            request,
            &mut emitter,
            "context".into(),
            command.display().to_string(),
            sessions.clone(),
        )
        .await
        {
            Ok(text) => text,
            Err(error) if restricted_network(&error) => {
                let _ = tokio::fs::remove_dir_all(root).await;
                return;
            }
            Err(error) => panic!("Pi test turn failed: {error}."),
        };
        assert_eq!(text, "done");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "done");
        assert!(!sessions.join(format!("crabbot-pi-{}.ts", std::process::id())).exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_invalid_sessions_and_failed_runtimes() {
        let (output, _) = tokio::sync::mpsc::channel(2);
        let mut emitter = Emitter::new(output);
        let request = SessionRequest {
            prompt: "task".into(),
            session: Some("../unsafe".into()),
            workspace: PathBuf::from("."),
        };
        assert!(
            run_pi(
                request,
                &mut emitter,
                "context".into(),
                "missing-pi-command".into(),
                PathBuf::from("/tmp/crabbot-pi-sessions"),
            )
            .await
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reads_a_completed_message_when_no_delta_was_emitted() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("crabbot-pi-message-{}", std::process::id()));
        let command = root.join("pi");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(
            &command,
            "#!/bin/sh\nIFS= read -r _\nprintf '%s\\n' '{\"type\":\"message_end\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"complete\"}]}}'\nprintf '%s\\n' '{\"type\":\"session_end\"}'\n",
        )
        .await
        .unwrap();
        tokio::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o700)).await.unwrap();
        let (output, mut events) = tokio::sync::mpsc::channel(2);
        let mut emitter = Emitter::new(output);
        let request =
            SessionRequest { prompt: "task".into(), session: None, workspace: root.clone() };
        let text = match run_pi(
            request,
            &mut emitter,
            "context".into(),
            command.display().to_string(),
            root.join("sessions"),
        )
        .await
        {
            Ok(text) => text,
            Err(error) if restricted_network(&error) => {
                let _ = tokio::fs::remove_dir_all(root).await;
                return;
            }
            Err(error) => panic!("Pi test turn failed: {error}."),
        };
        assert_eq!(text, "complete");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "complete");
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
