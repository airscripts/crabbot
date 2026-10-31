#![forbid(unsafe_code)]

use krabb_core::{
    plugin::{Emitter, serve_events},
    types::{Capability, CommandSpec, Hello, Protocol, Request, Response},
};

use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    time::timeout,
};

const LINE_LIMIT: usize = krabb_core::jsonl::MAX / 2;
const RPC_TIMEOUT: Duration = Duration::from_secs(120);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("acp", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    serve_events(hello(), |request, emitter| async move { call(request, emitter).await }).await
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "acp".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Agent],
        commands: vec![CommandSpec {
            name: "acp".into(),
            description: "Run a configured external Agent Client Protocol agent.".into(),
            interactive: false,
        }],
    }
}

async fn call(request: Request, mut emitter: Emitter) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method != "command" || params["name"] != "acp" {
        return Ok(None);
    }

    let args = params["args"].as_array().cloned().unwrap_or_default();
    let request = RunRequest::parse(&args)?;
    let mut result = run(request, &mut emitter).await?;

    if params["json"] != true {
        result["text"] = json!("");
    }

    Ok(Some(Response::ok(id, result)))
}

struct RunRequest {
    prompt: String,
    session: Option<String>,
    workspace: PathBuf,
}

impl RunRequest {
    fn parse(args: &[Value]) -> krabb_core::Result<Self> {
        let mut session = None;
        let mut workspace =
            std::env::var_os("KRABB_ROOT").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));

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
                            || krabb_core::Error::Denied("An ACP workspace is required.".into()),
                        )?;
                }

                value if !value.starts_with('-') => prompt.push(value.to_owned()),

                value => {
                    return Err(krabb_core::Error::Denied(format!("Unknown ACP option: {value}.")));
                }
            }

            index += 1;
        }

        let prompt = prompt.join(" ").trim().to_owned();

        if prompt.is_empty() {
            return Err(krabb_core::Error::Denied("An ACP prompt is required.".into()));
        }

        if session.as_ref().is_some_and(|value| !safe_id(value)) {
            return Err(krabb_core::Error::Denied("The ACP session ID is invalid.".into()));
        }

        Ok(Self { prompt, session, workspace })
    }
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

async fn run(request: RunRequest, emitter: &mut Emitter) -> krabb_core::Result<Value> {
    let command = std::env::var_os("KRABB_ACP_COMMAND")
        .map(PathBuf::from)
        .ok_or_else(|| krabb_core::Error::Denied("KRABB_ACP_COMMAND is not configured.".into()))?;

    let args = command_args()?;

    run_command(request, emitter, command, args).await
}

async fn run_command(
    request: RunRequest,
    emitter: &mut Emitter,
    command: PathBuf,
    args: Vec<String>,
) -> krabb_core::Result<Value> {
    let mut updates = Updates { emitter, pending: String::new(), text: String::new(), emitted: 0 };

    let workspace = std::fs::canonicalize(&request.workspace).map_err(|error| {
        krabb_core::Error::Denied(format!("The ACP workspace is unavailable: {error}."))
    })?;

    if !workspace.is_dir() {
        return Err(krabb_core::Error::Denied("The ACP workspace is not a directory.".into()));
    }

    let mut child = Command::new(command)
        .args(args)
        .current_dir(&workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            krabb_core::Error::Denied(format!("ACP agent could not start: {error}."))
        })?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| krabb_core::Error::Protocol("ACP agent stdin was unavailable.".into()))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| krabb_core::Error::Protocol("ACP agent stdout was unavailable.".into()))?;

    let mut writer = stdin;
    let mut reader = BufReader::new(stdout);

    let initialized = rpc(
        &mut writer,
        &mut reader,
        0,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": {"readTextFile": true, "writeTextFile": true}
            },
            "clientInfo": {
                "name": "krabb",
                "title": "Krabb",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
        &mut updates,
        &workspace,
    )
    .await?;

    if initialized["protocolVersion"] != 1 {
        return Err(krabb_core::Error::Protocol(
            "ACP agent did not negotiate stable protocol v1.".into(),
        ));
    }

    if request.session.is_some() && initialized["agentCapabilities"]["loadSession"] != true {
        return Err(krabb_core::Error::Denied(
            "This ACP agent does not support resuming sessions.".into(),
        ));
    }

    let setup = if let Some(session_id) = &request.session {
        rpc(
            &mut writer,
            &mut reader,
            1,
            "session/load",
            json!({"sessionId": session_id, "cwd": workspace, "mcpServers": []}),
            &mut updates,
            &workspace,
        )
        .await?
    } else {
        rpc(
            &mut writer,
            &mut reader,
            1,
            "session/new",
            json!({"cwd": workspace, "mcpServers": []}),
            &mut updates,
            &workspace,
        )
        .await?
    };

    let session_id =
        request.session.as_deref().or_else(|| setup["sessionId"].as_str()).ok_or_else(|| {
            krabb_core::Error::Protocol("ACP setup returned no session ID.".into())
        })?;

    let reply = rpc(
        &mut writer,
        &mut reader,
        2,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": request.prompt}]}),
        &mut updates,
        &workspace,
    )
    .await?;

    updates.flush().await?;

    drop(reader);
    drop(writer);

    let status = match timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
        Ok(status) => status?,

        Err(_) => {
            child.kill().await?;
            child.wait().await?
        }
    };

    if !status.success() {
        return Err(krabb_core::Error::Denied(format!("ACP agent exited with status {status}.")));
    }

    Ok(json!({
        "text": updates.text,
        "sessionId": session_id,
        "stopReason": reply["stopReason"]
    }))
}

fn command_args() -> krabb_core::Result<Vec<String>> {
    command_args_from_optional(std::env::var_os("KRABB_ACP_ARGS").as_deref())
}

fn command_args_from_optional(raw: Option<&std::ffi::OsStr>) -> krabb_core::Result<Vec<String>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };

    command_args_from(raw)
}

fn command_args_from(raw: &std::ffi::OsStr) -> krabb_core::Result<Vec<String>> {
    let raw = raw
        .to_str()
        .ok_or_else(|| krabb_core::Error::Denied("KRABB_ACP_ARGS is not valid UTF-8.".into()))?;

    let value: Value = serde_json::from_str(raw).map_err(|error| {
        krabb_core::Error::Denied(format!("KRABB_ACP_ARGS must be a JSON string array: {error}."))
    })?;

    let args = value.as_array().ok_or_else(|| {
        krabb_core::Error::Denied("KRABB_ACP_ARGS must be a JSON string array.".into())
    })?;

    args.iter()
        .map(|arg| {
            arg.as_str().map(str::to_owned).ok_or_else(|| {
                krabb_core::Error::Denied("KRABB_ACP_ARGS must contain only strings.".into())
            })
        })
        .collect()
}

async fn rpc(
    writer: &mut ChildStdin,
    reader: &mut BufReader<ChildStdout>,
    id: u64,
    method: &str,
    params: Value,
    updates: &mut Updates<'_>,
    workspace: &std::path::Path,
) -> krabb_core::Result<Value> {
    write_message(writer, json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).await?;

    loop {
        let message: Value = timeout(RPC_TIMEOUT, krabb_core::jsonl::read(reader, LINE_LIMIT))
            .await
            .map_err(|_| krabb_core::Error::Protocol("ACP agent response timed out.".into()))??
            .ok_or_else(|| krabb_core::Error::Protocol("ACP agent closed stdout.".into()))?;

        if message["method"] == "session/update" {
            stream_update(&message["params"]["update"], updates).await?;
            continue;
        }

        if message["method"] == "session/request_permission" {
            let request_id = message["id"].clone();
            let rejected = rejection(&message["params"]["options"]);
            write_message(
                writer,
                json!({"jsonrpc":"2.0","id":request_id,"result":{"outcome":rejected}}),
            )
            .await?;

            continue;
        }

        if let Some(method) = message["method"].as_str()
            && matches!(method, "fs/read_text_file" | "fs/write_text_file")
        {
            let request_id = message["id"].clone();
            let result =
                filesystem_request(method, &message["params"], workspace, updates.emitter).await;
            let reply = match result {
                Ok(result) => json!({"jsonrpc":"2.0","id":request_id,"result":result}),
                Err(error) => json!({
                    "jsonrpc":"2.0",
                    "id":request_id,
                    "error":{"code":-32000,"message":error.to_string()}
                }),
            };

            write_message(writer, reply).await?;
            continue;
        }

        if message["id"] == id {
            if let Some(error) = message.get("error") {
                return Err(krabb_core::Error::Denied(format!(
                    "ACP agent rejected {method}: {}.",
                    error["message"].as_str().unwrap_or("request failed")
                )));
            }

            return message
                .get("result")
                .cloned()
                .ok_or_else(|| krabb_core::Error::Protocol("ACP response had no result.".into()));
        }

        return Err(krabb_core::Error::Protocol(
            "ACP agent returned an unexpected JSON-RPC message.".into(),
        ));
    }
}

async fn filesystem_request(
    method: &str,
    params: &Value,
    workspace: &std::path::Path,
    emitter: &Emitter,
) -> krabb_core::Result<Value> {
    let path = params["path"]
        .as_str()
        .ok_or_else(|| krabb_core::Error::Denied("ACP filesystem path is required.".into()))?;

    let relative = workspace_relative(path, workspace)?;
    let (tool, args) = if method == "fs/read_text_file" {
        ("read", json!({"path":relative,"workspace":workspace}))
    } else {
        let content = params["content"].as_str().ok_or_else(|| {
            krabb_core::Error::Denied("ACP filesystem content is required.".into())
        })?;
        ("write", json!({"path":relative,"text":content,"workspace":workspace}))
    };

    let response =
        emitter.call("host/tool", json!({"name":tool,"args":args,"workspace":workspace})).await?;

    if let Some(error) = response.error {
        return Err(krabb_core::Error::Denied(error.message));
    }

    let result = response.result.unwrap_or(Value::Null);

    if tool == "read" {
        let content = result["text"]
            .as_str()
            .ok_or_else(|| krabb_core::Error::Protocol("Krabb read returned no text.".into()))?;

        let content = select_lines(content, params["line"].as_u64(), params["limit"].as_u64());
        Ok(json!({"content":content}))
    } else {
        Ok(json!({}))
    }
}

fn workspace_relative(path: &str, workspace: &std::path::Path) -> krabb_core::Result<String> {
    let path = PathBuf::from(path);

    if !path.is_absolute() {
        return Err(krabb_core::Error::Denied("ACP filesystem paths must be absolute.".into()));
    }

    let relative = path.strip_prefix(workspace).map_err(|_| {
        krabb_core::Error::Denied("ACP filesystem path leaves the selected workspace.".into())
    })?;

    if relative.components().any(|component| {
        matches!(component, std::path::Component::ParentDir | std::path::Component::RootDir)
    }) {
        return Err(krabb_core::Error::Denied("ACP filesystem path is invalid.".into()));
    }

    Ok(relative.to_string_lossy().into_owned())
}

fn select_lines(content: &str, line: Option<u64>, limit: Option<u64>) -> String {
    let start = line.unwrap_or(1).max(1) as usize - 1;
    let lines = content.lines().skip(start);
    let lines: Vec<_> = match limit {
        Some(limit) => lines.take(limit as usize).collect(),
        None => lines.collect(),
    };

    let mut selected = lines.join("\n");

    if content.ends_with('\n') && !lines.is_empty() {
        selected.push('\n');
    }

    selected
}

fn rejection(options: &Value) -> Value {
    options
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| matches!(item["kind"].as_str(), Some("reject_once" | "reject_always")))
        })
        .and_then(|item| item["optionId"].as_str())
        .map(|option| json!({"outcome":"selected","optionId":option}))
        .unwrap_or_else(|| json!({"outcome":"cancelled"}))
}

struct Updates<'a> {
    emitter: &'a mut Emitter,
    pending: String,
    text: String,
    emitted: usize,
}

impl Updates<'_> {
    async fn flush(&mut self) -> krabb_core::Result<()> {
        if !self.pending.is_empty() {
            self.emitter
                .event(json!({"kind":"text","text":std::mem::take(&mut self.pending)}))
                .await?;

            self.emitted += 1;
        }

        Ok(())
    }
}

async fn stream_update(update: &Value, updates: &mut Updates<'_>) -> krabb_core::Result<()> {
    let event = match update["sessionUpdate"].as_str() {
        Some("agent_message_chunk") => {
            if let Some(text) = update["content"]["text"].as_str() {
                if text.len() > LINE_LIMIT.saturating_sub(updates.text.len()) {
                    return Err(krabb_core::Error::Limit(
                        "ACP response exceeded its text limit.".into(),
                    ));
                }

                updates.text.push_str(text);
            }

            update["content"]["text"].as_str().map(|text| json!({"kind":"text","text":text}))
        }

        Some("tool_call") => update["title"]
            .as_str()
            .map(|title| json!({"kind":"text","text":format!("[External agent: {title}]")})),
        Some("tool_call_update") => update["status"]
            .as_str()
            .map(|status| json!({"kind":"text","text":format!("[External agent tool {status}]")})),
        _ => None,
    };

    if let Some(event) = event {
        let text = event["text"].as_str().unwrap_or_default();

        if text.len() > LINE_LIMIT.saturating_sub(updates.pending.len()) {
            return Err(krabb_core::Error::Limit("ACP response exceeded its text limit.".into()));
        }

        updates.pending.push_str(text);

        if updates.emitted < 255 {
            updates.flush().await?;
        }
    }

    Ok(())
}

async fn write_message(writer: &mut ChildStdin, message: Value) -> krabb_core::Result<()> {
    let mut bytes = serde_json::to_vec(&message)?;

    if bytes.len().saturating_add(1) > LINE_LIMIT {
        return Err(krabb_core::Error::Limit(
            "ACP request exceeds the JSON-RPC line limit.".into(),
        ));
    }

    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::run_command;
    use super::{
        RunRequest, command_args_from, command_args_from_optional, filesystem_request, hello,
        rejection, safe_id, select_lines, stream_update, workspace_relative,
    };

    use krabb_core::plugin::Emitter;
    use serde_json::json;

    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt, time::SystemTime};

    #[test]
    fn describes_the_acp_agent_command() {
        let hello = hello();

        assert_eq!(hello.id, "acp");
        assert_eq!(hello.capabilities, vec![krabb_core::types::Capability::Agent]);
        assert_eq!(hello.commands[0].name, "acp");
    }

    #[test]
    fn parses_prompt_workspace_and_resume_id() {
        let request = RunRequest::parse(&[
            json!("--workspace"),
            json!("/work/project"),
            json!("--session"),
            json!("session-1"),
            json!("review"),
            json!("changes"),
        ])
        .unwrap();

        assert_eq!(request.prompt, "review changes");
        assert_eq!(request.session.as_deref(), Some("session-1"));
        assert_eq!(request.workspace, PathBuf::from("/work/project"));
    }

    #[test]
    fn rejects_missing_prompts_and_unsafe_session_ids() {
        assert!(RunRequest::parse(&[]).is_err());
        assert!(!safe_id("../outside"));
        assert!(!safe_id(""));
        assert!(safe_id("session_1-abc"));
    }

    #[test]
    fn permission_requests_choose_a_rejection_or_cancel() {
        assert_eq!(
            rejection(&json!([{"optionId":"reject-once","kind":"reject_once"}])),
            json!({"outcome":"selected","optionId":"reject-once"})
        );

        assert_eq!(rejection(&json!([])), json!({"outcome":"cancelled"}));
    }

    #[test]
    fn confines_absolute_filesystem_paths_to_the_selected_workspace() {
        let workspace = std::env::temp_dir().join("work").join("project");
        let file = workspace.join("src").join("main.rs");
        let secret = workspace.parent().unwrap().join("secret.txt");
        let relative = PathBuf::from("src").join("main.rs").to_string_lossy().into_owned();

        assert_eq!(workspace_relative(file.to_str().unwrap(), &workspace).unwrap(), relative);

        assert!(workspace_relative(secret.to_str().unwrap(), &workspace).is_err());
        assert!(workspace_relative("src/main.rs", &workspace).is_err());
    }

    #[test]
    fn selects_requested_file_line_ranges() {
        assert_eq!(select_lines("one\ntwo\nthree\n", Some(2), Some(1)), "two\n");
        assert_eq!(select_lines("one\ntwo", None, Some(1)), "one");
    }

    #[test]
    fn validates_external_command_arguments() {
        assert!(command_args_from_optional(None).unwrap().is_empty());
        assert!(command_args_from(std::ffi::OsStr::new("[\"--quiet\"]")).is_ok());
        assert!(command_args_from(std::ffi::OsStr::new("{}")).is_err());
        assert!(command_args_from(std::ffi::OsStr::new("[1]")).is_err());
        assert!(command_args_from(std::ffi::OsStr::new("not-json")).is_err());
    }

    #[tokio::test]
    async fn rejects_invalid_acp_filesystem_requests_before_host_calls() {
        let workspace = std::env::temp_dir().join("work").join("project");
        let outside = workspace.parent().unwrap().join("outside").join("file");
        let inside = workspace.join("file");
        let (_sender, _receiver) = tokio::sync::mpsc::channel(1);
        let emitter = Emitter::new(_sender);

        assert!(
            filesystem_request("fs/read_text_file", &json!({}), &workspace, &emitter)
                .await
                .is_err()
        );

        assert!(
            filesystem_request(
                "fs/read_text_file",
                &json!({"path":outside.to_str().unwrap()}),
                &workspace,
                &emitter
            )
            .await
            .is_err()
        );

        assert!(
            filesystem_request(
                "fs/write_text_file",
                &json!({"path":inside.to_str().unwrap()}),
                &workspace,
                &emitter
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn maps_streamed_text_and_tool_status_without_fabricating_tools() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let mut emitter = Emitter::new(sender);
        let mut updates = super::Updates {
            emitter: &mut emitter,
            pending: String::new(),
            text: String::new(),
            emitted: 0,
        };

        stream_update(
            &json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}),
            &mut updates,
        )
        .await
        .unwrap();

        stream_update(&json!({"sessionUpdate":"tool_call","title":"Reading a file"}), &mut updates)
            .await
            .unwrap();

        stream_update(
            &json!({"sessionUpdate":"tool_call_update","status":"completed"}),
            &mut updates,
        )
        .await
        .unwrap();

        stream_update(&json!({"sessionUpdate":"ignored"}), &mut updates).await.unwrap();

        assert_eq!(updates.text, "hello");
        assert_eq!(receiver.recv().await.unwrap()["params"]["event"]["text"], "hello");
        assert_eq!(
            receiver.recv().await.unwrap()["params"]["event"]["text"],
            "[External agent: Reading a file]"
        );

        assert_eq!(
            receiver.recv().await.unwrap()["params"]["event"]["text"],
            "[External agent tool completed]"
        );
    }

    #[tokio::test]
    async fn coalesces_external_updates_after_the_notification_budget() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(256);
        let mut emitter = Emitter::new(sender);
        let mut updates = super::Updates {
            emitter: &mut emitter,
            pending: String::new(),
            text: String::new(),
            emitted: 0,
        };

        for _ in 0..300 {
            stream_update(
                &json!({"sessionUpdate":"agent_message_chunk","content":{"text":"piece"}}),
                &mut updates,
            )
            .await
            .unwrap();
        }

        updates.flush().await.unwrap();
        let mut text = String::new();
        let mut count = 0;

        while let Ok(event) = receiver.try_recv() {
            text.push_str(event["params"]["event"]["text"].as_str().unwrap());
            count += 1;
        }

        assert_eq!(count, 256);
        assert_eq!(text, "piece".repeat(300));
        assert_eq!(updates.text, text);

        assert!(
            stream_update(
                &json!({"sessionUpdate":"agent_message_chunk","content":{"text":"x".repeat(super::LINE_LIMIT)}}),
                &mut updates,
            )
            .await
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_unterminated_oversized_agent_output_without_waiting_for_eof() {
        let directory = std::env::temp_dir();
        let request = RunRequest { prompt: "test".into(), session: None, workspace: directory };
        let (sender, _events) = tokio::sync::mpsc::channel(1);
        let mut emitter = Emitter::new(sender);
        // Keep stdout open after exceeding the limit: rejection must precede EOF.
        let script =
            format!("head -c {} /dev/zero; read ignored; read ignored", super::LINE_LIMIT + 1);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_command(request, &mut emitter, "/bin/sh".into(), vec!["-c".into(), script]),
        )
        .await
        .expect("oversized frames must be rejected before EOF");

        assert!(result.unwrap_err().to_string().contains("Frame exceeds"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_an_external_agent_through_the_acp_v1_lifecycle() {
        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
        let directory = std::env::temp_dir().join(format!("krabb-acp-test-{nonce}"));
        fs::create_dir_all(&directory).unwrap();
        let script = directory.join("agent");
        let content = concat!(
            "#!/bin/sh\n",
            "while IFS= read -r line; do\n",
            "  case \"$line\" in\n",
            "    *'\"method\":\"initialize\"'*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":0,\"result\":{\"protocolVersion\":1,\"agentCapabilities\":{\"loadSession\":true},\"authMethods\":[{\"id\":\"login\",\"name\":\"Sign in\"}]}}' ;;\n",
            "    *'\"method\":\"session/new\"'*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"sessionId\":\"session-1\"}}' ;;\n",
            "    *'\"method\":\"session/load\"'*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}' ;;\n",
            "    *'\"method\":\"session/prompt\"'*)\n",
            "      printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"hello\"}}}}'\n",
            "      printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"session/request_permission\",\"params\":{\"options\":[]}}'\n",
            "      IFS= read -r permission_response\n",
            "      case \"$permission_response\" in *'\"outcome\":\"cancelled\"'*) ;; *) exit 5 ;; esac\n",
            "      printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"stopReason\":\"end_turn\"}}'\n",
            "      exit 0 ;;\n",
            "  esac\n",
            "done\n"
        );

        fs::write(&script, content).unwrap();
        let mut permissions = fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&script, permissions).unwrap();

        let request = RunRequest::parse(&[
            json!("--workspace"),
            json!(directory.display().to_string()),
            json!("say hello"),
        ])
        .unwrap();

        let (sender, mut events) = tokio::sync::mpsc::channel(4);
        let mut emitter = Emitter::new(sender);
        let result = run_command(request, &mut emitter, script.clone(), Vec::new()).await.unwrap();

        assert_eq!(result["text"], "hello");
        assert_eq!(result["sessionId"], "session-1");
        assert_eq!(result["stopReason"], "end_turn");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "hello");

        let request = RunRequest::parse(&[
            json!("--workspace"),
            json!(directory.display().to_string()),
            json!("--session"),
            json!("resume-2"),
            json!("continue"),
        ])
        .unwrap();

        let (sender, mut events) = tokio::sync::mpsc::channel(4);
        let mut emitter = Emitter::new(sender);
        let result = run_command(request, &mut emitter, script.clone(), Vec::new()).await.unwrap();

        assert_eq!(result["text"], "hello");
        assert_eq!(result["sessionId"], "resume-2");
        assert_eq!(events.recv().await.unwrap()["params"]["event"]["text"], "hello");

        for session in [None, Some("resume-2".into())] {
            let denied = content
                .replace(
                    r#""result":{"sessionId":"session-1"}"#,
                    r#""error":{"code":-32000,"message":"Authentication required"}"#,
                )
                .replace(
                    r#""id":1,"result":{}"#,
                    r#""id":1,"error":{"code":-32000,"message":"Authentication required"}"#,
                );

            fs::write(&script, denied).unwrap();
            let request =
                RunRequest { prompt: "hello".into(), workspace: directory.clone(), session };

            let (sender, _) = tokio::sync::mpsc::channel(4);
            let mut emitter = Emitter::new(sender);
            let error =
                run_command(request, &mut emitter, script.clone(), Vec::new()).await.unwrap_err();

            assert!(error.to_string().contains("Authentication required"));
        }

        fs::remove_dir_all(directory).unwrap();
    }

    use std::path::PathBuf;
}
