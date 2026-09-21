use std::{collections::BTreeSet, ffi::OsString, path::PathBuf, process::Stdio, time::Duration};

use crabbot_core::{
    jsonl,
    plugin::Emitter,
    types::{Content, Message, ModelReply, ModelRequest, Response, Role, ToolSpec},
};

use serde_json::{Value, json};
use tokio::{
    io::BufReader,
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout, timeout_at},
};

const BODY: usize = jsonl::MAX / 2;
const TOOLS: usize = 16;
const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const TURN_TIMEOUT: Duration = Duration::from_secs(115);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(890);

pub async fn command(params: &Value, mut emitter: Emitter) -> crabbot_core::Result<String> {
    command_with(params, &mut emitter, binary(), codex_home()).await
}

async fn command_with(
    params: &Value,
    emitter: &mut Emitter,
    binary: OsString,
    home: Option<PathBuf>,
) -> crabbot_core::Result<String> {
    let name = params["name"].as_str().unwrap_or_default();
    let args = params["args"].as_array().cloned().unwrap_or_default();

    if name != "codex" {
        return Err(denied("Unknown Codex command."));
    }

    let command = args.first().and_then(Value::as_str).unwrap_or_default();
    let mut server = Server::start_with(binary, home).await?;
    server.initialize().await?;

    match command {
        "login" => {
            let device = match args.get(1).and_then(Value::as_str) {
                None => false,
                Some("--device") => true,
                Some(_) => return Err(denied("Use crabbot codex login [--device].")),
            };

            login(&mut server, device, emitter).await
        }

        "status" if args.len() == 1 => {
            let result = server.call("account/read", json!({})).await?;

            if result["account"].is_null() {
                Ok("Codex is not signed in.".into())
            } else {
                Ok("Codex is signed in.".into())
            }
        }

        "logout" if args.len() == 1 => {
            server.call("account/logout", json!({})).await?;
            Ok("Codex signed out.".into())
        }

        _ => Err(denied("Use crabbot codex login [--device], status, or logout.")),
    }
}

async fn login(
    server: &mut Server,
    device: bool,
    emitter: &mut Emitter,
) -> crabbot_core::Result<String> {
    let kind = if device { "chatgptDeviceCode" } else { "chatgpt" };

    let result = server.call("account/login/start", json!({"type": kind})).await?;
    let login_id = result["loginId"]
        .as_str()
        .ok_or_else(|| denied("Codex did not return a login identifier."))?;

    let instructions = if device {
        let url = result["verificationUrl"]
            .as_str()
            .ok_or_else(|| denied("Codex did not return a device verification URL."))?;
        let code = result["userCode"]
            .as_str()
            .ok_or_else(|| denied("Codex did not return a device code."))?;
        format!("Open {url} and enter this one-time code: {code}\n")
    } else {
        let url = result["authUrl"]
            .as_str()
            .ok_or_else(|| denied("Codex did not return a sign-in URL."))?;
        format!("Open this URL to sign in to Codex: {url}\n")
    };

    emitter.event(json!({"kind": "text", "text": instructions})).await?;

    let deadline = Instant::now() + LOGIN_TIMEOUT;
    let mut notices = 0_usize;

    loop {
        let value = timeout_at(deadline, jsonl::read::<Value>(&mut server.input, jsonl::MAX))
            .await
            .map_err(|_| denied("Codex sign-in timed out."))??
            .ok_or_else(|| denied("Codex app-server closed during sign-in."))?;

        if !valid_rpc(&value) {
            return Err(denied("Codex app-server returned an invalid JSON-RPC message."));
        }

        notices = notices.saturating_add(1);

        if notices > 4096 {
            return Err(denied("Codex sent too many messages during sign-in."));
        }

        if value["method"] != "account/login/completed" {
            if value.get("id").is_some() && value.get("method").is_some() {
                return Err(denied("Codex app-server sent an unsupported sign-in request."));
            }

            continue;
        }

        let params = &value["params"];

        if params["loginId"].as_str().is_some_and(|id| id != login_id) {
            continue;
        }

        if params["success"] == true {
            return Ok("Codex sign-in completed.".into());
        }

        return Err(denied("Codex sign-in failed."));
    }
}

pub async fn generate(
    id: u64,
    input: ModelRequest,
    emitter: Emitter,
) -> crabbot_core::Result<Option<Response>> {
    generate_with(id, input, emitter, binary(), codex_home()).await
}

async fn generate_with(
    id: u64,
    input: ModelRequest,
    mut emitter: Emitter,
    binary: OsString,
    home: Option<PathBuf>,
) -> crabbot_core::Result<Option<Response>> {
    let mut server = Server::start_with(binary, home).await?;
    server.initialize().await?;
    let account = server.call("account/read", json!({})).await?;

    if account["account"].is_null() {
        return Err(denied("Codex is not signed in. Run crabbot codex login first."));
    }

    let tools = dynamic_tools(&input.tools)?;
    let allowed = input.tools.iter().map(|tool| tool.name.clone()).collect();
    let root = workspace(input.workspace.as_deref())?;
    let root = root.to_string_lossy().into_owned();
    let instructions = instructions(&input.messages)?;
    let thread = server
        .call(
            "thread/start",
            json!({
                "cwd": root.clone(),
                "runtimeWorkspaceRoots": [root.clone()],
                "model": input.model,
                "ephemeral": true,
                "approvalPolicy": "on-request",
                "sandbox": "read-only",
                "dynamicTools": tools,
                "developerInstructions": format!(
                    "Use only declared Crabbot tools for workspace operations. Never claim that an operation succeeded until a Crabbot tool confirms it.\n\n{instructions}"
                ),
                "config": {
                    "features": {
                        "shell_tool": false,
                        "browser_use": false,
                        "computer_use": false,
                        "view_image": false
                    },

                    "mcp_servers": {}
                }
            }),
        )
        .await?;

    let thread_id = thread["thread"]["id"]
        .as_str()
        .ok_or_else(|| denied("Codex did not return a thread identifier."))?;

    let input_items = prompt(&input.messages)?;
    let started = server
        .call(
            "turn/start",
            json!({
                "threadId": thread_id,
                "model": input.model,
                "input": input_items,
                "approvalPolicy": "on-request",
                "sandboxPolicy": {"type": "readOnly", "networkAccess": false},
                "runtimeWorkspaceRoots": [root]
            }),
        )
        .await?;

    let turn_id = started["turn"]["id"]
        .as_str()
        .or_else(|| started["turnId"].as_str())
        .ok_or_else(|| denied("Codex did not return a turn identifier."))?;

    let text = timeout(TURN_TIMEOUT, server.turn(thread_id, turn_id, &allowed, &mut emitter))
        .await
        .map_err(|_| denied("Codex exceeded the turn time limit."))??;

    let response = Response::ok(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: "stop".into(),
            input: None,
            output: None,
            events: Vec::new(),
        })?,
    );

    if serde_json::to_vec(&response)?.len().saturating_add(1) > jsonl::MAX {
        return Err(denied("Codex response exceeded the protocol frame limit."));
    }

    Ok(Some(response))
}

fn dynamic_tools(tools: &[ToolSpec]) -> crabbot_core::Result<Vec<Value>> {
    if tools.len() > TOOLS {
        return Err(denied("Crabbot supplied too many tools to Codex."));
    }

    let mut names = std::collections::BTreeSet::new();
    tools
        .iter()
        .map(|tool| {
            if tool.name.trim().is_empty() || !names.insert(tool.name.as_str()) {
                return Err(denied("Crabbot supplied invalid or duplicate tool names."));
            }

            Ok(json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description.as_deref().unwrap_or(""),
                "inputSchema": tool.schema
            }))
        })
        .collect()
}

fn prompt(messages: &[Message]) -> crabbot_core::Result<Vec<Value>> {
    let mut output = Vec::new();

    for message in messages {
        if message.role == Role::System {
            continue;
        }

        let tag = match message.role {
            Role::System => continue,
            Role::User => "\n<User>\n",
            Role::Assistant => "\n<Assistant>\n",
            Role::Tool => "\n<Tool Result>\n",
        };

        output.push(json!({"type": "text", "text": tag}));

        for content in &message.content {
            match content {
                Content::Text { text } => {
                    output.push(json!({"type": "text", "text": format!("{text}\n")}));
                }

                Content::Image { uri, alt } => {
                    let url = codex_image_url(uri)?;
                    output.push(json!({"type": "image", "url": url}));

                    if let Some(alt) = alt {
                        output.push(json!({"type": "text", "text": alt}));
                    }
                }

                Content::File { name, .. } => output.push(json!({
                    "type": "text",
                    "text": format!("[File attachment: {name}]\n")
                })),

                Content::Audio { .. } => output.push(json!({
                    "type": "text",
                    "text": "[Audio attachment omitted.]\n"
                })),
            }
        }
    }

    if output.is_empty() {
        return Err(denied("Codex input is empty."));
    }

    if serde_json::to_vec(&output)
        .map_err(|_| denied("Codex input could not be serialized."))?
        .len()
        > jsonl::MAX
    {
        return Err(denied("Codex input exceeded the request limit."));
    }

    Ok(output)
}

fn codex_image_url(uri: &str) -> crabbot_core::Result<&str> {
    let Some((mime, encoded)) =
        uri.strip_prefix("data:").and_then(|value| value.split_once(";base64,"))
    else {
        return Err(denied("Codex requires images as inline data URLs."));
    };

    if !matches!(mime, "image/png" | "image/jpeg" | "image/gif" | "image/webp")
        || encoded.is_empty()
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(denied("Codex received an invalid image data URL."));
    }

    Ok(uri)
}

fn instructions(messages: &[Message]) -> crabbot_core::Result<String> {
    let mut output = String::new();

    for message in messages.iter().filter(|message| message.role == Role::System) {
        for content in &message.content {
            if let Content::Text { text } = content {
                if text.len() > BODY.saturating_sub(output.len()) {
                    return Err(denied("Codex instructions exceeded the request limit."));
                }

                output.push_str(text);
                output.push('\n');
            }
        }
    }

    Ok(output)
}

struct Server {
    child: Child,
    input: BufReader<ChildStdout>,
    output: ChildStdin,
    next: u64,
}

impl Server {
    async fn start_with(binary: OsString, home: Option<PathBuf>) -> crabbot_core::Result<Self> {
        check_version(&binary, home.as_deref()).await?;
        let mut command = Command::new(&binary);
        command
            .args(["app-server", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        command.env_clear();

        for name in ["PATH", "TEMP", "TMP", "TMPDIR"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }

        if let Some(path) = home {
            command.env("CODEX_HOME", &path);

            if let Some(home) = path.parent().map(PathBuf::from) {
                command.env("HOME", &home);
                command.env("USERPROFILE", home);
            }
        }

        let mut child = command
            .spawn()
            .map_err(|error| denied(&format!("Codex app-server could not start: {error}.")))?;

        let output =
            child.stdin.take().ok_or_else(|| denied("Codex app-server input is unavailable."))?;

        let stdout =
            child.stdout.take().ok_or_else(|| denied("Codex app-server output is unavailable."))?;

        Ok(Self { child, input: BufReader::new(stdout), output, next: 1 })
    }

    async fn initialize(&mut self) -> crabbot_core::Result<()> {
        self.call(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "crabbot",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )
        .await?;

        self.notify("initialized", json!({})).await
    }

    async fn call(&mut self, method: &str, params: Value) -> crabbot_core::Result<Value> {
        let id = self.next;
        self.next =
            self.next.checked_add(1).ok_or_else(|| denied("Codex request IDs exhausted."))?;

        let request = json!({"id": id, "method": method, "params": params});
        jsonl::write(&mut self.output, &request).await?;
        timeout(RPC_TIMEOUT, async {
            let mut notices = 0_usize;

            loop {
                let value = jsonl::read::<Value>(&mut self.input, jsonl::MAX)
                    .await?
                    .ok_or_else(|| denied("Codex app-server closed its output."))?;

                if !valid_rpc(&value) {
                    return Err(denied("Codex app-server returned an invalid JSON-RPC message."));
                }

                if value["id"].as_u64() != Some(id) {
                    notices = notices.saturating_add(1);

                    if notices > 4096 {
                        return Err(denied("Codex sent too many messages before a response."));
                    }

                    if value.get("id").is_some() && value.get("method").is_some() {
                        self.reply_error(value["id"].clone(), -32601, "Unsupported Codex request.")
                            .await?;
                    }

                    continue;
                }

                if value.get("error").is_some_and(|error| !error.is_null()) {
                    return Err(denied("Codex app-server rejected a request."));
                }

                return value
                    .get("result")
                    .cloned()
                    .ok_or_else(|| denied("Codex app-server returned an empty response."));
            }
        })
        .await
        .map_err(|_| denied("Codex app-server request timed out."))?
    }

    async fn notify(&mut self, method: &str, params: Value) -> crabbot_core::Result<()> {
        jsonl::write(&mut self.output, &json!({"method": method, "params": params})).await
    }

    async fn turn(
        &mut self,
        thread: &str,
        turn: &str,
        allowed: &BTreeSet<String>,
        emitter: &mut Emitter,
    ) -> crabbot_core::Result<String> {
        let deadline = Instant::now() + TURN_TIMEOUT;

        let mut text = String::new();
        let mut pending = String::new();
        let mut calls = 0_usize;
        let mut notices = 0_usize;
        let mut ticker = tokio::time::interval(Duration::from_millis(80));
        ticker.tick().await;

        loop {
            let value = tokio::select! {
                result = timeout_at(deadline, jsonl::read::<Value>(&mut self.input, jsonl::MAX)) => {
                    result
                        .map_err(|_| denied("Codex exceeded the turn time limit."))??
                        .ok_or_else(|| denied("Codex app-server closed during a turn."))?
                }

                _ = ticker.tick(), if !pending.is_empty() => {
                    emit(&mut pending, emitter).await?;
                    continue;
                }
            };

            if !valid_rpc(&value) {
                return Err(denied("Codex app-server returned an invalid JSON-RPC message."));
            }

            notices = notices.saturating_add(1);

            if notices > 4096 {
                return Err(denied("Codex sent too many turn events."));
            }

            if value.get("method").is_none() {
                continue;
            }

            if value["method"] == "item/agentMessage/delta" {
                if value["params"]["threadId"] != thread || value["params"]["turnId"] != turn {
                    continue;
                }

                if let Some(delta) = value["params"]["delta"].as_str() {
                    append(delta, &mut text, &mut pending)?;

                    if pending.len() >= 128 {
                        emit(&mut pending, emitter).await?;
                    }
                }
            } else if value["method"] == "turn/completed" {
                if value["params"]["threadId"] != thread {
                    continue;
                }

                let result = &value["params"]["turn"];

                if result["id"] != turn || result["status"] != "completed" {
                    return Err(denied("Codex turn did not complete successfully."));
                }

                if let Some(final_text) = final_text(result) {
                    text = final_text;
                }

                emit(&mut pending, emitter).await?;
                return Ok(text);
            } else if value.get("id").is_some() {
                calls = calls.saturating_add(1);

                if calls > TOOLS {
                    return Err(denied("Codex exceeded the tool-call limit."));
                }

                self.tool(&value, thread, turn, allowed, emitter).await?;
            } else if value["method"] == "error" {
                return Err(denied("Codex app-server reported an error."));
            }
        }
    }

    async fn tool(
        &mut self,
        request: &Value,
        thread: &str,
        turn: &str,
        allowed: &BTreeSet<String>,
        emitter: &mut Emitter,
    ) -> crabbot_core::Result<()> {
        let id = request["id"].clone();

        if request["method"] != "item/tool/call" {
            self.reply_error(id, -32601, "Unsupported Codex request.").await?;
            return Ok(());
        }

        if let Some(message) = tool_denial(request, thread, turn, allowed) {
            self.reply_error(id, -32602, message).await?;
            return Ok(());
        }

        let params = &request["params"];
        let name = params["tool"].as_str().unwrap_or_default();
        let response =
            emitter.call("host/tool", json!({"name": name, "args": params["arguments"]})).await?;

        let (success, output) = if let Some(error) = response.error {
            (false, error.message)
        } else {
            let value = response.result.unwrap_or(Value::Null);
            (true, value["text"].as_str().unwrap_or_default().to_owned())
        };

        let mut reply = json!({
            "id": id,
            "result": {
                "success": success,
                "contentItems": [{"type": "inputText", "text": output}]
            }
        });

        if serde_json::to_vec(&reply)?.len().saturating_add(1) > jsonl::MAX {
            reply["result"]["contentItems"][0]["text"] =
                "Tool output exceeded the Codex protocol limit.".into();

            reply["result"]["success"] = false.into();
        }

        jsonl::write(&mut self.output, &reply).await
    }

    async fn reply_error(
        &mut self,
        id: Value,
        code: i32,
        message: &str,
    ) -> crabbot_core::Result<()> {
        jsonl::write(
            &mut self.output,
            &json!({"id": id, "error": {"code": code, "message": message}}),
        )
        .await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn final_text(turn: &Value) -> Option<String> {
    turn["items"].as_array()?.iter().rev().find_map(|item| {
        (item["type"] == "agentMessage").then(|| item["text"].as_str().map(str::to_owned))?
    })
}

fn tool_denial(
    request: &Value,
    thread: &str,
    turn: &str,
    allowed: &BTreeSet<String>,
) -> Option<&'static str> {
    let params = &request["params"];

    if params["threadId"] != thread || params["turnId"] != turn {
        return Some("The tool request does not match the active turn.");
    }

    let Some(name) = params["tool"].as_str() else {
        return Some("A tool name is required.");
    };

    if !allowed.contains(name) {
        return Some("The requested tool was not declared by Crabbot.");
    }

    if !params["arguments"].is_object() {
        return Some("Tool arguments must be an object.");
    }

    None
}

fn append(part: &str, text: &mut String, pending: &mut String) -> crabbot_core::Result<()> {
    if part.len() > BODY.saturating_sub(text.len()) {
        return Err(denied("Codex response exceeded the response limit."));
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

async fn check_version(
    binary: &std::ffi::OsStr,
    home: Option<&std::path::Path>,
) -> crabbot_core::Result<()> {
    let mut command = Command::new(binary);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    if let Some(path) = home {
        command.env("CODEX_HOME", path);
    }

    let output = timeout(Duration::from_secs(3), executable_output(&mut command))
        .await
        .map_err(|_| denied("Codex version check timed out."))?
        .map_err(|error| {
            denied(&format!(
                "Codex CLI could not be started ({error}). Install Codex CLI and ensure `codex` is on PATH, or set CRABBOT_CODEX_BINARY."
            ))
        })?;

    let version = String::from_utf8_lossy(&output.stdout);

    if !version_reported(output.status.success(), &version) {
        return Err(denied(
            "Codex CLI could not report its version. Reinstall it using the official setup guide or set CRABBOT_CODEX_BINARY.",
        ));
    }

    Ok(())
}

async fn executable_output(command: &mut Command) -> std::io::Result<std::process::Output> {
    for attempt in 0..3 {
        match command.output().await {
            Err(error) if attempt < 2 && executable_busy(&error) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            result => return result,
        }
    }

    unreachable!()
}

fn executable_busy(error: &std::io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        error.raw_os_error() == Some(26)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        false
    }
}

fn binary() -> OsString {
    std::env::var_os("CRABBOT_CODEX_BINARY").unwrap_or_else(|| "codex".into())
}

fn codex_home() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_CODEX_HOME").map(PathBuf::from).or_else(|| {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(|home| PathBuf::from(home).join(".codex"))
    })
}

fn denied(message: &str) -> crabbot_core::Error {
    crabbot_core::Error::Denied(message.into())
}

fn version_reported(success: bool, value: &str) -> bool {
    success && !value.trim().is_empty()
}

fn valid_rpc(value: &Value) -> bool {
    value.get("jsonrpc").is_none_or(|version| version == "2.0")
}

fn workspace(value: Option<&str>) -> crabbot_core::Result<PathBuf> {
    let path = value
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("CRABBOT_ROOT").map(PathBuf::from))
        .or_else(|| std::env::current_dir().ok())
        .ok_or_else(|| denied("A Crabbot workspace is required for Codex."))?;

    let path = path
        .canonicalize()
        .map_err(|_| denied("The configured Codex workspace is unavailable."))?;

    if !path.is_dir() {
        return Err(denied("The configured Codex workspace is not a directory."));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::{
        Server, check_version, command_with, dynamic_tools, final_text, generate_with,
        instructions, prompt, tool_denial, valid_rpc, version_reported, workspace,
    };

    use crabbot_core::types::{Content, Message, ModelRequest, Role, ToolSpec};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[cfg(unix)]
    fn write_binary(path: &std::path::Path, script: &str) -> std::io::Result<()> {
        use std::{
            fs::{OpenOptions, Permissions},
            io::Write,
            os::unix::fs::PermissionsExt,
        };

        let staging = path.with_extension("staging");
        let mut file = OpenOptions::new().create_new(true).write(true).open(&staging)?;
        file.write_all(script.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::set_permissions(&staging, Permissions::from_mode(0o700))?;
        std::fs::rename(staging, path)
    }

    #[test]
    fn builds_constrained_dynamic_tools() {
        let tools = vec![ToolSpec {
            name: "read".into(),
            description: Some("Read a file".into()),
            schema: json!({"type": "object"}),
        }];

        let value = dynamic_tools(&tools).unwrap();

        assert_eq!(value[0]["name"], "read");
        assert_eq!(value[0]["inputSchema"]["type"], "object");
        assert!(
            dynamic_tools(&[
                tools[0].clone(),
                ToolSpec { name: "read".into(), description: None, schema: json!({}) }
            ])
            .is_err()
        );
    }

    #[test]
    fn preserves_role_context_without_exposing_attachment_paths() {
        let messages = vec![
            Message {
                id: "system".into(),
                session: "s".into(),
                role: Role::System,
                sender: None,
                content: vec![Content::Text { text: "Rules".into() }],
            },
            Message {
                id: "user".into(),
                session: "s".into(),
                role: Role::User,
                sender: None,
                content: vec![Content::Image {
                    uri: "data:image/png;base64,aW1hZ2U=".into(),
                    alt: Some("A photo".into()),
                }],
            },
        ];

        let prompt = prompt(&messages).unwrap();

        assert!(instructions(&messages).unwrap().contains("Rules"));
        assert_eq!(prompt[1], json!({"type": "image", "url": "data:image/png;base64,aW1hZ2U="}));
        assert_eq!(prompt[2], json!({"type": "text", "text": "A photo"}));
        assert!(!serde_json::to_string(&prompt).unwrap().contains("/private/photo.png"));
    }

    #[test]
    fn extracts_only_completed_agent_text() {
        let turn = json!({
            "items": [
                {"type": "agentMessage", "text": "Earlier."},
                {"type": "functionCallOutput", "output": []},
                {"type": "agentMessage", "text": "Final."}
            ]
        });

        assert_eq!(final_text(&turn).as_deref(), Some("Final."));
        assert_eq!(final_text(&json!({"items": []})), None);
    }

    #[test]
    fn accepts_codex_cli_updates() {
        assert!(version_reported(true, "codex-cli 100.0.0"));
        assert!(version_reported(true, "codex-cli 1.0.0"));
        assert!(!version_reported(false, "codex-cli 1.0.0"));
        assert!(!version_reported(true, ""));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn retries_temporary_executable_busy_errors() {
        assert!(super::executable_busy(&std::io::Error::from_raw_os_error(26)));
        assert!(!super::executable_busy(&std::io::Error::from_raw_os_error(2)));
    }

    #[test]
    fn accepts_the_codex_app_server_wire_format() {
        assert!(valid_rpc(&json!({"id": 1, "result": {}})));
        assert!(valid_rpc(&json!({"jsonrpc": "2.0", "id": 1, "result": {}})));
        assert!(!valid_rpc(&json!({"jsonrpc": "1.0", "id": 1, "result": {}})));
    }

    #[tokio::test]
    async fn explains_the_codex_install_requirement() {
        let path =
            std::env::temp_dir().join(format!("crabbot-codex-missing-{}", std::process::id()));

        let error = check_version(path.as_os_str(), None).await.unwrap_err().to_string();

        assert!(error.contains("Install Codex CLI"));
        assert!(error.contains("CRABBOT_CODEX_BINARY"));
    }

    #[test]
    fn requires_tool_names_and_arguments_from_the_active_turn() {
        let allowed = ["read".to_owned()].into_iter().collect();
        let request = json!({
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "tool": "read",
                "arguments": {"path": "README.md"}
            }
        });

        assert_eq!(tool_denial(&request, "thread-1", "turn-1", &allowed), None);

        let mut other = request.clone();
        other["params"]["tool"] = "shell".into();

        assert_eq!(
            tool_denial(&other, "thread-1", "turn-1", &allowed),
            Some("The requested tool was not declared by Crabbot.")
        );

        let mut mismatched = request.clone();
        mismatched["params"]["turnId"] = "turn-2".into();

        assert!(tool_denial(&mismatched, "thread-1", "turn-1", &allowed).is_some());

        let mut invalid = request;
        invalid["params"]["arguments"] = json!("README.md");

        assert!(tool_denial(&invalid, "thread-1", "turn-1", &allowed).is_some());
    }

    #[test]
    fn resolves_a_canonical_workspace() {
        let path = workspace(Some(".")).unwrap();

        assert!(path.is_absolute());
        assert!(path.is_dir());
        assert!(workspace(Some("./crabbot-core/src/lib.rs")).is_err());
        assert!(workspace(Some("./missing-workspace")).is_err());
    }

    #[test]
    fn accepts_model_requests_without_a_workspace_field() {
        let request = json!({"model": "codex", "messages": [], "stream": true});
        let parsed: crabbot_core::types::ModelRequest = serde_json::from_value(request).unwrap();

        assert_eq!(parsed.workspace, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn negotiates_and_streams_an_app_server_turn() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path =
            std::env::temp_dir().join(format!("crabbot-codex-{}-{nonce}", std::process::id()));

        let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
    printf '%s\n' 'codex-cli 99.0.0'
    exit 0
fi
while IFS= read -r line; do
    case "$line" in
        *initialize*) printf '%s\n' '{"id":1,"result":{}}' ;;
        *thread/start*) printf '%s\n' '{"id":2,"result":{"thread":{"id":"thread-1"}}}' ;;
        *turn/start*)
            printf '%s\n' '{"id":3,"result":{"turn":{"id":"turn-1"}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Hello","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"Hello"}]}}}'
            ;;
    esac
done
"#;

        write_binary(&path, script).unwrap();

        let mut server = Server::start_with(path.as_os_str().to_owned(), None).await.unwrap();
        server.initialize().await.unwrap();
        let thread =
            server.call("thread/start", json!({"cwd": ".", "ephemeral": true})).await.unwrap();

        let turn =
            server.call("turn/start", json!({"threadId": thread["thread"]["id"]})).await.unwrap();

        let (output, mut events) = mpsc::channel(8);
        let mut emitter = crate::Emitter::new(output);
        let text = server
            .turn(
                thread["thread"]["id"].as_str().unwrap(),
                turn["turn"]["id"].as_str().unwrap(),
                &std::collections::BTreeSet::new(),
                &mut emitter,
            )
            .await
            .unwrap();

        assert_eq!(text, "Hello");
        let event = events.try_recv().unwrap();

        assert_eq!(event["params"]["event"]["text"], "Hello");
        drop(server);
        let _ = std::fs::remove_file(path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn generates_in_the_active_workspace_with_host_tools() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let binary = std::env::temp_dir()
            .join(format!("crabbot-codex-generate-{}-{nonce}", std::process::id()));

        let root = std::env::temp_dir()
            .join(format!("crabbot-codex-workspace-{}-{nonce}", std::process::id()));

        let script = r#"#!/bin/sh

if [ "$1" = "--version" ]; then
    printf '%s\n' 'codex-cli 99.0.0'
    exit 0
fi

while IFS= read -r line; do
    case "$line" in
        *initialize*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}' ;;

        *account/read*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"account":{"type":"chatgpt"}}}' ;;
        *thread/start*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"thread":{"id":"thread-1"}}}' ;;
        *turn/start*)
            printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"turn":{"id":"turn-1"}}}'
            printf '%s\n' '{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":"Hello","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"Hello"}]}}}'
            ;;
    esac
done
"#;
        std::fs::create_dir(&root).unwrap();
        write_binary(&binary, script).unwrap();

        let (output, mut events) = mpsc::channel(8);
        let emitter = crate::Emitter::new(output);
        let response = generate_with(
            9,
            ModelRequest {
                model: "codex-model".into(),
                workspace: Some(root.display().to_string()),
                messages: vec![Message {
                    id: "user".into(),
                    session: "session".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: "Hello".into() }],
                }],

                stream: true,
                tools: vec![ToolSpec {
                    name: "read".into(),
                    description: Some("Read a workspace file.".into()),
                    schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
                }],
            },
            emitter,
            binary.as_os_str().to_owned(),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.id, 9);
        let reply: crabbot_core::types::ModelReply =
            serde_json::from_value(response.result.unwrap()).unwrap();

        assert_eq!(reply.text, "Hello");
        assert_eq!(events.try_recv().unwrap()["params"]["event"]["text"], "Hello");
        let _ = std::fs::remove_file(binary);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn completes_device_code_login_through_the_app_server() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir()
            .join(format!("crabbot-codex-login-{}-{nonce}", std::process::id()));

        let script = r#"#!/bin/sh

if [ "$1" = "--version" ]; then
    printf '%s\n' 'codex-cli 99.0.0'
    exit 0
fi

while IFS= read -r line; do
    case "$line" in
        *initialize*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}' ;;

        *account/login/start*)
            printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://example.test/device","userCode":"ABCD-EFGH"}}'
            printf '%s\n' '{"jsonrpc":"2.0","method":"account/login/completed","params":{"loginId":"login-1","success":true}}'
            ;;
    esac
done
"#;
        write_binary(&path, script).unwrap();

        let (output, mut events) = mpsc::channel(8);
        let mut emitter = crate::Emitter::new(output);
        let result = command_with(
            &json!({"name": "codex", "args": ["login", "--device"]}),
            &mut emitter,
            path.as_os_str().to_owned(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result, "Codex sign-in completed.");
        let event = events.try_recv().unwrap();

        assert!(event["params"]["event"]["text"].as_str().unwrap().contains("ABCD-EFGH"));
        let _ = std::fs::remove_file(path);
    }
}
