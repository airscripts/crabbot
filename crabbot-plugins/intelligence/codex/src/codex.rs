use std::{collections::BTreeSet, ffi::OsString, path::PathBuf, process::Stdio, time::Duration};

use crabbot_core::{
    jsonl,
    plugin::Emitter,
    types::{Content, ContextUsage, Message, ModelReply, ModelRequest, Response, Role, ToolSpec},
};

use serde_json::{Value, json};
use tokio::{
    io::BufReader,
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{Instant, MissedTickBehavior, timeout, timeout_at},
};

const BODY: usize = jsonl::MAX / 2;
const TOOLS: usize = 16;
const STREAM_EVENTS: usize = 200;
// App-server startup and control requests may take up to one minute.
const RPC_TIMEOUT: Duration = Duration::from_secs(60);
const TURN_TIMEOUT: Duration = Duration::from_secs(115);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(890);

const COMMAND_HELP: &str = "Manage Codex sign-in and models.\n\nUsage: crab codex <COMMAND>\n\nCommands:\n  login [--device]  Sign in to your Codex account.\n  status            Show whether Codex is signed in.\n  logout            Sign out of Codex.\n  models            List models available to this Codex account.\n  help              Show this help.\n\nRun `crab codex <command> --help` for command-specific help.";

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

    if command.is_empty() || matches!(command, "help" | "--help" | "-h") {
        return Ok(COMMAND_HELP.into());
    }

    if args.iter().skip(1).filter_map(Value::as_str).any(|value| matches!(value, "--help" | "-h")) {
        return Ok(subcommand_help(command));
    }

    let valid = match command {
        "login" => args.len() == 1 || (args.len() == 2 && args[1] == "--device"),
        "status" | "logout" => args.len() == 1,
        "models" => args.len() == 1,
        _ => false,
    };

    if !valid {
        return Err(denied(&format!("Invalid Codex command.\n\n{COMMAND_HELP}")));
    }

    let mut server = Server::start_with(binary, home).await?;
    server.initialize().await?;

    match command {
        "login" => {
            login(&mut server, args.get(1).is_some_and(|value| value == "--device"), emitter).await
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

        "models" => models_list(&mut server).await,

        _ => Err(denied("Invalid Codex command.")),
    }
}

fn subcommand_help(command: &str) -> String {
    match command {
        "login" => "Sign in to your Codex account.\n\nUsage: crab codex login [--device]\n\nOptions:\n  --device  Use device-code sign-in for a headless host.\n  -h, --help  Show this help.".into(),
        "status" => "Show whether Codex is signed in.\n\nUsage: crab codex status\n\nOptions:\n  -h, --help  Show this help.".into(),
        "logout" => "Sign out of Codex.\n\nUsage: crab codex logout\n\nOptions:\n  -h, --help  Show this help.".into(),
        "models" => "List models available to this Codex account.\n\nUsage: crab codex models\n\nOptions:\n  -h, --help  Show this help.".into(),
        _ => format!("Unknown Codex command: {command}.\n\n{COMMAND_HELP}"),
    }
}

async fn models_list(server: &mut Server) -> crabbot_core::Result<String> {
    let mut cursor = None;
    let mut models = Vec::new();

    for _ in 0..10 {
        let mut params = json!({"limit": 100, "includeHidden": false});

        if let Some(cursor) = cursor.take() {
            params["cursor"] = json!(cursor);
        }

        let page = server.call("model/list", params).await?;

        if let Some(items) = page["data"].as_array() {
            models.extend(items.iter().filter_map(|item| {
                let id = item["id"].as_str()?;
                let display = item["displayName"].as_str().unwrap_or(id);
                let default = item["isDefault"] == true;

                Some(if default {
                    format!("  {id} - {display} (default)")
                } else {
                    format!("  {id} - {display}")
                })
            }));
        }

        cursor = page["nextCursor"].as_str().map(str::to_owned);

        if cursor.is_none() {
            break;
        }
    }

    if models.is_empty() {
        return Ok("No Codex models are available for this account.".into());
    }

    let mut output = format!("Codex models ({}):\n{}", models.len(), models.join("\n"));

    if cursor.is_some() {
        output.push_str("\nOnly the first 1000 models are shown.");
    }

    Ok(output)
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

pub fn session() -> Session {
    Session::new(binary(), codex_home())
}

pub struct Session {
    binary: OsString,
    home: Option<PathBuf>,
    server: Option<Server>,
}

fn context_usage(params: &Value) -> Option<ContextUsage> {
    let usage = &params["tokenUsage"];
    let used = usage["last"]["totalTokens"].as_u64()?;
    let limit = usage["modelContextWindow"].as_u64()?;

    (limit > 0).then_some(ContextUsage { used, limit })
}

impl Session {
    pub fn new(binary: OsString, home: Option<PathBuf>) -> Self {
        Self { binary, home, server: None }
    }

    pub async fn generate(
        &mut self,
        id: u64,
        input: ModelRequest,
        emitter: Emitter,
    ) -> crabbot_core::Result<Option<Response>> {
        if self.server.is_none() {
            let mut server = Server::start_with(self.binary.clone(), self.home.clone()).await?;

            server.initialize().await?;

            self.server = Some(server);
        }

        let result = generate_on(self.server.as_mut().unwrap(), id, input, emitter).await;

        if result.is_err() {
            self.server.take();
        }

        result
    }
}

async fn generate_on(
    server: &mut Server,
    id: u64,
    input: ModelRequest,
    mut emitter: Emitter,
) -> crabbot_core::Result<Option<Response>> {
    let account = server.call("account/read", json!({})).await?;

    if account["account"].is_null() {
        return Err(denied("Codex is not signed in. Run crabbot codex login first."));
    }

    let tools = dynamic_tools(&input.tools)?;
    let allowed = input.tools.iter().map(|tool| tool.name.clone()).collect();
    let root = workspace(input.workspace.as_deref())?;
    let root = root.to_string_lossy().into_owned();
    let instructions = instructions(&input.messages)?;
    let tool_guidance = tool_guidance(&input.tools);
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
                    "{tool_guidance}\n\n{instructions}"
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

    let context_usage = server.context_usage;

    let response = Response::ok(
        id,
        serde_json::to_value(ModelReply {
            text,
            stop: "stop".into(),
            input: None,
            output: None,
            context_usage,
            events: Vec::new(),
        })?,
    );

    if serde_json::to_vec(&response)?.len().saturating_add(1) > jsonl::MAX {
        return Err(denied("Codex response exceeded the protocol frame limit."));
    }

    Ok(Some(response))
}

fn tool_guidance(tools: &[ToolSpec]) -> String {
    if tools.is_empty() {
        return "Workspace tools are unavailable. Say so for workspace tasks; don't claim access or tool failures, and don't suggest shell instead.".into();
    }

    let names = tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>().join(", ");

    format!(
        "Available Crabbot tools: {names}. Use workspace-relative paths inside the active workspace. Use relevant tools for workspace tasks; say a tool is unavailable only after an error. Make one mutating call at a time and wait for its result. Report failures; don't retry unchanged. Never claim success without confirmation or repeat confirmed work through another tool. Don't suggest shell as a substitute."
    )
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

                Content::ToolCall { .. } => output.push(json!({
                    "type": "text",
                    "text": format!("{}\n", content.render())
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
    failed_tool_calls: BTreeSet<String>,
    context_usage: Option<ContextUsage>,
}

fn tool_call_key(name: &str, args: &Value) -> crabbot_core::Result<String> {
    serde_json::to_string(&(name, args))
        .map_err(|_| denied("Codex tool arguments could not be fingerprinted."))
}

fn tool_call_failed(
    failed: &BTreeSet<String>,
    name: &str,
    args: &Value,
) -> crabbot_core::Result<bool> {
    Ok(failed.contains(&tool_call_key(name, args)?))
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

        Ok(Self {
            child,
            input: BufReader::new(stdout),
            output,
            next: 1,
            failed_tool_calls: BTreeSet::new(),
            context_usage: None,
        })
    }

    async fn initialize(&mut self) -> crabbot_core::Result<()> {
        self.call(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "crabbot",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
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
                    let message = value["error"]["message"]
                        .as_str()
                        .map(str::trim)
                        .filter(|message| !message.is_empty())
                        .map(|message| message.chars().take(300).collect::<String>());

                    let code = value["error"]["code"].as_i64();
                    let error = match (message, code) {
                        (Some(message), _) => format!("Codex rejected {method}: {message}"),
                        (None, Some(code)) => format!("Codex rejected {method} (error {code})."),
                        (None, None) => format!("Codex rejected {method}."),
                    };

                    return Err(denied(&error));
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
        self.turn_with_interval(thread, turn, allowed, emitter, Duration::from_millis(500)).await
    }

    async fn turn_with_interval(
        &mut self,
        thread: &str,
        turn: &str,
        allowed: &BTreeSet<String>,
        emitter: &mut Emitter,
        interval: Duration,
    ) -> crabbot_core::Result<String> {
        self.failed_tool_calls.clear();

        self.context_usage = None;

        let deadline = Instant::now() + TURN_TIMEOUT;

        let mut text = String::new();
        let mut pending = String::new();
        let mut message_item: Option<String> = None;
        let mut calls = 0_usize;
        let mut notices = 0_usize;
        let mut stream_events = 0_usize;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            let value = {
                let read = timeout_at(deadline, jsonl::read::<Value>(&mut self.input, jsonl::MAX));
                tokio::pin!(read);

                loop {
                    tokio::select! {
                        result = &mut read => {
                            break result
                                .map_err(|_| denied("Codex exceeded the turn time limit."))??
                                .ok_or_else(|| denied("Codex app-server closed during a turn."))?;
                        }

                        _ = ticker.tick(), if !pending.is_empty() => {
                            emit(&mut pending, &mut stream_events, emitter).await?;
                        }
                    }
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

            if value["method"] == "thread/tokenUsage/updated" {
                let params = &value["params"];

                if params["threadId"] == thread && params["turnId"] == turn {
                    self.context_usage = context_usage(params);
                }

                continue;
            }

            if value["method"] == "item/agentMessage/delta" {
                if value["params"]["threadId"] != thread || value["params"]["turnId"] != turn {
                    continue;
                }

                if let Some(delta) = value["params"]["delta"].as_str() {
                    if let Some(item_id) = value["params"]["itemId"].as_str() {
                        if message_item.as_deref().is_some_and(|previous| previous != item_id) {
                            append("\n\n", &mut text, &mut pending)?;
                        }

                        message_item = Some(item_id.to_owned());
                    }

                    append(delta, &mut text, &mut pending)?;
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

                emit(&mut pending, &mut stream_events, emitter).await?;
                return Ok(text);
            } else if value.get("id").is_some() {
                calls = calls.saturating_add(1);

                if calls > TOOLS {
                    return Err(denied("Codex exceeded the tool-call limit."));
                }

                self.tool(&value, thread, turn, allowed, emitter).await?;
            } else if value["method"] == "error" {
                return Err(denied(&app_server_error(&value)));
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
        let args = &params["arguments"];
        let key = tool_call_key(name, args)?;

        if tool_call_failed(&self.failed_tool_calls, name, args)? {
            return Err(denied(
                "Codex stopped because the same tool call failed earlier in this turn. Check the tool error and workspace before retrying.",
            ));
        }

        let response = emitter.call("host/tool", json!({"name": name, "args": args})).await?;

        let (success, output) = if let Some(error) = response.error {
            self.failed_tool_calls.insert(key);
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
    let messages = turn["items"]
        .as_array()?
        .iter()
        .filter_map(|item| {
            (item["type"] == "agentMessage")
                .then(|| item["text"].as_str())
                .flatten()
                .filter(|text| !text.is_empty())
        })
        .collect::<Vec<_>>();

    (!messages.is_empty()).then(|| messages.join("\n\n"))
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

async fn emit(
    pending: &mut String,
    events: &mut usize,
    emitter: &mut Emitter,
) -> crabbot_core::Result<()> {
    if !pending.is_empty() {
        let text = std::mem::take(pending);

        if *events < STREAM_EVENTS {
            emitter.event(json!({"kind": "text", "text": text})).await?;
            *events += 1;
        }
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

fn app_server_error(value: &Value) -> String {
    let message = value
        .pointer("/params/error/message")
        .or_else(|| value.pointer("/params/message"))
        .or_else(|| value.pointer("/message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|message| !message.is_empty());

    let Some(message) = message else {
        return "Codex app-server reported an error.".into();
    };

    let message = message
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let message = message.chars().take(300).collect::<String>();

    format!("Codex app-server error: {message}")
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
        Server, check_version, command_with, context_usage, dynamic_tools, final_text,
        instructions, prompt, tool_call_failed, tool_call_key, tool_denial, tool_guidance,
        valid_rpc, version_reported, workspace,
    };

    use std::time::Duration;

    use crabbot_core::types::{Content, Message, ModelRequest, Role, ToolSpec};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[test]
    fn reads_context_window_and_latest_token_usage() {
        let usage = context_usage(&json!({
            "tokenUsage": {
                "modelContextWindow": 128000,
                "last": {"totalTokens": 12345}
            }
        }));

        assert_eq!(usage, Some(crabbot_core::types::ContextUsage { used: 12345, limit: 128000 }));

        assert_eq!(context_usage(&json!({"tokenUsage": {"last": {"totalTokens": 3}}})), None);
        assert_eq!(
            context_usage(&json!({
                "tokenUsage": {
                    "modelContextWindow": 0,
                    "last": {"totalTokens": 3}
                }
            })),
            None
        );
    }

    #[test]
    fn keeps_tool_guidance_compact_and_preserves_safety_rules() {
        let tools = vec![ToolSpec { name: "read".into(), description: None, schema: json!({}) }];
        let guidance = tool_guidance(&tools);

        assert!(guidance.len() < 500);
        assert!(guidance.contains("read"));
        assert!(guidance.contains("workspace-relative paths"));
        assert!(guidance.contains("only after an error"));
        assert!(guidance.contains("wait for its result"));
        assert!(guidance.contains("don't retry unchanged"));
        assert!(guidance.contains("repeat confirmed work through another tool"));
        assert!(guidance.contains("shell as a substitute"));

        let no_tools = tool_guidance(&[]);

        assert!(no_tools.len() < 150);
        assert!(no_tools.contains("Workspace tools are unavailable"));
        assert!(no_tools.contains("don't claim access or tool failures"));
    }

    #[test]
    fn reports_bounded_app_server_error_details() {
        let message = super::app_server_error(&json!({
            "method": "error",
            "params": {"error": {"message": "  session expired\nplease retry  "}}
        }));

        assert_eq!(message, "Codex app-server error: session expired please retry");
        assert_eq!(
            super::app_server_error(&json!({"method": "error"})),
            "Codex app-server reported an error."
        );

        assert!(
            super::app_server_error(&json!({"params": {"message": "x".repeat(400)}})).len() < 340
        );
    }

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
    fn describes_only_tools_declared_for_the_turn() {
        let tool = ToolSpec {
            name: "read".into(),
            description: Some("Read a workspace file.".into()),
            schema: json!({"type": "object"}),
        };

        let guidance = tool_guidance(&[tool]);

        assert!(guidance.contains("read"));
        assert!(guidance.contains("workspace-relative paths"));
        assert!(guidance.contains("only after an error"));
        assert!(guidance.contains("Make one mutating call at a time"));
        assert!(guidance.contains("repeat confirmed work through another tool"));

        let guidance = tool_guidance(&[]);

        assert!(guidance.contains("Workspace tools are unavailable"));
        assert!(guidance.contains("don't claim access or tool failures"));
    }

    #[test]
    fn detects_repeated_failed_tool_calls_by_name_and_arguments() {
        let args = json!({"path": "outside.txt", "text": "data"});
        let mut failed = std::collections::BTreeSet::new();

        assert!(!tool_call_failed(&failed, "write", &args).unwrap());
        failed.insert(tool_call_key("write", &args).unwrap());

        assert!(tool_call_failed(&failed, "write", &args).unwrap());
        assert!(!tool_call_failed(&failed, "write", &json!({"path": "inside.txt"})).unwrap());
        assert!(!tool_call_failed(&failed, "read", &args).unwrap());
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

        assert_eq!(final_text(&turn).as_deref(), Some("Earlier.\n\nFinal."));
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

    #[cfg(unix)]
    #[tokio::test]
    async fn shows_codex_command_help_without_starting_the_cli() {
        let path =
            std::env::temp_dir().join(format!("crabbot-codex-no-cli-{}", std::process::id()));

        let (output, _) = mpsc::channel(1);
        let mut emitter = crate::Emitter::new(output);

        let help = command_with(
            &json!({"name": "codex", "args": ["--help"]}),
            &mut emitter,
            path.as_os_str().to_owned(),
            None,
        )
        .await
        .unwrap();

        assert!(help.contains("login [--device]"));
        assert!(help.contains("models            List models"));
        assert!(!help.contains("models list"));

        let help = command_with(
            &json!({"name": "codex", "args": ["login", "--help"]}),
            &mut emitter,
            path.as_os_str().to_owned(),
            None,
        )
        .await
        .unwrap();

        assert!(help.contains("Usage: crab codex login [--device]"));

        let help = command_with(
            &json!({"name": "codex", "args": ["models", "--help"]}),
            &mut emitter,
            path.as_os_str().to_owned(),
            None,
        )
        .await
        .unwrap();

        assert!(help.contains("Usage: crab codex models"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lists_models_and_explains_app_server_rejections() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir()
            .join(format!("crabbot-codex-models-{}-{nonce}", std::process::id()));

        let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
    printf '%s\n' 'codex-cli 99.0.0'
    exit 0
fi
while IFS= read -r line; do
    case "$line" in
        *initialize*)
            case "$line" in
                *'"experimentalApi":true'*) printf '%s\n' '{"id":1,"result":{}}' ;;
                *) printf '%s\n' '{"id":1,"error":{"code":-32602,"message":"experimentalApi capability is required"}}' ;;
            esac
            ;;
        *model/list*) printf '%s\n' '{"id":2,"result":{"data":[{"id":"gpt-test","displayName":"Test model","isDefault":true}],"nextCursor":null}}' ;;
        *thread/start*) printf '%s\n' '{"id":3,"error":{"code":-32602,"message":"Invalid model: gpt-unknown"}}' ;;
    esac
done
"#;

        write_binary(&path, script).unwrap();
        let mut server = Server::start_with(path.as_os_str().to_owned(), None).await.unwrap();
        server.initialize().await.unwrap();

        let listing = super::models_list(&mut server).await.unwrap();
        assert!(listing.contains("gpt-test - Test model (default)"));

        let error = server.call("thread/start", json!({})).await.unwrap_err().to_string();
        assert!(error.contains("thread/start"));
        assert!(error.contains("Invalid model: gpt-unknown"));

        drop(server);
        let _ = std::fs::remove_file(path);
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
    async fn coalesces_a_long_app_server_stream_within_the_event_budget() {
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
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"First ","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"answer.","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Second ","itemId":"item-2","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"answer.","itemId":"item-2","threadId":"thread-1","turnId":"turn-1"}}'
            i=0

            while [ "$i" -lt 300 ]; do
                printf '%s' '{"method":"item/agentMessage/delta","params":{"delta":"'
                j=0

                while [ "$j" -lt 128 ]; do
                    printf 'x'
                    j=$((j + 1))
                done

                printf '%s\n' '","itemId":"item-2","threadId":"thread-1","turnId":"turn-1"}}'
                sleep 0.005
                i=$((i + 1))
            done

            printf '%s' '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"First answer."},{"id":"item-2","type":"agentMessage","text":"Second answer.'
            i=0

            while [ "$i" -lt 300 ]; do
                j=0

                while [ "$j" -lt 128 ]; do
                    printf 'x'
                    j=$((j + 1))
                done

                i=$((i + 1))
            done
            printf '%s\n' '"}]}}}'
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

        let (output, mut events) = mpsc::channel(256);
        let mut emitter = crate::Emitter::new(output);
        let text = server
            .turn_with_interval(
                thread["thread"]["id"].as_str().unwrap(),
                turn["turn"]["id"].as_str().unwrap(),
                &std::collections::BTreeSet::new(),
                &mut emitter,
                Duration::from_millis(1),
            )
            .await
            .unwrap();

        let expected = format!("First answer.\n\nSecond answer.{}", "x".repeat(38_400));

        assert_eq!(text, expected);

        let mut streamed = String::new();

        let mut event_count = 0;

        while let Ok(event) = events.try_recv() {
            event_count += 1;
            streamed.push_str(event["params"]["event"]["text"].as_str().unwrap());
        }

        assert!(event_count <= super::STREAM_EVENTS);
        assert!(streamed.len() < expected.len());
        assert!(expected.starts_with(&streamed));
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
    printf 'x\n' >> "$0.versions"
    printf '%s\n' 'codex-cli 99.0.0'
    exit 0
fi

while IFS= read -r line; do
    printf '%s\n' "$line" >> "$0.requests"
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    case "$line" in
        *\"method\":\"initialize\"*) printf 'x\n' >> "$0.initialized"; printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;

        *account/read*) printf '{"jsonrpc":"2.0","id":%s,"result":{"account":{"type":"chatgpt"}}}\n' "$id" ;;
        *thread/start*) printf 'x\n' >> "$0.threads"; printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-1"}}}\n' "$id" ;;
        *turn/start*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"turn":{"id":"turn-1"}}}\n' "$id"
            printf '%s\n' '{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"delta":"Hello","itemId":"item-1","threadId":"thread-1","turnId":"turn-1"}}'
            printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"Hello"}]}}}'
            ;;
    esac
done
"#;
        std::fs::create_dir(&root).unwrap();
        write_binary(&binary, script).unwrap();

        let (output, mut events) = mpsc::channel(8);
        let mut session = super::Session::new(binary.as_os_str().to_owned(), None);

        for id in [9, 10] {
            let response = session
                .generate(
                    id,
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
                    crate::Emitter::new(output.clone()),
                )
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.id, id);
            let reply: crabbot_core::types::ModelReply =
                serde_json::from_value(response.result.unwrap()).unwrap();

            assert_eq!(reply.text, "Hello");
        }

        assert_eq!(
            std::fs::read_to_string(format!("{}.initialized", binary.display()))
                .unwrap()
                .lines()
                .count(),
            1
        );

        assert_eq!(
            std::fs::read_to_string(format!("{}.versions", binary.display()))
                .unwrap()
                .lines()
                .count(),
            1
        );

        assert_eq!(
            std::fs::read_to_string(format!("{}.threads", binary.display()))
                .unwrap()
                .lines()
                .count(),
            2
        );

        assert!(
            std::fs::read_to_string(format!("{}.requests", binary.display()))
                .unwrap()
                .contains(
                    "Never claim success without confirmation or repeat confirmed work through another tool"
                )
        );

        assert_eq!(events.try_recv().unwrap()["params"]["event"]["text"], "Hello");
        let _ = std::fs::remove_file(format!("{}.initialized", binary.display()));
        let _ = std::fs::remove_file(format!("{}.versions", binary.display()));
        let _ = std::fs::remove_file(format!("{}.threads", binary.display()));
        let _ = std::fs::remove_file(format!("{}.requests", binary.display()));
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
