use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Protocol {
    pub major: u16,
    pub minor: u16,
}

impl Protocol {
    pub const CURRENT: Self = Self { major: 0, minor: 1 };

    pub const fn compatible(self, other: Self) -> bool {
        self.major == other.major && self.minor >= other.minor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Model,
    Vision,
    Channel,
    Store,
    Memory,
    Timer,
    Tool,
    Mcp,
    Speech,
    Client,
    Resource,
    Agent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Hello {
    pub protocol: Protocol,
    pub id: String,
    pub version: String,
    pub capabilities: Vec<Capability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<CommandSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandSpec {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub interactive: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Content {
    Text { text: String },
    Image { uri: String, alt: Option<String> },
    File { uri: String, name: String, mime: Option<String> },
    Audio { uri: String, mime: Option<String> },
}

impl Content {
    pub fn render(&self) -> String {
        match self {
            Self::Text { text } => text.clone(),

            Self::Image { alt, .. } => alt
                .as_deref()
                .map_or_else(|| "[Image attachment.]".into(), |alt| format!("[Image: {alt}]")),

            Self::File { name, .. } => format!("[File: {name}]"),
            Self::Audio { .. } => "[Audio attachment.]".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Message {
    pub id: String,
    pub session: String,
    pub role: Role,
    pub sender: Option<String>,
    pub content: Vec<Content>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequest {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    pub messages: Vec<Message>,
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelReply {
    pub text: String,
    pub stop: String,
    pub input: Option<u64>,
    pub output: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    Text { text: String },
    Tool { name: String, args: Value },
    Done { text: String },
    Error { message: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Request {
    Call { jsonrpc: String, id: u64, method: String, params: Value },
    Note { jsonrpc: String, method: String, params: Value },
}

impl Request {
    pub fn call(id: u64, method: impl Into<String>, params: Value) -> Self {
        Self::Call { jsonrpc: "2.0".into(), id, method: method.into(), params }
    }

    pub const fn id(&self) -> Option<u64> {
        match self {
            Self::Call { id, .. } => Some(*id),
            Self::Note { .. } => None,
        }
    }

    pub fn valid(&self) -> bool {
        match self {
            Self::Call { jsonrpc, .. } | Self::Note { jsonrpc, .. } => jsonrpc == "2.0",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn valid(&self) -> bool {
        self.jsonrpc == "2.0" && self.result.is_some() != self.error.is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub token: String,
    pub method: String,
    pub params: Value,
}

impl IpcRequest {
    pub fn call(
        id: u64,
        token: impl Into<String>,
        method: impl Into<String>,
        params: Value,
    ) -> Self {
        Self { jsonrpc: "2.0".into(), id, token: token.into(), method: method.into(), params }
    }

    pub fn valid(&self) -> bool {
        self.jsonrpc == "2.0" && !self.token.is_empty() && !self.method.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl IpcResponse {
    pub fn ok(id: u64, result: Value) -> Self {
        Self { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
    }

    pub fn fail(id: u64, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError { code, message: message.into(), data: None }),
        }
    }

    pub fn valid(&self) -> bool {
        self.jsonrpc == "2.0" && self.result.is_some() != self.error.is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Response {
    pub fn ok(id: u64, result: Value) -> Self {
        Self { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
    }

    pub fn fail(id: u64, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError { code, message: message.into(), data: None }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_requires_matching_major_and_supported_minor() {
        assert!(Protocol::CURRENT.compatible(Protocol { major: 0, minor: 0 }));
        assert!(!Protocol::CURRENT.compatible(Protocol { major: 1, minor: 0 }));
        assert!(!Protocol { major: 0, minor: 0 }.compatible(Protocol::CURRENT));
    }

    #[test]
    fn requests_and_responses_round_trip() {
        let call = Request::call(7, "generate", serde_json::json!({"model": "local"}));
        assert_eq!(call.id(), Some(7));

        let note = Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: serde_json::json!({}),
        };

        assert_eq!(note.id(), None);
        assert!(call.valid());
        assert!(note.valid());

        assert!(
            !Request::Call {
                jsonrpc: "1.0".into(),
                id: 1,
                method: "ping".into(),
                params: serde_json::json!({}),
            }
            .valid()
        );

        let response = Response::ok(7, serde_json::json!({"ok": true}));
        let encoded = serde_json::to_string(&response).unwrap();
        assert_eq!(serde_json::from_str::<Response>(&encoded).unwrap(), response);

        let failed = Response::fail(8, -1, "failed");
        assert_eq!(failed.error.as_ref().unwrap().message, "failed");
    }

    #[test]
    fn model_requests_accept_legacy_payloads_without_a_workspace() {
        let value = serde_json::json!({
            "model": "local",
            "messages": [],
            "stream": true
        });

        let request: ModelRequest = serde_json::from_value(value).unwrap();
        assert_eq!(request.workspace, None);
        assert!(serde_json::to_value(request).unwrap().get("workspace").is_none());
    }

    #[test]
    fn content_and_events_use_stable_tags() {
        let content = Content::Image { uri: "file://image".into(), alt: None };
        assert_eq!(serde_json::to_value(content).unwrap()["kind"], "image");

        assert_eq!(Content::Text { text: "hello".into() }.render(), "hello");
        assert_eq!(
            Content::Image { uri: "file://image".into(), alt: Some("diagram".into()) }.render(),
            "[Image: diagram]"
        );

        assert!(
            !Content::Image { uri: "file://image".into(), alt: None }.render().contains("file://")
        );

        assert_eq!(
            Content::File {
                uri: "file://note".into(),
                name: "note.txt".into(),
                mime: Some("text/plain".into())
            }
            .render(),
            "[File: note.txt]"
        );

        assert_eq!(
            Content::Audio { uri: "file://voice".into(), mime: None }.render(),
            "[Audio attachment.]"
        );

        let event = Event::Tool { name: "read".into(), args: serde_json::json!({}) };
        assert_eq!(serde_json::to_value(event).unwrap()["kind"], "tool");
        let legacy = serde_json::from_value::<Event>(serde_json::json!({
            "kind": "tool",
            "name": "read",
            "args": {},
            "approve": true
        }))
        .unwrap();

        assert!(matches!(legacy, Event::Tool { name, .. } if name == "read"));
    }

    #[test]
    fn validates_ipc_requests_and_builds_errors() {
        let mut request = IpcRequest::call(1, "token", "status", serde_json::json!({}));
        assert!(request.valid());
        request.token.clear();
        assert!(!request.valid());
        request.token = "token".into();
        request.method.clear();
        assert!(!request.valid());
        request.method = "status".into();
        request.jsonrpc = "1.0".into();
        assert!(!request.valid());
        let error = IpcResponse::fail(1, 400, "bad");
        assert_eq!(error.error.unwrap().code, 400);
        assert_eq!(Response::fail(2, 500, "failed").error.unwrap().message, "failed");
    }
}
