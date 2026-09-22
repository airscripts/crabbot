use super::*;
use crabbot_core::types::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn loopback_listener() -> Option<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
        Err(error) => panic!("Could not bind the Gemini test listener: {error}."),
    }
}

#[test]
fn builds_gemini_messages_and_tools() {
    let request = ModelRequest {
        model: "gemini-2.5-flash".into(),
        workspace: None,
        messages: vec![
            Message {
                id: "system".into(),
                session: "s".into(),
                role: Role::System,
                sender: None,
                content: vec![Content::Text { text: "Be concise.".into() }],
            },
            Message {
                id: "user".into(),
                session: "s".into(),
                role: Role::User,
                sender: None,
                content: vec![Content::Image { uri: "file://image".into(), alt: None }],
            },
        ],
        stream: false,
        tools: vec![crabbot_core::types::ToolSpec {
            name: "read".into(),
            description: Some("Read a file.".into()),
            schema: json!({"type": "object"}),
        }],
    };

    let body = request_body(&request).unwrap();

    assert_eq!(body["systemInstruction"]["parts"][0]["text"], "Be concise.");
    assert_eq!(body["contents"][0]["role"], "user");
    assert_eq!(body["tools"][0]["functionDeclarations"][0]["name"], "read");

    let mut input = request;
    input.messages.push(Message {
        id: "assistant-tool".into(),
        session: "s".into(),
        role: Role::Assistant,
        sender: None,
        content: vec![Content::Text {
            text: "I will read it.\n[Tool call read]: {\"path\":\"README.md\"}".into(),
        }],
    });

    input.messages.push(Message {
        id: "tool".into(),
        session: "s".into(),
        role: Role::Tool,
        sender: None,
        content: vec![Content::Text { text: "content".into() }],
    });

    let body = request_body(&input).unwrap();

    assert_eq!(body["contents"][1]["role"], "model");
    assert_eq!(body["contents"][1]["parts"][0]["text"], "I will read it.\n");
    assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["name"], "read");

    assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["args"]["path"], "README.md");

    assert_eq!(body["contents"][1]["parts"][1]["functionCall"]["id"], "assistant-tool-0");

    assert_eq!(body["contents"][2]["role"], "user");
    assert_eq!(body["contents"][2]["parts"][0]["functionResponse"]["name"], "read");

    assert_eq!(body["contents"][2]["parts"][0]["functionResponse"]["id"], "assistant-tool-0");

    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"]["output"],
        "content"
    );

    input.messages.push(Message {
        id: "media".into(),
        session: "s".into(),
        role: Role::User,
        sender: None,
        content: vec![
            Content::Image { uri: "file://image".into(), alt: Some("alt".into()) },
            Content::File {
                uri: "file://note".into(),
                name: "note.txt".into(),
                mime: Some("text/plain".into()),
            },
            Content::Audio { uri: "file://voice".into(), mime: None },
        ],
    });

    let body = request_body(&input).unwrap();

    assert_eq!(body["contents"][3]["parts"][0]["text"], "alt");
    assert_eq!(body["contents"][3]["parts"][1]["text"], "[File: note.txt]");
    assert_eq!(body["contents"][3]["parts"][2]["text"], "[Audio attachment.]");
}

#[test]
fn encodes_bounded_data_images_as_gemini_inline_parts() {
    let input = ModelRequest {
        model: "gemini-2.5-flash".into(),
        workspace: None,
        messages: vec![Message {
            id: "image".into(),
            session: "s".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Image {
                uri: "data:image/jpeg;base64,aW1hZ2U=".into(),
                alt: Some("photo".into()),
            }],
        }],
        stream: false,
        tools: Vec::new(),
    };

    let body = request_body(&input).unwrap();

    assert_eq!(
        body["contents"][0]["parts"][0],
        json!({"inlineData": {"mimeType": "image/jpeg", "data": "aW1hZ2U="}})
    );

    assert_eq!(body["contents"][0]["parts"][1]["text"], "photo");
}

#[test]
fn preserves_large_function_call_arguments() {
    let args = json!({"patch": "x".repeat(4097)});
    let input = ModelRequest {
        model: "gemini-2.5-flash".into(),
        workspace: None,
        messages: vec![Message {
            id: "assistant-large".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::Text { text: format!("[Tool call patch]: {args}") }],
        }],
        stream: false,
        tools: Vec::new(),
    };

    let body = request_body(&input).unwrap();

    assert_eq!(body["contents"][0]["parts"][0]["functionCall"]["args"], args);
    assert_eq!(body["contents"][0]["parts"][0]["functionCall"]["id"], "assistant-large-0");
}

#[test]
fn builds_tool_and_assistant_messages_without_matching_calls() {
    let input = ModelRequest {
        model: "default".into(),
        workspace: None,
        messages: vec![
            Message {
                id: "assistant-media".into(),
                session: "s".into(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Image { uri: "file://image".into(), alt: None }],
            },
            Message {
                id: "tool-unmatched".into(),
                session: "s".into(),
                role: Role::Tool,
                sender: Some("missing".into()),
                content: vec![Content::Image { uri: "file://image".into(), alt: None }],
            },
            Message {
                id: "user-media".into(),
                session: "s".into(),
                role: Role::User,
                sender: None,
                content: vec![Content::Audio { uri: "file://audio".into(), mime: None }],
            },
        ],
        stream: false,
        tools: Vec::new(),
    };

    let body = request_body(&input).unwrap();

    assert_eq!(body["contents"][0]["parts"][0]["text"], "[Image attachment.]");
    assert_eq!(body["contents"][1]["parts"][0]["functionResponse"]["name"], "missing");
    assert_eq!(body["contents"][2]["parts"][0]["text"], "[Audio attachment.]");
}

#[test]
fn groups_parallel_tool_results_in_one_user_content_block() {
    let input = ModelRequest {
        model: "gemini-3.8-flash".into(),
        workspace: None,
        messages: vec![
            Message {
                id: "assistant-tool".into(),
                session: "s".into(),
                role: Role::Assistant,
                sender: None,
                content: vec![
                    Content::ToolCall {
                        name: "read".into(),
                        args: json!({"path": "README.md"}),
                        id: Some("call-1".into()),
                        thought_signature: Some("sig-1".into()),
                    },
                    Content::ToolCall {
                        name: "patch".into(),
                        args: json!({"path": "Cargo.toml"}),
                        id: Some("call-2".into()),
                        thought_signature: None,
                    },
                ],
            },
            Message {
                id: "tool-1".into(),
                session: "s".into(),
                role: Role::Tool,
                sender: Some("read".into()),
                content: vec![Content::Text { text: "read result".into() }],
            },
            Message {
                id: "tool-2".into(),
                session: "s".into(),
                role: Role::Tool,
                sender: Some("patch".into()),
                content: vec![Content::Text { text: "patch result".into() }],
            },
        ],
        stream: false,
        tools: Vec::new(),
    };

    let body = request_body(&input).unwrap();
    let responses = &body["contents"][1];

    assert_eq!(body["contents"].as_array().unwrap().len(), 2);
    assert_eq!(responses["role"], "user");
    assert_eq!(responses["parts"].as_array().unwrap().len(), 2);
    assert_eq!(responses["parts"][0]["functionResponse"]["name"], "read");
    assert_eq!(responses["parts"][1]["functionResponse"]["name"], "patch");
}

#[test]
fn preserves_gemini_three_tool_signatures_and_migrates_legacy_calls() {
    let request = ModelRequest {
        model: "gemini-3.8-flash".into(),
        workspace: None,
        messages: vec![Message {
            id: "assistant-tool".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::ToolCall {
                name: "read".into(),
                args: json!({"path": "README.md"}),
                id: Some("call-1".into()),
                thought_signature: Some("sig-1".into()),
            }],
        }],
        stream: false,
        tools: vec![crabbot_core::types::ToolSpec {
            name: "read".into(),
            description: None,
            schema: json!({"type": "object"}),
        }],
    };

    let body = request_body(&request).unwrap();

    assert_eq!(
        body["contents"][0]["parts"][0],
        json!({
            "functionCall": {
                "id": "call-1",
                "name": "read",
                "args": {"path": "README.md"}
            },
            "thoughtSignature": "sig-1"
        })
    );

    let mut parallel = request.clone();
    parallel.model = "default".into();
    parallel.messages[0].content.push(Content::ToolCall {
        name: "read".into(),
        args: json!({"path": "Cargo.toml"}),
        id: Some("call-2".into()),
        thought_signature: None,
    });

    assert_eq!(effective_model(&parallel), DEFAULT_MODEL);

    let body = request_body(&parallel).unwrap();

    assert_eq!(body["contents"][0]["parts"][1]["functionCall"]["name"], "read");
    assert!(body["contents"][0]["parts"][1].get("thoughtSignature").is_none());

    let mut legacy = request;
    legacy.messages[0].content[0] =
        Content::Text { text: "[Tool call read]: {\"path\":\"README.md\"}".into() };

    let body = request_body(&legacy).unwrap();

    assert_eq!(
        body["contents"][0]["parts"][0]["text"],
        "[Tool call read]: {\"path\":\"README.md\"}"
    );

    assert!(body["contents"][0]["parts"][0].get("functionCall").is_none());
}

#[test]
fn migrates_legacy_tool_history_for_explicit_gemini_three() {
    let request = ModelRequest {
        model: "gemini-3.8-flash".into(),
        workspace: None,
        messages: vec![
            Message {
                id: "assistant-tool".into(),
                session: "s".into(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text {
                    text: "[Tool call read]: {\"path\":\"README.md\"}".into(),
                }],
            },
            Message {
                id: "tool".into(),
                session: "s".into(),
                role: Role::Tool,
                sender: Some("read".into()),
                content: vec![Content::Text { text: "content".into() }],
            },
            Message {
                id: "user".into(),
                session: "s".into(),
                role: Role::User,
                sender: None,
                content: vec![Content::Text { text: "Continue.".into() }],
            },
        ],
        stream: false,
        tools: Vec::new(),
    };

    let body = request_body(&request).unwrap();

    assert_eq!(body["contents"][0]["role"], "model");
    assert_eq!(
        body["contents"][0]["parts"][0]["text"],
        "[Tool call read]: {\"path\":\"README.md\"}"
    );

    assert_eq!(body["contents"][1]["role"], "user");
    assert_eq!(body["contents"][1]["parts"][0]["text"], "[Tool result read]: content");
    assert!(body["contents"][1]["parts"][0].get("functionResponse").is_none());
    assert_eq!(body["contents"][2]["parts"][0]["text"], "Continue.");
}

#[test]
fn routes_default_legacy_transcripts_to_gemini_two() {
    let request = ModelRequest {
        model: "default".into(),
        workspace: None,
        messages: vec![Message {
            id: "assistant-tool".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::Text {
                text: "I will read it.\n[Tool call read]: {\"path\":\"README.md\"}".into(),
            }],
        }],
        stream: false,
        tools: Vec::new(),
    };

    assert_eq!(effective_model(&request), LEGACY_MODEL);
    let body = request_body(&request).unwrap();

    assert_eq!(body["contents"][0]["parts"][1]["functionCall"]["name"], "read");
    assert!(body["contents"][0]["parts"][1].get("thoughtSignature").is_none());
}

#[test]
fn routes_the_host_default_model_to_gemini_default() {
    let request = ModelRequest {
        model: HOST_DEFAULT_MODEL.into(),
        workspace: None,
        messages: Vec::new(),
        stream: false,
        tools: Vec::new(),
    };

    assert_eq!(effective_model(&request), DEFAULT_MODEL);
}

#[test]
fn routes_default_signatureless_tool_history_to_gemini_two() {
    let request = ModelRequest {
        model: "default".into(),
        workspace: None,
        messages: vec![Message {
            id: "assistant-tool".into(),
            session: "s".into(),
            role: Role::Assistant,
            sender: None,
            content: vec![Content::ToolCall {
                name: "read".into(),
                args: json!({"path": "README.md"}),
                id: Some("call-1".into()),
                thought_signature: None,
            }],
        }],
        stream: false,
        tools: Vec::new(),
    };

    assert_eq!(effective_model(&request), LEGACY_MODEL);
    let body = request_body(&request).unwrap();

    assert_eq!(body["contents"][0]["parts"][0]["functionCall"]["name"], "read");
    assert!(body["contents"][0]["parts"][0].get("thoughtSignature").is_none());
}

#[tokio::test]
async fn rejects_notes_and_unknown_calls_before_credentials() {
    let client = reqwest::Client::new();

    let note = Request::Note { jsonrpc: "2.0".into(), method: "note".into(), params: json!({}) };

    assert!(generate(&client, note).await.unwrap().is_none());
    assert!(generate(&client, Request::call(1, "other", json!({}))).await.unwrap().is_none());
    assert!(generate(&client, Request::call(1, "generate", json!("bad"))).await.is_err());
}

#[tokio::test]
async fn validates_gemini_configuration_before_network_access() {
    let client = reqwest::Client::new();
    let request = ModelRequest {
        model: "test".into(),
        workspace: None,
        messages: Vec::new(),
        stream: false,
        tools: Vec::new(),
    };

    assert!(generate_configured(&client, 1, request.clone(), "key", "not a URL").await.is_err());

    assert!(
        generate_configured(&client, 1, request.clone(), "key", "http://example.com")
            .await
            .is_err()
    );

    assert!(generate_configured(&client, 1, request, "key", "http://127.0.0.1:1").await.is_err());
}

#[test]
fn parses_text_and_function_calls() {
    let value = json!({
        "candidates": [{"content": {"parts": [
            {"text": "I will read it."},
            {"functionCall": {
                "name": "read",
                "args": {"path": "README.md"},
                "id": "call-1"
            }, "thoughtSignature": "sig-1"}
        ]}}]
    });

    let (text, events) = parse_reply(&value);

    assert_eq!(text, "I will read it.");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        Event::Tool {
            name: "read".into(),
            args: json!({"path": "README.md"}),
            id: Some("call-1".into()),
            thought_signature: Some("sig-1".into()),
        }
    );

    assert_eq!(parse_reply(&json!({})), (String::new(), Vec::new()));
    assert_eq!(
        parse_reply(&json!({"candidates": [{"content": {"parts": [
            {"functionCall": {"args": {}}},
            {}
        ]}}]})),
        (String::new(), Vec::new())
    );
}

#[tokio::test]
async fn sends_a_bounded_request_and_parses_status() {
    let Some(listener) = loopback_listener().await else {
        return;
    };

    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.unwrap();
        let body = br#"{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}"#;

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );

        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    });

    let client = reqwest::Client::new();
    let request = ModelRequest {
        model: "test".into(),
        workspace: None,
        messages: Vec::new(),
        stream: false,
        tools: Vec::new(),
    };

    let response = generate_at(&client, 1, request, "key", &format!("http://{address}"))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response.result.unwrap()["text"], "ok");
    server.await.unwrap();
}

#[tokio::test]
async fn uses_the_default_model_and_parses_tool_replies() {
    let Some(listener) = loopback_listener().await else {
        return;
    };

    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let length = stream.read(&mut request).await.unwrap();

        assert!(
            std::str::from_utf8(&request[..length])
                .unwrap()
                .contains("POST /models/gemini-3.8-flash:generateContent?key=key HTTP/1.1")
        );

        let body = br#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","args":{"path":"README.md"}}}]}}]}"#;

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );

        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    });

    let request = ModelRequest {
        model: "default".into(),
        workspace: None,
        messages: Vec::new(),
        stream: false,
        tools: Vec::new(),
    };

    let response =
        generate_at(&reqwest::Client::new(), 1, request, "key", &format!("http://{address}"))
            .await
            .unwrap()
            .unwrap();

    assert_eq!(response.result.unwrap()["stop"], "tool");
    server.await.unwrap();
}

#[tokio::test]
async fn rejects_an_oversized_response_before_parsing() {
    let Some(listener) = loopback_listener().await else {
        return;
    };

    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let body = vec![b'x'; BODY_LIMIT + 1];
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );

        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    });

    let request = ModelRequest {
        model: "test".into(),
        workspace: None,
        messages: Vec::new(),
        stream: false,
        tools: Vec::new(),
    };

    assert!(
        generate_at(&reqwest::Client::new(), 1, request, "key", &format!("http://{address}"),)
            .await
            .is_err()
    );

    server.await.unwrap();
}

#[tokio::test]
async fn rejects_failed_and_invalid_responses() {
    for (status, body) in [
        ("403 Forbidden", br#"{"error":"denied"}"#.as_slice()),
        ("200 OK", br#"not-json"#.as_slice()),
    ] {
        let Some(listener) = loopback_listener().await else {
            return;
        };

        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );

            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let request = ModelRequest {
            model: "test".into(),
            workspace: None,
            messages: Vec::new(),
            stream: false,
            tools: Vec::new(),
        };

        assert!(
            generate_at(&reqwest::Client::new(), 1, request, "key", &format!("http://{address}"))
                .await
                .is_err()
        );

        server.await.unwrap();
    }
}
