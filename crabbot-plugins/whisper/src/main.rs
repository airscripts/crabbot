#![forbid(unsafe_code)]

#[cfg(not(test))]
use crabbot_core::plugin::serve_with;
use crabbot_core::{
    policy::Policy,
    types::{Capability, Hello, Protocol, Request, Response},
};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    time::{Duration, timeout},
};

const FRAME_HEADROOM: usize = 64 * 1024;
const OUTPUT_LIMIT: usize = (crabbot_core::jsonl::MAX - FRAME_HEADROOM) / 2;
const COMMAND_LIMIT: Duration = Duration::from_secs(120);
const ERROR_LIMIT: usize = 4 * 1024;

#[cfg(not(test))]
#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let root = media_root();
    let policy = Policy { root, ..Policy::default() };
    serve_with(hello(), move |request| {
        let policy = policy.clone();
        async move { call(&policy, request).await }
    })
    .await
}

#[cfg(not(test))]
fn media_root() -> Option<std::path::PathBuf> {
    std::env::var_os("CRABBOT_MEDIA")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME")
                .filter(|value| !value.is_empty())
                .map(|home| std::path::PathBuf::from(home).join("media"))
        })
}

async fn call(policy: &Policy, request: Request) -> crabbot_core::Result<Option<Response>> {
    let command = std::env::var("CRABBOT_WHISPER_COMMAND").ok();
    call_at(policy, request, command.as_deref()).await
}

async fn call_at(
    policy: &Policy,
    request: Request,
    command: Option<&str>,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    if method != "transcribe" {
        return Ok(None);
    }
    let path = params["path"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied("transcribe.path is required.".into()))?;
    let path = policy.path(path)?;
    let command = command.filter(|command| !command.trim().is_empty()).ok_or_else(|| {
        crabbot_core::Error::Denied("A Whisper command is not configured.".into())
    })?;
    let mut process = Command::new(command);
    process.arg(path);
    let output = capture(process)
        .await
        .map_err(|error| crabbot_core::Error::Denied(format!("Whisper failed: {error}.")))?;
    if !output.status.success() {
        return Err(crabbot_core::Error::Denied(format!(
            "Whisper failed: {}.",
            clip(&String::from_utf8_lossy(&output.stderr), ERROR_LIMIT)
        )));
    }
    let text = clip(&String::from_utf8_lossy(&output.stdout), OUTPUT_LIMIT);
    if text.is_empty() {
        return Err(crabbot_core::Error::Denied("Whisper returned no text.".into()));
    }
    let response = Response::ok(id, json!({"text": text}));
    if serde_json::to_vec(&response)?.len().saturating_add(1) > crabbot_core::jsonl::MAX {
        return Err(crabbot_core::Error::Denied(
            "Whisper response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

fn clip(value: &str, limit: usize) -> String {
    let mut clipped = value.trim().to_owned();
    if clipped.len() <= limit {
        return clipped;
    }
    let mut end = limit.saturating_sub(3).min(clipped.len());
    while !clipped.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    clipped.truncate(end);
    clipped.push('…');
    clipped
}

async fn capture(mut command: Command) -> std::io::Result<std::process::Output> {
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stdout.")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stderr.")
    })?;
    let result = timeout(COMMAND_LIMIT, async {
        let stdout = limited(stdout);
        let stderr = limited(stderr);
        let status = child.wait();
        let (stdout, stderr, status) = tokio::try_join!(stdout, stderr, status)?;
        Ok::<_, std::io::Error>(std::process::Output { status, stdout, stderr })
    })
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            stop(&mut child).await;
            Err(error)
        }
        Err(_) => {
            stop(&mut child).await;
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Process exceeded the execution time limit.",
            ))
        }
    }
}

async fn limited<R: AsyncRead + Unpin>(mut input: R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = input.read(&mut buffer).await?;
        if count == 0 {
            return Ok(bytes);
        }
        if count > OUTPUT_LIMIT.saturating_sub(bytes.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "Process output exceeds the size limit.",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

async fn stop(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn hello() -> Hello {
    Hello {
        protocol: Protocol::CURRENT,
        id: "whisper".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: vec![Capability::Speech],
        commands: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::{call, call_at, hello};
    use crabbot_core::{policy::Policy, types::Request};
    use serde_json::json;

    #[test]
    fn describes_speech_capability() {
        let hello = hello();
        assert_eq!(hello.id, "whisper");
        assert_eq!(hello.capabilities, vec![crabbot_core::types::Capability::Speech]);
    }

    #[tokio::test]
    async fn validates_transcription_requests() {
        let policy = Policy::default();
        assert!(call(&policy, Request::call(1, "unknown", json!({}))).await.unwrap().is_none());
        assert!(call(&policy, Request::call(2, "transcribe", json!({}))).await.is_err());
        assert!(
            call_at(&policy, Request::call(3, "transcribe", json!({"path": "voice"})), None)
                .await
                .is_err()
        );
    }

    fn write_transcriber(path: &std::path::Path, output: &str, error: &str, status: i32) {
        let source = path.with_extension("rs");
        let source_code = format!(
            "fn main() {{ print!({output:?}); eprint!({error:?}); std::process::exit({status}); }}"
        );

        std::fs::write(&source, source_code).unwrap();
        let result = std::process::Command::new("rustc")
            .args(["--edition", "2024"])
            .arg(&source)
            .arg("-o")
            .arg(path)
            .output()
            .unwrap();

        assert!(
            result.status.success(),
            "could not compile the transcriber fixture: {}",
            String::from_utf8_lossy(&result.stderr)
        );

        std::fs::remove_file(source).unwrap();
    }

    #[tokio::test]
    async fn runs_a_local_transcriber() {
        let root = std::env::temp_dir().join(format!("crabbot-whisper-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("voice"), b"audio").unwrap();
        let extension = if cfg!(windows) { "exe" } else { "" };
        let script = root.join(format!("transcriber.{extension}"));
        write_transcriber(&script, "hello from whisper\n", "", 0);
        let policy = Policy { root: Some(root.clone()), ..Policy::default() };
        let reply = call_at(
            &policy,
            Request::call(1, "transcribe", json!({"path": "voice"})),
            script.to_str(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(reply.result.unwrap()["text"], "hello from whisper");

        let empty = root.join(format!("empty.{extension}"));
        write_transcriber(&empty, "", "", 0);
        assert!(
            call_at(
                &policy,
                Request::call(2, "transcribe", json!({"path": "voice"})),
                empty.to_str()
            )
            .await
            .is_err()
        );

        let failed = root.join(format!("failed.{extension}"));
        write_transcriber(&failed, "", "nope\n", 1);
        assert!(
            call_at(
                &policy,
                Request::call(3, "transcribe", json!({"path": "voice"})),
                failed.to_str()
            )
            .await
            .is_err()
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn bounds_transcriber_output() {
        let error = super::limited(tokio::io::repeat(b'o')).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    }
}
