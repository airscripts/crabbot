#![cfg(any(target_os = "linux", windows))]

use std::time::Duration;

use krabb_core::{plugin::Process, types::Request};
use serde_json::json;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn restart_stops_an_active_approved_shell() {
    #[cfg(unix)]
    let command = "trap '' TERM; echo ready > ready; sleep 2; echo leaked > leaked";

    #[cfg(windows)]
    let command = "echo ready > ready & ping -n 3 127.0.0.1 > nul & echo leaked > leaked";

    restart_stops_shell("active", command).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn restart_stops_a_worker_that_attempts_to_detach() {
    let executable = std::env::current_exe().unwrap();
    let executable = executable.to_str().unwrap().replace('\'', "'\\''");
    let command = format!(
        "KRABB_TEST_DETACH=1 '{executable}' --exact detached_worker --ignored --nocapture & wait"
    );

    restart_stops_shell("detached", &command).await;
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "child fixture for restart_stops_a_worker_that_attempts_to_detach"]
fn detached_worker() {
    if std::env::var_os("KRABB_TEST_DETACH").is_none() {
        return;
    }

    let _ = rustix::process::setsid();
    let _ = rustix::process::setpgid(None, None);
    std::fs::write("ready", "ready").unwrap();
    std::thread::sleep(Duration::from_secs(2));
    std::fs::write("leaked", "leaked").unwrap();
}

async fn restart_stops_shell(name: &str, command: &str) {
    let root =
        std::env::temp_dir().join(format!("krabb-shell-cancel-{name}-{}", std::process::id()));

    std::fs::create_dir(&root).unwrap();
    let mut environment = std::collections::BTreeMap::<std::ffi::OsString, _>::new();
    environment.insert("PATH".into(), std::env::var_os("PATH").unwrap_or_default());

    if let Some(system_root) = std::env::var_os("SystemRoot") {
        environment.insert("SystemRoot".into(), system_root);
    }

    environment.insert("KRABB_ROOT".into(), root.clone().into_os_string());
    environment.insert("XDG_CONFIG_HOME".into(), root.clone().into_os_string());
    environment.insert("KRABB_SHELL".into(), "ask".into());

    let mut process = Process::start_with_env(
        env!("CARGO_BIN_EXE_krabb-plugin-tools"),
        std::iter::empty::<&str>(),
        environment,
    )
    .await
    .unwrap();

    {
        let call = process.call(Request::call(
            2,
            "shell",
            json!({
                "approve": true,
                "command": command
            }),
        ));

        tokio::pin!(call);

        timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut call => panic!("shell ended before cancellation: {result:?}"),

                    _ = sleep(Duration::from_millis(10)) => {
                        if root.join("ready").is_file() {
                            break;
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
    }

    process.restart().await.unwrap();

    let response = process.call(Request::call(3, "ping", json!({}))).await.unwrap();

    assert!(response.error.is_none());

    sleep(Duration::from_millis(2200)).await;

    assert!(!root.join("leaked").exists(), "cancelled shell wrote after restart");

    process.stop().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn output_limit_cleanup_preserves_protocol_framing() {
    let root = std::env::temp_dir().join(format!("krabb-shell-framing-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::write(
        root.join("output.cmd"),
        format!(
            "@echo off\r\nfor /L %%i in (1,1,20000) do echo {}\r\nping -n 60 127.0.0.1 > nul\r\n",
            "x".repeat(256)
        ),
    )
    .unwrap();

    let mut environment = std::collections::BTreeMap::<std::ffi::OsString, _>::new();
    environment.insert("PATH".into(), std::env::var_os("PATH").unwrap_or_default());

    if let Some(system_root) = std::env::var_os("SystemRoot") {
        environment.insert("SystemRoot".into(), system_root);
    }

    environment.insert("KRABB_ROOT".into(), root.clone().into_os_string());
    environment.insert("KRABB_SHELL".into(), "ask".into());

    let mut process = Process::start_with_env(
        env!("CARGO_BIN_EXE_krabb-plugin-tools"),
        std::iter::empty::<&str>(),
        environment,
    )
    .await
    .unwrap();

    let response = timeout(
        Duration::from_secs(30),
        process.call(Request::call(2, "shell", json!({"approve": true, "command": "output.cmd"}))),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(response.error.unwrap().message.contains("output exceeds the size limit"));

    let response = process.call(Request::call(3, "ping", json!({}))).await.unwrap();

    assert!(response.error.is_none());

    process.stop().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
