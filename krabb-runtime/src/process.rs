use std::{
    ffi::OsStr,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::Stdio,
};

use krabb_core::{Result, plugin::Process as CoreProcess};
use tokio::{
    process::Command,
    sync::{Mutex, OwnedMutexGuard},
    time::{Duration, timeout},
};

#[derive(Clone, Debug, Default)]
pub(crate) struct Cleanup {
    tasks: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<Result<()>>>>>,
}

struct Call {
    process: Option<OwnedMutexGuard<Option<Process>>>,
    cleanup: Cleanup,
    armed: bool,
}

impl Cleanup {
    pub async fn call_full_async<F, Fut, C, Cfut>(
        &self,
        process: std::sync::Arc<Mutex<Option<Process>>>,
        request: krabb_core::types::Request,
        note: F,
        handler: C,
    ) -> Result<krabb_core::types::Response>
    where
        F: FnMut(krabb_core::types::Request) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
        C: FnMut(krabb_core::types::Request) -> Cfut,
        Cfut: std::future::Future<Output = Result<krabb_core::types::Response>>,
    {
        let process = process.lock_owned().await;
        let mut call = Call { process: Some(process), cleanup: self.clone(), armed: true };
        let result = call
            .process
            .as_mut()
            .unwrap()
            .as_mut()
            .ok_or_else(|| krabb_core::Error::Protocol("Plugin is stopped.".into()))?
            .call_full_async(request, note, handler)
            .await;
        call.armed = result.is_err();
        result
    }

    pub async fn call(
        &self,
        process: std::sync::Arc<Mutex<Option<Process>>>,
        request: krabb_core::types::Request,
    ) -> Result<krabb_core::types::Response> {
        let process = process.lock_owned().await;
        self.call_locked(process, request).await
    }

    pub async fn call_locked(
        &self,
        process: OwnedMutexGuard<Option<Process>>,
        request: krabb_core::types::Request,
    ) -> Result<krabb_core::types::Response> {
        let mut call = Call { process: Some(process), cleanup: self.clone(), armed: true };
        let result = call
            .process
            .as_mut()
            .unwrap()
            .as_mut()
            .ok_or_else(|| krabb_core::Error::Protocol("Plugin is stopped.".into()))?
            .call(request)
            .await;
        call.armed = result.is_err();
        result
    }

    pub async fn finish(&self) -> Result<()> {
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("Cleanup lock is poisoned."));
        let mut result = Ok(());

        for task in tasks {
            let cleanup = task
                .await
                .map_err(|error| {
                    krabb_core::Error::Protocol(format!("Request cleanup failed: {error}"))
                })
                .and_then(|result| result);

            if result.is_ok() {
                result = cleanup;
            }
        }

        result
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let mut process = self.process.take().unwrap();
        let task = tokio::spawn(async move {
            if let Some(process) = process.as_mut() {
                let result = process.inner.restart().await;

                if let Some(containers) = &process.containers {
                    // Retain the request lock and turn until detached commands are gone.
                    while let Err(error) = containers.clear().await {
                        tracing::warn!(error = %super::diagnostic(error.to_string()),
                            "Cancelled tool container cleanup failed; retrying.");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }

                result?;
            }

            Ok(())
        });

        self.cleanup.tasks.lock().expect("Cleanup lock is poisoned.").push(task);
    }
}

pub(crate) struct Process {
    inner: CoreProcess,
    containers: Option<Containers>,
}

struct Containers {
    runtime: String,
    directory: PathBuf,
}

impl From<CoreProcess> for Process {
    fn from(inner: CoreProcess) -> Self {
        Self { inner, containers: None }
    }
}

impl Deref for Process {
    type Target = CoreProcess;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Process {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Process {
    #[cfg(all(test, unix))]
    pub async fn start_with<I, S>(path: impl AsRef<Path>, args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Ok(CoreProcess::start_with(path, args).await?.into())
    }

    pub async fn start_with_env<I, S, E, K, V>(
        path: impl AsRef<Path>,
        args: I,
        env: E,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
        E: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        let mut env = env
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_os_string(), value.as_ref().to_os_string()))
            .collect::<Vec<_>>();
        let runtime = env
            .iter()
            .find(|(key, _)| key == "KRABB_SANDBOX_RUNTIME")
            .and_then(|(_, value)| value.to_str())
            .filter(|value| matches!(*value, "docker" | "podman"));
        let containers = if let Some(runtime) = runtime {
            let parent = env
                .iter()
                .find(|(key, _)| key == "KRABB_HOME")
                .map(|(_, value)| PathBuf::from(value))
                .unwrap_or_else(std::env::temp_dir);
            let directory = parent.join(format!("krabb-containers-{}", super::ipc::token()?));
            let builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            let builder = {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = builder;
                builder.mode(0o700);
                builder
            };

            builder.create(&directory)?;
            #[cfg(windows)]
            krabb_file::service(&directory)?;
            Some(Containers { runtime: runtime.into(), directory })
        } else {
            None
        };

        env.retain(|(key, _)| key != "KRABB_SANDBOX_TRACKING");

        if let Some(containers) = &containers {
            env.push((
                "KRABB_SANDBOX_TRACKING".into(),
                containers.directory.as_os_str().to_owned(),
            ));
        }

        let inner = match CoreProcess::start_with_env(path, args, env).await {
            Ok(inner) => inner,

            Err(error) => {
                if let Some(containers) = &containers {
                    let _ = std::fs::remove_dir(&containers.directory);
                }

                return Err(error);
            }
        };

        Ok(Self { inner, containers })
    }

    pub async fn restart(&mut self) -> Result<()> {
        let result = self.inner.restart().await;

        if let Some(containers) = &self.containers {
            containers.clear().await?;
        }

        result
    }

    pub async fn stop(self) -> Result<()> {
        let result = self.inner.stop().await;

        if let Some(containers) = self.containers {
            containers.clear().await?;
            std::fs::remove_dir(containers.directory)?;
        }

        result
    }
}

impl Containers {
    async fn clear(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.directory)? {
            let path = entry?.path();
            let id = match std::fs::read_to_string(path.join("id")) {
                Ok(id) => id.trim().to_owned(),

                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::remove_dir_all(path)?;
                    continue;
                }

                Err(error) => return Err(error.into()),
            };

            if !(12..=128).contains(&id.len()) || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(krabb_core::Error::Protocol("Invalid tracked container ID.".into()));
            }

            let status = timeout(
                Duration::from_secs(10),
                Command::new(&self.runtime)
                    .args(["rm", "--force", &id])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .status(),
            )
            .await
            .map_err(|_| krabb_core::Error::Protocol("Container cleanup timed out.".into()))??;

            if !status.success() {
                // The plugin may have removed the container just before it was killed.
                let listed = timeout(
                    Duration::from_secs(10),
                    Command::new(&self.runtime)
                        .args(["ps", "--all", "--quiet", "--filter", &format!("id={id}")])
                        .stdin(Stdio::null())
                        .stderr(Stdio::null())
                        .kill_on_drop(true)
                        .output(),
                )
                .await
                .map_err(|_| {
                    krabb_core::Error::Protocol("Container cleanup timed out.".into())
                })??;

                if !listed.status.success() || !listed.stdout.is_empty() {
                    return Err(krabb_core::Error::Protocol("Container cleanup failed.".into()));
                }
            }

            std::fs::remove_dir_all(path)?;
        }

        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use krabb_core::types::Request;
    use std::{fs, os::unix::fs::PermissionsExt};

    #[tokio::test]
    async fn cancelled_calls_and_stop_remove_detached_containers() {
        let root = std::env::temp_dir()
            .join(format!("krabb-container-test-{}", crate::ipc::token().unwrap()));
        fs::create_dir(&root).unwrap();
        let plugin = root.join("plugin");
        let runtime = root.join("docker");
        fs::write(&plugin, r#"#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
root = pathlib.Path(os.environ['KRABB_ROOT'])
for line in sys.stdin:
    request = json.loads(line)
    method = request['method']
    if method == 'hello':
        result = {'id': 'tools', 'version': '0.1.0', 'protocol': {'major': 0, 'minor': 1}, 'capabilities': ['tool']}
    elif method == 'shell':
        directory = pathlib.Path(os.environ['KRABB_SANDBOX_TRACKING']) / 'active'
        directory.mkdir()
        child = subprocess.Popen([sys.executable, '-c', 'import time,pathlib; time.sleep(2); pathlib.Path("leaked").touch()'], cwd=root, start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (directory / 'id').write_text(f'{child.pid:012d}')
        (root / 'ready').touch()
        continue
    elif method == 'shutdown':
        break
    else:
        result = {'ok': True}
    print(json.dumps({'jsonrpc': '2.0', 'id': request['id'], 'result': result}), flush=True)
"#).unwrap();
        fs::write(
            &runtime,
            r#"#!/usr/bin/env python3
import os, pathlib, signal, sys
if not pathlib.Path(__file__).with_name('allow-removal').exists():
    sys.exit(1)
if sys.argv[1] == 'rm':
    try:
        os.kill(int(sys.argv[3]), signal.SIGKILL)
    except ProcessLookupError:
        pass
"#,
        )
        .unwrap();

        for path in [&plugin, &runtime] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }

        let mut process = Process::start_with_env(
            &plugin,
            std::iter::empty::<&str>(),
            [
                ("KRABB_ROOT", root.as_os_str()),
                ("PATH", OsStr::new("/usr/bin:/bin")),
                ("KRABB_SANDBOX_RUNTIME", OsStr::new("docker")),
            ],
        )
        .await
        .unwrap();
        let containers = process.containers.as_mut().unwrap();
        containers.runtime = runtime.display().to_string();
        let directory = containers.directory.clone();

        let process = std::sync::Arc::new(Mutex::new(Some(process)));
        let cleanup = Cleanup::default();

        {
            let response = cleanup.call(
                std::sync::Arc::clone(&process),
                Request::call(2, "shell", serde_json::json!({})),
            );
            tokio::pin!(response);
            timeout(Duration::from_secs(5), async {
                loop {
                    tokio::select! {
                        result = &mut response => panic!("shell finished early: {result:?}"),

                        _ = tokio::time::sleep(Duration::from_millis(10)) => {
                            if root.join("ready").exists() { break; }
                        }
                    }
                }
            })
            .await
            .unwrap();

            let queued = Cleanup::default();

            assert!(
                timeout(
                    Duration::from_millis(20),
                    queued.call(
                        std::sync::Arc::clone(&process),
                        Request::call(3, "ping", serde_json::json!({})),
                    )
                )
                .await
                .is_err()
            );

            assert!(queued.tasks.lock().unwrap().is_empty());
        }

        let finished = cleanup.finish();
        tokio::pin!(finished);

        assert!(timeout(Duration::from_millis(250), &mut finished).await.is_err());
        assert!(process.try_lock().is_err());
        assert!(directory.join("active/id").exists());
        fs::write(root.join("allow-removal"), "").unwrap();
        timeout(Duration::from_secs(5), finished).await.unwrap().unwrap();

        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        tokio::time::sleep(Duration::from_millis(2100)).await;

        assert!(!root.join("leaked").exists());
        fs::remove_file(root.join("ready")).unwrap();
        let mut process = process.lock().await.take().unwrap();
        start_shell(&mut process, &root).await;
        process.stop().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2100)).await;

        assert!(!root.join("leaked").exists());
        assert!(!directory.exists());
        fs::remove_dir_all(root).unwrap();
    }

    async fn start_shell(process: &mut Process, root: &Path) {
        let response = process.call(Request::call(2, "shell", serde_json::json!({})));
        tokio::pin!(response);
        timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut response => panic!("shell finished early: {result:?}"),

                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if root.join("ready").exists() { break; }
                    }
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cleanup_retains_failed_and_invalid_records_for_retry() {
        let directory = std::env::temp_dir()
            .join(format!("krabb-container-cleanup-{}", crate::ipc::token().unwrap()));
        fs::create_dir_all(directory.join("active")).unwrap();
        fs::write(directory.join("active/id"), "bad id").unwrap();
        let mut containers = Containers { runtime: "/bin/false".into(), directory };

        assert!(containers.clear().await.is_err());
        fs::write(containers.directory.join("active/id"), "0123456789abcdef").unwrap();

        assert!(containers.clear().await.is_err());
        assert!(containers.directory.join("active/id").exists());
        containers.runtime = "/bin/true".into();
        containers.clear().await.unwrap();

        assert!(!containers.directory.join("active").exists());
        fs::create_dir(containers.directory.join("creating")).unwrap();
        containers.clear().await.unwrap();
        fs::remove_dir(containers.directory).unwrap();
    }
}
