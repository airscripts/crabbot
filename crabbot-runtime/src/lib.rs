#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    process::{ExitCode, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, shells};
use crabbot_core::{
    plugin::Process,
    types::{
        Capability, CommandSpec, Content, Event, Hello, Message, ModelReply, ModelRequest,
        Protocol, Request, Response, Role, ToolSpec,
    },
};

use crabbot_file::save as save_file;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, Notify, RwLock, watch},
};

use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::LevelFilter;

pub(crate) mod approval;
pub(crate) mod ipc;
pub(crate) mod state;

const NAME: &str = "crabbot";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const BANNER: &str = concat!(
    "\n",
    " ██████╗ ██████╗   █████╗  ██████╗  ██████╗   ██████╗  ████████╗\n",
    "██╔════╝ ██╔══██╗ ██╔══██╗ ██╔══██╗ ██╔══██╗ ██╔═══██╗ ╚══██╔══╝\n",
    "██║      ██████╔╝ ███████║ ██████╔╝ ██████╔╝ ██║   ██║    ██║   \n",
    "██║      ██╔══██╗ ██╔══██║ ██╔══██╗ ██╔══██╗ ██║   ██║    ██║   \n",
    "╚██████╗ ██║  ██║ ██║  ██║ ██████╔╝ ██████╔╝ ╚██████╔╝    ██║   \n",
    " ╚═════╝ ╚═╝  ╚═╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝   ╚═════╝     ╚═╝   ",
);

const TOOL_STEPS: usize = 8;
const TOOL_CALLS: usize = 16;
const TURN_LIMIT: std::time::Duration = std::time::Duration::from_secs(300);
const TOKEN_LIMIT: u64 = 128_000;
const ARCHIVE_LIMIT: u64 = 64 * 1024 * 1024;
const ARCHIVE_EXPANDED: u64 = 256 * 1024 * 1024;
const ARCHIVE_FILES: usize = 10_000;
const DOWNLOAD_CONNECT: u64 = 10;
const DOWNLOAD_TIME: u64 = 120;
const MEDIA_LIMIT: u64 = 4 * 1024 * 1024;
const IMAGE_LIMIT: usize = 4 * 1024 * 1024;
const IMAGE_TOTAL_LIMIT: usize = 4 * 1024 * 1024;
const FILE_LIMIT: u64 = 64 * 1024;
const TEXT_LIMIT: usize = 256 * 1024;
const META_LIMIT: usize = 4 * 1024;
const CONTENT_LIMIT: usize = 64;
const CONTEXT_LIMIT: usize = 32 * 1024;
const MANIFEST_LIMIT: u64 = 1024 * 1024;
const LOCK_LIMIT: u64 = 8 * 1024 * 1024;
const COMMAND_TIME: Duration = Duration::from_secs(120);
const COMMAND_OUTPUT: usize = 2 * 1024 * 1024;
const ACK_ATTEMPTS: u32 = 3;
type PluginError = Box<dyn std::error::Error + Send + Sync>;
type PluginControl = Result<Option<serde_json::Value>, PluginError>;
type PluginTask = Pin<Box<dyn Future<Output = PluginControl> + Send>>;

pub(crate) struct Stop {
    signal: watch::Sender<bool>,
}

pub(crate) struct Cancellation {
    signal: watch::Sender<bool>,
    acknowledged: watch::Sender<bool>,
}

impl Cancellation {
    fn new() -> Self {
        let (signal, _) = watch::channel(false);
        let (acknowledged, _) = watch::channel(false);
        Self { signal, acknowledged }
    }

    fn request(&self) {
        self.signal.send_replace(true);
    }

    fn acknowledge(&self) {
        self.acknowledged.send_replace(true);
    }

    async fn cancelled(&self) {
        let mut signal = self.signal.subscribe();

        if !*signal.borrow() {
            let _ = signal.changed().await;
        }
    }

    async fn wait(&self) {
        let mut acknowledged = self.acknowledged.subscribe();

        if !*acknowledged.borrow() {
            let _ = acknowledged.changed().await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct Plugins {
    items: Arc<RwLock<BTreeMap<String, Arc<Live>>>>,
    loading: Arc<AsyncMutex<()>>,
    changed: Arc<Notify>,
}

impl Default for Plugins {
    fn default() -> Self {
        Self {
            items: Arc::new(RwLock::new(BTreeMap::new())),
            loading: Arc::new(AsyncMutex::new(())),
            changed: Arc::new(Notify::new()),
        }
    }
}

pub(crate) struct Live {
    hello: Hello,
    process: AsyncMutex<Option<Process>>,
}

impl Live {
    fn new(process: Process) -> Self {
        Self { hello: process.hello.clone(), process: AsyncMutex::new(Some(process)) }
    }

    fn supports(&self, capability: Capability) -> bool {
        self.hello.capabilities.contains(&capability)
    }

    async fn call(&self, request: Request) -> crabbot_core::Result<Response> {
        self.process
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crabbot_core::Error::Protocol("Plugin is stopped.".into()))?
            .call(request)
            .await
    }

    async fn call_full_async<F, Fut, C, Cfut>(
        &self,
        request: Request,
        note: F,
        call: C,
    ) -> crabbot_core::Result<Response>
    where
        F: FnMut(Request) -> Fut,
        Fut: Future<Output = crabbot_core::Result<()>>,
        C: FnMut(Request) -> Cfut,
        Cfut: Future<Output = crabbot_core::Result<Response>>,
    {
        self.process
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crabbot_core::Error::Protocol("Plugin is stopped.".into()))?
            .call_full_async(request, note, call)
            .await
    }

    async fn restart(&self) -> crabbot_core::Result<()> {
        self.process
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crabbot_core::Error::Protocol("Plugin is stopped.".into()))?
            .restart()
            .await
    }

    async fn stop(&self) -> crabbot_core::Result<()> {
        if let Some(process) = self.process.lock().await.take() {
            process.stop().await
        } else {
            Ok(())
        }
    }
}

impl Plugins {
    async fn get(&self, id: &str) -> Option<Arc<Live>> {
        self.items.read().await.get(id).cloned()
    }

    async fn insert(&self, plugin: Live) -> Option<Arc<Live>> {
        let old = self.items.write().await.insert(plugin.hello.id.clone(), Arc::new(plugin));
        self.changed.notify_one();
        old
    }

    async fn remove(&self, id: &str) -> Option<Arc<Live>> {
        let plugin = self.items.write().await.remove(id);

        if plugin.is_some() {
            self.changed.notify_one();
        }

        plugin
    }

    async fn find(&self, capability: Capability) -> Option<(String, Arc<Live>)> {
        self.items
            .read()
            .await
            .iter()
            .find(|(_, plugin)| plugin.hello.capabilities.contains(&capability))
            .map(|(id, plugin)| (id.clone(), Arc::clone(plugin)))
    }

    async fn all(&self) -> Vec<(String, Arc<Live>)> {
        self.items
            .read()
            .await
            .iter()
            .map(|(id, plugin)| (id.clone(), Arc::clone(plugin)))
            .collect()
    }
}

impl Stop {
    fn new() -> Self {
        let (signal, _) = watch::channel(false);
        Self { signal }
    }

    fn signal(&self) {
        self.signal.send_replace(true);
    }

    async fn notified(&self) {
        let mut signal = self.signal.subscribe();

        if *signal.borrow() {
            return;
        }

        let _ = signal.changed().await;
    }
}

fn sentence(value: impl Into<String>) -> String {
    let mut value = value.into();

    if let Some(first) = value.chars().next() {
        let mut title = first.to_uppercase().collect::<String>();
        title.push_str(&value[first.len_utf8()..]);
        value = title;
    }

    if !matches!(value.chars().last(), Some('.' | '!' | '?')) {
        value.push('.');
    }

    value
}

fn toml_position(input: &str, offset: usize) -> (usize, usize) {
    let bytes = input.as_bytes();
    let offset = offset.min(bytes.len());
    let line = bytes[..offset].iter().filter(|byte| **byte == b'\n').count() + 1;
    let column = bytes[..offset]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(offset, |position| offset.saturating_sub(position + 1))
        + 1;

    (line, column)
}

fn diagnostic(value: impl AsRef<str>) -> String {
    redact_diagnostic(value)
}

pub fn redact_diagnostic(value: impl AsRef<str>) -> String {
    redact(value.as_ref().as_bytes())
}

fn git_command() -> std::process::Command {
    let mut command = std::process::Command::new("git");

    for variable in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(variable);
    }

    command
}

fn command_output(command: &mut std::process::Command) -> std::io::Result<std::process::Output> {
    command_output_limited(command, None, u64::MAX)
}

fn command_output_limited(
    command: &mut std::process::Command,
    root: Option<&Path>,
    limit: u64,
) -> std::io::Result<std::process::Output> {
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stdout.")
    })?;

    let stderr = child.stderr.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Process has no stderr.")
    })?;

    let (output_tx, output_rx) = std::sync::mpsc::channel();
    let (error_tx, error_rx) = std::sync::mpsc::channel();
    let _output_thread = thread::spawn(move || {
        let _ = output_tx.send(read_output(stdout));
    });

    let _error_thread = thread::spawn(move || {
        let _ = error_tx.send(read_output(stderr));
    });

    let deadline = Instant::now() + COMMAND_TIME;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }

        if let Some(root) = root {
            let size = match staging_size(root) {
                Ok(size) => size,

                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error);
                }
            };

            if size > limit {
                let _ = child.kill();
                let _ = child.wait();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    "Process staging exceeds the size limit.",
                ));
            }
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Process exceeded the execution time limit.",
            ));
        }

        thread::sleep(Duration::from_millis(25));
    };

    if let Some(root) = root
        && staging_size(root)? > limit
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "Process staging exceeds the size limit.",
        ));
    }

    let stdout = output_rx.recv_timeout(Duration::from_secs(1)).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "Process output did not close.")
    })??;

    let stderr = error_rx.recv_timeout(Duration::from_secs(1)).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "Process error output did not close.")
    })??;

    Ok(std::process::Output { status, stdout, stderr })
}

fn staging_size(root: &Path) -> std::io::Result<u64> {
    let mut size = 0_u64;

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };

        size = size.saturating_add(if metadata.is_dir() {
            match staging_size(&path) {
                Ok(size) => size,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error),
            }
        } else {
            metadata.len()
        });
    }

    Ok(size)
}

fn read_output(input: impl Read) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    input.take((COMMAND_OUTPUT + 1) as u64).read_to_end(&mut output)?;

    if output.len() > COMMAND_OUTPUT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "Process output exceeds the size limit.",
        ));
    }

    Ok(output)
}

#[derive(Debug, Parser)]
#[command(
    name = NAME,
    version = VERSION,
    about = "Your last next agent.",
    before_help = BANNER,
    disable_help_flag = true,
    disable_version_flag = true
)]
struct Cli {
    #[arg(long, global = true, help = "Render command output as JSON where supported.")]
    json: bool,
    #[arg(long, global = true, help = "Show diagnostic error details.")]
    debug: bool,
    #[arg(long, global = true, help = "Show diagnostic progress and timing.")]
    verbose: bool,
    #[arg(
        short = 'v',
        visible_short_alias = 'V',
        long = "version",
        action = ArgAction::Version,
        help = "Print version."
    )]
    version_flag: Option<bool>,
    #[arg(
        short = 'h',
        visible_short_alias = 'H',
        long = "help",
        global = true,
        action = ArgAction::Help,
        required = false,
        help = "Print help."
    )]
    help: Option<bool>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Initialize the Crabbot home directory and configuration.")]
    Init(InitArgs),
    #[command(about = "Check configuration, plugins, credentials, and local state.")]
    Doctor(DoctorArgs),
    #[command(about = "Show installation and daemon health.")]
    Status(Output),
    #[command(about = "Print the Crabbot version.")]
    Version(Output),
    #[command(about = "Generate shell completion scripts.")]
    Completion {
        #[arg(value_enum, help = "Shell to generate completions for.")]
        shell: CompletionShell,
    },
    #[command(about = "Manage installed plugins.")]
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },

    #[command(about = "Manage durable agent sessions.")]
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },

    #[command(about = "Manage pending channel deliveries.")]
    Delivery {
        #[command(subcommand)]
        command: DeliveryCommand,
    },

    #[command(about = "Install, remove, and control the native service.")]
    Service {
        #[command(subcommand)]
        command: Option<ServiceCommand>,
    },

    #[command(about = "Export the local configuration and plugin lock.")]
    Export(CrabfileExport),
    #[command(about = "Validate a Crabfile without changing local state.")]
    Validate(CrabfileValidate),
    #[command(about = "Import a configuration and plugin lock.")]
    Import(CrabfileImport),
    #[command(external_subcommand, hide = true)]
    External(Vec<String>),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CompletionShell {
    Bash,
    Fish,
    #[value(name = "powershell")]
    PowerShell,
    Zsh,
}

#[derive(Debug, Args)]
struct CrabfileExport {
    #[arg(value_name = "PATH", help = "Write the Crabfile to PATH or a directory.")]
    destination: Option<PathBuf>,
    #[arg(long, value_name = "PATH", help = "Write the Crabfile to PATH or a directory.")]
    path: Option<PathBuf>,
    #[arg(long, help = "Overwrite an existing Crabfile.")]
    force: bool,
}

#[derive(Debug, Args)]
struct CrabfileValidate {
    #[arg(long, value_name = "PATH", help = "Read the Crabfile from PATH.")]
    path: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct CrabfileImport {
    #[arg(long, value_name = "PATH", help = "Read the Crabfile from PATH.")]
    path: Option<PathBuf>,
    #[arg(long, help = "Confirm the import without prompting.")]
    yes: bool,
    #[arg(long, help = "Replace existing local state.")]
    force: bool,
}

#[derive(Debug, Subcommand)]
enum PluginCommand {
    #[command(about = "List installed plugins.")]
    List(Output),
    #[command(about = "Install and activate a plugin.")]
    Install(Source),
    #[command(about = "Link a local plugin and activate it.")]
    Link(Source),
    #[command(about = "Update locked plugins.")]
    Update(Output),
    #[command(about = "Remove an installed plugin.")]
    Remove(Name),
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    #[command(about = "Create a durable session.")]
    New(SessionNew),
    #[command(about = "List durable sessions.")]
    List(Output),
    #[command(about = "Show a session transcript and metadata.")]
    Show(Id),
    #[command(about = "Fork a session into a new session.")]
    Fork(Fork),
    #[command(about = "Change the model used by a session.")]
    Model(SessionModel),
    #[command(about = "Cancel an active session turn.")]
    Cancel(Id),
    #[command(about = "Delete a session and reclaim its worktree.")]
    Delete(SessionDelete),
}

#[derive(Debug, Subcommand)]
enum DeliveryCommand {
    #[command(about = "List pending and uncertain deliveries.")]
    List(Output),
    #[command(about = "Retry a delivery explicitly.")]
    Retry(Name),
    #[command(about = "Drop a delivery without retrying it.")]
    Drop(Name),
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    #[command(about = "Install the native service definition.")]
    Install(ServiceInstall),
    #[command(about = "Remove the native service definition.")]
    Remove(ServiceRemove),
    #[command(about = "Show native service status.")]
    Status,
    #[command(about = "Start the native service.")]
    Start,
    #[command(about = "Stop the native service.")]
    Stop,
}

#[derive(Debug, Args)]
struct ServiceInstall {
    #[arg(long, help = "Replace an existing service definition.")]
    force: bool,
}

#[derive(Debug, Args)]
struct ServiceRemove {
    #[arg(long, help = "Confirm removing the service definition.")]
    yes: bool,
}

#[derive(Debug, Args)]
struct SessionNew {
    #[arg(help = "Session identifier.")]
    id: String,
    #[arg(long, default_value = "gpt-4o-mini", help = "Model identifier.")]
    model: String,
}

#[derive(Debug, Args)]
struct Fork {
    #[arg(help = "Source session identifier.")]
    source: String,
    #[arg(help = "Target session identifier.")]
    target: String,
}

#[derive(Debug, Args)]
struct SessionModel {
    #[arg(help = "Session identifier.")]
    id: String,
    #[arg(help = "Model identifier.")]
    model: String,
}

#[derive(Debug, Args)]
struct SessionDelete {
    #[arg(help = "Session identifier.")]
    id: String,
    #[arg(long, help = "Confirm deletion without prompting.")]
    yes: bool,
}

#[derive(Debug, Args)]
struct Output {
    #[arg(long, help = "Render the result as JSON.")]
    json: bool,
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long, help = "Recreate the default Crabbot configuration.")]
    force: bool,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    #[arg(long, help = "Create missing safe local state without overwriting configuration.")]
    fix: bool,
}

#[derive(Debug, Args)]
struct Source {
    #[arg(help = "Plugin identifier.")]
    id: String,
    #[arg(help = "Local path, Git URL, or verified archive.")]
    source: Option<String>,
    #[arg(long, value_name = "REVISION", help = "Pin a Git revision.")]
    revision: Option<String>,
    #[arg(long, help = "Confirm installation without prompting.")]
    yes: bool,
}

struct SourceRoot {
    path: PathBuf,
    temp: Option<PathBuf>,
}

struct DaemonLock {
    _file: std::fs::File,
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

impl Drop for SourceRoot {
    fn drop(&mut self) {
        if let Some(path) = &self.temp {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

#[derive(Debug, Args)]
struct Name {
    #[arg(help = "Identifier.")]
    id: String,
    #[arg(long, help = "Confirm the operation.")]
    yes: bool,
}

#[derive(Debug, Args)]
struct Id {
    #[arg(help = "Identifier.")]
    id: String,
}

#[derive(Debug, Parser)]
#[command(name = "ask", about = "Send one prompt through an intelligence plugin.")]
struct Ask {
    #[arg(long, help = "Intelligence plugin identifier.")]
    plugin: Option<String>,
    #[arg(long, default_value = "gpt-4o-mini", help = "Model identifier.")]
    model: String,
    #[arg(help = "Prompt words.")]
    prompt: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    update: String,
    shell: bool,
    approval: String,
    #[serde(default)]
    channels: BTreeMap<String, ChannelConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            update: "prompt".into(),
            shell: false,
            approval: "off".into(),
            channels: BTreeMap::new(),
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        match self.update.as_str() {
            "off" | "check" | "prompt" | "auto" => Ok(()),
            value => Err(format!("Unsupported update mode: {value}.")),
        }?;

        match self.approval.as_str() {
            "off" | "prompt" | "auto" => Ok(()),
            value => Err(format!("Unsupported approval mode: {value}.")),
        }
    }

    fn approval_mode(&self) -> ApprovalMode {
        match self.approval.as_str() {
            "prompt" => ApprovalMode::Prompt,
            "auto" => ApprovalMode::Auto,
            _ => ApprovalMode::Off,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApprovalMode {
    Off,
    Prompt,
    Auto,
}

impl ApprovalMode {
    const fn enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChannelConfig {
    #[serde(default)]
    allow: Vec<String>,
    mention: Option<String>,
    owner: Option<String>,
    #[serde(default)]
    admin: Vec<String>,
    #[serde(default)]
    member: Vec<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    thread: Vec<String>,
    #[serde(default = "worktree")]
    worktree: bool,
    #[serde(default)]
    tools: bool,
}

fn worktree() -> bool {
    true
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            allow: Vec::new(),
            mention: None,
            owner: None,
            admin: Vec::new(),
            member: Vec::new(),
            topic: Vec::new(),
            thread: Vec::new(),
            worktree: true,
            tools: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Manifest {
    id: String,
    version: String,
    protocol: Protocol,
    capabilities: Vec<String>,
    #[serde(default)]
    permissions: Vec<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    commands: Vec<CommandSpec>,
}

impl Manifest {
    fn validate(&self) -> Result<(), String> {
        if !valid(&self.id) {
            return Err("Plugin IDs must contain lowercase letters, digits, or hyphens.".into());
        }

        if self.version.trim().is_empty() {
            return Err("Plugin versions must not be empty.".into());
        }

        if !Protocol::CURRENT.compatible(self.protocol) {
            return Err("Plugin protocol is not compatible with this daemon.".into());
        }

        let mut capabilities = self.capabilities.clone();
        capabilities.sort();
        capabilities.dedup();

        if capabilities.len() != self.capabilities.len()
            || self.capabilities.iter().any(|capability| {
                !matches!(
                    capability.as_str(),
                    "model"
                        | "vision"
                        | "channel"
                        | "store"
                        | "memory"
                        | "timer"
                        | "tool"
                        | "mcp"
                        | "speech"
                        | "client"
                        | "resource"
                        | "agent"
                )
            })
        {
            return Err("Plugin capabilities must be known and unique.".into());
        }

        let known_permissions =
            ["network", "process", "filesystem", "memory", "timer", "terminal", "audio"];

        if self
            .permissions
            .iter()
            .any(|permission| !known_permissions.contains(&permission.as_str()))
        {
            return Err("Plugin permissions must be known.".into());
        }

        let mut commands =
            self.commands.iter().map(|command| command.name.clone()).collect::<Vec<_>>();
        commands.sort();
        commands.dedup();

        if commands.len() != self.commands.len()
            || self
                .commands
                .iter()
                .any(|command| !valid(&command.name) || command.description.trim().is_empty())
        {
            return Err("Plugin commands must be named uniquely and described.".into());
        }

        let mut secrets = self.secrets.clone();
        secrets.sort();
        secrets.dedup();

        if secrets.len() != self.secrets.len()
            || self.secrets.iter().any(|secret| {
                secret.is_empty()
                    || !secret.starts_with("CRABBOT_")
                    || !secret.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            })
        {
            return Err("Plugin secrets must be unique CRABBOT_ environment variable names.".into());
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Lock {
    plugins: BTreeMap<String, Entry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Entry {
    source: String,
    #[serde(default)]
    revision: String,
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    default: bool,
    #[serde(default)]
    hash: String,
    version: String,
    protocol: Protocol,
    capabilities: Vec<String>,
    #[serde(default)]
    permissions: Vec<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    linked: bool,
    #[serde(default)]
    commands: Vec<CommandSpec>,
}

struct Update {
    destination: PathBuf,
    backup: Option<PathBuf>,
}

pub async fn cli() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    if root_help_requested(&args) {
        print_help_with_plugins(&home());
        return ExitCode::SUCCESS;
    }

    let cli = Cli::parse();

    init_logging(cli.verbose, cli.debug, cli.json);
    main_with(cli).await
}

fn root_help_requested(args: &[String]) -> bool {
    if args.is_empty() {
        return true;
    }

    let mut command_seen = false;

    for (index, argument) in args.iter().enumerate() {
        match argument.as_str() {
            "--json" | "--debug" | "--verbose" => {}

            "-h" | "-H" | "--help" if !command_seen => return true,

            "help"
                if !command_seen
                    && args[index + 1..].iter().all(|argument| {
                        matches!(argument.as_str(), "--json" | "--debug" | "--verbose")
                    }) =>
            {
                return true;
            }

            _ => command_seen = true,
        }
    }

    false
}

pub async fn daemon() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging(false, false, false);
    serve().await
}

pub fn init_logging(verbose: bool, debug_mode: bool, json: bool) {
    if json {
        return;
    }

    let level = if debug_mode {
        LevelFilter::DEBUG
    } else if verbose {
        LevelFilter::INFO
    } else {
        LevelFilter::WARN
    };

    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .with_max_level(level)
        .try_init();
}

async fn main_with(cli: Cli) -> ExitCode {
    let json = cli.json;
    let debug = cli.debug;
    let verbose = cli.verbose;
    let command = command_label(&cli.command);
    let started = Instant::now();

    if verbose {
        info!("Command started.");
    }

    match run(cli).await {
        Ok(()) => {
            if verbose {
                info!(elapsed_ms = started.elapsed().as_millis(), "Command completed.");
            }

            ExitCode::SUCCESS
        }

        Err(error) => {
            let report =
                debug.then(|| write_debug_report(command, error.as_ref(), started.elapsed()));

            if json {
                let message = diagnostic(sentence(error.to_string()));
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({"error": message}))
                        .expect("serializing a JSON error cannot fail")
                );
            } else {
                let message = sentence(error.to_string());
                eprintln!("Error: {}", diagnostic(&message));

                if debug {
                    debug!(
                        error = %diagnostic(format!("{error:?}")),
                        elapsed_ms = started.elapsed().as_millis(),
                        "Command failure details."
                    );
                } else if verbose {
                    let details = error_chain(error.as_ref());

                    if details != error.to_string() {
                        info!(causes = %diagnostic(&details), "Command failure causes.");
                    }

                    info!(elapsed_ms = started.elapsed().as_millis(), "Command failed.");
                }
            }

            if let Some(report) = report {
                match report {
                    Ok(path) if !json => info!(path = %path.display(), "Debug report written."),

                    Ok(_) => {}

                    Err(report_error) if !json => {
                        warn!(error = %diagnostic(report_error.to_string()), "Debug report could not be written.")
                    }

                    Err(_) => {}
                }
            }

            ExitCode::FAILURE
        }
    }
}

fn command_label(command: &Command) -> &'static str {
    match command {
        Command::Init(_) => "init",
        Command::Doctor(_) => "doctor",
        Command::Status(_) => "status",
        Command::Version(_) => "version",
        Command::Completion { .. } => "completion",
        Command::Plugin { .. } => "plugin",
        Command::Session { .. } => "session",
        Command::Delivery { .. } => "delivery",
        Command::Service { .. } => "service",
        Command::Export(_) => "export",
        Command::Validate(_) => "validate",
        Command::Import(_) => "import",
        Command::External(_) => "external",
    }
}

fn write_debug_report(
    command: &str,
    error: &(dyn std::error::Error + 'static),
    elapsed: Duration,
) -> std::io::Result<PathBuf> {
    write_debug_report_at(&home(), command, error, elapsed)
}

fn write_debug_report_at(
    root: &Path,
    command: &str,
    error: &(dyn std::error::Error + 'static),
    elapsed: Duration,
) -> std::io::Result<PathBuf> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let path = root.join("debug").join(format!("crabbot-{stamp}-{}.log", std::process::id()));
    let chain = redact(error_chain(error).as_bytes());
    let details = redact(format!("{error:?}").as_bytes());
    let backtrace = std::backtrace::Backtrace::capture();
    let report = format!(
        "Crabbot debug report\nversion: {VERSION}\ncommand: {command}\nos: {}\narch: {}\nelapsed_ms: {}\nerror: {chain}\ndetails: {details}\nbacktrace: {backtrace}\n",
        std::env::consts::OS,
        std::env::consts::ARCH,
        elapsed.as_millis(),
    );

    secure(&path, report.as_bytes())?;
    Ok(path)
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();

    while let Some(cause) = source {
        messages.push(cause.to_string());
        source = cause.source();
    }

    messages.join(": ")
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = cli.json;

    match cli.command {
        Command::Init(args) => init(args.force, json)?,
        Command::Doctor(args) => doctor(args.fix, json)?,
        Command::Status(output) => status_command(output.json || json).await?,
        Command::Version(output) => version(output.json || json),
        Command::Completion { shell } => completion(shell)?,
        Command::Plugin { command } => plugin(command, json).await?,
        Command::Session { command } => session(command, json).await?,
        Command::Delivery { command } => delivery(command, json).await?,
        Command::Service { command } => service(command.unwrap_or(ServiceCommand::Status), json)?,
        Command::Export(args) => export_crabfile(args, json)?,
        Command::Validate(args) => validate_crabfile(args, json)?,
        Command::Import(args) => import_crabfile(args, json)?,
        Command::External(args) => plugin_command(args).await?,
    }

    Ok(())
}

fn completion(shell: CompletionShell) -> std::io::Result<()> {
    let mut command = Cli::command();
    let mut output = std::io::BufWriter::new(std::io::stdout().lock());
    let name = completion_name();

    generate_completion(shell, &mut command, name, &mut output);
    output.flush()
}

fn generate_completion(
    shell: CompletionShell,
    command: &mut clap::Command,
    name: &str,
    output: &mut dyn Write,
) {
    match shell {
        CompletionShell::Bash => generate(shells::Bash, command, name, output),
        CompletionShell::Fish => generate(shells::Fish, command, name, output),
        CompletionShell::PowerShell => generate(shells::PowerShell, command, name, output),
        CompletionShell::Zsh => generate(shells::Zsh, command, name, output),
    }
}

fn completion_name() -> &'static str {
    completion_name_from(std::env::args_os().next().as_deref())
}

fn completion_name_from(argv0: Option<&std::ffi::OsStr>) -> &'static str {
    let Some(value) = argv0.and_then(|value| value.to_str()) else {
        return NAME;
    };

    let name = value.rsplit(['/', '\\']).next().unwrap_or(value);
    let name = match name.rsplit_once('.') {
        Some((stem, extension)) if extension.eq_ignore_ascii_case("exe") => stem,
        _ => name,
    };

    if name.eq_ignore_ascii_case("crab") { "crab" } else { NAME }
}

fn print_help_with_plugins(root: &Path) {
    print!("{}", help_text_with_plugins(root));
}

fn help_text_with_plugins(root: &Path) -> String {
    let mut command = Cli::command();
    let mut help = command.render_help().to_string();
    let commands = plugin_commands(root);
    let mut plugin_help = String::from("\nPlugin Commands:\n");

    if commands.is_empty() {
        plugin_help.push_str("  (none available)\n");
    } else {
        let width = commands.keys().map(String::len).max().unwrap_or_default();

        for (name, owners) in commands {
            let description = if owners.len() == 1 {
                owners[0].1.description.as_str()
            } else {
                "conflicting registrations"
            };

            plugin_help.push_str(&format!("  {name:<width$}  {description}\n"));
        }
    }

    if let Some(index) = help.find("\nOptions:") {
        help.insert_str(index, &plugin_help);
    } else {
        help.push_str(&plugin_help);
    }

    help
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Crabfile {
    version: String,
    config: Config,
    plugins: Vec<CrabPlugin>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CrabPlugin {
    id: String,
    source: String,
    revision: String,
    version: String,
    capabilities: Vec<String>,
}

impl Crabfile {
    fn validate(&self) -> Result<(), String> {
        if self.version != CRABFILE_VERSION {
            return Err(format!(
                "unsupported version {} (expected {})",
                self.version, CRABFILE_VERSION
            ));
        }

        self.config.validate().map_err(|error| format!("configuration: {error}"))?;

        let mut ids = BTreeSet::new();

        for plugin in &self.plugins {
            if !valid(&plugin.id) {
                return Err(format!(
                    "plugin {} has an invalid ID; use lowercase letters, digits, or hyphens",
                    plugin.id
                ));
            }

            if plugin.source.trim().is_empty() {
                return Err(format!("plugin {} has an empty source", plugin.id));
            }

            if !ids.insert(&plugin.id) {
                return Err(format!("plugin {} is listed more than once", plugin.id));
            }
        }

        Ok(())
    }
}

const CRABFILE_VERSION: &str = "0.1";

fn export_crabfile(
    args: CrabfileExport,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    export_crabfile_at_mode(args, &home(), json)
}

#[cfg(test)]
fn export_crabfile_at(
    args: CrabfileExport,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    export_crabfile_at_mode(args, root, false)
}

fn export_crabfile_at_mode(
    args: CrabfileExport,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = crabfile_output_path(args.destination, args.path)?;

    if path.exists() && !args.force {
        return Err(format!(
            "Crabfile already exists at {}; use --force to overwrite it.",
            path.display()
        )
        .into());
    }

    let config = if root.join("config.toml").is_file() {
        toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?
    } else {
        Config::default()
    };

    let lock = load_lock_at(root)?;
    let mut plugins = lock
        .plugins
        .into_iter()
        .map(|(id, entry)| CrabPlugin {
            id,
            source: entry.source,
            revision: entry.revision,
            version: entry.version,
            capabilities: entry.capabilities,
        })
        .collect::<Vec<_>>();

    plugins.sort_by(|left, right| left.id.cmp(&right.id));
    let file = Crabfile { version: CRABFILE_VERSION.into(), config, plugins };
    let text = toml::to_string_pretty(&file)?;
    secure(&path, text.as_bytes())?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "action": "export",
                "path": path,
                "plugins": file.plugins.len(),
                "status": "exported"
            })
        );
    } else {
        println!("Exported {}.", path.display());
    }

    Ok(())
}

fn import_crabfile(
    args: CrabfileImport,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    import_crabfile_at_mode(args, &home(), json)
}

fn validate_crabfile(
    args: CrabfileValidate,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = args.path.unwrap_or_else(default_crabfile_path);
    let file = read_crabfile(&path)?;
    validate_crabfile_spec(&file, &path)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "path": path,
                "plugins": file.plugins.len(),
                "status": "valid",
                "version": file.version,
            }))?
        );
    } else {
        println!("Crabfile at {} is valid.", path.display());
    }

    Ok(())
}

#[cfg(test)]
fn import_crabfile_at(
    args: CrabfileImport,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    import_crabfile_at_mode(args, root, false)
}

fn import_crabfile_at_mode(
    args: CrabfileImport,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = args.path.unwrap_or_else(default_crabfile_path);

    if !args.yes {
        return Err("Import requires confirmation; re-run with --yes.".into());
    }

    let file = read_crabfile(&path)?;
    validate_crabfile_spec(&file, &path)?;

    let destination = root.join("config.toml");

    if destination.exists() && !args.force {
        return Err("Configuration already exists; use --force to replace it.".into());
    }

    let previous_config = std::fs::read(&destination).ok();
    init_at(root)?;
    secure(&destination, toml::to_string_pretty(&file.config)?.as_bytes())?;
    let result = (|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for plugin in &file.plugins {
            let revision = (!plugin.revision.is_empty() && plugin.revision != "local")
                .then(|| plugin.revision.clone());
            link_at(
                Source {
                    id: plugin.id.clone(),
                    source: Some(plugin.source.clone()),
                    revision,
                    yes: args.force,
                },
                false,
                root,
            )?;

            let lock = load_lock_at(root)?;
            let entry = lock
                .plugins
                .get(&plugin.id)
                .ok_or_else(|| format!("Plugin {} was not recorded after import.", plugin.id))?;

            if entry.version != plugin.version || entry.capabilities != plugin.capabilities {
                return Err(
                    format!("Plugin {} metadata did not match the Crabfile.", plugin.id).into()
                );
            }
        }

        Ok(())
    })();

    if let Err(error) = result {
        match previous_config {
            Some(bytes) => secure(&destination, &bytes)?,

            None => {
                let _ = std::fs::remove_file(&destination);
            }
        }

        return Err(error);
    }

    if json {
        println!(
            "{}",
            serde_json::json!({
                "action": "import",
                "path": path,
                "plugins": file.plugins.len(),
                "status": "imported"
            })
        );
    } else {
        println!(
            "Imported {} configuration and {} plugin entries while preserving local credentials.",
            path.display(),
            file.plugins.len()
        );
    }

    Ok(())
}

fn read_crabfile(path: &Path) -> Result<Crabfile, Box<dyn std::error::Error + Send + Sync>> {
    let source = std::fs::read_to_string(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "Crabfile not found at {}. Use --path <PATH> to choose a Crabfile.",
                path.display()
            )
        } else {
            format!("Could not read Crabfile at {}: {error}.", path.display())
        }
    })?;

    toml::from_str(&source).map_err(|error: toml::de::Error| {
        let location = error.span().map(|span| toml_position(&source, span.start));
        let location = location
            .map_or_else(String::new, |(line, column)| format!(" at line {line}, column {column}"));

        format!("Crabfile at {} is invalid{}: {}.", path.display(), location, error.message())
            .into()
    })
}

fn validate_crabfile_spec(
    file: &Crabfile,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    file.validate()
        .map_err(|error| format!("Crabfile at {} is invalid: {error}", path.display()).into())
}

fn default_crabfile_path() -> PathBuf {
    PathBuf::from("./Crabfile")
}

fn crabfile_output_path(
    destination: Option<PathBuf>,
    path: Option<PathBuf>,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let path = match (destination, path) {
        (Some(_), Some(_)) => {
            return Err("Choose either a positional export path or --path, not both.".into());
        }

        (Some(path), None) | (None, Some(path)) => path,
        (None, None) => default_crabfile_path(),
    };

    Ok(if path.is_dir() { path.join("Crabfile") } else { path })
}

async fn ask_at(ask: Ask, root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let plugin = model_plugin_at(root, ask.plugin.as_deref())?;
    let text = generate_at(Ask { plugin: Some(plugin), ..ask }, root, "cli").await?;
    println!("{text}");
    Ok(())
}

fn model_plugin_at(root: &Path, requested: Option<&str>) -> Result<String, PluginError> {
    let lock = load_lock_at(root)?;
    let configured =
        requested.map(str::to_owned).or_else(|| std::env::var("CRABBOT_MODEL_PLUGIN").ok());
    let plugin = configured.or_else(|| {
        lock.plugins
            .iter()
            .find(|(id, entry)| model_plugin_available(id, entry, root))
            .map(|(id, _)| id.clone())
    });

    let Some(plugin) = plugin else {
        return Err(
            "An installed intelligence plugin is needed in order to ask something to Crabbot."
                .into(),
        );
    };

    let Some(entry) = lock.plugins.get(&plugin) else {
        return Err(format!("Intelligence plugin {plugin} is not installed.").into());
    };

    if !model_plugin_available(&plugin, entry, root) {
        return Err(format!(
            "Intelligence plugin {plugin} is not installed or does not provide model capability."
        )
        .into());
    }

    Ok(plugin)
}

fn model_plugin_available(id: &str, entry: &Entry, root: &Path) -> bool {
    entry.capabilities.iter().any(|capability| capability == "model")
        && binary_at(id, root).is_some()
}

async fn generate_at(
    ask: Ask,
    root: &Path,
    session: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let prompt = ask.prompt.join(" ");
    let message = Message {
        id: "user".into(),
        session: session.into(),
        role: Role::User,
        sender: Some(session.into()),
        content: vec![Content::Text { text: prompt }],
    };

    let plugin = ask.plugin.ok_or("An intelligence plugin is required to ask Crabbot.")?;
    Ok(model_at(plugin, ask.model, root, vec![message]).await?.text)
}

async fn model_at(
    plugin: String,
    model: String,
    root: &Path,
    messages: Vec<Message>,
) -> Result<ModelReply, Box<dyn std::error::Error + Send + Sync>> {
    let path = binary_at(&plugin, root)
        .ok_or_else(|| format!("Plugin binary was not found: {plugin}."))?;

    let config = if root.join("config.toml").is_file() {
        toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?
    } else {
        Config::default()
    };

    config
        .validate()
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    let mut process = launch(root, path, &plugin, Some(Capability::Model), false, &config).await?;
    let result = async {
        let workspace = std::env::var_os("CRABBOT_ROOT").map(PathBuf::from);
        let response =
            process.call(model_request(2, &model, &messages, &[], workspace.as_deref())?).await?;
        let value = response.result.ok_or_else(|| {
            response
                .error
                .map_or_else(|| "Empty model response.".to_string(), |error| error.message)
        })?;

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(serde_json::from_value(value)?)
    }
    .await;

    let stopped = process.stop().await;

    match result {
        Err(error) => Err(error),

        Ok(reply) => {
            stopped?;
            Ok(reply)
        }
    }
}

async fn session(
    command: SessionCommand,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    session_at_json(with_session_json(command, json), &home(), json).await
}

async fn delivery(
    command: DeliveryCommand,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    delivery_at_json(with_delivery_json(command, json), &home(), json).await
}

fn with_session_json(command: SessionCommand, json: bool) -> SessionCommand {
    if !json {
        return command;
    }

    match command {
        SessionCommand::List(mut output) => {
            output.json = true;
            SessionCommand::List(output)
        }

        command => command,
    }
}

fn with_delivery_json(command: DeliveryCommand, json: bool) -> DeliveryCommand {
    if !json {
        return command;
    }

    match command {
        DeliveryCommand::List(mut output) => {
            output.json = true;
            DeliveryCommand::List(output)
        }

        command => command,
    }
}

#[cfg(test)]
async fn delivery_at(
    command: DeliveryCommand,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    delivery_at_json(command, root, false).await
}

async fn delivery_at_json(
    command: DeliveryCommand,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        DeliveryCommand::List(output) => {
            let value = if let Some(value) =
                control_at("delivery.list", serde_json::json!({}), root).await?
            {
                value
            } else {
                let _offline_lock = offline_lock(root)?;
                let store = sessions_at(root)?;
                serde_json::json!({
                    "items": store.outbox.iter().map(ipc::delivery).collect::<Vec<_>>()
                })
            };

            if output.json || json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else if value["items"].as_array().is_none_or(Vec::is_empty) {
                println!("No pending deliveries.");
            } else {
                for item in value["items"].as_array().into_iter().flatten() {
                    println!(
                        "{} ({}).",
                        item["id"].as_str().unwrap_or("unknown"),
                        item["status"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }

        DeliveryCommand::Retry(args) => {
            if !args.yes {
                return Err(
                    "Retrying a delivery requires --yes because a duplicate is possible.".into()
                );
            }

            let id = args.id.clone();

            if let Some(value) =
                control_at("delivery.retry", serde_json::json!({"id": id, "yes": true}), root)
                    .await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!(
                        "Queued delivery {} for retry.",
                        value["id"].as_str().unwrap_or_default()
                    );
                }

                return Ok(());
            }

            let _offline_lock = offline_lock(root)?;
            let mut store = sessions_at(root)?;
            store.retry_delivery(&args.id)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "retry",
                        "id": args.id,
                        "status": "queued"
                    }))?
                );
            } else {
                println!("Queued delivery {} for retry.", args.id);
            }
        }

        DeliveryCommand::Drop(args) => {
            if !args.yes {
                return Err("Dropping a delivery requires --yes.".into());
            }

            let id = args.id.clone();

            if let Some(value) =
                control_at("delivery.drop", serde_json::json!({"id": id, "yes": true}), root)
                    .await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("Dropped delivery {}.", value["id"].as_str().unwrap_or_default());
                }

                return Ok(());
            }

            let _offline_lock = offline_lock(root)?;
            let mut store = sessions_at(root)?;
            store.drop_delivery(&args.id)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "drop",
                        "id": args.id,
                        "status": "dropped"
                    }))?
                );
            } else {
                println!("Dropped delivery {}.", args.id);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
async fn session_at(
    command: SessionCommand,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    session_at_json(command, root, false).await
}

async fn session_at_json(
    command: SessionCommand,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        SessionCommand::New(args) => {
            let id = args.id.clone();
            let model = args.model.clone();

            if let Some(value) =
                control_at("session.new", serde_json::json!({"id": id, "model": model}), root)
                    .await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("Created session {}.", value["id"].as_str().unwrap_or_default());
                }

                return Ok(());
            }

            local_session_mode(SessionCommand::New(args), root, json)?;
        }

        SessionCommand::List(output) => {
            let value = if let Some(value) =
                control_at("session.list", serde_json::json!({}), root).await?
            {
                value
            } else {
                let _offline_lock = offline_lock(root)?;
                let store = sessions_at(root)?;
                serde_json::json!({
                    "items": store.sessions.values().map(ipc::summary).collect::<Vec<_>>()
                })
            };

            if output.json || json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else if value["items"].as_array().is_none_or(Vec::is_empty) {
                println!("No sessions.");
            } else {
                for item in value["items"].as_array().into_iter().flatten() {
                    println!(
                        "{} ({}).",
                        item["id"].as_str().unwrap_or("unknown"),
                        item["status"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }

        SessionCommand::Show(args) => {
            let value = if let Some(value) =
                control_at("session.get", serde_json::json!({"id": args.id}), root).await?
            {
                value
            } else {
                let _offline_lock = offline_lock(root)?;
                let store = sessions_at(root)?;
                ipc::detail(store.sessions.get(&args.id).ok_or("Session was not found.")?)
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("{}", session_text(&value));
            }
        }

        SessionCommand::Fork(args) => {
            let source = args.source.clone();
            let target = args.target.clone();

            if let Some(value) = control_at(
                "session.fork",
                serde_json::json!({"source": source, "target": target}),
                root,
            )
            .await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("Forked session {}.", value["id"].as_str().unwrap_or_default());
                }

                return Ok(());
            }

            local_session_mode(SessionCommand::Fork(args), root, json)?;
        }

        SessionCommand::Model(args) => {
            let id = args.id.clone();
            let model = args.model.clone();

            if let Some(value) =
                control_at("session.model", serde_json::json!({"id": id, "model": model}), root)
                    .await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!(
                        "Session {} now uses {}.",
                        value["id"].as_str().unwrap_or_default(),
                        value["model"].as_str().unwrap_or_default()
                    );
                }

                return Ok(());
            }

            local_session_mode(SessionCommand::Model(args), root, json)?;
        }

        SessionCommand::Cancel(args) => {
            let id = args.id.clone();

            if let Some(value) =
                control_at("session.cancel", serde_json::json!({"id": id}), root).await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("Cancelled session {}.", value["id"].as_str().unwrap_or_default());
                }

                return Ok(());
            }

            local_session_mode(SessionCommand::Cancel(args), root, json)?;
        }

        SessionCommand::Delete(args) => {
            if !args.yes {
                return Err("Deleting a session requires --yes.".into());
            }

            let id = args.id.clone();

            if let Some(value) =
                control_at("session.delete", serde_json::json!({"id": id}), root).await?
            {
                if json {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else if value["worktree"]["status"] == "pending" {
                    println!(
                        "{}",
                        sentence(format!(
                            "Deleted session {}, but worktree cleanup is pending: {}",
                            value["id"].as_str().unwrap_or_default(),
                            value["worktree"]["error"]
                                .as_str()
                                .unwrap_or("Retry at daemon startup.")
                        ))
                    );
                } else {
                    println!("Deleted session {}.", value["id"].as_str().unwrap_or_default());
                }

                return Ok(());
            }

            local_session_mode(SessionCommand::Delete(args), root, json)?;
        }
    }

    Ok(())
}

fn local_session(
    command: SessionCommand,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    local_session_json(command, root, false)
}

fn local_session_mode(
    command: SessionCommand,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if json { local_session_json(command, root, true) } else { local_session(command, root) }
}

fn local_session_json(
    command: SessionCommand,
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(root)?;
    let _offline_lock = offline_lock(root)?;
    let mut store = sessions_at(root)?;

    match command {
        SessionCommand::New(args) => {
            let id = args.id.clone();
            let model = args.model.clone();
            store.create(id.clone(), model.clone())?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "create",
                        "id": id,
                        "model": model
                    }))?
                );
            } else {
                println!("Created session {}.", args.id);
            }
        }

        SessionCommand::List(output) => {
            let value = serde_json::json!({
                "items": store.sessions.values().map(ipc::summary).collect::<Vec<_>>()
            });

            if output.json || json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else if store.sessions.is_empty() {
                println!("No sessions.");
            } else {
                for session in store.sessions.values() {
                    println!("{} ({}).", session.id, session.status);
                }
            }
        }

        SessionCommand::Show(args) => {
            let session = store.sessions.get(&args.id).ok_or("Session was not found.")?;
            let value = ipc::detail(session);

            if json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("{}", session_text(&value));
            }
        }

        SessionCommand::Fork(args) => {
            store.fork(&args.source, &args.target)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "fork",
                        "id": args.target,
                        "source": args.source
                    }))?
                );
            } else {
                println!("Forked session {}.", args.target);
            }
        }

        SessionCommand::Model(args) => {
            store.set_model(&args.id, &args.model)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "model",
                        "id": args.id,
                        "model": args.model
                    }))?
                );
            } else {
                println!("Session {} now uses {}.", args.id, args.model);
            }
        }

        SessionCommand::Cancel(args) => {
            store.cancel(&args.id)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "cancel",
                        "id": args.id,
                        "status": "cancelled"
                    }))?
                );
            } else {
                println!("Cancelled session {}.", args.id);
            }
        }

        SessionCommand::Delete(args) => {
            if !args.yes {
                return Err("Deleting a session requires --yes.".into());
            }

            store.remove(&args.id)?;

            let worktree_error = state::remove_worktree(&workspace_root(), &args.id).err();

            if json {
                let mut value = serde_json::json!({
                    "action": "delete",
                    "id": args.id,
                    "status": "deleted"
                });

                if let Some(error) = worktree_error {
                    value["worktree"] = serde_json::json!({
                        "status": "pending",
                        "error": error.to_string()
                    });
                }

                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                if let Some(error) = worktree_error {
                    println!(
                        "{}",
                        sentence(format!(
                            "Session {} was deleted, but its worktree could not be reclaimed: {}",
                            args.id, error
                        ))
                    );
                }

                println!("Deleted session {}.", args.id);
            }
        }
    }

    Ok(())
}

fn session_text(value: &serde_json::Value) -> String {
    let mut lines = vec![
        format!("Session: {}", value["id"].as_str().unwrap_or("unknown")),
        format!("Model: {}", value["model"].as_str().unwrap_or("unknown")),
        format!("Status: {}", value["status"].as_str().unwrap_or("unknown")),
    ];

    if let Some(workspace) = value["workspace"].as_str() {
        lines.push(format!("Workspace: {workspace}"));
    }

    lines.push("Transcript:".into());
    let messages = value["messages"].as_array();

    if messages.is_none_or(Vec::is_empty) {
        lines.push("  (empty)".into());
    } else {
        for message in messages.into_iter().flatten() {
            let role = message["role"].as_str().unwrap_or("unknown");
            let content = message["content"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|item| {
                    item["text"]
                        .as_str()
                        .or_else(|| item["alt"].as_str())
                        .or_else(|| item["name"].as_str())
                        .unwrap_or_else(|| item["kind"].as_str().unwrap_or("attachment"))
                })
                .collect::<Vec<_>>()
                .join(" ");

            lines.push(format!("  {role}: {content}"));
        }
    }

    lines.join("\n")
}

async fn control_at(
    method: &str,
    params: serde_json::Value,
    root: &Path,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    match ipc::call(root, method, params).await {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
            ) =>
        {
            Ok(None)
        }

        Err(error) => Err(error.into()),
    }
}

fn sessions_at(root: &Path) -> Result<state::Store, Box<dyn std::error::Error + Send + Sync>> {
    Ok(state::Store::load(root.join("sessions.json"))?)
}

fn binary_at(id: &str, root: &Path) -> Option<PathBuf> {
    if !valid(id) {
        return None;
    }

    let name = if cfg!(windows) {
        format!("crabbot-plugin-{id}.exe")
    } else {
        format!("crabbot-plugin-{id}")
    };

    let config = root.join("plugins").join(id).join("bin").join(&name);

    if config.is_file() {
        return Some(config);
    }

    None
}

fn valid(id: &str) -> bool {
    !id.is_empty()
        && id.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn home() -> PathBuf {
    std::env::var_os("CRABBOT_HOME").map(PathBuf::from).unwrap_or_else(|| {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::var_os("HOME")
                    .or_else(|| std::env::var_os("USERPROFILE"))
                    .map_or_else(|| PathBuf::from("."), PathBuf::from)
                    .join(".config")
            })
            .join(NAME)
    })
}

fn workspace_root() -> PathBuf {
    std::env::var_os("CRABBOT_ROOT").map_or_else(|| PathBuf::from("."), PathBuf::from)
}

fn init(force: bool, json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = home();

    if root.exists() && !force {
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "home": root,
                    "initialized": false,
                    "status": "already_initialized"
                })
            );
        } else {
            println!("Crabbot is already initialized at {}.", root.display());
        }

        return Ok(());
    }

    init_at_with_force(&root, force)?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "home": root,
                "initialized": true,
                "status": if force { "reinitialized" } else { "initialized" }
            })
        );
    } else {
        println!("Crabbot has been initialized at {}.\nHappy crabbing!", root.display());
    }

    Ok(())
}

fn init_at(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_at_with_force(root, false)
}

fn init_at_with_force(
    root: &Path,
    force: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(root.join("plugins"))?;
    let path = root.join("config.toml");

    if force || !path.exists() {
        let config = toml::to_string_pretty(&Config::default())?;
        secure(&path, config.as_bytes())?;
    }

    Ok(())
}

pub async fn serve() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_at(&home()).await
}

async fn serve_at(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel_id = std::env::var("CRABBOT_CHANNEL").unwrap_or_else(|_| "telegram".into());
    let model_id = std::env::var("CRABBOT_MODEL_PLUGIN").unwrap_or_else(|_| "codex".into());
    let model = std::env::var("CRABBOT_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
    serve_inner(root, &channel_id, &model_id, &model, false).await
}

async fn serve_inner(
    root: &Path,
    channel_id: &str,
    model_id: &str,
    model: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_at(root)?;
    let _daemon_lock = daemon_lock(root)?;
    recover_plugins(root)?;

    let config: Config = toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?;
    config
        .validate()
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    updates(root, &config.update)?;

    let loaded = state::Store::load(root.join("sessions.json"))?;
    let workspace = workspace_root();
    reclaim_worktrees(&workspace, &loaded);
    let sessions = Arc::new(Mutex::new(loaded));
    let pending = Arc::new(AsyncMutex::new(approval::Gate::new()?));
    let token = ipc::token()?;
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    secure(root.join("ipc.token"), token.as_bytes())?;
    secure(root.join("ipc.port"), port.to_string().as_bytes())?;
    let stop = Arc::new(Stop::new());
    let cancels = Arc::new(Mutex::new(BTreeMap::new()));
    let plugins = Plugins::default();
    let ipc_state = Arc::new(ipc::State {
        token,
        sessions: Arc::clone(&sessions),
        stop: Arc::clone(&stop),
        slots: Arc::new(tokio::sync::Semaphore::new(64)),
        cancels: Arc::clone(&cancels),
        root: workspace,
        home: root.to_owned(),
        approval_mode: config.approval.clone(),
        pending: Arc::clone(&pending),
        plugins: plugins.clone(),
        config: config.clone(),
        channel: channel_id.to_owned(),
        model: model_id.to_owned(),
    });

    let ipc_task = tokio::spawn(ipc::serve(listener, ipc_state));

    if channel_id == model_id {
        if let Err(error) = load_plugin(root, channel_id, None, force, &config, &plugins).await {
            warn!(plugin = channel_id, error = %diagnostic(sentence(error.to_string())), "Plugin failed to start.");
        }
    } else {
        if (force || ready(channel_id))
            && let Err(error) =
                load_plugin(root, channel_id, Some(Capability::Channel), force, &config, &plugins)
                    .await
        {
            warn!(plugin = channel_id, error = %diagnostic(sentence(error.to_string())), "Plugin failed to start.");
        }

        if (force || ready(model_id))
            && let Err(error) =
                load_plugin(root, model_id, Some(Capability::Model), force, &config, &plugins).await
        {
            warn!(plugin = model_id, error = %diagnostic(sentence(error.to_string())), "Plugin failed to start.");
        }
    }

    for id in installed_at(root) {
        if id != channel_id
            && id != model_id
            && let Err(error) = load_plugin(root, &id, None, force, &config, &plugins).await
        {
            warn!(plugin = %id, error = %diagnostic(sentence(error.to_string())), "Plugin failed to start.");
        }
    }

    info!(channel = channel_id, model = model_id, "Daemon is running on local IPC.");

    let approval_mode = config.approval_mode();
    let outcome = bridge_with_media(
        &plugins,
        channel_id,
        model_id,
        model,
        Arc::clone(&sessions),
        Arc::clone(&stop),
        Arc::clone(&cancels),
        config.channels,
        approval_mode,
        media_root_at(root),
        pending,
    )
    .await;

    stop.signal();
    let _ = ipc_task.await;
    let _ = std::fs::remove_file(root.join("ipc.token"));
    let _ = std::fs::remove_file(root.join("ipc.port"));
    let mut cleanup = None;

    for (_, process) in plugins.all().await {
        if let Err(error) = process.stop().await {
            cleanup.get_or_insert(error);
        }
    }

    outcome?;

    if let Some(error) = cleanup {
        return Err(error.into());
    }

    info!("Daemon stopped.");
    Ok(())
}

fn daemon_lock(root: &Path) -> std::io::Result<DaemonLock> {
    lock_at(root, "daemon.lock")
}

fn offline_lock(root: &Path) -> std::io::Result<DaemonLock> {
    lock_at(root, "daemon.lock").map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "The daemon owns the state; use the IPC client instead of opening state directly.",
            )
        } else {
            error
        }
    })
}

fn lock_at(root: &Path, name: &str) -> std::io::Result<DaemonLock> {
    std::fs::create_dir_all(root)?;
    let path = root.join(name);

    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Lock path cannot be a symbolic link.",
            ));
        }

        Ok(_) => {}

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;

    if let Err(error) = file.try_lock() {
        return match error {
            std::fs::TryLockError::WouldBlock => {
                Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "Lock is already held."))
            }

            std::fs::TryLockError::Error(error) => Err(error),
        };
    }

    file.set_len(0)?;
    use std::io::Write as _;
    writeln!(file, "{}", std::process::id())?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&path)?.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(&path, permissions)?;
    }

    Ok(DaemonLock { _file: file })
}

fn recover_plugins(root: &Path) -> std::io::Result<()> {
    let _plugins_lock = lock_at(root, ".plugins.lock")?;
    let lock = load_lock_at(root).map_err(std::io::Error::other)?;
    let plugins = root.join("plugins");
    let Ok(entries) = std::fs::read_dir(&plugins) else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();

        if let Some(id) =
            name.strip_prefix('.').and_then(|value| value.split_once(".backup-")).map(|(id, _)| id)
            && valid(id)
        {
            let backup = entry.path();
            let destination = plugins.join(id);

            if !lock.plugins.contains_key(id) {
                std::fs::remove_dir_all(backup)?;
            } else if !destination.exists() {
                std::fs::rename(backup, destination)?;
            } else {
                std::fs::remove_dir_all(backup)?;
            }
        } else if name.starts_with('.') && name.contains(".stage-") {
            std::fs::remove_dir_all(entry.path())?;
        }
    }

    Ok(())
}

fn reclaim_worktrees(root: &Path, store: &state::Store) {
    let directory = root.join(".crabbot/worktrees");
    let Ok(entries) = std::fs::read_dir(&directory) else {
        return;
    };

    let active = store.sessions.keys().cloned().collect::<std::collections::BTreeSet<_>>();

    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();

        if valid(&name) && !active.contains(&name) {
            let _ = state::remove_worktree(root, &name);
        }
    }
}

async fn launch(
    root: &Path,
    path: PathBuf,
    expected: &str,
    capability: Option<Capability>,
    force: bool,
    config: &Config,
) -> Result<Process, Box<dyn std::error::Error + Send + Sync>> {
    let manifest_path = path
        .parent()
        .and_then(Path::parent)
        .ok_or("Plugin manifest path is invalid.")?
        .join("crabbot-plugin.toml");

    let manifest = read_manifest(manifest_path.parent().unwrap_or(Path::new(".")))
        .ok_or_else(|| format!("Plugin manifest is invalid: {}.", manifest_path.display()))?;

    let mut expected_capabilities = manifest.capabilities.clone();
    expected_capabilities.sort();
    let mut expected_commands = manifest.commands.clone();
    expected_commands.sort_by(|left, right| left.name.cmp(&right.name));

    if !force {
        let lock = load_lock_at(root)?;
        let entry = lock
            .plugins
            .get(expected)
            .ok_or_else(|| format!("Plugin {expected} is not locked."))?;

        let actual = digest(&manifest_path, &path)?;

        if actual != entry.hash {
            return Err(format!("Plugin {expected} failed its launch integrity check.").into());
        }

        let mut locked_capabilities = entry.capabilities.clone();
        locked_capabilities.sort();

        if expected_capabilities != locked_capabilities {
            return Err(
                format!("Plugin {expected} lock capabilities do not match its manifest.").into()
            );
        }

        let mut locked_commands = entry.commands.clone();
        locked_commands.sort_by(|left, right| left.name.cmp(&right.name));

        if expected_commands != locked_commands {
            return Err(
                format!("Plugin {expected} lock commands do not match its manifest.").into()
            );
        }
    }

    let process = Process::start_with_env(
        path,
        std::iter::empty::<&std::ffi::OsStr>(),
        env_for(&manifest, config, root),
    )
    .await?;

    let mut actual_capabilities = process
        .hello
        .capabilities
        .iter()
        .map(capability_name)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    actual_capabilities.sort();
    let mut actual_commands = process.hello.commands.clone();
    actual_commands.sort_by(|left, right| left.name.cmp(&right.name));

    if process.hello.id != expected
        || capability.is_some_and(|capability| !process.hello.capabilities.contains(&capability))
        || expected_capabilities != actual_capabilities
        || expected_commands != actual_commands
    {
        let actual = process.hello.id.clone();
        let _ = process.stop().await;
        return Err(format!("Plugin {actual} did not advertise {expected} correctly.").into());
    }

    Ok(process)
}

pub(crate) async fn load_plugin(
    root: &Path,
    id: &str,
    capability: Option<Capability>,
    force: bool,
    config: &Config,
    plugins: &Plugins,
) -> Result<Hello, Box<dyn std::error::Error + Send + Sync>> {
    if !valid(id) {
        return Err(format!("Invalid plugin ID: {id}.").into());
    }

    let _loading = plugins.loading.lock().await;
    let path = binary_at(id, root).ok_or_else(|| format!("Plugin binary was not found: {id}."))?;
    let process = launch(root, path, id, capability, force, config).await?;
    let hello = process.hello.clone();

    if let Some(previous) = plugins.insert(Live::new(process)).await {
        tokio::spawn(async move {
            if let Err(error) = previous.stop().await {
                warn!(error = %diagnostic(sentence(error.to_string())), "Replaced plugin could not be stopped.");
            }
        });
    }

    info!(plugin = %hello.id, version = %hello.version, "Plugin loaded.");
    Ok(hello)
}

pub(crate) async fn unload_plugin(
    id: &str,
    plugins: &Plugins,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    if !valid(id) {
        return Err(format!("Invalid plugin ID: {id}.").into());
    }

    let _loading = plugins.loading.lock().await;
    let Some(plugin) = plugins.remove(id).await else {
        return Ok(false);
    };

    plugin.stop().await?;
    info!(plugin = id, "Plugin unloaded.");
    Ok(true)
}

fn capability_name(capability: &Capability) -> &'static str {
    match capability {
        Capability::Model => "model",
        Capability::Vision => "vision",
        Capability::Channel => "channel",
        Capability::Store => "store",
        Capability::Memory => "memory",
        Capability::Timer => "timer",
        Capability::Tool => "tool",
        Capability::Mcp => "mcp",
        Capability::Speech => "speech",
        Capability::Client => "client",
        Capability::Resource => "resource",
        Capability::Agent => "agent",
    }
}

fn native_command(name: &str) -> bool {
    matches!(
        name,
        "help"
            | "init"
            | "doctor"
            | "status"
            | "version"
            | "completion"
            | "plugin"
            | "session"
            | "delivery"
            | "service"
            | "ask"
            | "export"
            | "validate"
            | "import"
    )
}

fn validate_commands(
    root: &Path,
    id: &str,
    commands: &[CommandSpec],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for command in commands {
        if native_command(&command.name) {
            return Err(
                format!("Plugin {id} command {} is reserved by Crabbot.", command.name).into()
            );
        }

        for (other_id, entry) in &load_lock_at(root)?.plugins {
            if other_id != id && entry.commands.iter().any(|other| other.name == command.name) {
                return Err(format!(
                    "Plugin {id} command {} conflicts with plugin {other_id}.",
                    command.name
                )
                .into());
            }
        }
    }

    Ok(())
}

fn plugin_commands(root: &Path) -> BTreeMap<String, Vec<(String, CommandSpec)>> {
    let Ok(lock) = load_lock_at(root) else {
        return BTreeMap::new();
    };

    let mut commands = BTreeMap::new();

    for (id, entry) in &lock.plugins {
        for command in &entry.commands {
            commands
                .entry(command.name.clone())
                .or_insert_with(Vec::new)
                .push((id.clone(), command.clone()));
        }
    }

    if lock.plugins.iter().any(|(id, entry)| model_plugin_available(id, entry, root)) {
        commands.entry("ask".into()).or_default().push((
            "__runtime".into(),
            CommandSpec {
                name: "ask".into(),
                description: "Send one prompt through an intelligence plugin.".into(),
                interactive: false,
            },
        ));
    }

    commands
}

fn env_for(manifest: &Manifest, config: &Config, root: &Path) -> Vec<(String, String)> {
    const SAFE: &[&str] = &[
        "PATH",
        "TEMP",
        "TMP",
        "TMPDIR",
        "RUST_BACKTRACE",
        "CRABBOT_HOME",
        "CRABBOT_MEDIA",
        "CRABBOT_CODEX_HOME",
        "CRABBOT_CODEX_BINARY",
        "CRABBOT_ROOT",
        "CRABBOT_MODEL_PLUGIN",
        "CRABBOT_MODEL",
        "CRABBOT_KEYRING",
        "CRABBOT_MEMORY",
        "CRABBOT_TIMER",
        "CRABBOT_DB",
        "CRABBOT_WHISPER",
        "CRABBOT_WHISPER_COMMAND",
        "CRABBOT_CODEX_BASE_URL",
        "CRABBOT_OPENROUTER_MODEL",
        "CRABBOT_OPENROUTER_BASE_URL",
        "CRABBOT_OPENROUTER_REFERER",
        "CRABBOT_OPENROUTER_TITLE",
        "CRABBOT_CLAUDE_BASE_URL",
        "CRABBOT_GEMINI_BASE_URL",
        "CRABBOT_OLLAMA_HOST",
        "CRABBOT_PI_COMMAND",
        "CRABBOT_SIGNAL_COMMAND",
        "CRABBOT_SLACK_CHANNELS",
        "CRABBOT_DISCORD_GATEWAY_URL",
        "CRABBOT_DISCORD_INTENTS",
        "CRABBOT_WHATSAPP_PHONE",
        "CRABBOT_WHATSAPP_LISTEN",
        "CRABBOT_WHATSAPP_GRAPH_URL",
    ];

    let mut values = SAFE
        .iter()
        .filter_map(|name| std::env::var(name).ok().map(|value| ((*name).to_owned(), value)))
        .collect::<Vec<_>>();

    ensure_home(&mut values, root);
    ensure_codex_home(&mut values);

    if manifest.id == "tools" {
        values.push(("CRABBOT_SHELL".into(), if config.shell { "on" } else { "off" }.into()));

        for name in ["CRABBOT_SANDBOX_RUNTIME", "CRABBOT_SANDBOX_IMAGE"] {
            if let Some(value) = std::env::var_os(name) {
                values.push((name.into(), value.to_string_lossy().into_owned()));
            }
        }
    }

    for name in &manifest.secrets {
        if let Some(value) = secret(name) {
            values.push((name.clone(), value));
        }
    }

    values
}

fn ensure_home(values: &mut Vec<(String, String)>, root: &Path) {
    if !values.iter().any(|(name, _)| name == "CRABBOT_HOME") {
        values.push(("CRABBOT_HOME".into(), root.display().to_string()));
    }
}

fn ensure_codex_home(values: &mut Vec<(String, String)>) {
    if values.iter().any(|(name, _)| name == "CRABBOT_CODEX_HOME") {
        return;
    }

    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        values.push((
            "CRABBOT_CODEX_HOME".into(),
            PathBuf::from(home).join(".codex").display().to_string(),
        ));
    }
}

fn secret(name: &str) -> Option<String> {
    if !name.starts_with("CRABBOT_") {
        return None;
    }

    if let Ok(value) = std::env::var(name)
        && !value.trim().is_empty()
    {
        return Some(value);
    }

    if let Some(path) = std::env::var_os("CRABBOT_CREDENTIALS")
        && crabbot_file::private(&path).is_ok()
        && let Ok(text) = std::fs::read_to_string(path)
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
        && let Some(value) = value[name].as_str().filter(|value| !value.trim().is_empty())
    {
        return Some(value.to_owned());
    }

    None
}

fn ready(id: &str) -> bool {
    match id {
        "telegram" => {
            env_nonempty("CRABBOT_TELEGRAM_TOKEN")
                || keyring_ready("telegram")
                || file_credential(&["CRABBOT_TELEGRAM_TOKEN"])
        }

        "discord" => {
            env_nonempty("CRABBOT_DISCORD_TOKEN")
                || keyring_ready("discord")
                || file_credential(&["CRABBOT_DISCORD_TOKEN"])
        }

        "codex" => {
            env_nonempty("CRABBOT_CODEX_KEY")
                || keyring_ready("codex")
                || file_credential(&["CRABBOT_CODEX_KEY"])
                || codex_auth()
        }

        "claude" => {
            env_nonempty("CRABBOT_CLAUDE_KEY")
                || keyring_ready("claude")
                || file_credential(&["CRABBOT_CLAUDE_KEY"])
        }

        "ollama" => true,

        "openrouter" => {
            env_nonempty("CRABBOT_OPENROUTER_KEY") || file_credential(&["CRABBOT_OPENROUTER_KEY"])
        }

        "gemini" => env_nonempty("CRABBOT_GEMINI_KEY") || file_credential(&["CRABBOT_GEMINI_KEY"]),
        "signal" => env_nonempty("CRABBOT_SIGNAL_ACCOUNT"),
        "slack" => env_nonempty("CRABBOT_SLACK_BOT_TOKEN"),

        "whatsapp" => {
            (env_nonempty("CRABBOT_WHATSAPP_TOKEN") || file_credential(&["CRABBOT_WHATSAPP_TOKEN"]))
                && (env_nonempty("CRABBOT_WHATSAPP_APP_SECRET")
                    || file_credential(&["CRABBOT_WHATSAPP_APP_SECRET"]))
                && (env_nonempty("CRABBOT_WHATSAPP_VERIFY")
                    || file_credential(&["CRABBOT_WHATSAPP_VERIFY"]))
                && env_nonempty("CRABBOT_WHATSAPP_PHONE")
                && env_nonempty("CRABBOT_WHATSAPP_GRAPH_URL")
        }

        _ => binary_at(id, &home()).is_some(),
    }
}

fn updates(root: &Path, mode: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !matches!(mode, "off" | "check" | "prompt" | "auto") {
        return Err(format!("Unsupported update mode: {mode}.").into());
    }

    if mode == "off" {
        return Ok(());
    }

    let lock = load_lock_at(root)?;
    let mut changed_ids = Vec::new();

    for (id, entry) in &lock.plugins {
        match changed(root, id, entry) {
            Ok(true) => changed_ids.push(id.as_str()),

            Ok(false) => {}
            Err(error) => warn!(
                plugin = %id,
                error = %diagnostic(sentence(error.to_string())),
                "Plugin update check failed."
            ),
        }
    }

    let changed = changed_ids;

    if changed.is_empty() {
        return Ok(());
    }

    match mode {
        "check" => info!(plugins = %changed.join(", "), "Plugin updates are available."),
        "prompt" => info!("Plugin updates are available; run `crabbot plugin update` to review."),
        "auto" => update_at(root, false)?,
        _ => unreachable!("update mode was validated above"),
    }

    Ok(())
}

fn changed(
    root: &Path,
    id: &str,
    entry: &Entry,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let pin = entry.pinned.then_some(entry.revision.as_str());
    let source = resolve(&entry.source, pin)?;
    let Some(binary) = plugin_binary(&source.path, id, entry.default) else {
        return Ok(false);
    };

    let hash = digest(&source.path.join("crabbot-plugin.toml"), &binary)?;
    let revision_changed = entry.revision != "local"
        && !entry.revision.is_empty()
        && revision(&source.path) != entry.revision;

    Ok((hash != entry.hash || revision_changed) && root.join("plugins").join(id).is_dir())
}

fn plugin_name(id: &str) -> String {
    if cfg!(windows) { format!("crabbot-plugin-{id}.exe") } else { format!("crabbot-plugin-{id}") }
}

fn plugin_binary(source: &Path, id: &str, default: bool) -> Option<PathBuf> {
    let name = plugin_name(id);
    let binary = source.join("bin").join(&name);

    if binary.is_file() {
        return Some(binary);
    }

    if !default {
        return None;
    }

    let root = source.parent()?.parent()?;
    let binary = root.join("target/debug").join(name);
    binary.is_file().then_some(binary)
}

fn env_nonempty(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn keyring_ready(name: &str) -> bool {
    if std::env::var("CRABBOT_KEYRING").ok().as_deref() != Some("1") {
        return false;
    }

    keyring::Entry::new("dev.airscripts.crabbot", name)
        .ok()
        .and_then(|entry| entry.get_password().ok())
        .is_some_and(|value| !value.trim().is_empty())
}

fn file_credential(names: &[&str]) -> bool {
    let Some(path) = std::env::var_os("CRABBOT_CREDENTIALS") else {
        return false;
    };

    credential(&path, names)
}

fn credential(path: impl AsRef<Path>, names: &[&str]) -> bool {
    let path = path.as_ref();

    if crabbot_file::private(path).is_err() {
        return false;
    }

    let Ok(text) = std::fs::read_to_string(path) else {
        return false;
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };

    names.iter().any(|name| value[*name].as_str().is_some_and(|value| !value.trim().is_empty()))
}

fn codex_auth() -> bool {
    let binary = std::env::var_os("CRABBOT_CODEX_BINARY").unwrap_or_else(|| "codex".into());
    let mut command = std::process::Command::new(binary);
    command
        .args(["login", "status"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    if let Some(path) = codex_home() {
        command.env("CODEX_HOME", path);
    }

    let Ok(mut child) = command.spawn() else {
        return false;
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),

            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

fn codex_home() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_CODEX_HOME").map(PathBuf::from).or_else(|| {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(|home| PathBuf::from(home).join(".codex"))
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn bridge(
    plugins: &Plugins,
    channel_id: &str,
    model_id: &str,
    model: &str,
    sessions: Arc<Mutex<state::Store>>,
    stop: Arc<Stop>,
    cancels: Arc<Mutex<BTreeMap<String, Arc<Cancellation>>>>,
    channels: BTreeMap<String, ChannelConfig>,
    approval_mode: ApprovalMode,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let home = std::env::var_os("CRABBOT_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".config/crabbot"));

    bridge_with_media(
        plugins,
        channel_id,
        model_id,
        model,
        sessions,
        stop,
        cancels,
        channels,
        approval_mode,
        media_root_at(&home),
        Arc::new(AsyncMutex::new(approval::Gate::new()?)),
    )
    .await
}

fn media_root_at(home: &Path) -> PathBuf {
    std::env::var_os("CRABBOT_MEDIA").map(PathBuf::from).unwrap_or_else(|| home.join("media"))
}

#[allow(clippy::too_many_arguments)]
async fn bridge_with_media(
    plugins: &Plugins,
    channel_id: &str,
    model_id: &str,
    model: &str,
    sessions: Arc<Mutex<state::Store>>,
    stop: Arc<Stop>,
    cancels: Arc<Mutex<BTreeMap<String, Arc<Cancellation>>>>,
    channels: BTreeMap<String, ChannelConfig>,
    approval_mode: ApprovalMode,
    media_root: PathBuf,
    approvals: Arc<AsyncMutex<approval::Gate>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut offset = sessions.lock().map_err(|_| "Session lock is poisoned.")?.offset(channel_id);
    let mut call = 10_u64;
    let mut channel_failures = 0_u32;
    let mut provider_failures = 0_u32;

    loop {
        let Some(channel) = plugins.get(channel_id).await else {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => return Ok(()),
                _ = stop.notified() => return Ok(()),
                _ = plugins.changed.notified() => continue,
            }
        };

        let Some(provider) = plugins.get(model_id).await else {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => return Ok(()),
                _ = stop.notified() => return Ok(()),
                _ = plugins.changed.notified() => continue,
            }
        };

        let pending = sessions
            .lock()
            .map_err(|_| "Session lock is poisoned.")?
            .outbox
            .iter()
            .find(|delivery| {
                delivery.channel == channel_id && delivery.status == state::DeliveryStatus::Pending
            })
            .cloned();

        if let Some(delivery) = pending {
            sessions.lock().map_err(|_| "Session lock is poisoned.")?.sending(&delivery.id)?;
            let sent = tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => return Ok(()),
                _ = stop.notified() => return Ok(()),
                result = channel.call(delivery_request(channel_id, call, &delivery)?) => result,
            };

            call += 1;

            match sent {
                Ok(response) if response.error.is_none() && response.result.is_some() => {
                    sessions.lock().map_err(|_| "Session lock is poisoned.")?.ack(&delivery.id)?;
                }

                Ok(response) => {
                    let message = response.error.map_or_else(
                        || "Channel rejected the delivery without a reason.".into(),
                        |error| sentence(error.message),
                    );

                    warn!(channel = channel_id, error = %diagnostic(&message), "Channel delivery is uncertain.");
                    sessions
                        .lock()
                        .map_err(|_| "Session lock is poisoned.")?
                        .uncertain(&delivery.id, message)?;
                }

                Err(error) => {
                    let message = sentence(error.to_string());
                    warn!(channel = channel_id, error = %diagnostic(&message), "Channel delivery is uncertain.");
                    sessions
                        .lock()
                        .map_err(|_| "Session lock is poisoned.")?
                        .uncertain(&delivery.id, message)?;

                    if !recover(&channel, &mut channel_failures, "channel", stop.as_ref()).await? {
                        return Ok(());
                    }
                }
            }

            continue;
        }

        let queued = {
            let store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;
            store
                .sessions
                .values()
                .filter(|session| {
                    session.id.starts_with(&format!("{channel_id}-"))
                        && session.status != "cancelled"
                        && !session.queued.is_empty()
                })
                .find_map(|session| {
                    session.queued.first().cloned().map(|message| {
                        let roles =
                            session.queue_roles.get(&message.id).cloned().unwrap_or_default();
                        (
                            message,
                            session
                                .chat
                                .clone()
                                .or_else(|| {
                                    session
                                        .id
                                        .strip_prefix(&format!("{channel_id}-"))
                                        .and_then(|value| value.split("-thread-").next())
                                        .map(str::to_owned)
                                })
                                .unwrap_or_default(),
                            session.thread.clone(),
                            session.private,
                            roles,
                        )
                    })
                })
        };

        let response = if let Some((message, chat, thread, private, roles)) = queued.as_ref() {
            channel_failures = 0;
            let text = message.content.iter().map(Content::render).collect::<Vec<_>>().join("\n");
            Response::ok(
                call,
                serde_json::json!({
                    "events": [{
                        "id": message.id,
                        "chat": chat,
                        "private": private,
                        "sender": message.sender,
                        "roles": roles,
                        "text": text,
                        "content": message.content,
                        "thread": thread,
                        "topic": thread,
                    }]
                }),
            )
        } else {
            let polled = {
                let poll = channel.call(Request::call(
                    call,
                    "poll",
                    serde_json::json!({"offset": offset}),
                ));
                tokio::pin!(poll);
                tokio::select! {
                    biased;
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                    _ = stop.notified() => return Ok(()),
                    result = tokio::time::timeout(
                        std::time::Duration::from_secs(45),
                        &mut poll,
                    ) => match result {
                        Ok(Ok(response)) => Ok(response),
                        Ok(Err(error)) => Err(format!("Channel polling failed: {}", sentence(error.to_string()))),
                        Err(_) => Err("Channel polling timed out.".into()),
                    },
                }
            };

            let polled = match polled {
                Ok(response) => response,

                Err(message) => {
                    warn!(channel = channel_id, error = %diagnostic(&message), "Channel polling failed.");

                    if !recover(&channel, &mut channel_failures, "channel", stop.as_ref()).await? {
                        return Ok(());
                    }

                    continue;
                }
            };

            channel_failures = 0;
            polled
        };

        call += 1;

        let Some(value) = response.result else {
            if let Some(error) = response.error {
                warn!(channel = channel_id, error = %diagnostic(sentence(error.message)), "Channel polling failed.");
            }

            if !recover(&channel, &mut channel_failures, "channel", stop.as_ref()).await? {
                return Ok(());
            }

            continue;
        };

        let events = value["events"].as_array().cloned().unwrap_or_default();
        let channel_policy = channels.get(channel_id).cloned().unwrap_or_default();

        for event in events {
            let Some(event_key) = event_id(&event["id"]) else {
                continue;
            };

            let gateway_sequence = event["gateway_sequence"].as_u64();
            let next_offset = event["id"].as_i64().map(|value| value.saturating_add(1));

            if event["kind"] == "callback" {
                handle_callback(
                    &channel,
                    channel_id,
                    &event,
                    &channel_policy,
                    approval_mode,
                    &approvals,
                    &sessions,
                    &mut offset,
                    &mut call,
                )
                .await?;
                continue;
            }

            if event_key.len() > META_LIMIT {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let queued_event = queued.as_ref().is_some_and(|(queued, ..)| queued.id == event_key);

            if !queued_event
                && sessions
                    .lock()
                    .map_err(|_| "Session lock is poisoned.")?
                    .known(channel_id, &event_key)
            {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let Some(chat) = event["chat"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| event["chat"].as_i64().map(|value| value.to_string()))
            else {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            };

            if chat.len() > META_LIMIT {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let thread = event_id(&event["thread"]);

            if thread.as_ref().is_some_and(|value| value.len() > META_LIMIT) {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            if event["topic"].as_str().is_some_and(|value| value.len() > META_LIMIT)
                || event["text"].as_str().is_some_and(|value| value.len() > TEXT_LIMIT)
            {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            if event["sender"].as_str().is_some_and(|value| value.len() > META_LIMIT)
                || event["roles"].as_array().is_some_and(|roles| {
                    roles.len() > CONTENT_LIMIT
                        || roles
                            .iter()
                            .any(|role| role.as_str().is_some_and(|value| value.len() > META_LIMIT))
                })
            {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            // Queue entries already passed channel policy; preserve that decision across turns.
            if !queued_event && !allowed(&channel_policy, &event, &chat) {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let content = resolve_media(channel_id, &channel, content(&event), &mut call).await;
            let content = if provider.supports(Capability::Vision) {
                pin_media(content, &media_root)
            } else {
                content
            };

            let content = expand_media_files(channel_id, content, &media_root);
            let content =
                transcribe_media(plugins, channel_id, content, &media_root, &mut call).await;

            if content.is_empty() {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let session_name = session_id(channel_id, &chat, thread.as_deref());

            if !valid(&session_name) {
                warn!(channel = channel_id, event = %event_key, "Message has invalid routing metadata.");
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let message = Message {
                id: event_key.clone(),
                session: session_name.clone(),
                role: Role::User,
                sender: event["sender"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| event["sender"].as_i64().map(|id| id.to_string())),
                content,
            };

            let roles = event["roles"]
                .as_array()
                .into_iter()
                .flatten()
                .take(CONTENT_LIMIT)
                .filter_map(event_id)
                .collect::<Vec<_>>();

            let retry_message = message.clone();
            let event_id = message.id.clone();
            let (workspace, prepared, commit) = {
                let mut store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;
                let mut commit = false;
                let workspace = if !store.sessions.contains_key(&session_name)
                    && store.sessions.len() >= state::LIMIT
                {
                    warn!(session = %session_name, event = %event_id, "Session capacity reached; message rejected.");
                    commit = true;
                    None
                } else if event["private"] != true && channel_policy.worktree {
                    match isolate(&session_name) {
                        Ok(workspace) => workspace,

                        Err(error) => {
                            warn!(
                                event = %event_key,
                                error = %diagnostic(sentence(error.to_string())),
                                "Group workspace isolation failed."
                            );
                            commit = true;
                            None
                        }
                    }
                } else {
                    None
                };

                let prepared = if commit {
                    None
                } else if let Err(error) = store.ensure(&session_name, model) {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        warn!(session = %session_name, event = %event_id, "Session capacity reached; message rejected.");
                        commit = true;
                        None
                    } else {
                        error!(session = %session_name, error = %diagnostic(sentence(error.to_string())), "Session creation failed.");
                        break;
                    }
                } else if let Err(error) = store.route(
                    &session_name,
                    channel_id,
                    &chat,
                    thread.as_deref(),
                    event["private"] == true,
                ) {
                    error!(session = %session_name, error = %diagnostic(sentence(error.to_string())), "Message routing failed.");
                    break;
                } else {
                    let existing = store.sessions.get(&session_name).is_some_and(|session| {
                        session.messages.iter().any(|item| item.id == message.id)
                    });

                    if !queued_event
                        && store.sessions.get(&session_name).is_some_and(|session| {
                            existing || session.queued.iter().any(|item| item.id == message.id)
                        })
                    {
                        None
                    } else if !queued_event
                        && store
                            .sessions
                            .get(&session_name)
                            .is_some_and(|session| session.status == "working")
                    {
                        if let Err(error) =
                            store.queue_with_roles(&session_name, message, roles.clone())
                        {
                            if error.kind() == std::io::ErrorKind::WouldBlock {
                                warn!(session = %session_name, event = %event_id, "Session queue is full; message rejected.");
                                commit = true;
                                None
                            } else {
                                error!(
                                    session = %session_name,
                                    error = %diagnostic(sentence(error.to_string())),
                                    "Message queueing failed."
                                );
                                break;
                            }
                        } else {
                            None
                        }
                    } else {
                        let prepared = store.sessions.get(&session_name).map(|session| {
                            let mut history = session.messages.clone();

                            if !existing {
                                history.push(message.clone());

                                if history.len() > state::LIMIT {
                                    history.drain(..history.len() - state::LIMIT);
                                }
                            }

                            (history, session.model.clone())
                        });

                        if prepared.is_some()
                            && let Err(error) = store.begin_with_roles(
                                &session_name,
                                message.clone(),
                                roles.clone(),
                            )
                        {
                            error!(
                                session = %session_name,
                                error = %diagnostic(sentence(error.to_string())),
                                "Session turn could not start."
                            );
                            break;
                        }

                        prepared
                    }
                };

                (workspace, prepared, commit)
            };

            if commit {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            }

            let Some((history, session_model)) = prepared else {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
                continue;
            };

            let mut messages = Vec::with_capacity(history.len() + 1);
            let system = workspace.as_deref().map_or_else(
                || context(&session_name),
                |workspace| context_at(&session_name, workspace),
            );

            if let Some(system) = system {
                messages.push(system);
            }

            messages.extend(history);

            if !queued_event {
                commit_event(
                    &channel,
                    &sessions,
                    channel_id,
                    &event_key,
                    next_offset,
                    &mut offset,
                    gateway_sequence,
                    &mut call,
                )
                .await?;
            }

            let delivery_id = format!("{channel_id}-{event_id}");
            let mut failed_tool = None;
            let model_result = {
                let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel(32);
                let turn_cancel = cancellation(&cancels, &session_name);
                let mut stream_call = call.saturating_add(1_u64 << 32);
                let turn_deadline = tokio::time::Instant::now() + TURN_LIMIT;
                let answer = answer(
                    &provider,
                    plugins,
                    &session_model,
                    messages,
                    &session_name,
                    channel_id,
                    &chat,
                    thread.as_deref(),
                    &sessions,
                    workspace.as_deref(),
                    &media_root,
                    tools_allowed(&channel_policy, &event, &chat, approval_mode.enabled()),
                    approval_mode,
                    Arc::clone(&approvals),
                    &turn_cancel,
                    &stop,
                    turn_deadline,
                    &mut call,
                    &mut failed_tool,
                    stream_tx,
                );
                tokio::pin!(answer);
                let mut output = StreamOutput::new();
                let mut ticker = tokio::time::interval_at(
                    tokio::time::Instant::now() + std::time::Duration::from_millis(500),
                    std::time::Duration::from_millis(500),
                );
                let result = loop {
                    tokio::select! {
                        biased;
                        result = &mut answer => break result,

                        Some(notice) = stream_rx.recv() => {
                            match notice {
                                StreamNotice::Approval { chat: approval_chat, thread: approval_thread, text, approve, deny, deadline } => {
                                    if let Err(error) = wait_approval(
                                        &channel,
                                        channel_id,
                                        &session_model,
                                        &approval_chat,
                                        approval_thread.as_deref(),
                                        &text,
                                        &approve,
                                        &deny,
                                        &channel_policy,
                                        approval_mode,
                                        &approvals,
                                        &sessions,
                                        &mut offset,
                                        &mut stream_call,
                                        deadline,
                                        &turn_cancel,
                                        &stop,
                                    ).await {
                                        approvals.lock().await.cancel(&approve);
                                        warn!(error = %diagnostic(sentence(error.to_string())), "Inline approval failed.");
                                    }
                                }

                                notice => {
                                    output.apply(notice);

                                    if output.ready()
                                        && let Err(error) = flush_stream(
                                            &mut output, &channel, channel_id, &sessions, &session_name,
                                            &delivery_id, &chat, thread.as_deref(), &mut stream_call,
                                        ).await {
                                        output.disabled = true;
                                        warn!(error = %diagnostic(sentence(error.to_string())), "Channel streaming paused.");
                                    }
                                }
                            }
                        }

                        _ = ticker.tick(), if output.dirty => {
                            if let Err(error) = flush_stream(
                                &mut output, &channel, channel_id, &sessions, &session_name,
                                &delivery_id, &chat, thread.as_deref(), &mut stream_call,
                            ).await {
                                output.disabled = true;
                                warn!(error = %diagnostic(sentence(error.to_string())), "Channel streaming paused.");
                            }
                        }
                    }
                };

                while let Ok(notice) = stream_rx.try_recv() {
                    output.apply(notice);
                }

                if output.dirty
                    && !output.disabled
                    && let Err(error) = flush_stream(
                        &mut output,
                        &channel,
                        channel_id,
                        &sessions,
                        &session_name,
                        &delivery_id,
                        &chat,
                        thread.as_deref(),
                        &mut stream_call,
                    )
                    .await
                {
                    warn!(error = %diagnostic(sentence(error.to_string())), "Channel streaming paused.");
                }

                result
            };

            let model = match model_result {
                Ok(model) => model,

                Err(error) => {
                    let cancelled = sessions
                        .lock()
                        .map_err(|_| "Session lock is poisoned.")?
                        .sessions
                        .get(&session_name)
                        .is_some_and(|session| session.status == "cancelled");

                    if cancelled || error.to_string() == "The turn was cancelled." {
                        let _ = sessions
                            .lock()
                            .map_err(|_| "Session lock is poisoned.")?
                            .clear(&session_name, "cancelled");
                        let _ = provider.restart().await;

                        for (id, process) in plugins.all().await {
                            if id != channel_id {
                                let _ = process.restart().await;
                            }
                        }

                        acknowledge(&cancels, &session_name)?;
                        continue;
                    }

                    if error.to_string() == "The turn was interrupted." {
                        let _ = status(&sessions, &session_name, "interrupted");
                        return Ok(());
                    }

                    if let Some(id) = failed_tool {
                        restart_tool(plugins, &id).await;
                    }

                    warn!(session = %session_name, error = %diagnostic(sentence(error.to_string())), "Model turn failed.");
                    let failure = Message {
                        id: format!("failure-{call}"),
                        session: session_name.clone(),
                        role: Role::Assistant,
                        sender: None,
                        content: vec![Content::Text {
                            text: "I could not complete that turn.".into(),
                        }],
                    };
                    {
                        let mut store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;
                        let replay = store
                            .sessions
                            .get(&session_name)
                            .is_none_or(|session| session.phase != "unsafe");
                        let replied = match store.reply(
                            &session_name,
                            failure,
                            &delivery_id,
                            channel_id,
                            &chat,
                            thread.clone(),
                            "I could not complete that turn.",
                        ) {
                            Ok(()) => true,

                            Err(error) => {
                                error!(
                                    session = %session_name,
                                    error = %diagnostic(sentence(error.to_string())),
                                    "Model failure could not be persisted."
                                );
                                false
                            }
                        };

                        if !replied && let Err(error) = store.clear(&session_name, "interrupted") {
                            return Err(format!(
                                "Could not clear the failed turn lease: {}",
                                sentence(error.to_string())
                            )
                            .into());
                        }

                        if replay {
                            if let Err(error) =
                                store.queue_with_roles(&session_name, retry_message, roles)
                            {
                                error!(
                                    session = %session_name,
                                    error = %diagnostic(sentence(error.to_string())),
                                    "Model retry could not be queued."
                                );
                            }
                        } else {
                            warn!(
                                session = %session_name,
                                "Failed turn contained a mutating tool; it will not be retried."
                            );
                        }
                    }

                    if !recover(&provider, &mut provider_failures, "model", stop.as_ref()).await? {
                        return Ok(());
                    }

                    continue;
                }
            };

            provider_failures = 0;
            let assistant = Message {
                id: format!("assistant-{call}"),
                session: session_name.clone(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: model.text.clone() }],
            };

            let delivery = {
                let mut store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;

                if store
                    .sessions
                    .get(&session_name)
                    .is_some_and(|session| session.status == "cancelled")
                {
                    store.clear(&session_name, "cancelled")?;
                    drop(store);
                    acknowledge(&cancels, &session_name)?;
                    continue;
                }

                store.reply(
                    &session_name,
                    assistant,
                    &delivery_id,
                    channel_id,
                    &chat,
                    thread.clone(),
                    &model.text,
                )?;
                let delivery = store
                    .outbox
                    .iter()
                    .find(|delivery| delivery.id == delivery_id)
                    .cloned()
                    .ok_or("Persisted delivery was not found.")?;

                if delivery.status == state::DeliveryStatus::Uncertain {
                    None
                } else {
                    store.sending(&delivery_id)?;
                    Some(delivery)
                }
            };

            let Some(delivery) = delivery else {
                warn!(
                    session = %session_name,
                    "Channel delivery is uncertain; retry it only after checking the channel."
                );
                continue;
            };

            let request = delivery_request(channel_id, call, &delivery)?;
            let sent = tokio::select! {
                biased;

                _ = tokio::signal::ctrl_c() => {
                    let _ = status(&sessions, &session_name, "interrupted");
                    return Ok(());
                },

                _ = stop.notified() => {
                    let _ = status(&sessions, &session_name, "interrupted");
                    return Ok(());
                },

                result = channel.call(request) => match result {
                    Ok(sent) => sent,

                    Err(error) => {
                        let message = sentence(error.to_string());
                        warn!(channel = channel_id, error = %diagnostic(&message), "Channel delivery is uncertain.");
                        sessions.lock().map_err(|_| "Session lock is poisoned.")?.uncertain(&delivery_id, message)?;
                        status_if_present(&sessions, &session_name, "idle")?;

                        if !recover(&channel, &mut channel_failures, "channel", stop.as_ref()).await? {
                            return Ok(());
                        }

                        continue;
                    }
                },
            };

            call += 1;

            if let Some(error) = sent.error {
                let message = sentence(error.message);
                warn!(channel = channel_id, error = %diagnostic(&message), "Channel delivery is uncertain.");
                sessions
                    .lock()
                    .map_err(|_| "Session lock is poisoned.")?
                    .uncertain(&delivery_id, message)?;
            } else if sent.result.is_some() {
                sessions.lock().map_err(|_| "Session lock is poisoned.")?.ack(&delivery_id)?;
            }

            status_if_present(&sessions, &session_name, "idle")?;
        }
    }
}

fn retry_delay(attempts: u32) -> std::time::Duration {
    let seconds = 1_u64 << attempts.min(5);
    std::time::Duration::from_secs(seconds.min(30))
}

fn cancellation(
    cancels: &Arc<Mutex<BTreeMap<String, Arc<Cancellation>>>>,
    session: &str,
) -> Arc<Cancellation> {
    let mut cancels = cancels.lock().expect("Cancellation lock is poisoned.");
    Arc::clone(cancels.entry(session.into()).or_insert_with(|| Arc::new(Cancellation::new())))
}

fn acknowledge(
    cancels: &Arc<Mutex<BTreeMap<String, Arc<Cancellation>>>>,
    session: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(cancel) =
        cancels.lock().map_err(|_| "Cancellation lock is poisoned.")?.remove(session)
    {
        cancel.acknowledge();
    }

    Ok(())
}

fn cancelled(
    sessions: &Arc<Mutex<state::Store>>,
    session: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    Ok(sessions
        .lock()
        .map_err(|_| "Session lock is poisoned.")?
        .sessions
        .get(session)
        .is_some_and(|value| value.status == "cancelled"))
}

async fn recover(
    process: &Live,
    failures: &mut u32,
    name: &str,
    stop: &Stop,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    *failures = failures.saturating_add(1);

    if *failures >= 3 {
        warn!(plugin = name, "Plugin restart circuit is open; stopping retries.");
        return Ok(false);
    }

    let delay = retry_delay(*failures);
    tokio::select! {
        biased;
        _ = stop.notified() => return Ok(false),

        _ = tokio::time::sleep(delay) => {}
    }

    process.restart().await?;
    info!(plugin = name, "Plugin restarted after a transport failure.");
    Ok(true)
}

fn content(event: &serde_json::Value) -> Vec<Content> {
    if let Some(values) = event["content"].as_array() {
        let content = values
            .iter()
            .take(CONTENT_LIMIT)
            .filter_map(|value| serde_json::from_value::<Content>(value.clone()).ok())
            .collect::<Vec<_>>();

        if !content.is_empty() {
            return bound(content);
        }
    }

    event["text"]
        .as_str()
        .filter(|text| !text.is_empty())
        .map(|text| bound(vec![Content::Text { text: text.into() }]))
        .unwrap_or_default()
}

async fn resolve_media(
    channel_id: &str,
    channel: &Live,
    mut content: Vec<Content>,
    call: &mut u64,
) -> Vec<Content> {
    if !matches!(channel_id, "telegram" | "discord" | "whatsapp" | "signal" | "slack") {
        return content;
    }

    for item in &mut content {
        let uri = match item {
            Content::Image { uri, .. } | Content::File { uri, .. } | Content::Audio { uri, .. }
                if media_uri(uri, channel_id) =>
            {
                uri.clone()
            }

            _ => continue,
        };

        let request = Request::call(*call, "media", serde_json::json!({"uri": uri}));
        *call = (*call).saturating_add(1);
        let Ok(response) = channel.call(request).await else {
            warn!(channel = channel_id, "Media could not be resolved; retaining its reference.");
            continue;
        };

        let Some(resolved) = response
            .result
            .and_then(|value| value["uri"].as_str().map(str::to_owned))
            .filter(|value| value.starts_with("file://"))
        else {
            warn!(channel = channel_id, "Media could not be resolved; retaining its reference.");
            continue;
        };

        match item {
            Content::Image { uri, .. } | Content::File { uri, .. } | Content::Audio { uri, .. } => {
                *uri = clip(resolved, META_LIMIT);
            }

            Content::Text { .. } => {}
        }
    }

    content
}

async fn transcribe_media(
    plugins: &Plugins,
    channel_id: &str,
    content: Vec<Content>,
    media_root: &Path,
    call: &mut u64,
) -> Vec<Content> {
    if !matches!(channel_id, "telegram" | "discord" | "whatsapp") {
        return content;
    }

    let speech = plugins.find(Capability::Speech).await;
    let mut output = Vec::with_capacity(content.len());

    for item in content {
        let Content::Audio { uri, .. } = &item else {
            output.push(item);
            continue;
        };

        let Some(path) = media_file(uri, media_root) else {
            warn!(
                channel = channel_id,
                "Voice attachment is unavailable or outside the media cache."
            );
            output.push(unavailable_voice());
            continue;
        };

        let Some((id, plugin)) = &speech else {
            warn!(
                channel = channel_id,
                "Voice transcription is unavailable because no speech plugin is loaded."
            );
            output.push(unavailable_voice());
            continue;
        };

        let request = Request::call(*call, "transcribe", serde_json::json!({"path": path}));
        *call = (*call).saturating_add(1);
        let response = plugin.call(request).await;
        let transcript = match response {
            Ok(response) => response
                .result
                .and_then(|value| value["text"].as_str().map(str::to_owned))
                .map(|text| clip(text, TEXT_LIMIT))
                .filter(|text| !text.trim().is_empty()),

            Err(_) => {
                let _ = plugin.restart().await;
                None
            }
        };

        if let Some(text) = transcript {
            if std::fs::remove_file(&path).is_err() {
                warn!(
                    channel = channel_id,
                    "Transcription completed, but the raw voice file could not be removed."
                );
            }

            output.push(Content::Text { text });
        } else {
            warn!(plugin = %id, "Voice transcription failed.");
            output.push(unavailable_voice());
        }
    }

    output
}

fn expand_media_files(channel_id: &str, content: Vec<Content>, media_root: &Path) -> Vec<Content> {
    if !matches!(channel_id, "telegram" | "discord" | "whatsapp") {
        return content;
    }

    let mut output = Vec::with_capacity(content.len());

    for item in content {
        let Content::File { uri, name, mime } = &item else {
            output.push(item);
            continue;
        };

        let text = media_file(uri, media_root)
            .filter(|path| {
                std::fs::metadata(path).is_ok_and(|metadata| metadata.len() <= FILE_LIMIT)
            })
            .filter(|_| safe_text_file(name, mime.as_deref()))
            .and_then(|path| std::fs::read(path).ok())
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .filter(|text| {
                !text.is_empty()
                    && !text.chars().any(|character| {
                        character.is_control() && !matches!(character, '\n' | '\r' | '\t')
                    })
            });

        match text {
            Some(text) => output.push(Content::Text {
                text: format!("[File: {}]\n{text}", clip(name.clone(), META_LIMIT)),
            }),

            None => output.push(Content::Text {
                text: "[File attachment omitted because it is unsafe or unsupported.]".into(),
            }),
        }
    }

    bound(output)
}

fn media_uri(uri: &str, channel_id: &str) -> bool {
    match channel_id {
        "telegram" => uri.starts_with("telegram://file/"),
        "discord" => uri.starts_with("discord://attachment/"),
        "whatsapp" => uri.starts_with("whatsapp://media/"),
        "signal" => uri.starts_with("signal://attachment/"),
        "slack" => uri.starts_with("slack://file/"),
        _ => false,
    }
}

fn pin_media(content: Vec<Content>, media_root: &Path) -> Vec<Content> {
    let pinned = media_root.join("pinned");
    let pinned_root = std::fs::canonicalize(&pinned).ok();
    let mut output = Vec::with_capacity(content.len());

    for item in content {
        let Content::Image { uri, alt } = &item else {
            output.push(item);
            continue;
        };

        let Some(source) = media_file(uri, media_root) else {
            output.push(item);
            continue;
        };

        if pinned_root.as_ref().is_some_and(|root| source.starts_with(root)) {
            output.push(item);
            continue;
        }

        let Some((_, bytes)) = read_image(&source, IMAGE_LIMIT) else {
            output.push(item);
            continue;
        };

        let mut digest = Sha256::new();
        digest.update(uri.as_bytes());
        digest.update(&bytes);
        let destination = pinned.join(format!("{:x}.bin", digest.finalize()));

        if std::fs::create_dir_all(&pinned).is_ok()
            && crabbot_file::save(&destination, &bytes).is_ok()
        {
            output.push(Content::Image {
                uri: format!("file://{}", destination.display()),
                alt: alt.clone(),
            });
        } else {
            output.push(item);
        }
    }

    output
}

fn safe_text_file(name: &str, mime: Option<&str>) -> bool {
    let extension = Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    let extension_allowed = matches!(
        extension.as_str(),
        "bash"
            | "c"
            | "conf"
            | "cpp"
            | "css"
            | "csv"
            | "go"
            | "h"
            | "hpp"
            | "html"
            | "ini"
            | "java"
            | "js"
            | "json"
            | "jsonl"
            | "kt"
            | "log"
            | "md"
            | "py"
            | "rb"
            | "rs"
            | "sh"
            | "sql"
            | "swift"
            | "toml"
            | "ts"
            | "tsx"
            | "txt"
            | "xml"
            | "yaml"
            | "yml"
    );

    match mime.map(str::trim).map(str::to_ascii_lowercase) {
        Some(mime) if mime.starts_with("text/") => true,
        Some(mime)
            if matches!(
                mime.as_str(),
                "application/json"
                    | "application/toml"
                    | "application/xml"
                    | "application/yaml"
                    | "application/x-yaml"
                    | "application/octet-stream"
            ) =>
        {
            extension_allowed
        }

        Some(_) => false,
        None => extension_allowed,
    }
}

fn unavailable_voice() -> Content {
    Content::Text { text: "[Voice message omitted because transcription is unavailable.]".into() }
}

fn media_file(uri: &str, root: &Path) -> Option<PathBuf> {
    let path = PathBuf::from(uri.strip_prefix("file://")?);
    let metadata = std::fs::symlink_metadata(&path).ok()?;

    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MEDIA_LIMIT {
        return None;
    }

    let root = std::fs::canonicalize(root).ok()?;
    let path = std::fs::canonicalize(path).ok()?;
    (path.starts_with(root) && path.is_file()).then_some(path)
}

fn prepare_images(messages: &mut [Message], media_root: &Path) {
    let mut total = 0_usize;

    for message in messages {
        message.content = std::mem::take(&mut message.content)
            .into_iter()
            .map(|content| match content {
                Content::Image { uri, alt } if uri.starts_with("file://") => {
                    let remaining = IMAGE_TOTAL_LIMIT.saturating_sub(total);
                    let image = media_file(&uri, media_root)
                        .and_then(|path| read_image(&path, IMAGE_LIMIT.min(remaining)));

                    match image {
                        Some((mime, bytes)) => {
                            total = total.saturating_add(bytes.len());
                            Content::Image {
                                uri: format!("data:{mime};base64,{}", STANDARD.encode(bytes)),
                                alt,
                            }
                        }

                        None => unavailable_image(alt),
                    }
                }

                Content::Image { uri, alt } if uri.starts_with("data:image/") => {
                    match image_data_size(&uri) {
                        Some(size)
                            if size <= IMAGE_LIMIT
                                && size <= IMAGE_TOTAL_LIMIT.saturating_sub(total) =>
                        {
                            total = total.saturating_add(size);
                            Content::Image { uri, alt }
                        }

                        _ => unavailable_image(alt),
                    }
                }

                Content::Image { uri, alt }
                    if uri.starts_with("https://") && uri.len() <= META_LIMIT =>
                {
                    Content::Image { uri, alt }
                }

                Content::Image { alt, .. } => unavailable_image(alt),
                content => content,
            })
            .collect();
    }
}

fn read_image(path: &Path, limit: usize) -> Option<(&'static str, Vec<u8>)> {
    if limit == 0 {
        return None;
    }

    let file = std::fs::File::open(path).ok()?;

    if !file.metadata().ok()?.is_file() {
        return None;
    }

    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    file.take(limit.saturating_add(1) as u64).read_to_end(&mut bytes).ok()?;

    if bytes.len() > limit {
        return None;
    }

    let mime = image_mime(&bytes)?;
    Some((mime, bytes))
}

fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else {
        None
    }
}

fn image_data_size(uri: &str) -> Option<usize> {
    let (mime, encoded) = uri.strip_prefix("data:image/")?.split_once(";base64,")?;

    if !matches!(mime, "png" | "jpeg" | "gif" | "webp")
        || encoded.len() > IMAGE_LIMIT.saturating_add(2).saturating_div(3) * 4
    {
        return None;
    }

    let bytes = STANDARD.decode(encoded).ok()?;
    let expected = match mime {
        "png" => "image/png",
        "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => return None,
    };

    (image_mime(&bytes)? == expected).then_some(bytes.len())
}

fn unavailable_image(alt: Option<String>) -> Content {
    Content::Text {
        text: alt.map_or_else(
            || "[Image attachment omitted because it is unavailable or unsupported.]".into(),
            |alt| format!("[Image attachment unavailable. Description: {}]", clip(alt, META_LIMIT)),
        ),
    }
}

fn bound(mut content: Vec<Content>) -> Vec<Content> {
    content.truncate(CONTENT_LIMIT);
    let mut text = TEXT_LIMIT;

    for item in &mut content {
        match item {
            Content::Text { text: value } => {
                *value = clip(std::mem::take(value), text);
                text = text.saturating_sub(value.len());
            }

            Content::Image { uri, alt } => {
                *uri = clip(std::mem::take(uri), META_LIMIT);
                *alt = alt.take().map(|value| clip(value, META_LIMIT));
            }

            Content::File { uri, name, mime } => {
                *uri = clip(std::mem::take(uri), META_LIMIT);
                *name = clip(std::mem::take(name), META_LIMIT);
                *mime = mime.take().map(|value| clip(value, META_LIMIT));
            }

            Content::Audio { uri, mime } => {
                *uri = clip(std::mem::take(uri), META_LIMIT);
                *mime = mime.take().map(|value| clip(value, META_LIMIT));
            }
        }
    }

    content
}

fn clip(mut value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }

    let mut end = limit.saturating_sub(3).min(value.len());

    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }

    value.truncate(end);
    value.push('…');
    value
}

fn event_id(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::to_owned).or_else(|| value.as_i64().map(|value| value.to_string()))
}

fn session_id(channel: &str, chat: &str, thread: Option<&str>) -> String {
    thread.map_or_else(
        || format!("{channel}-{chat}"),
        |thread| format!("{channel}-{chat}-thread-{thread}"),
    )
}

fn context(session: &str) -> Option<Message> {
    let root = workspace_root();
    context_at(session, &root)
}

fn context_at(session: &str, root: &Path) -> Option<Message> {
    let current = std::fs::canonicalize(root).ok()?;
    let boundary = std::env::var_os("CRABBOT_ROOT")
        .and_then(|path| std::fs::canonicalize(path).ok())
        .unwrap_or_else(|| current.clone());

    let mut text = String::new();
    let mut path = current.clone();

    loop {
        let separator = if text.is_empty() { 0 } else { 2 };

        let remaining = CONTEXT_LIMIT.saturating_sub(text.len()).saturating_sub(separator);

        if remaining == 0 {
            break;
        }

        if let Some(value) = context_file(&path.join("AGENTS.md"), remaining) {
            if !text.is_empty() {
                text.push_str("\n\n");
            }

            text.push_str(&value);
        }

        if path == boundary {
            break;
        }

        let Some(parent) = path.parent() else {
            break;
        };

        path = parent.to_path_buf();
    }

    if text.trim().is_empty() {
        return None;
    }

    (!text.trim().is_empty()).then_some(Message {
        id: "system-context".into(),
        session: session.into(),
        role: Role::System,
        sender: None,
        content: vec![Content::Text { text }],
    })
}

fn context_file(path: &Path, limit: usize) -> Option<String> {
    if limit == 0 {
        return None;
    }

    if std::fs::symlink_metadata(path).ok()?.file_type().is_symlink() {
        return None;
    }

    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take((limit as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;

    if bytes.len() > limit {
        bytes.truncate(limit);
    }

    let mut value = String::from_utf8_lossy(&bytes).into_owned();

    while value.len() > limit {
        value.pop();
    }

    (!value.trim().is_empty()).then_some(value)
}

fn allowed(policy: &ChannelConfig, event: &serde_json::Value, chat: &str) -> bool {
    let private = event["private"] == true;

    if !private && policy.allow.is_empty() {
        return false;
    }

    if !private && !policy.allow.is_empty() && !policy.allow.iter().any(|value| value == chat) {
        return false;
    }

    if let Some(marker) = policy.mention.as_deref()
        && !event["text"].as_str().is_some_and(|text| mentions(text, marker))
    {
        return false;
    }

    if !policy.topic.is_empty() && !policy.topic.iter().any(|value| same_id(&event["topic"], value))
    {
        return false;
    }

    if !policy.thread.is_empty()
        && !policy.thread.iter().any(|value| same_id(&event["thread"], value))
    {
        return false;
    }

    let sender = event_id(&event["sender"]).unwrap_or_default();
    let roles =
        event["roles"].as_array().into_iter().flatten().filter_map(event_id).collect::<Vec<_>>();

    let restricted =
        policy.owner.is_some() || !policy.admin.is_empty() || !policy.member.is_empty();

    !restricted
        || policy.owner.as_deref() == Some(sender.as_str())
        || policy
            .admin
            .iter()
            .any(|value| value == &sender || roles.iter().any(|role| role == value))
        || policy
            .member
            .iter()
            .any(|value| value == &sender || roles.iter().any(|role| role == value))
}

fn tools_allowed(
    policy: &ChannelConfig,
    event: &serde_json::Value,
    chat: &str,
    approvals: bool,
) -> bool {
    if !approvals || !policy.tools || !allowed(policy, event, chat) {
        return false;
    }

    if event["private"] != true {
        return true;
    }

    let sender = event_id(&event["sender"]).unwrap_or_default();
    let roles =
        event["roles"].as_array().into_iter().flatten().filter_map(event_id).collect::<Vec<_>>();

    policy.allow.iter().any(|value| value == chat)
        || policy.owner.as_deref() == Some(sender.as_str())
        || policy
            .admin
            .iter()
            .any(|value| value == &sender || roles.iter().any(|role| role == value))
        || policy
            .member
            .iter()
            .any(|value| value == &sender || roles.iter().any(|role| role == value))
}

fn mentions(text: &str, marker: &str) -> bool {
    let marker = marker.trim();

    if marker.is_empty() {
        return true;
    }

    text.split(|value: char| !value.is_alphanumeric() && value != '_' && value != '-')
        .any(|value| value.eq_ignore_ascii_case(marker.trim_start_matches('@')))
}

fn same_id(value: &serde_json::Value, expected: &str) -> bool {
    event_id(value).is_some_and(|value| value == expected)
}

fn isolate(session: &str) -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    isolate_at(session, &workspace_root())
}

fn isolate_at(
    session: &str,
    root: &Path,
) -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    if !valid(session) {
        return Ok(None);
    }

    let root = match std::fs::canonicalize(root) {
        Ok(root) => root,
        Err(_) => return Ok(None),
    };

    let check = command_output(git_command().args([
        "-C",
        &root.display().to_string(),
        "rev-parse",
        "--is-inside-work-tree",
    ]));

    if !check.is_ok_and(|output| output.status.success()) {
        return Err("The configured workspace is not a Git worktree.".into());
    }

    let path = root.join(".crabbot/worktrees").join(session);

    if std::fs::symlink_metadata(&path).is_ok() {
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err("The isolated worktree path cannot be a symbolic link.".into());
        }

        let path = std::fs::canonicalize(path)?;

        if !path.starts_with(&root) {
            return Err("The isolated room worktree leaves the configured root.".into());
        }

        let check = command_output(git_command().args([
            "-C",
            &path.display().to_string(),
            "rev-parse",
            "--is-inside-work-tree",
        ]))?;

        if !check.status.success() {
            return Err("The isolated room worktree is not a Git worktree.".into());
        }

        return Ok(Some(path));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        let parent = std::fs::canonicalize(parent)?;

        if !parent.starts_with(&root) {
            return Err("The isolated worktree path leaves the configured root.".into());
        }
    }

    let output = command_output(
        git_command()
            .args(["-C", &root.display().to_string(), "worktree", "add", "--detach"])
            .arg(&path)
            .arg("HEAD"),
    )?;

    if !output.status.success() {
        return Err("The isolated room worktree could not be created.".into());
    }

    let path = std::fs::canonicalize(path)?;

    if !path.starts_with(&root) {
        return Err("The isolated worktree leaves the configured root.".into());
    }

    Ok(Some(path))
}

fn send_params(
    channel_id: &str,
    delivery_id: &str,
    chat: &str,
    text: &str,
    thread: Option<&str>,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    if channel_id == "discord" {
        return Ok(serde_json::json!({"delivery": delivery_id, "channel": chat, "text": text}));
    }

    if channel_id != "telegram" {
        let mut value = serde_json::json!({"delivery": delivery_id, "chat": chat, "text": text});

        if let Some(thread) = thread {
            value["thread"] = serde_json::Value::String(thread.into());
        }

        return Ok(value);
    }

    let mut value = serde_json::json!({
        "chat": chat.parse::<i64>().map_err(|_| "Telegram chat ID was invalid.")?,
        "delivery": delivery_id,
        "text": text
    });

    if let Some(thread) = thread {
        value["thread"] = serde_json::Value::String(thread.into());
    }

    Ok(value)
}

fn delivery_request(
    channel: &str,
    call: u64,
    delivery: &state::Delivery,
) -> Result<Request, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(message_id) = delivery.message_id.as_deref() {
        Ok(Request::call(
            call,
            "edit",
            edit_params(
                channel,
                &delivery.id,
                &delivery.chat,
                message_id,
                &delivery.text,
                delivery.thread.as_deref(),
            )?,
        ))
    } else {
        Ok(Request::call(
            call,
            "send",
            send_params(
                channel,
                &delivery.id,
                &delivery.chat,
                &delivery.text,
                delivery.thread.as_deref(),
            )?,
        ))
    }
}

fn edit_params(
    channel: &str,
    delivery: &str,
    chat: &str,
    message: &str,
    text: &str,
    thread: Option<&str>,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    if channel == "discord" {
        return Ok(serde_json::json!({
            "delivery": delivery,
            "channel": chat,
            "message": message,
            "text": text,
        }));
    }

    if channel != "telegram" {
        return Err("This channel does not support message edits.".into());
    }

    let mut params = serde_json::json!({
        "delivery": delivery,
        "chat": chat.parse::<i64>().map_err(|_| "Telegram chat ID was invalid.")?,
        "message": message.parse::<i64>().map_err(|_| "Telegram message ID was invalid.")?,
        "text": text,
    });

    if let Some(thread) = thread {
        params["thread"] = serde_json::Value::String(thread.into());
    }

    Ok(params)
}

fn status(
    sessions: &Arc<Mutex<state::Store>>,
    id: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sessions.lock().map_err(|_| "Session lock is poisoned.")?.set_status(id, value)?;
    Ok(())
}

fn status_if_present(
    sessions: &Arc<Mutex<state::Store>>,
    id: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;

    if store.sessions.contains_key(id) {
        store.set_status(id, value)?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn commit_event(
    channel_process: &Live,
    sessions: &Arc<Mutex<state::Store>>,
    channel: &str,
    id: &str,
    offset: Option<i64>,
    current: &mut i64,
    gateway_sequence: Option<u64>,
    call: &mut u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sessions.lock().map_err(|_| "Session lock is poisoned.")?.commit(channel, id, offset)?;

    if let Some(next) = offset {
        *current = (*current).max(next);
    }

    if let Some(sequence) = gateway_sequence {
        let mut attempts = 0;

        loop {
            let response = channel_process
                .call(Request::call(*call, "ack", serde_json::json!({"sequence": sequence})))
                .await;

            *call += 1;

            match response {
                Ok(response) if response.error.is_none() && response.result.is_some() => break,

                Ok(response) => {
                    let message = response.error.map_or_else(
                        || "Channel acknowledgement was empty.".into(),
                        |error| {
                            format!("Channel acknowledgement failed: {}", sentence(error.message))
                        },
                    );

                    warn!(error = %diagnostic(&message), "Channel acknowledgement failed.");
                }

                Err(error) => warn!(
                    error = %diagnostic(sentence(error.to_string())),
                    "Channel acknowledgement transport failed."
                ),
            }

            attempts += 1;

            if attempts >= ACK_ATTEMPTS {
                return Err("Channel acknowledgement failed after recovery attempts.".into());
            }

            tokio::time::sleep(retry_delay(attempts)).await;
            channel_process.restart().await?;
            info!("Channel plugin restarted after an acknowledgement failure.");
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_callback(
    channel: &Live,
    channel_id: &str,
    event: &serde_json::Value,
    policy: &ChannelConfig,
    mode: ApprovalMode,
    approvals: &Arc<AsyncMutex<approval::Gate>>,
    sessions: &Arc<Mutex<state::Store>>,
    offset: &mut i64,
    call: &mut u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let id = event_id(&event["id"]).ok_or("Callback event has no ID.")?;
    let next_offset = event["id"].as_i64().map(|value| value.saturating_add(1));
    let gateway_sequence = event["gateway_sequence"].as_u64();
    let Some(chat) = event_id(&event["chat"]) else {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    };

    let Some(callback_id) = event["callback_id"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= META_LIMIT)
    else {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    };

    let thread = event_id(&event["thread"]);
    let token = event["data"].as_str().filter(|value| value.len() <= 64).unwrap_or_default();
    let mut authority = event.clone();

    if let Some(mention) = policy.mention.as_deref() {
        authority["text"] = serde_json::Value::String(mention.into());
    }

    let authorized = tools_allowed(policy, &authority, &chat, mode.enabled());
    let decision =
        approvals.lock().await.resolve(token, channel_id, &chat, thread.as_deref(), authorized);

    let message = match decision {
        Some(true) => "Approval accepted.",
        Some(false) => "The request was denied.",
        None => "This approval is invalid or expired.",
    };

    let callback = channel
        .call(Request::call(
            *call,
            "callback",
            serde_json::json!({"id": callback_id, "text": message}),
        ))
        .await;

    *call = (*call).saturating_add(1);

    if let Err(error) = callback {
        warn!(error = %diagnostic(sentence(error.to_string())), "Channel approval acknowledgement failed.");
    }

    commit_event(channel, sessions, channel_id, &id, next_offset, offset, gateway_sequence, call)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn queue_during_turn(
    channel: &Live,
    channel_id: &str,
    model: &str,
    event: &serde_json::Value,
    policy: &ChannelConfig,
    sessions: &Arc<Mutex<state::Store>>,
    offset: &mut i64,
    call: &mut u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(id) = event_id(&event["id"]) else {
        return Ok(());
    };

    let next_offset = event["id"].as_i64().map(|value| value.saturating_add(1));
    let gateway_sequence = event["gateway_sequence"].as_u64();

    if id.len() > META_LIMIT
        || event["text"].as_str().is_some_and(|value| value.len() > TEXT_LIMIT)
        || event["sender"].as_str().is_some_and(|value| value.len() > META_LIMIT)
        || event["roles"].as_array().is_some_and(|roles| {
            roles.len() > CONTENT_LIMIT
                || roles
                    .iter()
                    .any(|role| role.as_str().is_some_and(|value| value.len() > META_LIMIT))
        })
    {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    }

    if sessions.lock().map_err(|_| "Session lock is poisoned.")?.known(channel_id, &id) {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    }

    let Some(chat) = event["chat"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| event["chat"].as_i64().map(|value| value.to_string()))
    else {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    };

    let thread = event_id(&event["thread"]);

    if chat.len() > META_LIMIT
        || thread.as_ref().is_some_and(|value| value.len() > META_LIMIT)
        || !allowed(policy, event, &chat)
    {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    }

    let content = content(event);

    if content.is_empty() {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    }

    let session = session_id(channel_id, &chat, thread.as_deref());

    if !valid(&session) {
        return commit_event(
            channel,
            sessions,
            channel_id,
            &id,
            next_offset,
            offset,
            gateway_sequence,
            call,
        )
        .await;
    }

    let roles = event["roles"]
        .as_array()
        .into_iter()
        .flatten()
        .take(CONTENT_LIMIT)
        .filter_map(event_id)
        .collect::<Vec<_>>();

    let message = Message {
        id: id.clone(),
        session: session.clone(),
        role: Role::User,
        sender: event["sender"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| event["sender"].as_i64().map(|value| value.to_string())),
        content,
    };

    let mut rejected = false;
    {
        let mut store = sessions.lock().map_err(|_| "Session lock is poisoned.")?;

        if !store.sessions.contains_key(&session) && store.sessions.len() >= state::LIMIT {
            rejected = true;
        } else {
            if !store.sessions.contains_key(&session)
                && event["private"] != true
                && policy.worktree
                && let Err(error) = isolate(&session)
            {
                warn!(
                    event = %id,
                    error = %diagnostic(sentence(error.to_string())),
                    "Group workspace isolation failed."
                );
                rejected = true;
            }

            if !rejected {
                store.ensure(&session, model)?;
                store.route(
                    &session,
                    channel_id,
                    &chat,
                    thread.as_deref(),
                    event["private"] == true,
                )?;

                if let Err(error) = store.queue_with_roles(&session, message, roles) {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        rejected = true;
                    } else {
                        return Err(error.into());
                    }
                }
            }
        }
    }

    if rejected {
        warn!(event = %id, "Message was rejected because the session queue is unavailable.");
    }

    commit_event(channel, sessions, channel_id, &id, next_offset, offset, gateway_sequence, call)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn wait_approval(
    channel: &Live,
    channel_id: &str,
    model: &str,
    chat: &str,
    thread: Option<&str>,
    text: &str,
    approve: &str,
    deny: &str,
    policy: &ChannelConfig,
    mode: ApprovalMode,
    approvals: &Arc<AsyncMutex<approval::Gate>>,
    sessions: &Arc<Mutex<state::Store>>,
    offset: &mut i64,
    call: &mut u64,
    deadline: tokio::time::Instant,
    cancel: &Cancellation,
    stop: &Stop,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let params = approval_params(channel_id, chat, thread, text, approve, deny)?;
    let sent = channel.call(Request::call(*call, "approval", params)).await;
    *call = (*call).saturating_add(1);

    match sent {
        Ok(response) if response.error.is_none() && response.result.is_some() => {}

        Ok(response) => {
            approvals.lock().await.cancel(approve);
            return Err(response
                .error
                .map_or_else(
                    || "The channel rejected the approval request.".into(),
                    |error| error.message,
                )
                .into());
        }

        Err(error) => {
            approvals.lock().await.cancel(approve);
            return Err(error.into());
        }
    }

    loop {
        if !approvals.lock().await.has_pending() {
            return Ok(());
        }

        let poll =
            channel.call(Request::call(*call, "poll", serde_json::json!({"offset": *offset})));

        let result = tokio::select! {
            biased;

            _ = tokio::signal::ctrl_c() => {
                approvals.lock().await.cancel(approve);
                return Err("The turn was interrupted.".into());
            }

            _ = stop.notified() => {
                approvals.lock().await.cancel(approve);
                return Err("The turn was interrupted.".into());
            }

            _ = cancel.cancelled() => {
                approvals.lock().await.cancel(approve);
                return Err("The turn was cancelled.".into());
            }

            result = tokio::time::timeout_at(deadline, poll) => result,
        };

        *call = (*call).saturating_add(1);
        let response = match result {
            Ok(Ok(response)) => response,

            Ok(Err(error)) => {
                approvals.lock().await.cancel(approve);
                return Err(error.into());
            }

            Err(_) => {
                approvals.lock().await.cancel(approve);
                return Err("The approval request expired.".into());
            }
        };

        let Some(value) = response.result else {
            approvals.lock().await.cancel(approve);
            return Err(response
                .error
                .map_or_else(
                    || "The channel returned an empty response.".into(),
                    |error| error.message,
                )
                .into());
        };

        let events = value["events"].as_array().cloned().unwrap_or_default();

        for event in events {
            if event["kind"] == "callback" {
                handle_callback(
                    channel, channel_id, &event, policy, mode, approvals, sessions, offset, call,
                )
                .await?;
            } else {
                queue_during_turn(
                    channel, channel_id, model, &event, policy, sessions, offset, call,
                )
                .await?;
            }
        }
    }
}

fn tools() -> Vec<ToolSpec> {
    [
        ("read", "Read a text file.", &["path"] as &[&str]),
        ("write", "Write a text file with approval.", &["path", "text"]),
        ("list", "List workspace entries.", &["path"]),
        ("search", "Search workspace text.", &["path", "text"]),
        ("patch", "Apply an approved Git patch.", &["text"]),
        ("git", "Inspect or manage approved Git worktrees.", &["args"]),
        ("shell", "Run an approved shell command.", &["command"]),
    ]
    .into_iter()
    .map(|(name, description, required)| {
        let properties = required
            .iter()
            .map(|key| {
                let value = match *key {
                    "path" | "text" | "command" => serde_json::json!({"type": "string"}),
                    "args" => serde_json::json!({"type": "array", "items": {"type": "string"}}),
                    _ => serde_json::json!({}),
                };

                ((*key).to_owned(), value)
            })
            .collect::<serde_json::Map<_, _>>();

        ToolSpec {
            name: name.into(),
            description: Some(description.into()),
            schema: serde_json::json!({
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false,
            }),
        }
    })
    .collect()
}

enum StreamNotice {
    Text(String),
    Tool(String),
    Approval {
        chat: String,
        thread: Option<String>,
        text: String,
        approve: String,
        deny: String,
        deadline: tokio::time::Instant,
    },
}

struct StreamOutput {
    text: String,
    display: String,
    message_id: Option<String>,
    dirty: bool,
    disabled: bool,
    updated: tokio::time::Instant,
}

impl StreamOutput {
    fn new() -> Self {
        Self {
            text: String::new(),
            display: String::new(),
            message_id: None,
            dirty: false,
            disabled: false,
            updated: tokio::time::Instant::now(),
        }
    }

    fn apply(&mut self, notice: StreamNotice) {
        match notice {
            StreamNotice::Text(text) => {
                self.text = clip(format!("{}{}", self.text, text), TEXT_LIMIT);
                self.display.clone_from(&self.text);
            }

            StreamNotice::Tool(name) => {
                self.text.clear();
                self.display = format!("Working with {}…", clip(name, META_LIMIT));
            }

            StreamNotice::Approval { .. } => {}
        }

        self.dirty = !self.display.is_empty();
    }

    fn ready(&self) -> bool {
        self.dirty && self.updated.elapsed() >= std::time::Duration::from_millis(500)
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_stream(
    output: &mut StreamOutput,
    channel: &Live,
    channel_id: &str,
    sessions: &Arc<Mutex<state::Store>>,
    session: &str,
    delivery_id: &str,
    chat: &str,
    thread: Option<&str>,
    call: &mut u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !output.dirty || output.disabled {
        return Ok(());
    }

    if !stream_fits(channel_id, &output.display) {
        output.dirty = false;
        return Ok(());
    }

    let request = if let Some(message_id) = output.message_id.as_deref() {
        sessions
            .lock()
            .map_err(|_| "Session lock is poisoned.")?
            .stream_sending(delivery_id, output.display.clone())?;
        Request::call(
            *call,
            "edit",
            edit_params(channel_id, delivery_id, chat, message_id, &output.display, thread)?,
        )
    } else {
        sessions.lock().map_err(|_| "Session lock is poisoned.")?.start_stream(
            session,
            delivery_id,
            channel_id,
            chat,
            thread.map(str::to_owned),
            output.display.clone(),
        )?;
        Request::call(
            *call,
            "send",
            send_params(channel_id, delivery_id, chat, &output.display, thread)?,
        )
    };

    let response = channel.call(request).await;
    *call = call.saturating_add(1);
    let response = match response {
        Ok(response) if response.error.is_none() => response,

        Ok(response) => {
            let message = response.error.map_or_else(
                || "Channel rejected the stream update.".into(),
                |error| error.message,
            );
            sessions
                .lock()
                .map_err(|_| "Session lock is poisoned.")?
                .uncertain(delivery_id, sentence(message.clone()))?;
            return Err(message.into());
        }

        Err(error) => {
            let message = sentence(error.to_string());
            sessions
                .lock()
                .map_err(|_| "Session lock is poisoned.")?
                .uncertain(delivery_id, message.clone())?;
            return Err(message.into());
        }
    };

    if output.message_id.is_none() {
        let message_id =
            response.result.as_ref().and_then(|result| channel_message_id(channel_id, result));

        let Some(message_id) = message_id else {
            let message = "Channel returned no stream message ID.";
            sessions
                .lock()
                .map_err(|_| "Session lock is poisoned.")?
                .uncertain(delivery_id, message)?;
            return Err(message.into());
        };

        sessions
            .lock()
            .map_err(|_| "Session lock is poisoned.")?
            .stream_started(delivery_id, message_id.clone())?;
        output.message_id = Some(message_id);
    } else {
        sessions.lock().map_err(|_| "Session lock is poisoned.")?.stream_updated(delivery_id)?;
    }

    output.dirty = false;
    output.updated = tokio::time::Instant::now();
    Ok(())
}

fn stream_fits(channel: &str, text: &str) -> bool {
    match channel {
        "telegram" => text.encode_utf16().count() <= 4_096,
        "discord" => text.chars().count() <= 2_000,
        _ => false,
    }
}

fn channel_message_id(channel: &str, result: &serde_json::Value) -> Option<String> {
    match channel {
        "telegram" => result["message_id"].as_i64().map(|id| id.to_string()),
        "discord" => result["id"].as_str().map(str::to_owned),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn answer(
    provider: &Live,
    plugins: &Plugins,
    model: &str,
    mut messages: Vec<Message>,
    session: &str,
    channel: &str,
    chat: &str,
    thread: Option<&str>,
    sessions: &Arc<Mutex<state::Store>>,
    workspace: Option<&Path>,
    media_root: &Path,
    tools_enabled: bool,
    approval_mode: ApprovalMode,
    approvals: Arc<AsyncMutex<approval::Gate>>,
    cancel: &Cancellation,
    stop: &Stop,
    deadline: tokio::time::Instant,
    call: &mut u64,
    failed_tool: &mut Option<String>,
    notices: tokio::sync::mpsc::Sender<StreamNotice>,
) -> Result<ModelReply, Box<dyn std::error::Error + Send + Sync>> {
    if messages.iter().any(|message| {
        message.content.iter().any(|content| matches!(content, Content::Image { .. }))
    }) && !provider.supports(Capability::Vision)
    {
        return Err("The selected model provider does not support image inputs.".into());
    }

    prepare_images(&mut messages, media_root);
    let mut tokens = 0_u64;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool_plugin = plugins.find(Capability::Tool).await;
    let specs = if tool_plugin.is_some() {
        tools_enabled.then(tools).unwrap_or_default()
    } else {
        Vec::new()
    };

    for step in 0..=TOOL_STEPS {
        let request = model_request(*call, model, &messages, &specs, workspace)?;
        let mut notes = Vec::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
        let host_plugins = plugins.clone();
        let host_sessions = Arc::clone(sessions);
        let host_session = session.to_owned();
        let host_workspace = workspace.map(Path::to_path_buf);
        let host_calls = Arc::clone(&calls);
        let host_tools_enabled = tools_enabled;
        let host_approval_mode = approval_mode;
        let host_approvals = Arc::clone(&approvals);
        let host_channel = channel.to_owned();
        let host_chat = chat.to_owned();
        let host_thread = thread.map(str::to_owned);
        let host_notices = notices.clone();
        let request_stream = provider.call_full_async(
            request,
            move |note| {
                let sender = sender.clone();
                async move {
                    sender.send(note).await.map_err(|_| {
                        crabbot_core::Error::Protocol("Model event receiver is unavailable.".into())
                    })
                }
            },
            move |request| {
                let plugins = host_plugins.clone();
                let sessions = Arc::clone(&host_sessions);
                let session = host_session.clone();
                let workspace = host_workspace.clone();
                let calls = Arc::clone(&host_calls);
                let approvals = Arc::clone(&host_approvals);
                let channel = host_channel.clone();
                let chat = host_chat.clone();
                let thread = host_thread.clone();
                let notices = host_notices.clone();
                async move {
                    host_tool(
                        request,
                        &plugins,
                        &sessions,
                        &session,
                        workspace.as_deref(),
                        host_tools_enabled,
                        host_approval_mode,
                        approvals,
                        &channel,
                        &chat,
                        thread.as_deref(),
                        notices,
                        calls,
                        deadline,
                    )
                    .await
                }
            },
        );

        tokio::pin!(request_stream);
        let mut receiver_closed = false;
        let reply = loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => return Err("The turn was interrupted.".into()),
                _ = stop.notified() => return Err("The turn was interrupted.".into()),
                _ = cancel.cancelled() => return Err("The turn was cancelled.".into()),

                result = tokio::time::timeout_at(deadline, &mut request_stream) => {
                    break result.map_err(|_| "The turn exceeded its time limit.")??;
                }

                note = receiver.recv(), if !receiver_closed => {
                    match note {
                        Some(note) => {
                            if let Some(event) = stream_event(note.clone()) {
                                match event {
                                    Event::Text { text } | Event::Done { text } => {
                                        notices.send(StreamNotice::Text(text)).await.map_err(|_| {
                                            "The channel stream receiver is unavailable."
                                        })?;
                                    }

                                    Event::Tool { name, .. } => {
                                        notices.send(StreamNotice::Tool(name)).await.map_err(|_| {
                                            "The channel stream receiver is unavailable."
                                        })?;
                                    }

                                    Event::Error { .. } => {}
                                }
                            }

                            notes.push(note);
                        }

                        None => receiver_closed = true,
                    }
                }
            }
        };

        while let Some(note) = receiver.recv().await {
            notes.push(note);
        }

        *call += 1;

        if sessions
            .lock()
            .map_err(|_| "Session lock is poisoned.")?
            .sessions
            .get(session)
            .is_some_and(|value| value.status == "cancelled")
        {
            return Err("The turn was cancelled.".into());
        }

        let Some(value) = reply.result else {
            if let Some(error) = reply.error {
                return Err(format!("Model request failed: {}", sentence(error.message)).into());
            }

            return Err("Model returned an empty response.".into());
        };

        let mut parsed: ModelReply = serde_json::from_value(value)?;
        merge_stream(&mut parsed, notes);
        parsed.text = clip(parsed.text, TEXT_LIMIT);
        tokens = tokens.saturating_add(parsed.input.unwrap_or_default());
        tokens = tokens.saturating_add(parsed.output.unwrap_or_default());

        if tokens > TOKEN_LIMIT {
            return Err("The turn exceeded its token limit.".into());
        }

        if parsed.events.is_empty() {
            return Ok(parsed);
        }

        if step == TOOL_STEPS {
            return Err("The turn exceeded the tool step limit.".into());
        }

        let assistant_text = assistant(&parsed);

        if !assistant_text.is_empty() {
            let assistant = Message {
                id: format!("assistant-tool-{call}"),
                session: session.into(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: assistant_text }],
            };

            messages.push(assistant.clone());
            sessions.lock().map_err(|_| "Session lock is poisoned.")?.push(session, assistant)?;
            status(sessions, session, "working")?;
        }

        let mut called = false;

        for event in &parsed.events {
            let Event::Tool { name, args, .. } = event else {
                if let Event::Error { message } = event {
                    return Err(message.clone().into());
                }

                continue;
            };

            if calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= TOOL_CALLS {
                return Err("The turn exceeded its tool-call limit.".into());
            }

            if !tools_enabled {
                return Err("Tool access is disabled for this channel.".into());
            }

            notices
                .send(StreamNotice::Tool(name.clone()))
                .await
                .map_err(|_| "The channel stream receiver is unavailable.")?;

            if cancelled(sessions, session)? {
                return Err("The turn was cancelled.".into());
            }

            let output = tokio::time::timeout_at(
                deadline,
                execute_tool(
                    plugins,
                    *call,
                    name,
                    args.clone(),
                    approval_mode,
                    Arc::clone(&approvals),
                    channel,
                    chat,
                    thread,
                    session,
                    sessions,
                    workspace,
                    notices.clone(),
                    deadline,
                    failed_tool,
                ),
            );

            let output = tokio::select! {
                biased;
                _ = stop.notified() => return Err("The turn was interrupted.".into()),
                _ = cancel.cancelled() => return Err("The turn was cancelled.".into()),
                result = output => match result {
                    Ok(result) => result?,

                    Err(_) => {
                        if failed_tool.is_none()
                            && let Some((id, _)) = plugins.find(Capability::Tool).await
                        {
                            *failed_tool = Some(id);
                        }

                        return Err("The turn exceeded its time limit.".into());
                    }
                },
            };

            *call += 1;
            let message = Message {
                id: format!("tool-{call}"),
                session: session.into(),
                role: Role::Tool,
                sender: Some(name.clone()),
                content: vec![Content::Text { text: output }],
            };

            messages.push(message.clone());
            sessions.lock().map_err(|_| "Session lock is poisoned.")?.push(session, message)?;
            status(sessions, session, "working")?;
            called = true;
        }

        if !called {
            return Ok(parsed);
        }
    }

    Err("The turn exceeded the tool step limit.".into())
}

fn model_request(
    call: u64,
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
    workspace: Option<&Path>,
) -> Result<Request, Box<dyn std::error::Error + Send + Sync>> {
    let mut history = messages.to_vec();

    loop {
        let request = Request::call(
            call,
            "generate",
            serde_json::to_value(ModelRequest {
                model: model.into(),
                workspace: workspace.map(|path| path.to_string_lossy().into_owned()),
                messages: history.clone(),
                stream: true,
                tools: tools.to_vec(),
            })?,
        );

        if serde_json::to_vec(&request)?.len().saturating_add(1) <= crabbot_core::jsonl::MAX {
            return Ok(request);
        }

        let required = history
            .iter()
            .rposition(|message| message.role == Role::User)
            .or_else(|| history.iter().rposition(|message| message.role != Role::System));

        let Some(index) = history
            .iter()
            .enumerate()
            .find(|(index, message)| message.role != Role::System && Some(*index) != required)
            .map(|(index, _)| index)
        else {
            return Err("Model context exceeds the protocol frame limit.".into());
        };

        history.remove(index);
    }
}

fn assistant(reply: &ModelReply) -> String {
    let mut text = reply.text.clone();

    for event in &reply.events {
        match event {
            Event::Text { text: delta } | Event::Done { text: delta } => {
                text.push_str(&clip(delta.clone(), TEXT_LIMIT.saturating_sub(text.len())));
            }

            Event::Tool { name, args } => {
                if !text.is_empty() {
                    text.push('\n');
                }

                text.push_str("[Tool call ");
                text.push_str(name);
                text.push_str("]: ");
                text.push_str(&clip(args.to_string(), META_LIMIT));
            }

            Event::Error { .. } => {}
        }
    }

    clip(text, TEXT_LIMIT)
}

fn stream_event(note: Request) -> Option<Event> {
    let Request::Note { method, params, .. } = note else {
        return None;
    };

    if method != "event" {
        return None;
    }

    params
        .get("event")
        .cloned()
        .or(Some(params))
        .and_then(|value| serde_json::from_value(value).ok())
}

fn merge_stream(reply: &mut ModelReply, notes: Vec<Request>) {
    let mut text = String::new();

    for event in notes.into_iter().filter_map(stream_event) {
        match event {
            Event::Text { text: part } | Event::Done { text: part } => text.push_str(&part),
            event => reply.events.push(event),
        }
    }

    if reply.text.is_empty() {
        reply.text = text;
    }
}

fn mutating(name: &str, args: &serde_json::Value) -> bool {
    match name {
        "write" | "patch" | "shell" => true,
        "git" => args["args"].as_array().is_some_and(|values| {
            values.len() >= 2
                && values[0].as_str() == Some("worktree")
                && matches!(values[1].as_str(), Some("add" | "remove"))
        }),

        _ => false,
    }
}

async fn tool(
    plugins: &Plugins,
    id: u64,
    name: &str,
    mut args: serde_json::Value,
    approve: bool,
    workspace: Option<&Path>,
    failed_tool: &mut Option<String>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let (id_plugin, plugin) =
        plugins.find(Capability::Tool).await.ok_or("The tools plugin is not installed.")?;

    if let serde_json::Value::Object(values) = &mut args {
        values.insert("approve".into(), serde_json::Value::Bool(approve));

        if let Some(workspace) = workspace {
            values.insert(
                "workspace".into(),
                serde_json::Value::String(workspace.display().to_string()),
            );
        }
    } else {
        return Err("Tool arguments must be an object.".into());
    }

    let response = match plugin.call(Request::call(id, name, args)).await {
        Ok(response) => response,

        Err(error) => {
            *failed_tool = Some(id_plugin);
            return Err(error.into());
        }
    };

    if let Some(error) = response.error {
        return Err(error.message.into());
    }

    let value = response.result.ok_or("The tool returned no result.")?;
    Ok(value["text"].as_str().map_or_else(
        || clip(value.to_string(), TEXT_LIMIT),
        |text| clip(text.to_owned(), TEXT_LIMIT),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_tool(
    plugins: &Plugins,
    id: u64,
    name: &str,
    args: serde_json::Value,
    approval_mode: ApprovalMode,
    approvals: Arc<AsyncMutex<approval::Gate>>,
    channel: &str,
    chat: &str,
    thread: Option<&str>,
    session: &str,
    sessions: &Arc<Mutex<state::Store>>,
    workspace: Option<&Path>,
    notices: tokio::sync::mpsc::Sender<StreamNotice>,
    deadline: tokio::time::Instant,
    failed_tool: &mut Option<String>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let changes = mutating(name, &args);
    let approve = match (changes, approval_mode) {
        (false, _) => false,

        (true, ApprovalMode::Off) => {
            return Err("Approval policy blocks mutating tools.".into());
        }

        (true, ApprovalMode::Auto) => true,

        (true, ApprovalMode::Prompt) => {
            let target = approval::Target {
                channel: channel.into(),
                chat: chat.into(),
                thread: thread.map(str::to_owned),
                session: session.into(),
                tool: name.into(),
                args: args.clone(),
            };

            let challenge = approvals.lock().await.issue(target)?;
            let text = approval_text(name, &args);

            if notices
                .send(StreamNotice::Approval {
                    chat: chat.into(),
                    thread: thread.map(str::to_owned),
                    text,
                    approve: challenge.approve.clone(),
                    deny: challenge.deny,
                    deadline,
                })
                .await
                .is_err()
            {
                approvals.lock().await.cancel(&challenge.approve);
                return Err("The channel approval handler is unavailable.".into());
            }

            match challenge.answer.await {
                Ok(true) => true,
                Ok(false) => return Ok("The request was denied. No changes were made.".into()),
                Err(_) => return Err("The approval request is no longer available.".into()),
            }
        }
    };

    if changes && approve {
        sessions.lock().map_err(|_| "Session lock is poisoned.")?.phase(session, "unsafe")?;
    }

    tool(plugins, id, name, args, approve, workspace, failed_tool).await
}

fn approval_text(name: &str, args: &serde_json::Value) -> String {
    let details = match name {
        "write" => format!("write to {}", args["path"].as_str().unwrap_or("a workspace path")),

        "patch" => {
            format!("apply a patch containing {} bytes", args["text"].as_str().map_or(0, str::len))
        }

        "shell" => format!(
            "run this shell command: {}",
            clip(args["command"].as_str().unwrap_or("unknown").into(), 1_000)
        ),
        "git" => format!("run this Git operation: {}", clip(args["args"].to_string(), 1_000)),
        _ => format!("run the {name} tool"),
    };

    format!("Crabbot requests approval to {details}.")
}

fn approval_params(
    channel: &str,
    chat: &str,
    thread: Option<&str>,
    text: &str,
    approve: &str,
    deny: &str,
) -> crabbot_core::Result<serde_json::Value> {
    let mut params = match channel {
        "telegram" => serde_json::json!({
            "chat": chat
                .parse::<i64>()
                .map_err(|_| crabbot_core::Error::Denied("Telegram chat ID was invalid.".into()))?,
            "text": text,
            "approve": approve,
            "deny": deny,
        }),

        "discord" => serde_json::json!({
            "channel": chat,
            "text": text,
            "approve": approve,
            "deny": deny,
        }),

        _ => {
            return Err(crabbot_core::Error::Denied(
                "The channel does not support inline approvals.".into(),
            ));
        }
    };

    if channel == "telegram"
        && let Some(thread) = thread
    {
        params["thread"] = serde_json::Value::String(thread.into());
    }

    Ok(params)
}

#[allow(clippy::too_many_arguments)]
async fn host_tool(
    request: Request,
    plugins: &Plugins,
    sessions: &Arc<Mutex<state::Store>>,
    session: &str,
    workspace: Option<&Path>,
    tools_enabled: bool,
    approval_mode: ApprovalMode,
    approvals: Arc<AsyncMutex<approval::Gate>>,
    channel: &str,
    chat: &str,
    thread: Option<&str>,
    notices: tokio::sync::mpsc::Sender<StreamNotice>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    deadline: tokio::time::Instant,
) -> crabbot_core::Result<Response> {
    let Request::Call { id, method, params, .. } = request else {
        return Err(crabbot_core::Error::Protocol("Host tool requests require an ID.".into()));
    };

    if method != "host/tool" {
        return Ok(Response::fail(id, -32601, "method not found"));
    }

    if !tools_enabled {
        return Ok(Response::fail(id, -32000, "Tool access is disabled for this channel."));
    }

    if calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= TOOL_CALLS {
        return Ok(Response::fail(id, -32000, "The turn exceeded its tool-call limit."));
    }

    if cancelled(sessions, session).unwrap_or(true) {
        return Ok(Response::fail(id, -32000, "The turn was cancelled."));
    }

    let Some(name) = params["name"].as_str() else {
        return Ok(Response::fail(id, -32602, "A tool name is required."));
    };

    let args = params.get("args").cloned().unwrap_or(serde_json::Value::Null);

    let mut failed_tool = None;
    let output = match tokio::time::timeout_at(
        deadline,
        execute_tool(
            plugins,
            id,
            name,
            args,
            approval_mode,
            approvals,
            channel,
            chat,
            thread,
            session,
            sessions,
            workspace,
            notices,
            deadline,
            &mut failed_tool,
        ),
    )
    .await
    {
        Ok(Ok(output)) => output,

        Ok(Err(error)) => {
            if let Some(failed) = failed_tool {
                restart_tool(plugins, &failed).await;
            }

            return Ok(Response::fail(id, -32000, sentence(error.to_string())));
        }

        Err(_) => {
            if let Some((failed, _)) = plugins.find(Capability::Tool).await {
                restart_tool(plugins, &failed).await;
            }

            return Ok(Response::fail(id, -32000, "The turn exceeded its time limit."));
        }
    };

    Ok(Response::ok(id, serde_json::json!({"output": output})))
}

async fn restart_tool(plugins: &Plugins, id: &str) {
    let Some(process) = plugins.get(id).await else {
        return;
    };

    match process.restart().await {
        Ok(()) => info!(plugin = id, "Plugin restarted after a tool transport failure."),
        Err(restart) => warn!(
            plugin = id,
            error = %diagnostic(sentence(restart.to_string())),
            "Plugin could not restart after a tool transport failure."
        ),
    }
}

fn installed_at(root: &Path) -> Vec<String> {
    let root = root.join("plugins");
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };

    let mut ids = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .collect::<Vec<_>>();

    ids.sort();
    ids
}

fn doctor(fix: bool, json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    doctor_at(&home(), fix, json)
}

fn doctor_healthy(
    config_present: bool,
    config_valid: Option<bool>,
    plugins_directory_present: bool,
) -> bool {
    config_present && config_valid == Some(true) && plugins_directory_present
}

fn doctor_at(
    root: &Path,
    fix: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = root.join("config.toml");
    let plugins_path = root.join("plugins");
    let config_was_present = config_path.is_file();
    let plugins_directory_was_present = plugins_path.is_dir();
    let mut repairs = Vec::new();

    if fix {
        init_at(root)?;

        if !config_was_present && config_path.is_file() {
            repairs.push("created_config");
        }

        if !plugins_directory_was_present && plugins_path.is_dir() {
            repairs.push("created_plugins_directory");
        }
    }

    let config_present = root.join("config.toml").is_file();
    let plugins_directory_present = root.join("plugins").is_dir();
    let mut config_valid = None;

    if config_present {
        let config: Config = toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?;
        config.validate().map_err(|error| format!("Config is invalid: {error}"))?;
        config_valid = Some(true);
    }

    let lock = load_lock_at(root)?;

    for (id, entry) in &lock.plugins {
        let binary =
            binary_at(id, root).ok_or_else(|| format!("Plugin binary is missing: {id}."))?;

        let manifest = root.join("plugins").join(id).join("crabbot-plugin.toml");

        if digest(&manifest, &binary)? != entry.hash {
            return Err(format!("Plugin integrity check failed: {id}.").into());
        }
    }

    let healthy = doctor_healthy(config_present, config_valid, plugins_directory_present);
    let health = if healthy {
        serde_json::json!({"status": "healthy"})
    } else {
        serde_json::json!({
            "status": "unhealthy",
            "suggestion": "crabbot doctor --fix"
        })
    };

    if json {
        if fix {
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({"repairs": repairs}))?);
        } else {
            let value = serde_json::json!({
                "config": {"present": config_present, "valid": config_valid},
                "health": health,
                "home": root,
                "plugin_integrity": "valid",
                "plugins": {
                    "directory_present": plugins_directory_present,
                    "installed": lock.plugins.keys().collect::<Vec<_>>(),
                },
                "protocol": {
                    "major": Protocol::CURRENT.major,
                    "minor": Protocol::CURRENT.minor,
                },
            });

            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    } else if fix {
        if repairs.is_empty() {
            println!("No repairs were needed.");
        } else {
            let descriptions = repairs
                .iter()
                .map(|repair| match *repair {
                    "created_config" => "default config",
                    "created_plugins_directory" => "plugins directory",
                    _ => repair,
                })
                .collect::<Vec<_>>();
            println!("Repairs applied: {}.", descriptions.join(", "));
        }
    } else {
        println!("Home: {}.", root.display());
        println!("Config present: {}.", config_present);
        println!("Plugins directory present: {}.", plugins_directory_present);
        println!("Protocol: {}.{}.", Protocol::CURRENT.major, Protocol::CURRENT.minor);

        if config_valid.is_some() {
            println!("Config is valid.");
        }

        println!("Plugin integrity is valid.");

        if healthy {
            println!("Crabbot is healthy.");
        } else {
            println!(
                "Crabbot is unhealthy. Run crabbot doctor --fix to repair missing local state."
            );
        }
    }

    Ok(())
}

fn service(
    command: ServiceCommand,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = service_path();

    if json { service_at_json(&path, command, true) } else { service_at(&path, command) }
}

const SERVICE_VARIABLES: &[&str] = &[
    "CRABBOT_ROOT",
    "CRABBOT_KEYRING",
    "CRABBOT_CHANNEL",
    "CRABBOT_MODEL_PLUGIN",
    "CRABBOT_MODEL",
    "CRABBOT_MEMORY",
    "CRABBOT_TIMER",
    "CRABBOT_DB",
    "CRABBOT_MEDIA",
    "CRABBOT_WHISPER",
    "CRABBOT_WHISPER_COMMAND",
    "CRABBOT_CODEX_BASE_URL",
    "CRABBOT_OPENROUTER_MODEL",
    "CRABBOT_OPENROUTER_BASE_URL",
    "CRABBOT_OPENROUTER_REFERER",
    "CRABBOT_OPENROUTER_TITLE",
    "CRABBOT_CLAUDE_BASE_URL",
    "CRABBOT_GEMINI_BASE_URL",
    "CRABBOT_PI_COMMAND",
    "CRABBOT_SIGNAL_COMMAND",
    "CRABBOT_SLACK_CHANNELS",
    "CRABBOT_OLLAMA_HOST",
    "CRABBOT_DISCORD_GATEWAY_URL",
    "CRABBOT_DISCORD_INTENTS",
    "CRABBOT_WHATSAPP_PHONE",
    "CRABBOT_WHATSAPP_LISTEN",
    "CRABBOT_WHATSAPP_GRAPH_URL",
    "CRABBOT_CODEX_HOME",
    "CRABBOT_CODEX_BINARY",
    "CRABBOT_SANDBOX_RUNTIME",
    "CRABBOT_SANDBOX_IMAGE",
];

const SERVICE_SECRETS: &[&str] = &[
    "CRABBOT_TELEGRAM_TOKEN",
    "CRABBOT_DISCORD_TOKEN",
    "CRABBOT_CODEX_KEY",
    "CRABBOT_OPENROUTER_KEY",
    "CRABBOT_CLAUDE_KEY",
    "CRABBOT_GEMINI_KEY",
    "CRABBOT_OLLAMA_API_KEY",
    "CRABBOT_WHATSAPP_TOKEN",
    "CRABBOT_WHATSAPP_APP_SECRET",
    "CRABBOT_WHATSAPP_VERIFY",
    "CRABBOT_SIGNAL_ACCOUNT",
    "CRABBOT_SLACK_BOT_TOKEN",
    "CRABBOT_SLACK_APP_TOKEN",
];

fn service_environment(
    path: &Path,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
    let environment = std::env::vars().collect::<BTreeMap<_, _>>();
    service_environment_from(path, home(), &environment)
}

fn service_environment_from(
    path: &Path,
    root: PathBuf,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut values = vec![("CRABBOT_HOME".into(), service_path_value(root.clone())?)];

    for name in SERVICE_VARIABLES {
        if let Some(value) = environment.get(*name) {
            let value = PathBuf::from(value);
            let value = if matches!(
                *name,
                "CRABBOT_ROOT"
                    | "CRABBOT_MEMORY"
                    | "CRABBOT_TIMER"
                    | "CRABBOT_DB"
                    | "CRABBOT_CODEX_HOME"
            ) {
                service_path_value(value)?
            } else {
                value.to_string_lossy().into_owned()
            };

            values.push(((*name).into(), value));
        }
    }

    let configured = environment.get("CRABBOT_CREDENTIALS").map(PathBuf::from);
    let mut credentials = serde_json::Map::new();
    let mut direct = false;

    if let Some(path) = configured.as_deref()
        && path.exists()
    {
        crabbot_file::private(path).map_err(|error| {
            std::io::Error::new(error.kind(), format!("Credential file is unavailable: {error}."))
        })?;

        let value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
        let object = value.as_object().ok_or("Credential file must contain a JSON object.")?;

        for (name, value) in object {
            if let Some(value) = value.as_str() {
                credentials.insert(name.clone(), serde_json::Value::String(value.into()));
            }
        }
    }

    let mut names = SERVICE_SECRETS.iter().map(|name| (*name).to_owned()).collect::<Vec<_>>();

    if let Ok(entries) = std::fs::read_dir(root.join("plugins")) {
        for entry in entries.flatten() {
            if let Some(manifest) = read_manifest(&entry.path()) {
                for name in manifest.secrets {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
        }
    }

    for name in names {
        if let Some(value) = environment.get(&name)
            && !value.trim().is_empty()
        {
            direct = true;
            credentials.insert(name, serde_json::Value::String(value.clone()));
        }
    }

    if direct {
        let path = service_credentials_path(path);
        secure(&path, serde_json::to_vec_pretty(&credentials)?)?;
        values.push(("CRABBOT_CREDENTIALS".into(), service_path_value(path)?));
    } else if let Some(path) = configured {
        values.push(("CRABBOT_CREDENTIALS".into(), service_path_value(path)?));
    }

    Ok(values)
}

fn service_path_value(path: PathBuf) -> std::io::Result<String> {
    if path.is_absolute() {
        return Ok(path.display().to_string());
    }

    Ok(std::env::current_dir()?.join(path).display().to_string())
}

fn service_credentials_path(path: &Path) -> PathBuf {
    let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("crabbot.service");
    path.with_file_name(format!("{name}.credentials.json"))
}

fn service_at(
    path: &Path,
    command: ServiceCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_at_with(path, command, service_action)
}

fn service_at_json(
    path: &Path,
    command: ServiceCommand,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_at_with_mode(path, command, |path, start| service_action_with(path, start, json), json)
}

fn service_at_with(
    path: &Path,
    command: ServiceCommand,
    mut action: impl FnMut(&Path, bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_at_with_mode(path, command, &mut action, false)
}

fn service_at_with_mode(
    path: &Path,
    command: ServiceCommand,
    mut action: impl FnMut(&Path, bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        ServiceCommand::Install(args) => {
            match std::fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err("Service definition cannot be a symbolic link.".into());
                }

                Ok(_) if !args.force => {
                    return Err(format!(
                        "Service definition already exists at {}; use --force to replace it.",
                        path.display()
                    )
                    .into());
                }

                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }

            let executable = daemon_executable()?;
            let environment = service_environment(path)?;

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let definition = service_text(&executable, &environment);
            #[cfg(target_os = "windows")]
            {
                windows_service_install(&executable, &environment, args.force, path, &definition)?;
            }

            #[cfg(not(target_os = "windows"))]
            secure(path, definition.as_bytes())?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "install",
                        "path": path,
                        "status": "installed",
                    }))?
                );
            } else {
                println!("Wrote the service definition to {}.", path.display());
                println!("Run `crabbot service start` to activate it.");
            }
        }

        ServiceCommand::Remove(args) => {
            if !args.yes {
                return Err("Removing the service requires --yes.".into());
            }

            let installed = path.exists();

            if installed {
                action(path, false)?;
            }

            #[cfg(target_os = "windows")]
            if installed {
                windows_service_remove()?;
            }

            if installed {
                std::fs::remove_file(path)?;
            }

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "remove",
                        "path": path,
                        "status": if installed { "removed" } else { "not installed" },
                    }))?
                );
            } else if installed {
                println!("Removed the service definition from {}.", path.display());
            } else {
                println!("No service definition was found.");
            }
        }

        ServiceCommand::Status => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "path": path,
                        "status": if path.is_file() { "installed" } else { "not installed" },
                    }))?
                );
            } else {
                println!(
                    "Service definition: {}.",
                    if path.is_file() {
                        path.display().to_string()
                    } else {
                        "not installed".into()
                    }
                );
            }
        }

        ServiceCommand::Start => action(path, true)?,
        ServiceCommand::Stop => action(path, false)?,
    }

    Ok(())
}

fn daemon_executable() -> std::io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    let name = if cfg!(windows) { "crabbot-daemon.exe" } else { "crabbot-daemon" };

    let sibling = current.parent().unwrap_or(Path::new(".")).join(name);

    if sibling.is_file() {
        return Ok(sibling);
    }

    #[cfg(test)]
    return Ok(current);

    #[cfg(not(test))]
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "The crabbot-daemon executable was not found next to crabbot.",
    ))
}

#[cfg(target_os = "windows")]
struct WindowsServiceSnapshot {
    bin_path: String,
    start: &'static str,
    environment: Option<String>,
}

#[cfg(target_os = "windows")]
fn windows_service_install(
    executable: &Path,
    environment: &[(String, String)],
    force: bool,
    path: &Path,
    definition: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let executable = executable.display().to_string();
    let snapshot = if force { windows_service_snapshot()? } else { None };

    let previous_definition = match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    let mut created = false;

    let result = (|| {
        if snapshot.is_some() {
            windows_service_configure(&executable, "auto")?;
        } else {
            windows_service_create(&executable)?;
            created = true;
        }

        let data = environment
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("\\0");

        let output = command_output(std::process::Command::new("reg.exe").args([
            "add",
            r"HKLM\SYSTEM\CurrentControlSet\Services\Crabbot",
            "/v",
            "Environment",
            "/t",
            "REG_MULTI_SZ",
            "/d",
            data.as_str(),
            "/f",
        ]))?;

        if !output.status.success() {
            return Err(format!(
                "Service environment installation failed: {}",
                windows_command_detail(&output)
            )
            .into());
        }

        secure(path, definition.as_bytes())?;

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })();

    if let Err(error) = result {
        let rollback = windows_service_rollback(
            created,
            snapshot.as_ref(),
            path,
            previous_definition.as_deref(),
        );

        return match rollback {
            Ok(()) => Err(error),

            Err(rollback_error) => {
                Err(format!("{error}; service rollback failed: {rollback_error}").into())
            }
        };
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_service_create(
    executable: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(
        std::process::Command::new("sc.exe")
            .args(["create", "Crabbot", "binPath=", executable, "start=", "auto"]),
    )?;

    if !output.status.success() {
        return Err(
            format!("Service installation failed: {}", windows_command_detail(&output)).into()
        );
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_service_configure(
    executable: &str,
    start: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(
        std::process::Command::new("sc.exe")
            .args(["config", "Crabbot", "binPath=", executable, "start=", start]),
    )?;

    if !output.status.success() {
        return Err(
            format!("Service configuration failed: {}", windows_command_detail(&output)).into()
        );
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_service_snapshot()
-> Result<Option<WindowsServiceSnapshot>, Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(std::process::Command::new("sc.exe").args(["qc", "Crabbot"]))?;

    if !output.status.success() {
        let detail = windows_command_detail(&output);
        let lower = detail.to_ascii_lowercase();

        if detail.contains("1060") || lower.contains("does not exist") {
            return Ok(None);
        }

        return Err(format!("Service query failed: {detail}").into());
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let bin_path = windows_service_field(&text, "BINARY_PATH_NAME")
        .ok_or("Service query did not return a binary path.")?;
    let start_type = windows_service_field(&text, "START_TYPE")
        .ok_or("Service query did not return a start type.")?;
    let start = match start_type.split_whitespace().next() {
        Some("0") => "boot",
        Some("1") => "system",
        Some("2") => "auto",
        Some("3") => "demand",
        Some("4") => "disabled",
        _ => return Err("Service query returned an unsupported start type.".into()),
    };

    Ok(Some(WindowsServiceSnapshot {
        bin_path,
        start,
        environment: windows_service_environment()?,
    }))
}

#[cfg(target_os = "windows")]
fn windows_service_field(output: &str, name: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let (field, value) = line.split_once(':')?;

        (field.trim() == name).then(|| value.trim().to_owned())
    })
}

#[cfg(target_os = "windows")]
fn windows_service_environment() -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>
{
    let output = command_output(std::process::Command::new("reg.exe").args([
        "query",
        r"HKLM\SYSTEM\CurrentControlSet\Services\Crabbot",
        "/v",
        "Environment",
    ]))?;

    if !output.status.success() {
        let detail = windows_command_detail(&output);
        let lower = detail.to_ascii_lowercase();

        if lower.contains("unable to find") || lower.contains("not found") {
            return Ok(None);
        }

        return Err(format!("Service environment query failed: {detail}").into());
    }

    let text = String::from_utf8_lossy(&output.stdout);
    Ok(windows_registry_field(&text, "Environment"))
}

#[cfg(target_os = "windows")]
fn windows_registry_field(output: &str, name: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let line = line.trim_start();
        let rest = line.strip_prefix(name)?;

        if !rest.chars().next().is_some_and(char::is_whitespace) {
            return None;
        }

        let rest = rest.trim_start();
        let type_end = rest.find(char::is_whitespace)?;
        let value = rest[type_end..].trim_start();

        Some(value.to_owned())
    })
}

#[cfg(target_os = "windows")]
fn windows_service_rollback(
    created: bool,
    snapshot: Option<&WindowsServiceSnapshot>,
    path: &Path,
    previous_definition: Option<&[u8]>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut errors = Vec::new();

    if let Some(snapshot) = snapshot {
        if let Err(error) = windows_service_configure(&snapshot.bin_path, snapshot.start) {
            errors.push(error.to_string());
        }

        let environment = match &snapshot.environment {
            Some(value) => command_output(std::process::Command::new("reg.exe").args([
                "add",
                r"HKLM\SYSTEM\CurrentControlSet\Services\Crabbot",
                "/v",
                "Environment",
                "/t",
                "REG_MULTI_SZ",
                "/d",
                value,
                "/f",
            ])),
            None => command_output(std::process::Command::new("reg.exe").args([
                "delete",
                r"HKLM\SYSTEM\CurrentControlSet\Services\Crabbot",
                "/v",
                "Environment",
                "/f",
            ])),
        };

        match environment {
            Ok(output) if output.status.success() => {}
            Ok(output) => errors.push(format!(
                "could not restore service environment: {}",
                windows_command_detail(&output)
            )),
            Err(error) => errors.push(format!("could not restore service environment: {error}")),
        }
    } else if created {
        if let Err(error) = windows_service_remove() {
            errors.push(error.to_string());
        }
    }

    match previous_definition {
        Some(bytes) => {
            if let Err(error) = secure(path, bytes) {
                errors.push(format!("could not restore service definition: {error}"));
            }
        }

        None => {
            if let Err(error) = std::fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                errors.push(format!("could not restore service definition: {error}"));
            }
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors.join("; ").into()) }
}

#[cfg(target_os = "windows")]
fn windows_command_detail(output: &std::process::Output) -> String {
    let detail = format!(
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let detail = detail.trim();

    if detail.is_empty() {
        "the service manager returned no details".into()
    } else {
        sentence(detail)
    }
}

#[cfg(target_os = "windows")]
fn windows_service_remove() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(std::process::Command::new("sc.exe").args(["delete", "Crabbot"]))?;

    if !output.status.success() {
        let detail = format!(
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        return Err(format!("Service removal failed: {}", sentence(detail.trim())).into());
    }

    Ok(())
}

fn service_action(
    path: &Path,
    start: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_action_with(path, start, false)
}

fn service_action_with(
    path: &Path,
    start: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !path.is_file() {
        return Err("Service definition is not installed.".into());
    }

    #[cfg(target_os = "windows")]
    if !start && !windows_service_running()? {
        if json {
            println!("{}", serde_json::json!({"action": "stop", "status": "stopped"}));
        } else {
            println!("Service stopped.");
        }

        return Ok(());
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = start;
        return Err("Native service actions are unsupported on this platform.".into());
    }

    #[cfg(target_os = "linux")]
    let (program, args): (&str, Vec<String>) = if start {
        (
            "systemctl",
            vec!["--user".into(), "enable".into(), "--now".into(), "crabbot.service".into()],
        )
    } else {
        (
            "systemctl",
            vec!["--user".into(), "disable".into(), "--now".into(), "crabbot.service".into()],
        )
    };

    #[cfg(target_os = "macos")]
    let (program, args): (&str, Vec<String>) = if start {
        ("launchctl", vec!["load".into(), "-w".into(), path.display().to_string()])
    } else {
        ("launchctl", vec!["unload".into(), "-w".into(), path.display().to_string()])
    };

    #[cfg(target_os = "windows")]
    let (program, args): (&str, Vec<String>) = if start {
        ("sc.exe", vec!["start".into(), "Crabbot".into()])
    } else {
        ("sc.exe", vec!["stop".into(), "Crabbot".into()])
    };

    let output = command_output(std::process::Command::new(program).args(args))?;

    if !output.status.success() {
        let detail = format!(
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let detail = detail.trim();
        let detail =
            if detail.is_empty() { "the service manager returned no details" } else { detail };

        return Err(format!("Service action failed: {}", sentence(detail)).into());
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "action": if start { "start" } else { "stop" },
                "status": if start { "started" } else { "stopped" },
            }))?
        );
    } else {
        let status = if start { "started" } else { "stopped" };

        println!("Service {status}.");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_service_running() -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(std::process::Command::new("sc.exe").args(["query", "Crabbot"]))?;

    if !output.status.success() {
        let detail = format!(
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let lower = detail.to_ascii_lowercase();

        if detail.contains("1060") || lower.contains("does not exist") {
            return Ok(false);
        }

        return Err(format!("Service query failed: {}", sentence(detail.trim())).into());
    }

    let state = String::from_utf8_lossy(&output.stdout);
    Ok(["RUNNING", "START_PENDING", "STOP_PENDING", "PAUSE_PENDING"]
        .iter()
        .any(|value| state.contains(value)))
}

fn service_path() -> PathBuf {
    let user = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    #[cfg(target_os = "linux")]
    {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| user.join(".config"))
            .join("systemd/user/crabbot.service")
    }

    #[cfg(target_os = "macos")]
    {
        user.join("Library/LaunchAgents/dev.airscripts.crabbot.plist")
    }

    #[cfg(target_os = "windows")]
    {
        user.join("crabbot-service.xml")
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        user.join("crabbot.service")
    }
}

fn service_text(executable: &Path, environment: &[(String, String)]) -> String {
    let executable = executable.display().to_string();

    #[cfg(target_os = "linux")]
    {
        let environment = environment
            .iter()
            .map(|(name, value)| format!("Environment=\"{name}={}\"\n", systemd(value)))
            .collect::<String>();

        format!(
            "[Unit]\nDescription=Crabbot agent\nAfter=network-online.target\n\n[Service]\nExecStart=\"{}\"\n{}Restart=on-failure\n\n[Install]\nWantedBy=default.target\n",
            executable.replace('\\', "\\\\").replace('"', "\\\""),
            environment
        )
    }

    #[cfg(target_os = "macos")]
    {
        let environment = environment
            .iter()
            .map(|(name, value)| {
                format!("<key>{}</key><string>{}</string>", xml(name.into()), xml(value.clone()))
            })
            .collect::<String>();

        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>dev.airscripts.crabbot</string><key>ProgramArguments</key><array><string>{}</string></array><key>EnvironmentVariables</key><dict>{}</dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n",
            xml(executable),
            environment
        )
    }

    #[cfg(target_os = "windows")]
    {
        let environment = environment
            .iter()
            .map(|(name, value)| {
                format!(
                    "<variable name=\"{}\" value=\"{}\"/>",
                    xml(name.into()),
                    xml(value.clone())
                )
            })
            .collect::<String>();

        format!(
            "<service><name>Crabbot</name><command>{}</command><environment>{}</environment><start>auto</start></service>\n",
            xml(executable),
            environment
        )
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        format!("{}\n", executable)
    }
}

#[cfg(target_os = "linux")]
fn systemd(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('$', "\\$").replace('\n', "\\n")
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn xml(value: String) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn version(json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"name": NAME, "version": VERSION}))
                .expect("serializing version JSON cannot fail")
        );
    } else {
        println!("{NAME} {VERSION}");
    }
}

async fn status_command(json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = home();
    let config_ok = root.join("config.toml").is_file()
        && toml::from_str::<Config>(&std::fs::read_to_string(root.join("config.toml"))?)
            .is_ok_and(|config| config.validate().is_ok());

    let lock = load_lock_at(&root)?;
    let installed = lock.plugins.len();
    let model = std::env::var("CRABBOT_MODEL_PLUGIN").unwrap_or_else(|_| "codex".into());
    let channel = std::env::var("CRABBOT_CHANNEL").unwrap_or_else(|_| "telegram".into());
    let model_ready = lock.plugins.get(&model).is_some_and(|entry| {
        entry.capabilities.iter().any(|capability| capability == "model") && ready(&model)
    });

    let channel_ready = lock.plugins.get(&channel).is_some_and(|entry| {
        entry.capabilities.iter().any(|capability| capability == "channel") && ready(&channel)
    });

    let conflicts = plugin_commands(&root).values().any(|owners| owners.len() > 1);
    let daemon = ipc::call(&root, "status", serde_json::json!({})).await.is_ok();
    let health = if config_ok && model_ready && channel_ready && !conflicts {
        "healthy"
    } else {
        "attention"
    };

    let value = serde_json::json!({
        "version": VERSION,
        "health": health,
        "daemon": if daemon { "running" } else { "stopped" },
        "intelligence": capability_status(&lock, "model"),
        "messaging": capability_status(&lock, "channel"),
        "plugins": installed,
    });

    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("{}", status_text(&value, installed));
    }

    Ok(())
}

fn capability_status(lock: &Lock, capability: &str) -> serde_json::Value {
    let plugins = lock
        .plugins
        .iter()
        .filter(|(id, entry)| entry.capabilities.iter().any(|item| item == capability) && ready(id))
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();

    serde_json::json!({
        "status": if plugins.is_empty() { "not configured" } else { "configured" },
        "plugins": plugins,
    })
}

fn capability_text(value: &serde_json::Value) -> String {
    let status = value["status"].as_str().unwrap_or("not configured");
    let plugins = value["plugins"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();

    if status == "configured" && !plugins.is_empty() {
        format!("configured ({})", plugins.join(", "))
    } else {
        status.into()
    }
}

fn status_text(value: &serde_json::Value, installed: usize) -> String {
    format!(
        "{:<15}{}\n{:<15}{}\n{:<15}{}\n{:<15}{}\n{:<15}{}\n{:<15}{} installed",
        "Version:",
        value["version"].as_str().unwrap_or(VERSION),
        "Health:",
        value["health"].as_str().unwrap_or("attention"),
        "Daemon:",
        value["daemon"].as_str().unwrap_or("stopped"),
        "Intelligence:",
        capability_text(&value["intelligence"]),
        "Messaging:",
        capability_text(&value["messaging"]),
        "Plugins:",
        installed
    )
}

async fn plugin_command(args: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(name) = args.first() else {
        return Err("A plugin command is required.".into());
    };

    let root = home();
    let commands = plugin_commands(&root);
    let Some(owners) = commands.get(name) else {
        print_help_with_plugins(&root);
        return Ok(());
    };

    if owners.len() != 1 {
        return Err(format!("Plugin command {name} has conflicting registrations.").into());
    }

    let (plugin, _) =
        owners.first().cloned().ok_or_else(|| format!("Plugin command {name} was not found."))?;

    if name == "ask" {
        return ask_plugin_command(&args[1..], &root).await;
    }

    if args.get(1).is_some_and(|argument| argument == "--help" || argument == "-h") {
        println!("Usage: crabbot {name} [arguments...]");
        println!("{}", owners[0].1.description);
        return Ok(());
    }

    let path = binary_at(&plugin, &root)
        .ok_or_else(|| format!("Plugin binary was not found: {plugin}."))?;

    let config = if root.join("config.toml").is_file() {
        toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?
    } else {
        Config::default()
    };

    let mut process = launch(&root, path, &plugin, None, false, &config).await?;
    let timeout = if name == "codex" && args.get(1).is_some_and(|argument| argument == "login") {
        std::time::Duration::from_secs(960)
    } else {
        std::time::Duration::from_secs(120)
    };

    let result = if name == "code" {
        let model_id = std::env::var("CRABBOT_MODEL_PLUGIN").unwrap_or_else(|_| "codex".into());
        let model_path = binary_at(&model_id, &root).ok_or_else(|| {
            format!("The configured intelligence plugin {model_id} is not installed.")
        })?;

        let model = Arc::new(AsyncMutex::new(Some(
            launch(&root, model_path, &model_id, Some(Capability::Model), false, &config).await?,
        )));
        let tools = binary_at("tools", &root).map(|path| async {
            launch(&root, path, "tools", Some(Capability::Tool), false, &config).await
        });

        let tools = Arc::new(AsyncMutex::new(match tools {
            Some(future) => Some(future.await?),
            None => None,
        }));

        let workspace = workspace_root().canonicalize().unwrap_or_else(|_| workspace_root());
        let model_process = Arc::clone(&model);
        let tool_process = Arc::clone(&tools);
        let approval_mode = config.approval_mode();
        process
            .call_full_timeout_async(
                Request::call(1, "command", serde_json::json!({"name": name, "args": &args[1..]})),
                timeout,
                |request| {
                    if let Request::Note { method, params, .. } = request
                        && method == "event"
                        && let Some(text) = params["event"]["text"].as_str()
                    {
                        use std::io::Write;

                        print!("{text}");
                        let _ = std::io::stdout().flush();
                    }

                    std::future::ready(Ok(()))
                },
                |request| {
                    agent_request(
                        request,
                        Arc::clone(&model_process),
                        Arc::clone(&tool_process),
                        workspace.clone(),
                        approval_mode,
                    )
                },
            )
            .await
    } else {
        process
            .call_stream_timeout_async(
                Request::call(1, "command", serde_json::json!({"name": name, "args": &args[1..]})),
                timeout,
                |request| {
                    if let Request::Note { method, params, .. } = request
                        && method == "event"
                        && let Some(text) = params["event"]["text"].as_str()
                    {
                        use std::io::Write;

                        print!("{text}");
                        let _ = std::io::stdout().flush();
                    }

                    std::future::ready(Ok(()))
                },
            )
            .await
    };

    let stopped = process.stop().await;
    let response = result?;
    stopped?;

    if let Some(error) = response.error {
        return Err(error.message.into());
    }

    let value = response.result.ok_or("Plugin command returned no result.")?;

    if let Some(text) = value.as_str() {
        println!("{text}");
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }

    Ok(())
}

async fn ask_plugin_command(
    args: &[String],
    root: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let argv = std::iter::once("ask".to_owned()).chain(args.iter().cloned());
    let ask = match Ask::try_parse_from(argv) {
        Ok(ask) => ask,

        Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
            print!("{error}");
            return Ok(());
        }

        Err(error) => return Err(error.into()),
    };

    ask_at(ask, root).await
}

async fn agent_request(
    request: Request,
    model: Arc<AsyncMutex<Option<Process>>>,
    tools: Arc<AsyncMutex<Option<Process>>>,
    workspace: PathBuf,
    approval_mode: ApprovalMode,
) -> std::result::Result<Response, crabbot_core::Error> {
    let Request::Call { id, method, params, .. } = request else {
        return Err(crabbot_core::Error::Protocol("Agent host requests require an ID.".into()));
    };

    match method.as_str() {
        "host/model" => {
            let mut model = model.lock().await;
            let Some(provider) = model.as_mut() else {
                return Ok(Response::fail(id, -32000, "No intelligence provider is available."));
            };

            provider.call(Request::call(id, "generate", params)).await
        }

        "host/tool" => {
            let mut tools = tools.lock().await;
            let Some(tool_process) = tools.as_mut() else {
                return Ok(Response::fail(id, -32000, "The tools plugin is not installed."));
            };

            let Some(name) = params["name"].as_str() else {
                return Ok(Response::fail(id, -32602, "A tool name is required."));
            };

            let mut args = params["args"].clone();
            let approved = if mutating(name, &args) {
                match approval_mode {
                    ApprovalMode::Off => false,
                    ApprovalMode::Auto => true,
                    ApprovalMode::Prompt => prompt_code_approval(name, &args),
                }
            } else {
                false
            };

            if mutating(name, &args) && !approved {
                return Ok(Response::fail(id, -32000, "Crabbot approval blocked the tool."));
            }

            let Some(values) = args.as_object_mut() else {
                return Ok(Response::fail(id, -32602, "Tool arguments must be an object."));
            };

            values.insert("approve".into(), serde_json::Value::Bool(approved));
            values.insert(
                "workspace".into(),
                serde_json::Value::String(workspace.display().to_string()),
            );
            tool_process.call(Request::call(id, name, args)).await
        }

        _ => Ok(Response::fail(id, -32601, "Agent host method not found.")),
    }
}

fn prompt_code_approval(name: &str, args: &serde_json::Value) -> bool {
    use std::io::{self, Write};

    print!(
        "Crabbot requests approval to run {name} with {}. Approve? [y/N] ",
        clip(args.to_string(), 400)
    );
    let _ = io::stdout().flush();
    let mut answer = String::new();

    if io::stdin().read_line(&mut answer).is_err() {
        return false;
    }

    matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

async fn plugin(
    command: PluginCommand,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        PluginCommand::List(output) => list(output.json || json)?,

        PluginCommand::Install(source) => {
            let manifest = link(source, false)?;
            let activation = activate_at(&home(), &manifest.id).await?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "install",
                        "id": manifest.id,
                        "version": manifest.version,
                        "activation": if activation.is_some() { "active" } else { "deferred" }
                    }))?
                );
            } else {
                println!("Installed {} {}.", manifest.id, manifest.version);
                print_activation(&manifest.id, activation);
            }
        }

        PluginCommand::Link(source) => {
            let manifest = link(source, true)?;
            let activation = activate_at(&home(), &manifest.id).await?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "link",
                        "id": manifest.id,
                        "version": manifest.version,
                        "activation": if activation.is_some() { "active" } else { "deferred" }
                    }))?
                );
            } else {
                println!("Linked {} {}.", manifest.id, manifest.version);
                print_activation(&manifest.id, activation);
            }
        }

        PluginCommand::Update(output) => update_plugins(output.json || json).await?,

        PluginCommand::Remove(name) => {
            if !name.yes {
                return Err("Removing a plugin requires --yes.".into());
            }

            if !valid(&name.id) {
                return Err(format!("Invalid plugin ID: {}.", name.id).into());
            }

            let id = name.id.clone();
            let unloaded =
                unload_at(&home(), &id).await?.is_some_and(|value| value["unloaded"] == true);

            if let Err(error) = remove(name) {
                if let Err(reload) = activate_at(&home(), &id).await {
                    return Err(format!(
                        "Plugin removal failed: {}. Runtime restoration also failed: {}.",
                        sentence(error.to_string()),
                        sentence(reload.to_string())
                    )
                    .into());
                }

                return Err(error);
            }

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "action": "remove",
                        "id": id,
                        "status": "removed",
                        "unloaded": unloaded
                    }))?
                );
            } else {
                if unloaded {
                    println!("Plugin {id} was unloaded from the running daemon.");
                }

                println!("Removed {}.", id);
            }
        }
    }

    Ok(())
}

fn print_activation(id: &str, value: Option<serde_json::Value>) {
    match value {
        Some(value) => println!(
            "Plugin {} {} is active in the running daemon.",
            value["id"].as_str().unwrap_or(id),
            value["version"].as_str().unwrap_or("unknown")
        ),
        None => println!("Plugin {id} is installed and will load when the daemon starts."),
    }
}

async fn activate_at(
    root: &Path,
    id: &str,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    control_at("plugin.load", serde_json::json!({"id": id}), root).await
}

async fn unload_at(
    root: &Path,
    id: &str,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    control_at("plugin.unload", serde_json::json!({"id": id}), root).await
}

async fn active_at(
    root: &Path,
) -> Result<Option<Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(value) = control_at("plugin.active", serde_json::json!({}), root).await? else {
        return Ok(None);
    };

    Ok(Some(active_ids(value)?))
}

fn active_ids(value: serde_json::Value) -> Result<Vec<String>, PluginError> {
    let items =
        value["items"].as_array().ok_or("Daemon returned an invalid active plugin list.")?;

    let mut active = Vec::with_capacity(items.len());

    for item in items {
        let id = item.as_str().ok_or("Daemon returned an invalid active plugin ID.")?;

        if !valid(id) {
            return Err("Daemon returned an invalid active plugin ID.".into());
        }

        active.push(id.to_owned());
    }

    Ok(active)
}

async fn update_plugins(json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    update_plugins_at(&home(), json).await
}

async fn update_plugins_at(
    root: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(active) = active_at(root).await? else {
        return update_at(root, json);
    };

    let unload_root = root.to_path_buf();
    let activate_root = root.to_path_buf();
    update_live(
        root,
        json,
        active,
        move |id| {
            let root = unload_root.clone();
            Box::pin(async move { unload_at(&root, &id).await })
        },
        move |id| {
            let root = activate_root.clone();
            Box::pin(async move { activate_at(&root, &id).await })
        },
    )
    .await
}

async fn update_live<U, A>(
    root: &Path,
    json: bool,
    active: Vec<String>,
    mut unload: U,
    mut activate: A,
) -> Result<(), PluginError>
where
    U: FnMut(String) -> PluginTask,
    A: FnMut(String) -> PluginTask,
{
    if active.is_empty() {
        return update_at(root, json);
    }

    let mut unloaded = Vec::with_capacity(active.len());

    for id in &active {
        match unload(id.clone()).await {
            Ok(Some(value)) if value["unloaded"] == true => unloaded.push(id.clone()),

            Ok(Some(_)) => {}

            Ok(None) => {
                let restore = restore_plugins(&unloaded, &mut activate).await;
                return Err(format!(
                    "The daemon became unavailable before {id} could be unloaded.{}",
                    restore
                )
                .into());
            }

            Err(error) => {
                let restore = restore_plugins(&unloaded, &mut activate).await;
                return Err(format!(
                    "Could not unload {id} before updating plugins: {}.{}",
                    sentence(error.to_string()),
                    restore
                )
                .into());
            }
        }
    }

    let update = update_at(root, json);
    let restore = restore_plugins(&unloaded, &mut activate).await;

    match (update, restore.is_empty()) {
        (Ok(()), true) => Ok(()),

        (Ok(()), false) => {
            Err(format!("Plugins were updated, but runtime activation was incomplete.{}", restore)
                .into())
        }

        (Err(error), true) => Err(error),
        (Err(error), false) => Err(format!(
            "Plugin updates failed: {} Runtime restoration was also incomplete.{}",
            sentence(error.to_string()),
            restore
        )
        .into()),
    }
}

async fn restore_plugins<A>(ids: &[String], activate: &mut A) -> String
where
    A: FnMut(String) -> PluginTask,
{
    let mut failures = Vec::new();

    for id in ids {
        match activate(id.clone()).await {
            Ok(Some(_)) => {}
            Ok(None) => failures.push(format!(
                " {id} could not be reloaded because the daemon is unavailable; it will load when the daemon starts."
            )),
            Err(error) => failures.push(format!(
                " {id} could not be reloaded: {}",
                sentence(error.to_string())
            )),
        }
    }

    if failures.is_empty() { String::new() } else { failures.join("") }
}

fn update_at(home_root: &Path, json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _lock = lock_at(home_root, ".plugins.lock")?;
    let mut lock = load_lock_at(home_root)?;
    let mut rows = Vec::new();
    let mut failed = false;

    let ids = lock.plugins.keys().cloned().collect::<Vec<_>>();

    for id in ids {
        let previous_lock = lock.clone();
        let previous = lock.plugins.get(&id).cloned().ok_or("Plugin lock entry disappeared.")?;
        let result = {
            let entry = lock.plugins.get(&id).ok_or("Plugin lock entry disappeared.")?;
            update_one(home_root, &id, entry)
        };

        match result {
            Ok((manifest, hash, source_revision, update)) => {
                {
                    let entry =
                        lock.plugins.get_mut(&id).ok_or("Plugin lock entry disappeared.")?;

                    entry.version = manifest.version.clone();
                    entry.revision = source_revision;
                    entry.hash = hash;
                    entry.protocol = manifest.protocol;
                    entry.capabilities = manifest.capabilities.clone();
                    entry.permissions = manifest.permissions.clone();
                    entry.secrets = manifest.secrets.clone();
                    entry.commands = manifest.commands.clone();
                }

                if let Err(error) = save_lock_at(home_root, &lock) {
                    if let Some(entry) = lock.plugins.get_mut(&id) {
                        *entry = previous;
                    }

                    let restored_lock = save_lock_at(home_root, &previous_lock);
                    let restored_plugin = rollback_update(&update);
                    let detail = if let Err(restore) = restored_lock {
                        format!(" Rollback failed: {}.", sentence(restore.to_string()))
                    } else if let Err(restore) = restored_plugin {
                        format!(" Rollback failed: {}.", sentence(restore.to_string()))
                    } else {
                        String::new()
                    };

                    failed = true;
                    rows.push(serde_json::json!({
                        "id": id,
                        "status": "failed",
                        "error": format!("Could not save the plugin lock: {}{detail}", sentence(error.to_string()))
                    }));
                } else {
                    finish_update(&update);
                    rows.push(serde_json::json!({
                        "id": id,
                        "version": manifest.version,
                        "status": "updated"
                    }));
                }
            }

            Err(error) => {
                failed = true;
                rows.push(serde_json::json!({
                    "id": id,
                    "status": "failed",
                    "error": sentence(error.to_string())
                }));
            }
        }
    }

    if !json && rows.is_empty() {
        println!("No plugins to update.");
    } else if !json {
        for row in &rows {
            let status = row["status"].as_str().unwrap_or("unknown");
            let id = row["id"].as_str().unwrap_or("unknown");

            if status == "updated" {
                println!("Updated {id} to {}.", row["version"].as_str().unwrap_or("unknown"));
            } else {
                println!("Could not update {id}: {}", row["error"].as_str().unwrap_or("unknown"));
            }
        }
    }

    if failed {
        let details = rows
            .iter()
            .filter(|row| row["status"] == "failed")
            .map(|row| {
                format!(
                    "{}: {}",
                    row["id"].as_str().unwrap_or("unknown"),
                    row["error"].as_str().unwrap_or("unknown error")
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("One or more plugin updates failed: {details}").into());
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "items": rows,
                "status": if rows.is_empty() { "no changes" } else { "updated" },
            }))?
        );
    }

    Ok(())
}

fn update_one(
    home_root: &Path,
    id: &str,
    entry: &Entry,
) -> Result<(Manifest, String, String, Update), Box<dyn std::error::Error + Send + Sync>> {
    if !valid(id) {
        return Err(format!("Invalid plugin ID: {id}.").into());
    }

    let pin = entry.pinned.then_some(entry.revision.as_str());
    let source = resolve(&entry.source, pin)?;
    let manifest = read_manifest(&source.path)
        .ok_or_else(|| format!("Manifest not found in {}.", source.path.display()))?;

    manifest.validate().map_err(|error| format!("Invalid plugin manifest: {error}"))?;

    if manifest.id != id {
        return Err(format!("Manifest ID {} does not match {id}.", manifest.id).into());
    }

    if !Protocol::CURRENT.compatible(manifest.protocol) {
        return Err(format!("Plugin {id} requires unsupported protocol.").into());
    }

    if manifest.capabilities != entry.capabilities
        || manifest.permissions != entry.permissions
        || manifest.secrets != entry.secrets
        || manifest.commands != entry.commands
    {
        return Err(format!(
            "Plugin {id} changes declared capabilities, permissions, secrets, or commands; review it with plugin link --yes."
        )
        .into());
    }

    validate_commands(home_root, id, &manifest.commands)?;

    let name = plugin_name(id);
    let Some(binary) = plugin_binary(&source.path, id, entry.default) else {
        return Err(format!("Binary {name} was not found; build the plugin first.").into());
    };

    let source_revision = revision(&source.path);

    let plugins = home_root.join("plugins");
    std::fs::create_dir_all(&plugins)?;
    let nonce = format!("{}-{}", std::process::id(), now());
    let stage = plugins.join(format!(".{id}.stage-{nonce}"));
    let backup = plugins.join(format!(".{id}.backup-{nonce}"));
    let destination = plugins.join(id);

    if std::fs::canonicalize(&destination).is_ok_and(|path| path == source.path) {
        return Err("A plugin source cannot be its managed destination.".into());
    }

    let had_destination = destination.exists();
    let result = (|| -> Result<(String, Update), Box<dyn std::error::Error + Send + Sync>> {
        std::fs::create_dir_all(stage.join("bin"))?;
        std::fs::copy(source.path.join("crabbot-plugin.toml"), stage.join("crabbot-plugin.toml"))?;
        let staged = stage.join("bin").join(&name);

        if entry.linked && source.temp.is_none() {
            link_binary(&binary, &staged)?;
        } else {
            copy_binary(&binary, &staged)?;
        }

        let hash = digest(&stage.join("crabbot-plugin.toml"), &staged)?;
        let config = if home_root.join("config.toml").is_file() {
            toml::from_str(&std::fs::read_to_string(home_root.join("config.toml"))?)?
        } else {
            Config::default()
        };

        config
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        health(home_root, staged.clone(), id, &config)?;

        if had_destination {
            std::fs::rename(&destination, &backup)?;
        }

        if let Err(error) = std::fs::rename(&stage, &destination) {
            if backup.exists() {
                let _ = std::fs::rename(&backup, &destination);
            }

            return Err(error.into());
        }

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            hash,
            Update {
                destination: destination.clone(),
                backup: had_destination.then_some(backup.clone()),
            },
        ))
    })();

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&stage);
    }

    let (hash, update) = result?;
    Ok((manifest, hash, source_revision, update))
}

fn finish_update(update: &Update) {
    if let Some(backup) = &update.backup {
        let _ = std::fs::remove_dir_all(backup);
    }
}

fn rollback_update(update: &Update) -> std::io::Result<()> {
    if update.destination.exists() {
        std::fs::remove_dir_all(&update.destination)?;
    }

    if let Some(backup) = &update.backup {
        std::fs::rename(backup, &update.destination)?;
    }

    Ok(())
}

fn health(
    root: &Path,
    path: PathBuf,
    expected: &str,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = root.to_path_buf();
    let expected = expected.to_owned();
    let config = config.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;

        runtime.block_on(async move {
            let process = launch(&root, path, &expected, None, true, &config).await?;
            process.stop().await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
    })
    .join()
    .map_err(|_| "Plugin health check panicked.")??;
    Ok(())
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_nanos() as u64)
}

fn link(
    source: Source,
    linked: bool,
) -> Result<Manifest, Box<dyn std::error::Error + Send + Sync>> {
    link_at(source, linked, &home())
}

fn link_at(
    source: Source,
    linked: bool,
    home_root: &Path,
) -> Result<Manifest, Box<dyn std::error::Error + Send + Sync>> {
    let _lock = lock_at(home_root, ".plugins.lock")?;

    if !valid(&source.id) {
        return Err(format!("Invalid plugin ID: {}.", source.id).into());
    }

    let default_source = source.source.is_none();
    let origin = source
        .source
        .clone()
        .unwrap_or_else(|| PathBuf::from("crabbot-plugins").join(&source.id).display().to_string());

    let root = resolve(&origin, source.revision.as_deref())?;
    let root_path = root.path.clone();

    let manifest = read_manifest(&root_path)
        .ok_or_else(|| format!("Manifest not found in {}.", root_path.display()))?;

    if manifest.id != source.id {
        return Err(format!("Manifest ID {} does not match {}.", manifest.id, source.id).into());
    }

    if !Protocol::CURRENT.compatible(manifest.protocol) {
        return Err(format!("Plugin {} requires unsupported protocol.", manifest.id).into());
    }

    manifest.validate().map_err(|error| format!("Invalid plugin manifest: {error}"))?;
    validate_commands(home_root, &source.id, &manifest.commands)?;

    let dest = home_root.join("plugins").join(&source.id);

    if root_path == std::fs::canonicalize(&dest).unwrap_or_default() {
        return Err("A plugin source cannot be its managed destination.".into());
    }

    if dest.exists() && !source.yes {
        return Err(format!("Plugin {} already exists; use --yes to replace it.", source.id).into());
    }

    let name = if cfg!(windows) {
        format!("crabbot-plugin-{}.exe", source.id)
    } else {
        format!("crabbot-plugin-{}", source.id)
    };

    let Some(binary) = plugin_binary(&root_path, &source.id, default_source) else {
        return Err(format!("Binary {name} was not found; build the plugin first.").into());
    };

    let plugins = home_root.join("plugins");
    std::fs::create_dir_all(&plugins)?;
    let nonce = format!("{}-{}", std::process::id(), now());
    let stage = plugins.join(format!(".{}.stage-{nonce}", source.id));
    let backup = plugins.join(format!(".{}.backup-{nonce}", source.id));
    let result = (|| {
        std::fs::create_dir_all(stage.join("bin"))?;
        std::fs::copy(root_path.join("crabbot-plugin.toml"), stage.join("crabbot-plugin.toml"))?;
        let staged = stage.join("bin").join(&name);

        if linked && root.temp.is_none() {
            link_binary(&binary, &staged)?;
        } else {
            copy_binary(&binary, &staged)?;
        }

        let config = if home_root.join("config.toml").is_file() {
            toml::from_str(&std::fs::read_to_string(home_root.join("config.toml"))?)?
        } else {
            Config::default()
        };

        config
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        health(home_root, staged.clone(), &source.id, &config)?;

        let mut lock = load_lock_at(home_root)?;
        let previous_lock = lock.clone();
        lock.plugins.insert(
            source.id.clone(),
            Entry {
                source: canonical_source(&origin),
                revision: revision(&root_path),
                pinned: source.revision.as_deref().is_some_and(|value| value != "local"),
                hash: digest(&stage.join("crabbot-plugin.toml"), &staged)?,
                version: manifest.version.clone(),
                protocol: manifest.protocol,
                capabilities: manifest.capabilities.clone(),
                permissions: manifest.permissions.clone(),
                secrets: manifest.secrets.clone(),
                commands: manifest.commands.clone(),
                linked,
                default: default_source,
            },
        );

        if dest.exists() {
            std::fs::rename(&dest, &backup)?;
        }

        if let Err(error) = std::fs::rename(&stage, &dest) {
            if backup.exists() {
                let _ = std::fs::rename(&backup, &dest);
            }

            return Err(error.into());
        }

        if let Err(error) = save_lock_at(home_root, &lock) {
            let restored_lock = save_lock_at(home_root, &previous_lock);
            let restored_plugin = (|| {
                if dest.exists() {
                    std::fs::remove_dir_all(&dest)?;
                }

                if backup.exists() {
                    std::fs::rename(&backup, &dest)?;
                }

                Ok::<(), std::io::Error>(())
            })();

            if let Err(restore) = restored_lock {
                return Err(format!(
                    "Could not save the plugin lock: {}. Rollback failed: {}.",
                    sentence(error.to_string()),
                    sentence(restore.to_string())
                )
                .into());
            }

            if let Err(restore) = restored_plugin {
                return Err(format!(
                    "Could not save the plugin lock: {}. Rollback failed: {}.",
                    sentence(error.to_string()),
                    sentence(restore.to_string())
                )
                .into());
            }

            return Err(error);
        }

        if backup.exists() {
            let _ = std::fs::remove_dir_all(&backup);
        }

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&stage);
    }

    result?;

    Ok(manifest)
}

fn copy_binary(source: &Path, destination: &Path) -> std::io::Result<u64> {
    let bytes = std::fs::copy(source, destination)?;

    #[cfg(unix)]
    {
        let mut permissions = std::fs::metadata(destination)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(destination, permissions)?;
    }

    Ok(bytes)
}

fn digest(
    manifest: &Path,
    binary: &Path,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    const LIMIT: u64 = 256 * 1024 * 1024;
    let mut hash = Sha256::new();
    hash.update(std::fs::read(manifest)?);
    let mut file = std::fs::File::open(binary)?;

    if file.metadata()?.len() > LIMIT {
        return Err("Plugin binary exceeds the integrity-check size limit.".into());
    }

    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let count = std::io::Read::read(&mut file, &mut buffer)?;

        if count == 0 {
            break;
        }

        hash.update(&buffer[..count]);
    }

    Ok(format!("{:x}", hash.finalize()))
}

fn revision(path: impl AsRef<Path>) -> String {
    command_output(git_command().args(["-C"]).arg(path.as_ref()).args(["rev-parse", "HEAD"]))
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(|| "local".into(), |value| value.trim().into())
}

fn resolve(
    source: &str,
    wanted: Option<&str>,
) -> Result<SourceRoot, Box<dyn std::error::Error + Send + Sync>> {
    let (source, expected) =
        source.split_once("#sha256=").map_or((source, None), |(value, hash)| (value, Some(hash)));

    if let Some(expected) = expected
        && (expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("Archive SHA-256 checksums must contain 64 hexadecimal characters.".into());
    }

    let source_path = source.strip_prefix("file://").map(PathBuf::from);

    if let Some(path) =
        source_path.or_else(|| Path::new(source).exists().then(|| PathBuf::from(source)))
    {
        let path = std::fs::canonicalize(path)?;

        if path.is_file() {
            if !archive(&path) {
                return Err(
                    "Plugin source files must be .tar, .tar.gz, .tgz, or .zip archives.".into()
                );
            }

            let temp = archive_temp()?;

            if let Err(error) = verify_archive(&path, expected) {
                let _ = std::fs::remove_dir_all(&temp);
                return Err(error);
            }

            if let Err(error) = unpack(&path, &temp) {
                let _ = std::fs::remove_dir_all(&temp);
                return Err(error);
            }

            let root = match archive_root(&temp) {
                Ok(root) => root,

                Err(error) => {
                    let _ = std::fs::remove_dir_all(&temp);
                    return Err(error);
                }
            };

            return Ok(SourceRoot { path: root, temp: Some(temp) });
        }

        if let Some(wanted) = wanted {
            let actual = revision(&path);

            if wanted != "local" && (actual == "local" || actual != wanted) {
                return Err(format!("Source revision does not match {wanted}.").into());
            }
        }

        return Ok(SourceRoot { path, temp: None });
    }

    let url = source.strip_prefix("git+").unwrap_or(source);

    if archive_url(url) {
        if !url.starts_with("https://") {
            return Err("Remote archive sources must use HTTPS.".into());
        }

        let Some(expected) = expected else {
            return Err("Remote archive sources require a #sha256= checksum.".into());
        };

        let temp = archive_temp()?;
        let archive_name = url
            .split('?')
            .next()
            .and_then(|value| value.rsplit('/').next())
            .filter(|value| !value.is_empty())
            .unwrap_or("source.tar");

        let archive_path = temp.join(archive_name);
        let result = (|| {
            download(url, &archive_path)?;
            verify_archive(&archive_path, Some(expected))?;
            unpack(&archive_path, &temp)?;
            let root = archive_root(&temp)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(root)
        })();

        return match result {
            Ok(root) => Ok(SourceRoot { path: root, temp: Some(temp) }),

            Err(error) => {
                let _ = std::fs::remove_dir_all(&temp);
                Err(error)
            }
        };
    }

    if embedded(url) {
        return Err("Plugin source URLs must not contain embedded credentials.".into());
    }

    if url.starts_with("http://") {
        return Err("Remote Git sources must use HTTPS or SSH.".into());
    }

    let git = url.starts_with("https://")
        || url.starts_with("ssh://")
        || url.starts_with("git@")
        || url.starts_with("file://")
        || url.ends_with(".git");

    if !git {
        return Err("Remote plugin sources must use a Git URL.".into());
    }

    let staging = temp("crabbot-source")?;
    let result = (|| {
        let repo = staging.join("repo");
        let output = command_output_limited(
            git_command().args(["clone", "--depth", "1", "--no-tags"]).arg(url).arg(&repo),
            Some(&staging),
            ARCHIVE_EXPANDED,
        )?;

        if !output.status.success() {
            return Err(format!(
                "Git source could not be cloned: {}",
                sentence(redact(&output.stderr))
            )
            .into());
        }

        if let Some(wanted) = wanted.filter(|value| !value.is_empty()) {
            let checkout = command_output_limited(
                git_command().args(["-C"]).arg(&repo).args(["checkout", "--detach", wanted]),
                Some(&staging),
                ARCHIVE_EXPANDED,
            )?;

            if !checkout.status.success() {
                let fetch = command_output_limited(
                    git_command()
                        .args(["-C"])
                        .arg(&repo)
                        .args(["fetch", "--depth", "1", "origin", wanted]),
                    Some(&staging),
                    ARCHIVE_EXPANDED,
                )?;

                if !fetch.status.success() {
                    return Err(format!(
                        "Git source revision could not be fetched: {}",
                        sentence(redact(&fetch.stderr))
                    )
                    .into());
                }

                let checkout = command_output_limited(
                    git_command().args(["-C"]).arg(&repo).args(["checkout", "--detach", wanted]),
                    Some(&staging),
                    ARCHIVE_EXPANDED,
                )?;

                if !checkout.status.success() {
                    return Err(
                        format!("Git source revision could not be checked out: {wanted}.").into()
                    );
                }
            }
        }

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(SourceRoot {
            path: repo,
            temp: Some(staging.clone()),
        })
    })();

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }

    result
}

fn archive(path: &Path) -> bool {
    let value = path.to_string_lossy().to_ascii_lowercase();
    archive_url(&value)
}

fn archive_url(value: &str) -> bool {
    let value = value.split('?').next().unwrap_or(value);
    value.ends_with(".tar")
        || value.ends_with(".tar.gz")
        || value.ends_with(".tgz")
        || value.ends_with(".zip")
}

fn canonical_source(source: &str) -> String {
    let (base, fragment) =
        source.split_once('#').map_or((source, None), |(base, fragment)| (base, Some(fragment)));

    let path = base
        .strip_prefix("file://")
        .map(PathBuf::from)
        .or_else(|| Path::new(base).exists().then(|| PathBuf::from(base)));

    let base = path.and_then(|path| std::fs::canonicalize(path).ok()).map_or_else(
        || base.to_owned(),
        |path| {
            if source.starts_with("file://") {
                format!("file://{}", path.display())
            } else {
                path.display().to_string()
            }
        },
    );

    fragment.map_or(base.clone(), |fragment| format!("{base}#{fragment}"))
}

fn embedded(value: &str) -> bool {
    let (scheme, rest) = if let Some(rest) = value.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = value.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = value.strip_prefix("ssh://") {
        ("ssh", rest)
    } else {
        return false;
    };

    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let Some((user, _)) = authority.split_once('@') else {
        return false;
    };

    scheme != "ssh" || user.contains(':')
}

fn archive_temp() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    temp("crabbot-archive")
}

fn temp(prefix: &str) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..8 {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)?;
        let suffix = bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{suffix}", std::process::id()));

        match std::fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    let mut permissions = std::fs::metadata(&path)?.permissions();
                    permissions.set_mode(0o700);
                    std::fs::set_permissions(&path, permissions)?;
                }

                return Ok(std::fs::canonicalize(path)?);
            }

            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }

    Err("Could not create a private temporary directory.".into())
}

fn verify_archive(
    path: &Path,
    expected: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::fs::metadata(path)?.len() > ARCHIVE_LIMIT {
        return Err("Archive exceeds the size limit.".into());
    }

    let Some(expected) = expected else {
        return Err("Archive sources require a #sha256= checksum.".into());
    };

    let mut hash = Sha256::new();
    hash.update(std::fs::read(path)?);
    let actual = format!("{:x}", hash.finalize());

    if actual != expected.to_ascii_lowercase() {
        return Err("Archive checksum did not match the requested SHA-256.".into());
    }

    Ok(())
}

fn unpack(
    archive_path: &Path,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_archive(archive_path)?;
    archive_bounds(archive_path)?;
    let name = archive_path.to_string_lossy().to_ascii_lowercase();
    let (program, args): (&str, Vec<String>) = if name.ends_with(".zip") {
        (
            "unzip",
            vec![
                "-q".into(),
                archive_path.display().to_string(),
                "-d".into(),
                destination.display().to_string(),
            ],
        )
    } else if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        (
            "tar",
            vec![
                "-xzf".into(),
                archive_path.display().to_string(),
                "-C".into(),
                destination.display().to_string(),
            ],
        )
    } else {
        (
            "tar",
            vec![
                "-xf".into(),
                archive_path.display().to_string(),
                "-C".into(),
                destination.display().to_string(),
            ],
        )
    };

    let output = command_output(std::process::Command::new(program).args(args))?;

    if !output.status.success() {
        return Err(
            format!("Archive extraction failed: {}", sentence(redact(&output.stderr))).into()
        );
    }

    safe_archive(destination)?;
    Ok(())
}

fn archive_bounds(path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let name = path.to_string_lossy().to_ascii_lowercase();
    let (program, args): (&str, Vec<String>) = if name.ends_with(".zip") {
        ("unzip", vec!["-l".into(), path.display().to_string()])
    } else if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        ("tar", vec!["-tvzf".into(), path.display().to_string()])
    } else {
        ("tar", vec!["-tvf".into(), path.display().to_string()])
    };

    let output = archive_listing(program, &args)?;
    let mut files = 0_usize;
    let mut bytes = 0_u64;

    for line in String::from_utf8_lossy(&output).lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();

        if fields.is_empty() {
            continue;
        }

        let size = if name.ends_with(".zip") {
            fields.first().and_then(|value| value.parse::<u64>().ok()).filter(|_| fields.len() >= 4)
        } else {
            tar_size(&fields)
        };

        let Some(size) = size else {
            if !name.ends_with(".zip") {
                return Err("Archive size listing could not be parsed.".into());
            }

            continue;
        };

        files = files.saturating_add(1);
        bytes = bytes.saturating_add(size);

        if files > ARCHIVE_FILES || bytes > ARCHIVE_EXPANDED {
            return Err("Archive expands beyond its file or size limits.".into());
        }
    }

    Ok(())
}

fn tar_size(fields: &[&str]) -> Option<u64> {
    fields.iter().enumerate().skip(1).find_map(|(index, field)| {
        if !tar_date(field) {
            return None;
        }

        fields[..index].iter().rev().find_map(|value| value.parse::<u64>().ok())
    })
}

const TAR_MONTHS: [&str; 12] =
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

fn tar_date(value: &str) -> bool {
    let bytes = value.as_bytes();

    let iso_date = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..].iter().all(u8::is_ascii_digit);

    iso_date || TAR_MONTHS.contains(&value)
}

fn validate_archive(path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let name = path.to_string_lossy().to_ascii_lowercase();
    let (program, args): (&str, Vec<String>) = if name.ends_with(".zip") {
        ("unzip", vec!["-Z1".into(), path.display().to_string()])
    } else if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        ("tar", vec!["-tzf".into(), path.display().to_string()])
    } else {
        ("tar", vec!["-tf".into(), path.display().to_string()])
    };

    let output = archive_listing(program, &args)?;

    for value in String::from_utf8_lossy(&output).lines() {
        let path = std::path::Path::new(value.trim());

        if path.is_absolute()
            || path.components().any(|part| part == std::path::Component::ParentDir)
        {
            return Err("Archive contains an unsafe path.".into());
        }
    }

    validate_archive_types(path, name.ends_with(".zip"))
}

fn validate_archive_types(
    path: &Path,
    zip: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (program, args): (&str, Vec<String>) = if zip {
        ("unzip", vec!["-Z".into(), "-v".into(), path.display().to_string()])
    } else {
        ("tar", vec!["-tvf".into(), path.display().to_string()])
    };

    let output = archive_listing(program, &args)?;

    for line in String::from_utf8_lossy(&output).lines() {
        let kind = if zip {
            line.strip_prefix("  Unix file attributes (")
                .and_then(|value| value.split_once("): "))
                .and_then(|(_, value)| value.trim_start().as_bytes().first().copied())
        } else {
            line.as_bytes().first().copied()
        };

        if kind.is_some_and(|kind| !matches!(kind, b'-' | b'd')) {
            return Err("Archive contains a link or special file, which is not allowed.".into());
        }
    }

    Ok(())
}

fn archive_listing(
    program: &str,
    args: &[String],
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let output = command_output(std::process::Command::new(program).args(args))?;

    if !output.status.success() {
        return Err("Archive listing failed.".into());
    }

    Ok(output.stdout)
}

fn safe_archive(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = std::fs::canonicalize(root)?;
    let mut files = 0_usize;
    let mut bytes = 0_u64;
    safe_archive_at(&root, &mut files, &mut bytes)
}

fn safe_archive_at(
    root: &Path,
    files: &mut usize,
    bytes: &mut u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;

        if kind.is_symlink() {
            return Err("Archive contains a symbolic link, which is not allowed.".into());
        }

        #[cfg(unix)]
        if kind.is_file() && std::os::unix::fs::MetadataExt::nlink(&std::fs::metadata(&path)?) > 1 {
            return Err("Archive contains a hard link, which is not allowed.".into());
        }

        if !std::fs::canonicalize(&path)?.starts_with(root) {
            return Err("Archive contains a path outside its extraction root.".into());
        }

        if kind.is_dir() {
            safe_archive_at(&path, files, bytes)?;
        } else if kind.is_file() {
            *files = files.saturating_add(1);
            *bytes = bytes.saturating_add(std::fs::metadata(&path)?.len());

            if *files > ARCHIVE_FILES || *bytes > ARCHIVE_EXPANDED {
                return Err("Archive expands beyond its file or size limits.".into());
            }
        }
    }

    Ok(())
}

fn archive_root(temp: &Path) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if temp.join("crabbot-plugin.toml").is_file() {
        return Ok(temp.to_path_buf());
    }

    let roots = std::fs::read_dir(temp)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join("crabbot-plugin.toml").is_file())
        .collect::<Vec<_>>();

    if roots.len() == 1 {
        Ok(roots[0].clone())
    } else {
        Err("Archive must contain one plugin root with crabbot-plugin.toml.".into())
    }
}

fn download(url: &str, destination: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let connect = DOWNLOAD_CONNECT.to_string();
    let time = DOWNLOAD_TIME.to_string();
    let root = destination.parent().ok_or("Archive destination has no staging directory.")?;
    let output = match command_output_limited(
        std::process::Command::new("curl")
            .args([
                "--fail",
                "--silent",
                "--show-error",
                "--location",
                "--proto",
                "=https",
                "--tlsv1.2",
                "--connect-timeout",
                &connect,
                "--max-time",
                &time,
                "--max-filesize",
                &ARCHIVE_LIMIT.to_string(),
                "--output",
            ])
            .arg(destination)
            .arg(url),
        Some(root),
        ARCHIVE_LIMIT,
    ) {
        Ok(output) => output,

        Err(error) => {
            let _ = std::fs::remove_file(destination);
            return Err(error.into());
        }
    };

    if !output.status.success() {
        return Err(format!("Archive download failed: {}", sentence(redact(&output.stderr))).into());
    }

    if std::fs::metadata(destination)?.len() > ARCHIVE_LIMIT {
        let _ = std::fs::remove_file(destination);
        return Err("Archive download exceeded the size limit.".into());
    }

    Ok(())
}

fn redact(value: &[u8]) -> String {
    let mut text = String::from_utf8_lossy(value).into_owned();

    for scheme in ["https://", "http://", "ssh://"] {
        let mut cursor = 0;

        while let Some(found) = text[cursor..].find(scheme) {
            let start = cursor + found;
            let tail = &text[start + scheme.len()..];
            let end = tail
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '"' | '\'' | ')' | ']')
                })
                .map_or(text.len(), |offset| start + scheme.len() + offset);
            let host = &text[start + scheme.len()..end];

            if let Some(at) = host.find('@') {
                let begin = start + scheme.len();
                text.replace_range(begin..begin + at, "[redacted]");
                cursor = begin + "[redacted]".len();
            } else {
                cursor = end;
            }
        }
    }

    text.lines()
        .map(|line| {
            if line.to_ascii_lowercase().contains("authorization:") {
                "Authorization: [redacted]"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn link_binary(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, destination)
    }

    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(source, destination)
            .or_else(|_| std::fs::copy(source, destination).map(|_| ()))
    }
}

fn remove(name: Name) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    remove_at(name, &home())
}

fn remove_at(name: Name, home_root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _lock = lock_at(home_root, ".plugins.lock")?;

    if !valid(&name.id) {
        return Err(format!("Invalid plugin ID: {}.", name.id).into());
    }

    if !name.yes {
        return Err("Removing a plugin requires --yes.".into());
    }

    let mut lock = load_lock_at(home_root)?;
    let previous_lock = lock.clone();
    let dest = home_root.join("plugins").join(&name.id);
    let plugins = home_root.join("plugins");
    let backup = plugins.join(format!(".{}.backup-remove-{}", name.id, now()));
    let had_destination = dest.exists();

    if !lock.plugins.contains_key(&name.id) && !had_destination {
        return Err(format!("Plugin {} is not installed.", name.id).into());
    }

    if had_destination {
        std::fs::rename(&dest, &backup)?;
    }

    lock.plugins.remove(&name.id);

    if let Err(error) = save_lock_at(home_root, &lock) {
        let restored_lock = save_lock_at(home_root, &previous_lock);
        let restored_plugin =
            if had_destination { std::fs::rename(&backup, &dest) } else { Ok(()) };

        if let Err(restore) = restored_lock {
            return Err(format!(
                "Could not save the plugin lock: {}. Rollback failed: {}.",
                sentence(error.to_string()),
                sentence(restore.to_string())
            )
            .into());
        }

        if let Err(restore) = restored_plugin {
            return Err(format!(
                "Could not save the plugin lock: {}. Rollback failed: {}.",
                sentence(error.to_string()),
                sentence(restore.to_string())
            )
            .into());
        }

        return Err(error);
    }

    if had_destination {
        let _ = std::fs::remove_dir_all(backup);
    }

    Ok(())
}

fn load_lock_at(root: &Path) -> Result<Lock, Box<dyn std::error::Error + Send + Sync>> {
    let path = root.join("plugins.lock");

    #[cfg(windows)]
    recover_secure_backup(&path)?;

    let bytes = match read_bounded(&path, LOCK_LIMIT) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Lock::default()),
        Err(error) => return Err(error.into()),
    };

    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(windows)]
fn recover_secure_backup(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }

    let Some(parent) = path.parent() else {
        return Ok(());
    };

    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(());
    };

    let prefix = format!(".{name}.backup-");
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    let mut backups = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .filter(|entry| {
            std::fs::symlink_metadata(entry.path())
                .is_ok_and(|metadata| metadata.file_type().is_file())
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    backups.sort();

    if let Some(backup) = backups.into_iter().next() {
        std::fs::rename(backup, path)?;
    }

    Ok(())
}

fn save_lock_at(root: &Path, lock: &Lock) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(root)?;
    let bytes = serde_json::to_vec_pretty(lock)?;

    if bytes.len() as u64 > LOCK_LIMIT {
        return Err("Plugin lock exceeds the size limit.".into());
    }

    secure(root.join("plugins.lock"), bytes)?;

    Ok(())
}

fn secure(path: impl AsRef<Path>, content: impl AsRef<[u8]>) -> std::io::Result<()> {
    save_file(path, content)
}

fn list(json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    list_at(&home(), json)
}

fn list_at(root: &Path, json: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = root.join("plugins");
    let mut rows = Vec::new();

    if root.exists() {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }

            let id = entry.file_name().to_string_lossy().into_owned();
            let manifest = read_manifest_checked(&path)
                .map_err(|error| format!("Plugin {id} is invalid: {error}."))?;
            rows.push(manifest);
        }
    }

    rows.sort_by(|a, b| a.id.cmp(&b.id));

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "items": rows,
            }))?
        );
    } else if rows.is_empty() {
        println!("No plugins installed.");
    } else {
        for row in rows {
            let commands =
                row.commands.iter().map(|command| command.name.as_str()).collect::<Vec<_>>();

            if commands.is_empty() {
                println!("{} {} ({}).", row.id, row.version, row.capabilities.join(", "));
            } else {
                println!(
                    "{} {} ({}, commands: {}).",
                    row.id,
                    row.version,
                    row.capabilities.join(", "),
                    commands.join(", ")
                );
            }
        }
    }

    Ok(())
}

fn read_manifest(root: &Path) -> Option<Manifest> {
    read_manifest_checked(root).ok()
}

fn read_manifest_checked(
    root: &Path,
) -> Result<Manifest, Box<dyn std::error::Error + Send + Sync>> {
    let path = root.join("crabbot-plugin.toml");
    let bytes = read_bounded(&path, MANIFEST_LIMIT).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("manifest not found at {}", path.display())
        } else {
            format!("could not read manifest at {}: {error}", path.display())
        }
    })?;
    let manifest: Manifest =
        toml::from_slice(&bytes).map_err(|error| format!("manifest parse failed: {error}"))?;
    manifest.validate().map_err(|error| format!("manifest validation failed: {error}"))?;
    Ok(manifest)
}

fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;

    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "File cannot be a symbolic link.",
        ));
    }

    if !metadata.is_file() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "File is not regular."));
    }

    if metadata.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "File exceeds the size limit.",
        ));
    }

    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;

    if bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "File exceeds the size limit.",
        ));
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        ARCHIVE_LIMIT, BANNER, CRABFILE_VERSION, Cancellation, Cli, Command, CommandSpec,
        CompletionShell, Config, CrabPlugin, Crabfile, CrabfileExport, CrabfileImport,
        CrabfileValidate, DeliveryCommand, DoctorArgs, Fork, Id, InitArgs, Live, Manifest, NAME,
        Name, Output, Plugins, Process, ServiceCommand, SessionCommand, SessionDelete,
        SessionModel, SessionNew, Sha256, Source, Stop, answer, archive, archive_root, archive_url,
        assistant, binary_at, canonical_source, changed, channel_message_id,
        command_output_limited, commit_event, completion_name_from, crabfile_output_path,
        daemon_lock, default_crabfile_path, delivery_at, delivery_request, download, embedded,
        ensure_home, env_for, export_crabfile_at, generate_completion, import_crabfile_at, init_at,
        installed_at, isolate_at, local_session, plugin_binary, read_manifest, reclaim_worktrees,
        recover, recover_plugins, redact, resolve, restart_tool, revision, safe_archive,
        send_params, service_at, service_at_with, service_environment_from, service_path_value,
        service_text, session_at, stream_fits, tool, update_at, validate_archive, verify_archive,
        write_debug_report_at,
    };

    use base64::Engine;
    use clap::{CommandFactory, Parser, error::ErrorKind};
    use crabbot_core::types::{
        Capability, Content, Event, Message, ModelReply, Protocol, Request, Role,
    };

    use sha2::Digest;
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn test_root(label: &str) -> PathBuf {
        let nonce =
            SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |value| value.as_nanos());
        std::env::temp_dir().join(format!("crabbot-{label}-{}-{nonce}", std::process::id()))
    }

    fn approval_finished(sessions: &Arc<Mutex<super::state::Store>>) -> bool {
        let Ok(store) = sessions.lock() else {
            return false;
        };

        let Some(session) = store.sessions.get("telegram-7") else {
            return false;
        };

        session.status == "idle"
            && session.messages.iter().any(|message| {
                message.content.iter().any(|content| {
                    matches!(content, Content::Text { text } if text == "The note was written.")
                })
            })
    }

    async fn registry(processes: impl IntoIterator<Item = Process>) -> Plugins {
        let plugins = Plugins::default();

        for process in processes {
            plugins.insert(Live::new(process)).await;
        }

        plugins
    }

    async fn stop_registry(plugins: &Plugins) {
        for (_, plugin) in plugins.all().await {
            plugin.stop().await.unwrap();
        }
    }

    #[test]
    fn debug_report_is_redacted_and_private() {
        let root = test_root("debug-report");
        let error = std::io::Error::other(
            "request failed at https://user:secret@example.com/path Authorization: Bearer token",
        );
        let path = write_debug_report_at(&root, "ask", &error, Duration::from_millis(12)).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("command: ask"));
        assert!(text.contains("elapsed_ms: 12"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("Bearer token"));
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;
        #[cfg(unix)]
        assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o600);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn formats_status_as_aligned_lines() {
        let value = serde_json::json!({
            "version": super::VERSION,
            "health": "attention",
            "daemon": "stopped",
            "intelligence": {
                "status": "configured",
                "plugins": ["codex", "gemini"]
            },
            "messaging": {
                "status": "configured",
                "plugins": ["telegram", "discord"]
            },
        });

        assert_eq!(
            super::status_text(&value, 0),
            format!(
                "Version:       {}\nHealth:        attention\nDaemon:        stopped\nIntelligence:  configured (codex, gemini)\nMessaging:     configured (telegram, discord)\nPlugins:       0 installed",
                super::VERSION
            )
        );

        assert_eq!(
            super::capability_text(&serde_json::json!({
                "status": "not configured",
                "plugins": [],
            })),
            "not configured"
        );
    }

    #[test]
    fn advisory_lock_releases_after_drop() {
        let root = test_root("lock");
        let first = daemon_lock(&root).unwrap();
        drop(first);
        assert!(daemon_lock(&root).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn aborts_process_when_staging_exceeds_limit() {
        let root = test_root("staging-limit");
        fs::create_dir_all(&root).unwrap();
        let output = command_output_limited(
            std::process::Command::new("sh").args([
                "-c",
                "printf 123456 > \"$1\"; sleep 1",
                "sh",
                root.join("payload").to_str().unwrap(),
            ]),
            Some(&root),
            5,
        );
        assert_eq!(output.unwrap_err().kind(), std::io::ErrorKind::FileTooLarge);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn offline_lock_ignores_stale_ipc_files() {
        let root = test_root("offline-lock");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("ipc.token"), "stale").unwrap();
        fs::write(root.join("ipc.port"), "1").unwrap();

        let lock = super::offline_lock(&root).unwrap();

        drop(lock);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn default_is_safe() {
        let config = Config::default();
        assert_eq!(config.update, "prompt");
        assert!(!config.shell);
        assert_eq!(config.approval, "off");
        assert!(config.validate().is_ok());
        assert!(
            Config { update: "unsafe".into(), shell: false, ..Config::default() }
                .validate()
                .is_err()
        );

        for update in ["off", "check", "auto"] {
            assert!(
                Config { update: update.into(), shell: false, ..Config::default() }
                    .validate()
                    .is_ok()
            );
        }

        for approval in ["off", "prompt", "auto"] {
            assert!(Config { approval: approval.into(), ..Config::default() }.validate().is_ok());
        }

        assert!(Config { approval: "unsafe".into(), ..Config::default() }.validate().is_err());
    }

    #[test]
    fn crabfile_validation_reports_the_first_error() {
        let mut file =
            Crabfile { version: "0.2".into(), config: Config::default(), plugins: vec![] };
        assert_eq!(file.validate().unwrap_err(), "unsupported version 0.2 (expected 0.1)");

        file.version = CRABFILE_VERSION.into();
        file.config.update = "invalid".into();
        assert!(file.validate().unwrap_err().starts_with("configuration:"));

        file.config = Config::default();
        file.plugins.push(CrabPlugin {
            id: "Bad ID".into(),
            source: "local".into(),
            revision: String::new(),
            version: "0.1.0".into(),
            capabilities: vec![],
        });

        assert!(file.validate().unwrap_err().contains("invalid ID"));
    }

    #[test]
    fn covers_runtime_helpers() {
        assert_eq!(super::sentence("hello"), "Hello.");
        assert_eq!(super::sentence("Already!"), "Already!");
        assert_eq!(super::sentence(""), ".");

        assert_eq!(
            super::diagnostic("https://user:secret@example.com/path"),
            "https://[redacted]@example.com/path"
        );

        assert_eq!(
            super::redact_diagnostic("Authorization: Bearer secret"),
            "Authorization: [redacted]"
        );

        for capability in [
            Capability::Model,
            Capability::Vision,
            Capability::Channel,
            Capability::Store,
            Capability::Memory,
            Capability::Timer,
            Capability::Tool,
            Capability::Mcp,
            Capability::Speech,
            Capability::Client,
            Capability::Resource,
            Capability::Agent,
        ] {
            assert!(!super::capability_name(&capability).is_empty());
        }

        for name in [
            "help",
            "init",
            "doctor",
            "status",
            "version",
            "completion",
            "plugin",
            "session",
            "delivery",
            "service",
            "ask",
            "export",
            "validate",
            "import",
        ] {
            assert!(super::native_command(name));
        }

        assert!(!super::native_command("custom"));
        assert!(super::valid("memory-2"));
        assert!(!super::valid("Memory"));
        assert!(!super::valid(""));

        for approval in ["off", "prompt", "auto"] {
            let config = Config { approval: approval.into(), ..Config::default() };
            assert_eq!(config.approval_mode().enabled(), approval != "off");
        }

        assert_eq!(super::ApprovalMode::Off, super::ApprovalMode::Off);
        assert!(super::plugin_name("memory").contains("memory"));
        assert_eq!(super::command_label(&Command::Init(InitArgs { force: false })), "init");
        assert_eq!(super::command_label(&Command::Doctor(DoctorArgs { fix: false })), "doctor");
        assert_eq!(super::command_label(&Command::Status(Output { json: false })), "status");
        assert_eq!(super::command_label(&Command::Version(Output { json: false })), "version");
        assert_eq!(
            super::command_label(&Command::Completion { shell: CompletionShell::Bash }),
            "completion"
        );

        assert_eq!(
            super::command_label(&Command::Plugin {
                command: super::PluginCommand::List(Output { json: false })
            }),
            "plugin"
        );

        assert_eq!(
            super::command_label(&Command::Session {
                command: SessionCommand::List(Output { json: false })
            }),
            "session"
        );

        assert_eq!(
            super::command_label(&Command::Delivery {
                command: super::DeliveryCommand::List(Output { json: false })
            }),
            "delivery"
        );

        assert_eq!(
            super::command_label(&Command::Service { command: Some(ServiceCommand::Status) }),
            "service"
        );

        assert_eq!(
            super::command_label(&Command::Export(super::CrabfileExport {
                destination: None,
                path: None,
                force: false,
            })),
            "export"
        );

        assert_eq!(
            super::command_label(&Command::Validate(super::CrabfileValidate { path: None })),
            "validate"
        );

        assert_eq!(
            super::command_label(&Command::Import(super::CrabfileImport {
                path: None,
                yes: false,
                force: false
            })),
            "import"
        );

        assert_eq!(super::command_label(&Command::External(vec!["custom".into()])), "external");

        let session = SessionCommand::List(Output { json: false });
        assert!(matches!(
            super::with_session_json(session, true),
            SessionCommand::List(Output { json: true })
        ));

        let delivery = super::DeliveryCommand::List(Output { json: false });
        assert!(matches!(
            super::with_delivery_json(delivery, true),
            super::DeliveryCommand::List(Output { json: true })
        ));

        let command = super::git_command();
        assert_eq!(command.get_program(), "git");

        assert!(
            super::command_output_limited(&mut std::process::Command::new("true"), None, 0).is_ok()
        );

        assert_eq!(super::read_output(std::io::Cursor::new(b"output")).unwrap(), b"output");
        assert!(
            super::read_output(std::io::Cursor::new(vec![b'x'; super::COMMAND_OUTPUT + 1]))
                .is_err()
        );

        let root = test_root("helpers");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("file"), b"123").unwrap();
        assert_eq!(super::staging_size(&root).unwrap(), 3);
        assert_eq!(
            super::staging_size(&root.join("missing")).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        assert!(super::read_bounded(&root.join("file"), 3).is_ok());
        assert!(super::read_bounded(&root.join("file"), 2).is_err());
        assert!(super::read_bounded(&root.join("missing"), 3).is_err());
        assert!(super::read_bounded(&root, 3).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn config_deserializes_with_defaults() {
        let config: Config = toml::from_str("[channels.telegram]\nallow = [\"123\"]\n").unwrap();

        assert_eq!(config.update, "prompt");
        assert!(!config.shell);
        assert_eq!(config.approval, "off");
        assert_eq!(config.channels["telegram"].allow, vec!["123"]);
        assert!(toml::from_str::<Config>("unknown = true\n").is_err());
    }

    #[test]
    fn validates_manifests_and_environment() {
        let manifest = Manifest {
            id: "tools".into(),
            version: "0.1.0".into(),
            protocol: Protocol::CURRENT,
            capabilities: vec!["tool".into()],
            permissions: vec!["filesystem".into()],
            secrets: vec!["CRABBOT_TOOLS_KEY".into()],
            commands: Vec::new(),
        };

        assert!(manifest.validate().is_ok());
        let mut invalid = manifest;
        invalid.id = "Tools".into();
        assert!(invalid.validate().is_err());
        invalid.id = "tools".into();
        invalid.version.clear();
        assert!(invalid.validate().is_err());
        invalid.version = "0.1.0".into();
        invalid.protocol = Protocol { major: 9, minor: 0 };
        assert!(invalid.validate().is_err());
        invalid.protocol = Protocol::CURRENT;
        invalid.capabilities = vec!["tool".into(), "tool".into()];
        assert!(invalid.validate().is_err());
        invalid.capabilities = vec!["unknown".into()];
        assert!(invalid.validate().is_err());
        invalid.capabilities = vec!["tool".into()];
        invalid.permissions = vec!["unknown".into()];
        assert!(invalid.validate().is_err());
        invalid.permissions.clear();
        invalid.secrets = vec!["bad-name".into()];
        assert!(invalid.validate().is_err());
        invalid.secrets = vec!["OPENAI_API_KEY".into()];
        assert!(invalid.validate().is_err());
        invalid.secrets = vec!["CRABBOT_TOOLS_KEY".into(), "CRABBOT_TOOLS_KEY".into()];
        assert!(invalid.validate().is_err());

        let values = env_for(
            &Manifest {
                id: "tools".into(),
                version: "0.1.0".into(),
                protocol: Protocol::CURRENT,
                capabilities: vec!["tool".into()],
                permissions: Vec::new(),
                secrets: Vec::new(),
                commands: Vec::new(),
            },
            &Config::default(),
            Path::new("/tmp/crabbot-home"),
        );
        assert!(values.iter().any(|(key, value)| key == "CRABBOT_SHELL" && value == "off"));

        let values = env_for(
            &Manifest {
                id: "codex".into(),
                version: "0.1.0".into(),
                protocol: Protocol::CURRENT,
                capabilities: vec!["model".into()],
                permissions: Vec::new(),
                secrets: Vec::new(),
                commands: Vec::new(),
            },
            &Config::default(),
            Path::new("/tmp/crabbot-home"),
        );

        if std::env::var_os("CRABBOT_HOME").is_none() {
            assert!(
                values
                    .iter()
                    .any(|(key, value)| key == "CRABBOT_HOME" && value == "/tmp/crabbot-home")
            );
        }

        assert!(!values.iter().any(|(key, _)| { matches!(key.as_str(), "HOME" | "USERPROFILE") }));
    }

    #[test]
    fn propagates_default_home_with_database_override() {
        let mut values = vec![("CRABBOT_DB".into(), "/tmp/crabbot.db".into())];
        ensure_home(&mut values, Path::new("/tmp/crabbot-home"));
        assert!(
            values.iter().any(|(key, value)| key == "CRABBOT_HOME" && value == "/tmp/crabbot-home")
        );
    }

    #[test]
    fn recovers_locks_and_plugin_staging() {
        let root = std::env::temp_dir().join(format!("crabbot-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("plugins")).unwrap();
        let lock = daemon_lock(&root).unwrap();
        assert!(daemon_lock(&root).is_err());
        drop(lock);
        let stale = root.join("stale.lock");
        fs::write(&stale, "not-a-pid").unwrap();
        let lock = super::lock_at(&root, "stale.lock").unwrap();
        drop(lock);
        fs::create_dir_all(root.join("plugins/.echo.backup-test")).unwrap();
        fs::create_dir_all(root.join("plugins/.echo.stage-test")).unwrap();
        let active = super::lock_at(&root, ".plugins.lock").unwrap();
        let error = recover_plugins(&root).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(root.join("plugins/.echo.stage-test").exists());
        drop(active);
        let mut plugins = BTreeMap::new();
        plugins.insert(
            "echo".into(),
            super::Entry {
                source: "echo".into(),
                revision: "local".into(),
                pinned: false,
                default: false,
                hash: String::new(),
                version: "0.1.0".into(),
                protocol: Protocol::CURRENT,
                capabilities: Vec::new(),
                permissions: Vec::new(),
                secrets: Vec::new(),
                commands: Vec::new(),
                linked: false,
            },
        );
        fs::write(root.join("plugins.lock"), serde_json::to_vec(&super::Lock { plugins }).unwrap())
            .unwrap();
        recover_plugins(&root).unwrap();
        assert!(root.join("plugins/echo").is_dir());
        assert!(!root.join("plugins/.echo.stage-test").exists());
        fs::create_dir_all(root.join("plugins/.gone.backup-remove-test")).unwrap();
        recover_plugins(&root).unwrap();
        assert!(!root.join("plugins/.gone.backup-remove-test").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recovery_obeys_stop_signal() {
        let script = "while IFS= read -r line; do case \"$line\" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"test\",\"version\":\"0.1.0\",\"capabilities\":[]}}' ;; esac; done";
        let process = Process::start_with("sh", ["-c", script]).await.unwrap();
        let process = Live::new(process);
        let stop = Stop::new();
        stop.signal();
        let mut failures = 0;
        assert!(!recover(&process, &mut failures, "test", &stop).await.unwrap());
        process.stop().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_signal_remains_visible_to_later_waiters() {
        let stop = Stop::new();
        stop.signal();
        tokio::time::timeout(std::time::Duration::from_secs(1), stop.notified()).await.unwrap();
    }

    #[test]
    fn covers_routing_helpers() {
        assert!(super::mentions("hello", ""));
        assert!(super::mentions("Hi @Crabbot", "@crabbot"));
        assert!(!super::mentions("Hi crab", "crabbot"));
        assert!(super::same_id(&serde_json::json!(7), "7"));
        assert!(!super::same_id(&serde_json::Value::Null, "7"));
        let cancels = Arc::new(Mutex::new(BTreeMap::new()));
        let first = super::cancellation(&cancels, "session");
        let second = super::cancellation(&cancels, "session");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            send_params("matrix", "delivery", "room", "hello", Some("topic")).unwrap(),
            serde_json::json!({"delivery": "delivery", "chat": "room", "text": "hello", "thread": "topic"})
        );
        let store = super::state::Store::default();
        let missing =
            std::env::temp_dir().join(format!("crabbot-worktrees-{}", std::process::id()));
        reclaim_worktrees(&missing, &store);
    }

    #[test]
    fn service_definition_has_a_restart_policy() {
        let text = service_text(std::path::Path::new("/tmp/crabbot"), &[]);
        assert!(text.contains("/tmp/crabbot"));
        assert!(!text.contains("serve"));
        #[cfg(target_os = "linux")]
        {
            assert!(text.contains("Restart=on-failure"));
            assert!(!text.contains("ExecStart=\\\""));
        }

        let _ = ServiceCommand::Status;
    }

    #[test]
    fn service_definition_preserves_paths_and_uses_credential_file() {
        let environment = vec![
            ("CRABBOT_HOME".into(), "/tmp/crabbot-home".into()),
            ("CRABBOT_ROOT".into(), "/tmp/crabbot-root".into()),
            ("CRABBOT_CREDENTIALS".into(), "/tmp/crabbot-credentials.json".into()),
        ];
        let text = service_text(std::path::Path::new("/tmp/crabbot"), &environment);
        assert!(text.contains("CRABBOT_HOME"));
        assert!(text.contains("/tmp/crabbot-home"));
        assert!(text.contains("CRABBOT_ROOT"));
        assert!(text.contains("/tmp/crabbot-root"));
        assert!(text.contains("CRABBOT_CREDENTIALS"));
        assert!(text.contains("/tmp/crabbot-credentials.json"));
        assert!(!text.contains("secret"));
    }

    #[test]
    fn service_environment_materializes_declared_credentials() {
        let root = test_root("service-env");
        fs::create_dir_all(&root).unwrap();
        let configured = root.join("credentials.json");
        super::secure(&configured, br#"{"CRABBOT_TELEGRAM_TOKEN":"old"}"#).unwrap();
        let mut environment = BTreeMap::new();
        environment.insert("CRABBOT_ROOT".into(), root.display().to_string());
        environment.insert("CRABBOT_MEMORY".into(), "memory.json".into());
        environment.insert("CRABBOT_TIMER".into(), "timer.json".into());
        environment.insert("CRABBOT_DB".into(), "crabbot.db".into());
        environment.insert("CRABBOT_SANDBOX_RUNTIME".into(), "docker".into());
        environment.insert("CRABBOT_SANDBOX_IMAGE".into(), "local/tool:latest".into());
        environment.insert("CRABBOT_CREDENTIALS".into(), configured.display().to_string());
        environment.insert("CRABBOT_CODEX_KEY".into(), "secret".into());
        let service = root.join("crabbot.service");
        let values = service_environment_from(&service, root.clone(), &environment).unwrap();
        assert!(values.iter().any(|(name, value)| {
            name == "CRABBOT_HOME" && value == &root.display().to_string()
        }));

        for (name, path) in [
            ("CRABBOT_MEMORY", "memory.json"),
            ("CRABBOT_TIMER", "timer.json"),
            ("CRABBOT_DB", "crabbot.db"),
        ] {
            let expected = service_path_value(PathBuf::from(path)).unwrap();
            assert!(
                values
                    .iter()
                    .any(|(value_name, value)| { value_name == name && value == &expected })
            );
        }

        let credential_path = super::service_credentials_path(&service);
        assert!(values.iter().any(|(name, value)| {
            name == "CRABBOT_CREDENTIALS" && value == &credential_path.display().to_string()
        }));

        let credentials: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&credential_path).unwrap()).unwrap();
        assert_eq!(credentials["CRABBOT_TELEGRAM_TOKEN"], "old");
        assert_eq!(credentials["CRABBOT_CODEX_KEY"], "secret");
        assert!(
            values
                .iter()
                .any(|(name, value)| name == "CRABBOT_SANDBOX_RUNTIME" && value == "docker")
        );
        assert!(
            values.iter().any(
                |(name, value)| name == "CRABBOT_SANDBOX_IMAGE" && value == "local/tool:latest"
            )
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manages_service_definition() {
        let path = std::env::temp_dir().join(format!("crabbot-service-{}", std::process::id()));
        let _ = fs::remove_file(&path);
        service_at(&path, ServiceCommand::Install(super::ServiceInstall { force: false })).unwrap();
        assert!(path.is_file());
        assert!(
            service_at(&path, ServiceCommand::Install(super::ServiceInstall { force: false }))
                .is_err()
        );
        service_at(&path, ServiceCommand::Install(super::ServiceInstall { force: true })).unwrap();
        service_at(&path, ServiceCommand::Status).unwrap();
        let mut stopped = false;
        service_at_with(
            &path,
            ServiceCommand::Remove(super::ServiceRemove { yes: true }),
            |_, start| {
                assert!(!start);
                stopped = true;
                Ok(())
            },
        )
        .unwrap();
        assert!(stopped);
        assert!(!path.exists());
        service_at(&path, ServiceCommand::Remove(super::ServiceRemove { yes: true })).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_service_definition() {
        let root =
            std::env::temp_dir().join(format!("crabbot-service-link-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let target = root.join("target");
        let path = root.join("service");
        std::os::unix::fs::symlink(&target, &path).unwrap();

        let error =
            service_at(&path, ServiceCommand::Install(super::ServiceInstall { force: false }))
                .unwrap_err()
                .to_string();
        assert!(error.contains("symbolic link"));
        assert!(!target.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discovers_plugins_and_detects_source_changes() {
        let root = std::env::temp_dir().join(format!("crabbot-discovery-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        init_at(&root).unwrap();
        let plugin = root.join("plugins/echo");
        fs::create_dir_all(plugin.join("bin")).unwrap();
        fs::write(
            plugin.join("crabbot-plugin.toml"),
            "id = 'echo'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['model']\n",
        )
        .unwrap();
        let binary = plugin.join("bin").join(super::plugin_name("echo"));
        fs::write(&binary, "binary").unwrap();
        assert_eq!(installed_at(&root), vec!["echo"]);
        assert_eq!(binary_at("echo", &root), Some(binary.clone()));
        assert!(binary_at("../escape", &root).is_none());
        let default_source = root.join("repo/crabbot-plugins/echo");
        let default_binary = root.join("repo/target/debug").join(super::plugin_name("echo"));
        fs::create_dir_all(&default_source).unwrap();
        fs::create_dir_all(default_binary.parent().unwrap()).unwrap();
        fs::write(&default_binary, "default binary").unwrap();
        assert_eq!(plugin_binary(&default_source, "echo", true), Some(default_binary));
        let entry = super::Entry {
            source: plugin.display().to_string(),
            revision: String::new(),
            pinned: false,
            default: false,
            hash: String::new(),
            version: "0.1.0".into(),
            protocol: Protocol::CURRENT,
            capabilities: vec!["model".into()],
            permissions: Vec::new(),
            secrets: Vec::new(),
            commands: Vec::new(),
            linked: false,
        };

        assert!(changed(&root, "echo", &entry).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_modes_are_explicit() {
        let root = std::env::temp_dir().join(format!("crabbot-updates-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        assert!(super::updates(&root, "off").is_ok());
        assert!(super::updates(&root, "check").is_ok());
        assert!(super::updates(&root, "prompt").is_ok());
        assert!(super::updates(&root, "auto").is_ok());
        assert!(super::updates(&root, "invalid").is_err());
        assert!(
            super::changed(
                &root,
                "missing",
                &super::Entry {
                    source: root.join("missing").display().to_string(),
                    revision: String::new(),
                    pinned: false,
                    default: false,
                    hash: String::new(),
                    version: "0.1.0".into(),
                    protocol: Protocol::CURRENT,
                    capabilities: Vec::new(),
                    permissions: Vec::new(),
                    secrets: Vec::new(),
                    commands: Vec::new(),
                    linked: false,
                }
            )
            .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_round_trips() {
        let value = Manifest {
            id: "echo".into(),
            version: "0.1.0".into(),
            protocol: Protocol::CURRENT,
            capabilities: vec!["model".into()],
            permissions: Vec::new(),
            secrets: Vec::new(),
            commands: Vec::new(),
        };

        let text = toml::to_string(&value).unwrap();
        let parsed: Manifest = toml::from_str(&text).unwrap();
        assert_eq!(parsed.id, "echo");
    }

    #[test]
    fn plugin_commands_reserve_native_names_and_detect_duplicates() {
        assert!(super::native_command("status"));
        assert!(!super::native_command("tui"));
        let root = test_root("commands");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let mut lock = super::Lock::default();
        lock.plugins.insert(
            "first".into(),
            super::Entry {
                source: "first".into(),
                revision: String::new(),
                pinned: false,
                default: false,
                hash: String::new(),
                version: "0.1.0".into(),
                protocol: Protocol::CURRENT,
                capabilities: Vec::new(),
                permissions: Vec::new(),
                secrets: Vec::new(),
                commands: vec![CommandSpec {
                    name: "login".into(),
                    description: "Log in.".into(),
                    interactive: false,
                }],

                linked: false,
            },
        );
        super::save_lock_at(&root, &lock).unwrap();
        let help = super::help_text_with_plugins(&root);
        assert!(help.contains("Plugin Commands:"));
        assert!(help.contains("login"));
        assert!(help.find("Plugin Commands:") < help.find("Options:"));
        assert!(
            super::validate_commands(
                &root,
                "second",
                &[CommandSpec {
                    name: "login".into(),
                    description: "Log in.".into(),
                    interactive: false,
                }]
            )
            .is_err()
        );
        assert!(
            super::validate_commands(
                &root,
                "second",
                &[CommandSpec {
                    name: "status".into(),
                    description: "Shadow status.".into(),
                    interactive: false,
                }]
            )
            .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ask_is_registered_only_for_an_installed_model_plugin() {
        let root = test_root("ask-command");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("plugins/fake/bin")).unwrap();
        let mut lock = super::Lock::default();
        lock.plugins.insert(
            "fake".into(),
            super::Entry {
                source: "local".into(),
                revision: "local".into(),
                pinned: false,
                default: false,
                hash: String::new(),
                version: "0.1.0".into(),
                protocol: Protocol::CURRENT,
                capabilities: vec!["model".into()],
                permissions: Vec::new(),
                secrets: Vec::new(),
                linked: false,
                commands: Vec::new(),
            },
        );
        super::save_lock_at(&root, &lock).unwrap();
        assert!(!super::plugin_commands(&root).contains_key("ask"));

        let binary = root.join("plugins/fake/bin").join(super::plugin_name("fake"));
        fs::write(binary, b"plugin").unwrap();
        let commands = super::plugin_commands(&root);
        assert_eq!(commands["ask"][0].0, "__runtime");
        assert_eq!(super::model_plugin_at(&root, Some("fake")).unwrap(), "fake");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn banner_rows_have_consistent_width() {
        let widths = BANNER
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.chars().count())
            .collect::<Vec<_>>();
        assert!(!widths.is_empty());
        assert!(widths.iter().all(|width| *width == widths[0]));
        assert!(BANNER.contains(" ██████╗"));
        assert!(BANNER.contains("╚═════╝"));
        assert!(!BANNER.ends_with('\n'));
    }

    #[test]
    fn cli_exposes_global_modes_and_aliases() {
        let cli =
            Cli::try_parse_from(["crabbot", "--json", "--debug", "--verbose", "status"]).unwrap();
        assert!(cli.json);
        assert!(cli.debug);
        assert!(cli.verbose);

        let cli = Cli::try_parse_from(["crabbot", "version", "--json"]).unwrap();
        assert!(matches!(cli.command, Command::Version(Output { json: true })));

        let cli = Cli::try_parse_from(["crabbot", "status", "--json"]).unwrap();
        assert!(matches!(cli.command, Command::Status(Output { json: true })));

        let cli = Cli::try_parse_from(["crabbot", "--json", "doctor"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "doctor", "--fix"]).unwrap();
        assert!(matches!(cli.command, Command::Doctor(DoctorArgs { fix: true })));

        let cli = Cli::try_parse_from(["crabbot", "service", "status", "--json"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "service", "install", "--force"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Service {
                command: Some(ServiceCommand::Install(super::ServiceInstall { force: true }))
            }
        ));

        let cli = Cli::try_parse_from(["crabbot", "service", "remove", "--yes"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Service {
                command: Some(ServiceCommand::Remove(super::ServiceRemove { yes: true }))
            }
        ));

        let cli = Cli::try_parse_from(["crabbot", "plugin", "remove", "tools", "--yes", "--json"])
            .unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "session", "new", "main", "--json"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "session", "show", "main", "--json"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "session", "delete", "main", "--yes", "--json"])
            .unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "delivery", "retry", "item", "--yes", "--json"])
            .unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "export", "--json"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["crabbot", "export", "."]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Export(CrabfileExport { destination: Some(path), path: None, force: false })
                if path == Path::new(".")
        ));

        let cli = Cli::try_parse_from(["crabbot", "export", "--force"]).unwrap();
        assert!(matches!(cli.command, Command::Export(CrabfileExport { force: true, .. })));

        let cli =
            Cli::try_parse_from(["crabbot", "validate", "--path", "Crabfile", "--json"]).unwrap();
        assert!(cli.json);
        assert!(
            matches!(cli.command, Command::Validate(CrabfileValidate { path: Some(path) }) if path == Path::new("Crabfile"))
        );

        assert_eq!(default_crabfile_path(), Path::new("./Crabfile"));
        assert_eq!(
            crabfile_output_path(None, Some(PathBuf::from("."))).unwrap(),
            Path::new("./Crabfile")
        );
        assert!(
            crabfile_output_path(Some(PathBuf::from("one")), Some(PathBuf::from("two"))).is_err()
        );

        let help = Cli::command().render_help().to_string();
        assert!(help.contains("Initialize the Crabbot home directory and configuration."));
        assert!(help.contains("-h, --help"));
        assert!(help.contains("alias: -H"));
        assert!(help.contains("-v, --version"));
        assert!(help.contains("--verbose"));
        assert!(!help.contains("--verbose..."));
        assert!(!help.contains("Plugin Commands:"));

        for argument in ["-h", "-H", "--help"] {
            let error = Cli::try_parse_from(["crabbot", argument]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        }

        for argument in ["-v", "-V", "--version"] {
            let error = Cli::try_parse_from(["crabbot", argument]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DisplayVersion);
        }
    }

    #[test]
    fn root_help_detection_leaves_subcommand_help_to_clap() {
        assert!(super::root_help_requested(&[]));
        assert!(super::root_help_requested(&["help".into()]));
        assert!(super::root_help_requested(&["--json".into(), "help".into()]));
        assert!(super::root_help_requested(&["--help".into()]));
        assert!(!super::root_help_requested(&["help".into(), "status".into()]));
        assert!(!super::root_help_requested(&["status".into(), "--help".into()]));
    }

    #[test]
    fn cli_parses_completion_shells_and_generates_commands() {
        for (name, expected) in [
            ("bash", "complete -F"),
            ("fish", "complete -c crabbot"),
            ("powershell", "Register-ArgumentCompleter"),
            ("zsh", "#compdef crabbot"),
        ] {
            let cli = Cli::try_parse_from(["crabbot", "completion", name]).unwrap();
            let Command::Completion { shell } = cli.command else {
                panic!("completion command was not parsed");
            };

            let mut output = Vec::new();
            generate_completion(shell, &mut Cli::command(), NAME, &mut output);
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(expected), "completion output for {name} was unexpected");
        }

        assert!(Cli::try_parse_from(["crabbot", "completion", "nu"]).is_err());

        assert_eq!(completion_name_from(Some(std::ffi::OsStr::new("/usr/bin/crab"))), "crab");
        assert_eq!(completion_name_from(Some(std::ffi::OsStr::new("C:\\bin\\crab.exe"))), "crab");
        assert_eq!(completion_name_from(Some(std::ffi::OsStr::new("/usr/bin/crabbot"))), NAME);
        assert_eq!(completion_name_from(Some(std::ffi::OsStr::new("other"))), NAME);

        let cli = Cli::try_parse_from(["crabbot", "init", "--force"]).unwrap();
        assert!(matches!(cli.command, Command::Init(InitArgs { force: true })));
    }

    #[test]
    fn init_preserves_existing_state_without_force() {
        let root = test_root("init-force");
        let _ = fs::remove_dir_all(&root);

        super::init_at_with_force(&root, false).unwrap();
        fs::write(root.join("config.toml"), "update = 'auto'\n").unwrap();
        fs::write(root.join("plugins/keep"), "plugin state").unwrap();

        super::init_at_with_force(&root, false).unwrap();
        assert_eq!(fs::read_to_string(root.join("config.toml")).unwrap(), "update = 'auto'\n");
        assert!(root.join("plugins/keep").exists());

        super::init_at_with_force(&root, true).unwrap();
        assert!(
            fs::read_to_string(root.join("config.toml")).unwrap().contains("update = \"prompt\"")
        );
        assert!(root.join("plugins/keep").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn doctor_fix_creates_only_missing_safe_state() {
        let root = test_root("doctor-fix");
        let _ = fs::remove_dir_all(&root);

        super::doctor_at(&root, false, false).unwrap();
        assert!(!root.exists());

        super::doctor_at(&root, true, false).unwrap();
        assert!(root.join("config.toml").is_file());
        assert!(root.join("plugins").is_dir());

        fs::write(root.join("config.toml"), "update = 'auto'\n").unwrap();
        super::doctor_at(&root, true, false).unwrap();
        assert_eq!(fs::read_to_string(root.join("config.toml")).unwrap(), "update = 'auto'\n");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn doctor_health_requires_valid_required_state() {
        assert!(super::doctor_healthy(true, Some(true), true));
        assert!(!super::doctor_healthy(false, None, true));
        assert!(!super::doctor_healthy(true, Some(false), true));
        assert!(!super::doctor_healthy(true, Some(true), false));
    }

    #[test]
    fn command_parser_keeps_prompt_words() {
        let ask =
            super::Ask::try_parse_from(["ask", "--model", "local", "hello", "world"]).unwrap();
        assert_eq!(ask.prompt, vec!["hello", "world"]);
        assert_eq!(ask.plugin, None);

        let ask = super::Ask::try_parse_from(["ask", "hello"]).unwrap();
        assert_eq!(ask.model, "gpt-4o-mini");
        let cli = Cli::try_parse_from(["crabbot", "ask", "hello"]).unwrap();
        assert!(matches!(cli.command, Command::External(args) if args == vec!["ask", "hello"]));
    }

    #[test]
    fn manifest_reader_rejects_wrong_files() {
        let root = std::env::temp_dir().join(format!("crabbot-manifest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("crabbot-plugin.toml"), "id = 'echo'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['model']\n").unwrap();
        assert_eq!(read_manifest(&root).unwrap().id, "echo");
        fs::write(root.join("crabbot-plugin.toml"), "broken = true\n").unwrap();
        assert!(read_manifest(&root).is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plugin_list_rejects_invalid_visible_manifests() {
        let root = test_root("plugin-list-invalid");
        fs::create_dir_all(root.join("plugins/broken")).unwrap();
        fs::write(root.join("plugins/broken/crabbot-plugin.toml"), "broken = true\n").unwrap();

        assert!(super::list_at(&root, true).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plugin_ids_are_single_safe_path_components() {
        assert!(super::valid("telegram"));
        assert!(super::valid("plugin-2"));
        assert!(!super::valid("../escape"));
        assert!(!super::valid("Telegram"));
    }

    #[test]
    fn sentence_keeps_terminal_punctuation() {
        assert_eq!(super::sentence("ready"), "Ready.");
        assert_eq!(super::sentence("Ready"), "Ready.");
        assert_eq!(super::sentence("Ready!"), "Ready!");
        assert_eq!(super::sentence("Ready?"), "Ready?");
        assert_eq!(super::sentence("Ready."), "Ready.");
    }

    #[test]
    fn preserves_tool_calls_in_model_context() {
        let reply = ModelReply {
            text: String::new(),
            stop: "tool".into(),
            input: None,
            output: None,
            events: vec![Event::Tool {
                name: "read".into(),
                args: serde_json::json!({"path": "note.txt"}),
            }],
        };

        assert_eq!(assistant(&reply), "[Tool call read]: {\"path\":\"note.txt\"}");
    }

    #[test]
    fn preserves_stream_text_events_in_model_context() {
        let reply = ModelReply {
            text: String::new(),
            stop: "stop".into(),
            input: None,
            output: None,
            events: vec![
                Event::Text { text: "Hello ".into() },
                Event::Text { text: "world".into() },
                Event::Done { text: "!".into() },
            ],
        };

        assert_eq!(assistant(&reply), "Hello world!");
    }

    #[test]
    fn normalizes_stream_notifications() {
        let note = Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: serde_json::json!({"event": {"kind": "text", "text": "part"}}),
        };

        assert_eq!(super::stream_event(note), Some(Event::Text { text: "part".into() }));
        assert!(
            super::stream_event(Request::Note {
                jsonrpc: "2.0".into(),
                method: "other".into(),
                params: serde_json::json!({}),
            })
            .is_none()
        );
    }

    #[test]
    fn merges_streamed_text_without_duplication() {
        let note = |text: &str| Request::Note {
            jsonrpc: "2.0".into(),
            method: "event".into(),
            params: serde_json::json!({"event": {"kind": "text", "text": text}}),
        };

        let mut reply = ModelReply {
            text: "Hello world".into(),
            stop: "stop".into(),
            input: None,
            output: None,
            events: Vec::new(),
        };

        super::merge_stream(&mut reply, vec![note("Hello "), note("world")]);
        assert_eq!(reply.text, "Hello world");
        assert!(reply.events.is_empty());

        reply.text.clear();
        super::merge_stream(&mut reply, vec![note("Hello "), note("world")]);
        assert_eq!(reply.text, "Hello world");
    }

    #[test]
    fn bounds_model_history_by_frame_size() {
        let messages = (0..40)
            .map(|index| Message {
                id: index.to_string(),
                session: "test".into(),
                role: Role::User,
                sender: None,
                content: vec![Content::Text { text: "x".repeat(256 * 1024) }],
            })
            .collect::<Vec<_>>();
        let request = super::model_request(1, "model", &messages, &[], None).unwrap();
        let encoded = serde_json::to_vec(&request).unwrap();
        assert!(encoded.len() < crabbot_core::jsonl::MAX);
        let retained =
            serde_json::to_value(request).unwrap()["params"]["messages"].as_array().unwrap().len();
        assert!(retained < messages.len());
        let current = Message {
            id: "current".into(),
            session: "test".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "current".into() }],
        };

        let mut history = messages;
        history.push(current.clone());
        let request = super::model_request(1, "model", &history, &[], None).unwrap();
        let value = serde_json::to_value(request).unwrap();
        let retained = value["params"]["messages"].as_array().unwrap();
        assert!(retained.iter().any(|message| message["id"] == "current"));
        let current = Message {
            id: "current".into(),
            session: "test".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "x".repeat(crabbot_core::jsonl::MAX) }],
        };

        assert!(super::model_request(1, "model", &[current], &[], None).is_err());
    }

    #[test]
    fn marks_mutating_tools() {
        assert!(super::mutating("write", &serde_json::json!({})));
        assert!(super::mutating("patch", &serde_json::json!({})));
        assert!(super::mutating("shell", &serde_json::json!({})));
        assert!(super::mutating("git", &serde_json::json!({"args": ["worktree", "add", "path"]})));
        assert!(super::mutating(
            "git",
            &serde_json::json!({"args": ["worktree", "remove", "path"]})
        ));
        assert!(!super::mutating("read", &serde_json::json!({})));
        assert!(!super::mutating("git", &serde_json::json!({"args": ["status"]})));
    }

    #[test]
    fn prepares_approval_text_and_channel_requests() {
        assert!(
            super::approval_text("write", &serde_json::json!({"path": "note.txt"}))
                .contains("write to note.txt")
        );
        assert!(
            super::approval_text("patch", &serde_json::json!({"text": "diff"})).contains("4 bytes")
        );
        assert!(
            super::approval_text("shell", &serde_json::json!({"command": "cargo test"}))
                .contains("cargo test")
        );
        assert!(
            super::approval_text("git", &serde_json::json!({"args": ["status"]}))
                .contains("status")
        );
        assert!(super::approval_text("read", &serde_json::json!({})).contains("read tool"));

        let telegram =
            super::approval_params("telegram", "7", Some("9"), "Approve?", "allow", "deny")
                .unwrap();
        assert_eq!(telegram["chat"], 7);
        assert_eq!(telegram["thread"], "9");
        assert!(super::approval_params("telegram", "bad", None, "Approve?", "a", "d").is_err());

        let discord = super::approval_params(
            "discord",
            "channel",
            Some("thread"),
            "Approve?",
            "allow",
            "deny",
        )
        .unwrap();
        assert_eq!(discord["channel"], "channel");
        assert!(discord.get("thread").is_none());
        assert!(super::approval_params("other", "room", None, "Approve?", "a", "d").is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn opens_the_plugin_restart_circuit() {
        let script = "while IFS= read -r line; do case \"$line\" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"test\",\"version\":\"0.1.0\",\"capabilities\":[]}}' ;; *shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done";
        let process = Process::start_with("sh", ["-c", script]).await.unwrap();
        let process = Live::new(process);
        let mut failures = 3;
        assert!(!recover(&process, &mut failures, "test", &Stop::new()).await.unwrap());
        assert_eq!(failures, 4);
        process.stop().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn streams_text_through_a_persisted_channel_message() {
        let root = test_root("stream-channel");
        fs::create_dir_all(&root).unwrap();
        let log = root.join("requests.log");
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *send*) echo send >> '__LOG__'; printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"message_id":99}}' ;; *edit*) echo edit >> '__LOG__'; printf '%s\n' '{"jsonrpc":"2.0","id":8,"result":{"message_id":99}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#
            .replace("__LOG__", &log.display().to_string())
            .replace("\\\\n", "\\n")
            .replace("\\\"", "\"");
        let process = Process::start_with("sh", ["-c", &script]).await.unwrap();
        let channel = Live::new(process);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        {
            let mut store = sessions.lock().unwrap();
            store.create("main", "model").unwrap();
            store
                .begin(
                    "main",
                    Message {
                        id: "user".into(),
                        session: "main".into(),
                        role: Role::User,
                        sender: Some("sender".into()),
                        content: vec![Content::Text { text: "question".into() }],
                    },
                )
                .unwrap();
        }

        let mut output = super::StreamOutput::new();
        output.apply(super::StreamNotice::Tool("read".into()));
        let mut call = 7;
        super::flush_stream(
            &mut output,
            &channel,
            "telegram",
            &sessions,
            "main",
            "telegram-1",
            "7",
            None,
            &mut call,
        )
        .await
        .unwrap();
        output.apply(super::StreamNotice::Text("answer".into()));
        super::flush_stream(
            &mut output,
            &channel,
            "telegram",
            &sessions,
            "main",
            "telegram-1",
            "7",
            None,
            &mut call,
        )
        .await
        .unwrap();

        {
            let store = sessions.lock().unwrap();
            assert_eq!(store.outbox[0].message_id.as_deref(), Some("99"));
            assert_eq!(store.outbox[0].text, "answer");
            assert_eq!(store.outbox[0].status, super::state::DeliveryStatus::Streaming);
            assert_eq!(
                fs::read_to_string(&log).unwrap().lines().collect::<Vec<_>>(),
                ["send", "edit"]
            );
        }

        channel.stop().await.unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keeps_failed_stream_sends_uncertain() {
        let root = test_root("stream-failure");
        fs::create_dir_all(&root).unwrap();
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *send*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"error":{"code":-1,"message":"Delivery was rejected."}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#
            .replace("\\\\n", "\\n")
            .replace("\\\"", "\"");
        let process = Process::start_with("sh", ["-c", &script]).await.unwrap();
        let channel = Live::new(process);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        {
            let mut store = sessions.lock().unwrap();
            store.create("main", "model").unwrap();
            store
                .begin(
                    "main",
                    Message {
                        id: "user".into(),
                        session: "main".into(),
                        role: Role::User,
                        sender: Some("sender".into()),
                        content: vec![Content::Text { text: "question".into() }],
                    },
                )
                .unwrap();
        }

        let mut output = super::StreamOutput::new();
        output.apply(super::StreamNotice::Tool("read".into()));
        let mut call = 7;
        assert!(
            super::flush_stream(
                &mut output,
                &channel,
                "telegram",
                &sessions,
                "main",
                "telegram-1",
                "7",
                None,
                &mut call,
            )
            .await
            .is_err()
        );
        {
            let mut store = sessions.lock().unwrap();
            store
                .reply(
                    "main",
                    Message {
                        id: "assistant".into(),
                        session: "main".into(),
                        role: Role::Assistant,
                        sender: None,
                        content: vec![Content::Text { text: "complete".into() }],
                    },
                    "telegram-1",
                    "telegram",
                    "7",
                    None,
                    "complete",
                )
                .unwrap();
            assert_eq!(store.outbox[0].status, super::state::DeliveryStatus::Uncertain);
            assert_eq!(store.outbox[0].text, "complete");
            assert!(store.outbox[0].message_id.is_none());
        }

        channel.stop().await.unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepares_channel_replies() {
        assert_eq!(
            super::send_params("discord", "delivery", "room", "hello", None).unwrap()["channel"],
            "room"
        );
        assert_eq!(
            super::send_params("telegram", "delivery", "7", "hello", None).unwrap()["chat"],
            7
        );
        assert_eq!(
            super::send_params("telegram", "delivery", "7", "hello", Some("4")).unwrap()["thread"],
            "4"
        );
        assert!(super::send_params("telegram", "delivery", "room", "hello", None).is_err());
        assert_eq!(
            super::edit_params("discord", "delivery", "room", "42", "hello", None).unwrap()["message"],
            "42"
        );
        assert_eq!(
            super::edit_params("telegram", "delivery", "7", "42", "hello", Some("4")).unwrap()["thread"],
            "4"
        );
        let delivery = super::state::Delivery {
            id: "delivery".into(),
            channel: "discord".into(),
            chat: "room".into(),
            thread: None,
            text: "updated".into(),
            attempts: 0,
            created: 0,
            status: super::state::DeliveryStatus::Pending,
            last_error: None,
            message_id: Some("42".into()),
            updated: 0,
        };

        assert!(matches!(
            delivery_request("discord", 12, &delivery).unwrap(),
            Request::Call { method, .. } if method == "edit"
        ));
        assert_eq!(
            channel_message_id("telegram", &serde_json::json!({"message_id": 9})),
            Some("9".into())
        );
        assert_eq!(
            channel_message_id("discord", &serde_json::json!({"id": "9"})),
            Some("9".into())
        );
        assert!(stream_fits("telegram", &"🙂".repeat(2_048)));
        assert!(!stream_fits("telegram", &"🙂".repeat(2_049)));
        assert!(stream_fits("discord", &"x".repeat(2_000)));
        assert!(!stream_fits("discord", &"x".repeat(2_001)));
        assert!(!stream_fits("unknown", "hello"));
        let context_root =
            std::env::temp_dir().join(format!("crabbot-context-{}", std::process::id()));
        let _ = fs::remove_dir_all(&context_root);
        fs::create_dir_all(context_root.join("nested")).unwrap();
        assert!(super::context_at("test", &context_root).is_none());
        fs::write(context_root.join("AGENTS.md"), "   ").unwrap();
        assert!(super::context_at("test", &context_root).is_none());
        fs::write(context_root.join("AGENTS.md"), "Use the workspace.").unwrap();
        fs::write(context_root.join("nested/AGENTS.md"), "Use the nested workspace.").unwrap();
        assert_eq!(
            super::context_at("test", &context_root.join("nested")).unwrap().role,
            crabbot_core::types::Role::System
        );
        let context = super::context_at("test", &context_root.join("nested")).unwrap();
        let rendered = context.content[0].render();
        assert!(rendered.contains("Use the nested workspace."));
        assert!(!rendered.contains("Use the workspace."));
        fs::write(context_root.join("nested/AGENTS.md"), "x".repeat(super::CONTEXT_LIMIT * 4))
            .unwrap();
        let bounded = super::context_at("test", &context_root.join("nested")).unwrap();
        assert!(bounded.content[0].render().len() <= super::CONTEXT_LIMIT);
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let outside = context_root.join("outside.md");
            fs::write(&outside, "outside instructions").unwrap();
            fs::remove_file(context_root.join("nested/AGENTS.md")).unwrap();
            symlink(&outside, context_root.join("nested/AGENTS.md")).unwrap();
            assert!(super::context_at("test", &context_root.join("nested")).is_none());
        }

        let _ = fs::remove_dir_all(context_root);
        assert!(super::allowed(
            &super::ChannelConfig::default(),
            &serde_json::json!({"private": true, "text": "hello"}),
            "7"
        ));
        let mut policy = super::ChannelConfig { tools: true, ..Default::default() };
        assert!(!super::tools_allowed(
            &policy,
            &serde_json::json!({"private": true, "sender": "7", "text": "hello"}),
            "7",
            true
        ));
        policy.allow.push("7".into());
        assert!(super::tools_allowed(
            &policy,
            &serde_json::json!({"private": true, "sender": "7", "text": "hello"}),
            "7",
            true
        ));
        assert!(!super::tools_allowed(
            &policy,
            &serde_json::json!({"private": true, "sender": "7", "text": "hello"}),
            "7",
            false
        ));
        assert!(!super::allowed(
            &super::ChannelConfig::default(),
            &serde_json::json!({"private": false, "text": "hello"}),
            "7"
        ));
        assert!(!super::allowed(
            &super::ChannelConfig { allow: vec!["8".into()], mention: None, ..Default::default() },
            &serde_json::json!({"text": "hello"}),
            "7"
        ));
        assert!(!super::allowed(
            &super::ChannelConfig {
                allow: Vec::new(),
                mention: Some("@crabbot".into()),
                ..Default::default()
            },
            &serde_json::json!({"text": "hello"}),
            "7"
        ));
        assert!(super::env_nonempty("PATH"));
        assert!(!super::env_nonempty("CRABBOT_TEST_MISSING"));
        assert!(!super::file_credential(&["CRABBOT_TEST_MISSING"]));
        let credentials =
            std::env::temp_dir().join(format!("crabbot-credentials-{}.json", std::process::id()));
        fs::write(&credentials, r#"{"CRABBOT_CODEX_KEY":"secret"}"#).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&credentials).unwrap().permissions();
            permissions.set_mode(0o600);
            fs::set_permissions(&credentials, permissions).unwrap();
        }

        assert!(super::credential(&credentials, &["CRABBOT_CODEX_KEY"]));
        assert!(!super::credential(&credentials, &["MISSING"]));
        fs::write(&credentials, "broken").unwrap();
        assert!(!super::credential(&credentials, &["CRABBOT_CODEX_KEY"]));
        let _ = fs::remove_file(credentials);
        assert_eq!(super::event_id(&serde_json::json!("abc")), Some("abc".into()));
        assert_eq!(super::event_id(&serde_json::json!(7)), Some("7".into()));
        assert_eq!(super::event_id(&serde_json::Value::Null), None);
        assert_eq!(super::retry_delay(0), std::time::Duration::from_secs(1));
        assert_eq!(super::retry_delay(5), std::time::Duration::from_secs(30));
        assert_eq!(super::retry_delay(u32::MAX), std::time::Duration::from_secs(30));
        let media = serde_json::json!({
            "content": [
                {"kind": "text", "text": "caption"},
                {"kind": "image", "uri": "https://example.test/image", "alt": null},
                {"kind": "file", "uri": "https://example.test/file", "name": "file.txt", "mime": null}
            ]
        });

        assert_eq!(super::content(&media).len(), 3);
        assert_eq!(
            super::content(&serde_json::json!({"content":[{"kind":"unknown"}],"text":"fallback"}))
                .len(),
            1
        );
        let clipped = super::content(&serde_json::json!({"text": "x".repeat(300 * 1024)}));
        assert!(clipped[0].render().len() <= super::TEXT_LIMIT);
        assert!(super::content(&serde_json::json!({"text":""})).is_empty());
        assert!(super::allowed(
            &super::ChannelConfig {
                allow: vec!["7".into()],
                mention: Some("@bot".into()),
                ..Default::default()
            },
            &serde_json::json!({"private": false, "text": "hello @bot"}),
            "7"
        ));
        let event = serde_json::json!({
            "private": false,
            "text": "hello",
            "sender": "owner",
            "topic": 4,
            "thread": "thread"
        });

        assert!(super::allowed(
            &super::ChannelConfig {
                allow: vec!["7".into()],
                owner: Some("owner".into()),
                topic: vec!["4".into()],
                thread: vec!["thread".into()],
                ..Default::default()
            },
            &event,
            "7"
        ));
        assert!(!super::allowed(
            &super::ChannelConfig {
                allow: vec!["7".into()],
                admin: vec!["admin".into()],
                ..Default::default()
            },
            &event,
            "7"
        ));
    }

    #[tokio::test]
    async fn main_reports_success_and_failure() {
        assert_eq!(
            super::main_with(Cli {
                json: false,
                debug: false,
                verbose: false,
                version_flag: None,
                help: None,
                command: Command::Version(Output { json: false }),
            })
            .await,
            std::process::ExitCode::SUCCESS
        );
        assert_eq!(
            super::main_with(Cli {
                json: false,
                debug: false,
                verbose: false,
                version_flag: None,
                help: None,
                command: Command::External(Vec::new())
            })
            .await,
            std::process::ExitCode::FAILURE
        );
    }

    #[cfg(unix)]
    #[test]
    fn secure_files_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("crabbot-secure-{}", std::process::id()));
        super::secure(&path, b"secret").unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn runs_non_interactive_commands() {
        for command in [
            Command::Version(Output { json: true }),
            Command::Status(Output { json: true }),
            Command::Service { command: None },
            Command::Doctor(DoctorArgs { fix: false }),
        ] {
            let result = super::run(Cli {
                json: false,
                debug: false,
                verbose: false,
                version_flag: None,
                help: None,
                command,
            })
            .await;
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn manages_local_sessions_and_plugins() {
        let root = test_root("host");
        let memory_binary = super::plugin_name("memory");
        let _ = fs::remove_dir_all(&root);
        local_session(SessionCommand::List(Output { json: false }), &root).unwrap();
        local_session(
            SessionCommand::New(SessionNew { id: "main".into(), model: "test".into() }),
            &root,
        )
        .unwrap();
        local_session(SessionCommand::List(Output { json: false }), &root).unwrap();
        local_session(SessionCommand::List(Output { json: true }), &root).unwrap();
        local_session(SessionCommand::Show(Id { id: "main".into() }), &root).unwrap();
        local_session(
            SessionCommand::Fork(Fork { source: "main".into(), target: "copy".into() }),
            &root,
        )
        .unwrap();
        local_session(
            SessionCommand::Model(SessionModel { id: "copy".into(), model: "next".into() }),
            &root,
        )
        .unwrap();
        local_session(SessionCommand::Cancel(Id { id: "copy".into() }), &root).unwrap();

        super::init_at(&root).unwrap();
        super::init_at(&root).unwrap();
        fs::create_dir_all(root.join("plugins/memory/bin")).unwrap();
        write_test_plugin(
            &root.join("plugins/memory/bin").join(&memory_binary),
            "memory",
            "memory",
            "0.1.0",
        );

        assert!(super::binary_at("memory", &root).is_some());
        assert!(super::binary_at("missing", &root).is_none());
        assert!(super::binary_at("../escape", &root).is_none());
        assert_eq!(super::installed_at(&root), vec!["memory"]);
        assert!(super::installed_at(&root.join("missing")).is_empty());

        let plugin_root = root.join("source");
        fs::create_dir_all(plugin_root.join("bin")).unwrap();
        fs::write(
            plugin_root.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['memory']\n",
        )
        .unwrap();

        write_test_plugin(
            &plugin_root.join("bin").join(&memory_binary),
            "memory",
            "memory",
            "0.1.0",
        );

        let source = Source {
            id: "memory".into(),
            source: Some(plugin_root.display().to_string()),
            revision: None,
            yes: true,
        };

        super::link_at(source, true, &root).unwrap();
        assert_eq!(super::installed_at(&root), vec!["memory"]);
        assert!(
            super::link_at(
                Source {
                    id: "memory".into(),
                    source: Some(root.join("plugins/memory").display().to_string()),
                    revision: None,
                    yes: true,
                },
                true,
                &root,
            )
            .is_err()
        );
        #[cfg(unix)]
        assert!(
            fs::symlink_metadata(root.join("plugins/memory/bin").join(&memory_binary))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        super::list_at(&root, true).unwrap();
        super::list_at(&root, false).unwrap();
        let lock = super::load_lock_at(&root).unwrap();
        assert!(lock.plugins.contains_key("memory"));
        assert_eq!(lock.plugins["memory"].hash.len(), 64);
        assert!(!lock.plugins["memory"].revision.is_empty());

        fs::write(
            plugin_root.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.2.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['memory']\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions =
                fs::metadata(plugin_root.join("bin").join(&memory_binary)).unwrap().permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(plugin_root.join("bin").join(&memory_binary), permissions).unwrap();
        }

        update_at(&root, true).unwrap();
        assert_eq!(read_manifest(&root.join("plugins/memory")).unwrap().version, "0.2.0");

        fs::write(
            plugin_root.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.3.0'\ncapabilities = ['memory']\npermissions = ['network']\n",
        )
        .unwrap();
        assert!(update_at(&root, true).is_err());
        assert_eq!(read_manifest(&root.join("plugins/memory")).unwrap().version, "0.2.0");
        fs::write(
            plugin_root.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.2.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['memory']\n",
        )
        .unwrap();

        assert!(
            super::link_at(
                Source {
                    id: "memory".into(),
                    source: Some(plugin_root.display().to_string()),
                    revision: None,
                    yes: false
                },
                true,
                &root,
            )
            .is_err()
        );
        assert!(
            super::link_at(
                Source { id: "bad_id".into(), source: None, revision: None, yes: true },
                false,
                &root,
            )
            .is_err()
        );
        fs::write(
            plugin_root.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.1.0'\nprotocol = { major = 1, minor = 0 }\ncapabilities = ['memory']\n",
        )
        .unwrap();
        assert!(
            super::link_at(
                Source {
                    id: "memory".into(),
                    source: Some(plugin_root.display().to_string()),
                    revision: None,
                    yes: true,
                },
                true,
                &root,
            )
            .is_err()
        );
        fs::remove_file(plugin_root.join("bin").join(&memory_binary)).unwrap();
        assert!(
            super::link_at(
                Source {
                    id: "memory".into(),
                    source: Some(plugin_root.display().to_string()),
                    revision: None,
                    yes: true,
                },
                true,
                &root,
            )
            .is_err()
        );
        assert!(update_at(&root, false).is_err());
        super::remove_at(Name { id: "memory".into(), yes: true }, &root).unwrap();
        assert!(super::installed_at(&root).is_empty());
        super::list_at(&root, false).unwrap();
        assert!(super::remove_at(Name { id: "missing".into(), yes: false }, &root).is_err());
        assert!(super::remove_at(Name { id: "missing".into(), yes: true }, &root).is_err());
        assert!(
            super::plugin(
                super::PluginCommand::Install(Source {
                    id: "bad_id".into(),
                    source: None,
                    revision: None,
                    yes: true,
                }),
                false,
            )
            .await
            .is_err()
        );
        assert!(super::remove(Name { id: "bad_id".into(), yes: true }).is_err());
        local_session(SessionCommand::List(Output { json: false }), &root).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn covers_offline_delivery_and_crabfile_commands() {
        let root = test_root("offline-delivery");
        let _ = fs::remove_dir_all(&root);
        super::init_at(&root).unwrap();

        let mut store = super::state::Store::load(root.join("sessions.json")).unwrap();
        store.create("main", "test").unwrap();

        store
            .reply(
                "main",
                Message {
                    id: "message".into(),
                    session: "main".into(),
                    role: Role::User,
                    sender: None,
                    content: vec![Content::Text { text: "hello".into() }],
                },
                "delivery",
                "telegram",
                "chat",
                None,
                "reply",
            )
            .unwrap();

        store.uncertain("delivery", "interrupted").unwrap();

        delivery_at(DeliveryCommand::List(Output { json: false }), &root).await.unwrap();
        delivery_at(DeliveryCommand::List(Output { json: true }), &root).await.unwrap();

        assert!(
            delivery_at(DeliveryCommand::Retry(Name { id: "delivery".into(), yes: false }), &root)
                .await
                .is_err()
        );

        delivery_at(DeliveryCommand::Retry(Name { id: "delivery".into(), yes: true }), &root)
            .await
            .unwrap();

        assert!(
            delivery_at(DeliveryCommand::Drop(Name { id: "missing".into(), yes: true }), &root)
                .await
                .is_err()
        );

        delivery_at(DeliveryCommand::Drop(Name { id: "delivery".into(), yes: true }), &root)
            .await
            .unwrap();

        let crabfile = root.join("Crabfile");
        export_crabfile_at(
            CrabfileExport { destination: None, path: Some(crabfile.clone()), force: false },
            &root,
        )
        .unwrap();

        assert!(
            export_crabfile_at(
                CrabfileExport { destination: None, path: Some(crabfile.clone()), force: false },
                &root,
            )
            .is_err()
        );

        export_crabfile_at(
            CrabfileExport { destination: None, path: Some(crabfile.clone()), force: true },
            &root,
        )
        .unwrap();

        let export_dir = root.join("export");
        fs::create_dir_all(&export_dir).unwrap();
        export_crabfile_at(
            CrabfileExport { destination: Some(export_dir.clone()), path: None, force: false },
            &root,
        )
        .unwrap();
        assert!(export_dir.join("Crabfile").is_file());

        let error = import_crabfile_at(
            CrabfileImport { path: Some(crabfile.clone()), yes: false, force: false },
            &root,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("requires confirmation"));

        let missing = root.join("missing-crabfile");
        let error = import_crabfile_at(
            CrabfileImport { path: Some(missing.clone()), yes: true, force: false },
            &root,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("Crabfile not found"));
        assert!(error.contains(&missing.display().to_string()));

        let invalid = root.join("invalid-crabfile");
        fs::write(&invalid, "version = [\n").unwrap();
        let error = import_crabfile_at(
            CrabfileImport { path: Some(invalid.clone()), yes: true, force: false },
            &root,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("Crabfile at"));
        assert!(error.contains("is invalid at line 1, column"));

        import_crabfile_at(
            CrabfileImport { path: Some(root.join("Crabfile")), yes: true, force: true },
            &root,
        )
        .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn updates_active_plugins_without_restarting_the_daemon() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_root("plugin-update-live");
        let _ = fs::remove_dir_all(&root);
        super::init_at(&root).unwrap();

        for (id, capability) in [("memory", "memory"), ("tools", "tool")] {
            let source = root.join("sources").join(id);
            fs::create_dir_all(source.join("bin")).unwrap();
            fs::write(
                source.join("crabbot-plugin.toml"),
                format!(
                    "id = '{id}'\nversion = '0.1.0'\nprotocol = {{ major = 0, minor = 1 }}\ncapabilities = ['{capability}']\n"
                ),
            )
            .unwrap();
            let binary = source.join(format!("bin/crabbot-plugin-{id}"));
            write_test_plugin(&binary, id, capability, "0.1.0");
            fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
            super::link_at(
                Source {
                    id: id.into(),
                    source: Some(source.display().to_string()),
                    revision: None,
                    yes: true,
                },
                true,
                &root,
            )
            .unwrap();
        }

        for (id, capability) in [("memory", "memory"), ("tools", "tool")] {
            let source = root.join("sources").join(id);
            fs::write(
                source.join("crabbot-plugin.toml"),
                format!(
                    "id = '{id}'\nversion = '0.2.0'\nprotocol = {{ major = 0, minor = 1 }}\ncapabilities = ['{capability}']\n"
                ),
            )
            .unwrap();
            write_test_plugin(
                &source.join(format!("bin/crabbot-plugin-{id}")),
                id,
                capability,
                "0.2.0",
            );
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let unload_calls = Arc::clone(&calls);
        let mut unload = move |id: String| {
            let calls = Arc::clone(&unload_calls);
            Box::pin(async move {
                calls.lock().unwrap().push(format!("unload:{id}"));
                Ok(Some(serde_json::json!({"unloaded": true})))
            }) as super::PluginTask
        };

        let activate_calls = Arc::clone(&calls);
        let mut activate = move |id: String| {
            let calls = Arc::clone(&activate_calls);
            Box::pin(async move {
                calls.lock().unwrap().push(format!("activate:{id}"));
                Ok(Some(serde_json::json!({"loaded": true})))
            }) as super::PluginTask
        };

        super::update_live(&root, false, vec!["memory".into()], &mut unload, &mut activate)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["unload:memory", "activate:memory"]);

        fs::write(
            root.join("sources/memory/crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.3.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['memory']\npermissions = ['network']\n",
        )
        .unwrap();
        assert!(
            super::update_live(&root, false, vec!["memory".into()], &mut unload, &mut activate,)
                .await
                .is_err()
        );
        assert_eq!(
            *calls.lock().unwrap(),
            ["unload:memory", "activate:memory", "unload:memory", "activate:memory"]
        );

        calls.lock().unwrap().clear();
        let unload_calls = Arc::clone(&calls);
        let mut unload = move |id: String| {
            let calls = Arc::clone(&unload_calls);
            Box::pin(async move {
                calls.lock().unwrap().push(format!("unload:{id}"));

                if id == "tools" {
                    return Err(std::io::Error::other("Plugin could not stop.").into());
                }

                Ok(Some(serde_json::json!({"unloaded": true})))
            }) as super::PluginTask
        };

        let activate_calls = Arc::clone(&calls);
        let mut activate = move |id: String| {
            let calls = Arc::clone(&activate_calls);
            Box::pin(async move {
                calls.lock().unwrap().push(format!("activate:{id}"));
                Ok(Some(serde_json::json!({"loaded": true})))
            }) as super::PluginTask
        };

        let error = super::update_live(
            &root,
            false,
            vec!["memory".into(), "tools".into()],
            &mut unload,
            &mut activate,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Could not unload tools"));
        assert_eq!(*calls.lock().unwrap(), ["unload:memory", "unload:tools", "activate:memory"]);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn updates_plugins_offline_when_the_daemon_is_absent() {
        let root = test_root("plugin-update-offline");
        let _ = fs::remove_dir_all(&root);
        super::init_at(&root).unwrap();

        assert_eq!(
            super::active_ids(serde_json::json!({"items": ["memory", "tools"]})).unwrap(),
            ["memory", "tools"]
        );
        assert!(super::active_ids(serde_json::json!({})).is_err());
        assert!(super::active_ids(serde_json::json!({"items": ["../escape"]})).is_err());

        super::update_plugins_at(&root, false).await.unwrap();

        let mut unload = |_: String| {
            Box::pin(async { Ok(Some(serde_json::json!({"unloaded": true}))) }) as super::PluginTask
        };

        let mut activate = |_: String| Box::pin(async { Ok(None) }) as super::PluginTask;
        let error =
            super::update_live(&root, false, vec!["memory".into()], &mut unload, &mut activate)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("daemon is unavailable"));

        let mut unload = |id: String| {
            Box::pin(async move {
                if id == "tools" {
                    return Ok(None);
                }

                Ok(Some(serde_json::json!({"unloaded": true})))
            }) as super::PluginTask
        };

        let mut activate = |_: String| {
            Box::pin(async { Ok(Some(serde_json::json!({"loaded": true}))) }) as super::PluginTask
        };

        let error = super::update_live(
            &root,
            false,
            vec!["memory".into(), "tools".into()],
            &mut unload,
            &mut activate,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("before tools could be unloaded"));

        let mut unload = |_: String| {
            Box::pin(async { Ok(Some(serde_json::json!({"unloaded": true}))) }) as super::PluginTask
        };

        let mut activate = |id: String| {
            Box::pin(
                async move { Err(std::io::Error::other(format!("Could not reload {id}.")).into()) },
            ) as super::PluginTask
        };

        let error =
            super::update_live(&root, false, vec!["memory".into()], &mut unload, &mut activate)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("activation was incomplete"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(any(unix, windows))]
    fn write_test_plugin(path: &Path, id: &str, capability: &str, version: &str) {
        #[cfg(unix)]
        fs::write(
            path,
            format!(
                "#!/bin/sh\nwhile IFS= read -r line; do\ncase \"$line\" in\n*hello*) printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocol\":{{\"major\":0,\"minor\":1}},\"id\":\"{id}\",\"version\":\"{version}\",\"capabilities\":[\"{capability}\"]}}}}' ;;\n*shutdown*) printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{{\"ok\":true}}}}'; exit 0 ;;\nesac\ndone\n"
            ),
        )
        .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).unwrap();
        }

        #[cfg(windows)]
        {
            let source = path.with_extension("rs");
            let source_code = r##"
use std::io::{BufRead, Write};

fn main() {
    for line in std::io::stdin().lock().lines().map_while(Result::ok) {
        let id = line
            .split_once("\"id\":")
            .and_then(|(_, rest)| rest.split(',').next())
            .unwrap_or("1");
        let response = if line.contains("\"method\":\"hello\"") {
            format!(r#"{{"jsonrpc":"2.0","id":{},"result":{{"protocol":{{"major":0,"minor":1}},"id":"__PLUGIN_ID__","version":"__PLUGIN_VERSION__","capabilities":["__PLUGIN_CAPABILITY__"]}}}}"#, id)
        } else if line.contains("\"method\":\"shutdown\"") {
            format!(r#"{{"jsonrpc":"2.0","id":{},"result":{{"ok":true}}}}"#, id)
        } else {
            continue;
        };
        println!("{response}");
        std::io::stdout().flush().unwrap();
        if line.contains("\"method\":\"shutdown\"") {
            break;
        }
    }
}
"##
            .replace("__PLUGIN_ID__", id)
            .replace("__PLUGIN_VERSION__", version)
            .replace("__PLUGIN_CAPABILITY__", capability);
            fs::write(&source, source_code).unwrap();

            let output = std::process::Command::new("rustc")
                .args(["--edition", "2024"])
                .arg(&source)
                .arg("-o")
                .arg(path)
                .output()
                .unwrap();

            assert!(
                output.status.success(),
                "could not compile the Windows test plugin: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            fs::remove_file(source).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolves_git_sources_and_verified_archives() {
        let root = std::env::temp_dir().join(format!("crabbot-sources-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let repo = root.join("repo");
        fs::create_dir_all(repo.join("bin")).unwrap();
        fs::write(
            repo.join("crabbot-plugin.toml"),
            "id = 'memory'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['memory']\n",
        )
        .unwrap();
        fs::write(repo.join("bin/crabbot-plugin-memory"), "plugin").unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "init", "-q"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", "user.email", "test@example.com"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", "user.name", "Crabbot Test"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", "commit.gpgsign", "false"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "add", "."])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "commit", "-qm", "initial"])
                .status()
                .unwrap()
                .success()
        );
        let head = revision(&repo);
        let cloned = resolve(&format!("git+file://{}", repo.display()), Some(&head)).unwrap();
        assert!(cloned.path.join("crabbot-plugin.toml").is_file());
        assert!(resolve(&format!("git+file://{}", repo.display()), Some("wrong")).is_err());
        assert!(
            std::process::Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "commit", "--allow-empty", "-qm", "moved"])
                .status()
                .unwrap()
                .success()
        );
        let pinned = resolve(&format!("git+file://{}", repo.display()), Some(&head)).unwrap();
        assert_eq!(revision(&pinned.path), head);

        let archive = root.join("memory.tar.gz");
        assert!(
            std::process::Command::new("tar")
                .args(["-czf", archive.to_str().unwrap(), "-C", root.to_str().unwrap(), "repo"])
                .status()
                .unwrap()
                .success()
        );
        let mut hash = Sha256::new();
        hash.update(fs::read(&archive).unwrap());
        let checksum = format!("{:x}", hash.finalize());
        let extracted =
            resolve(&format!("file://{}#sha256={checksum}", archive.display()), None).unwrap();
        assert!(extracted.path.join("crabbot-plugin.toml").is_file());
        assert!(resolve(&format!("file://{}", archive.display()), None).is_err());
        assert!(
            resolve(&format!("file://{}#sha256={}", archive.display(), "0".repeat(64)), None,)
                .is_err()
        );
        assert!(resolve("https://example.com/plugin.zip", None).is_err());
        assert!(resolve(&root.join("plain.txt").display().to_string(), None).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_unsafe_archive_inputs() {
        let root = std::env::temp_dir().join(format!("crabbot-archive-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("plain.txt"), "plain").unwrap();
        assert_eq!(
            canonical_source(&format!("file://{}", root.join("plain.txt").display())),
            format!("file://{}", root.join("plain.txt").canonicalize().unwrap().display())
        );
        assert!(verify_archive(&root.join("plain.txt"), None).is_err());
        let oversized = root.join("oversized.tar");
        std::fs::File::create(&oversized).unwrap().set_len(ARCHIVE_LIMIT + 1).unwrap();
        assert!(verify_archive(&oversized, Some(&"0".repeat(64))).is_err());
        assert!(download("https://127.0.0.1:1/missing.tar", &root.join("missing")).is_err());
        assert!(!archive_url("https://example.com/plugin.txt"));
        assert!(archive_url("plugin.tar"));
        assert!(archive_url("plugin.tgz"));
        assert!(archive_url("plugin.zip?download=1"));
        assert!(archive(Path::new("plugin.tar.gz")));
        assert!(!archive(Path::new("plugin.txt")));
        let redacted = redact(b"https://user:secret@example.com/path Authorization: Bearer token");
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("Bearer token"));
        assert!(embedded("https://user@example.com/repo.git"));
        assert!(embedded("ssh://git:secret@example.com/repo.git"));
        assert!(!embedded("ssh://git@example.com/repo.git"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validates_archive_roots_and_contents() {
        let root =
            std::env::temp_dir().join(format!("crabbot-archive-root-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(
            root.join("crabbot-plugin.toml"),
            "id = 'echo'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = []\n",
        )
        .unwrap();
        fs::write(root.join("nested/data"), "data").unwrap();
        assert_eq!(archive_root(&root).unwrap(), root);
        safe_archive(&root).unwrap();
        let outer = root.join("outer");
        fs::create_dir_all(outer.join("echo")).unwrap();
        fs::copy(root.join("crabbot-plugin.toml"), outer.join("echo/crabbot-plugin.toml")).unwrap();
        assert_eq!(archive_root(&outer).unwrap(), outer.join("echo"));
        let empty = root.join("empty");
        fs::create_dir_all(&empty).unwrap();
        assert!(archive_root(&empty).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_archive_links_before_extraction() {
        let root = test_root("archive-links");
        fs::create_dir_all(root.join("symlink")).unwrap();
        fs::write(root.join("symlink/target"), "data").unwrap();
        std::os::unix::fs::symlink("target", root.join("symlink/link")).unwrap();
        let symlink_archive = root.join("symlink.tar");
        assert!(
            std::process::Command::new("tar")
                .args([
                    "-cf",
                    symlink_archive.to_str().unwrap(),
                    "-C",
                    root.join("symlink").to_str().unwrap(),
                    "target",
                    "link",
                ])
                .status()
                .unwrap()
                .success()
        );
        assert!(validate_archive(&symlink_archive).is_err());

        fs::create_dir_all(root.join("hardlink")).unwrap();
        fs::write(root.join("hardlink/source"), "data").unwrap();
        fs::hard_link(root.join("hardlink/source"), root.join("hardlink/link")).unwrap();
        let hardlink_archive = root.join("hardlink.tar");
        assert!(
            std::process::Command::new("tar")
                .args([
                    "-cf",
                    hardlink_archive.to_str().unwrap(),
                    "-C",
                    root.join("hardlink").to_str().unwrap(),
                    "source",
                    "link",
                ])
                .status()
                .unwrap()
                .success()
        );
        assert!(validate_archive(&hardlink_archive).is_err());

        let zip_root = root.join("zip");
        fs::create_dir_all(&zip_root).unwrap();
        fs::write(zip_root.join("target"), "data").unwrap();
        let regular_archive = root.join("regular.zip");
        assert!(
            std::process::Command::new("zip")
                .current_dir(&zip_root)
                .args(["-q", regular_archive.to_str().unwrap(), "target"])
                .status()
                .unwrap()
                .success()
        );
        assert!(validate_archive(&regular_archive).is_ok());

        std::os::unix::fs::symlink("target", zip_root.join("link")).unwrap();
        let symlink_zip = root.join("symlink.zip");
        assert!(
            std::process::Command::new("zip")
                .current_dir(&zip_root)
                .args(["-q", "-y", symlink_zip.to_str().unwrap(), "target", "link"])
                .status()
                .unwrap()
                .success()
        );
        assert!(validate_archive(&symlink_zip).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn isolates_group_workspaces() {
        let root = std::env::temp_dir().join(format!("crabbot-isolate-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["-C", root.to_str().unwrap(), "init", "-q"])
                .status()
                .unwrap()
                .success()
        );
        fs::write(root.join("README"), "test").unwrap();

        for args in [
            vec!["-C", root.to_str().unwrap(), "config", "user.email", "test@example.com"],
            vec!["-C", root.to_str().unwrap(), "config", "user.name", "Crabbot Test"],
            vec!["-C", root.to_str().unwrap(), "config", "commit.gpgsign", "false"],
            vec!["-C", root.to_str().unwrap(), "add", "."],
            vec!["-C", root.to_str().unwrap(), "commit", "-qm", "initial"],
        ] {
            assert!(std::process::Command::new("git").args(args).status().unwrap().success());
        }

        assert!(isolate_at("telegram-7", &root).unwrap().is_some());
        assert!(isolate_at("telegram-7", &root).unwrap().is_some());
        assert!(isolate_at("../escape", &root).unwrap().is_none());
        fs::write(root.join(".crabbot/worktrees/telegram-7/dirty"), "keep me").unwrap();
        assert!(super::state::remove_worktree(&root, "telegram-7").is_err());
        assert!(root.join(".crabbot/worktrees/telegram-7/dirty").is_file());
        fs::remove_file(root.join(".crabbot/worktrees/telegram-7/dirty")).unwrap();
        super::state::remove_worktree(&root, "telegram-7").unwrap();
        assert!(!root.join(".crabbot/worktrees/telegram-7").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn falls_back_when_daemon_is_absent() {
        let root = std::env::temp_dir().join(format!("crabbot-control-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        assert!(super::control_at("status", serde_json::json!({}), &root).await.unwrap().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_telegram_media_references() {
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *media*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"uri":"file:///tmp/media.bin"}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let channel = Process::start_with("sh", ["-c", script]).await.unwrap();
        let channel = Live::new(channel);
        let content = super::resolve_media(
            "telegram",
            &channel,
            vec![Content::Image { uri: "telegram://file/id".into(), alt: None }],
            &mut 2,
        )
        .await;
        assert_eq!(content[0], Content::Image { uri: "file:///tmp/media.bin".into(), alt: None });
        channel.stop().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn transcribes_voice_with_the_speech_plugin_and_removes_raw_audio() {
        let root = test_root("voice");
        let media = root.join("media");
        fs::create_dir_all(&media).unwrap();
        let voice = media.join("voice.ogg");
        fs::write(&voice, b"voice").unwrap();
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"whisper","version":"0.1.0","capabilities":["speech"]}}' ;; *transcribe*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"text":"recognized words"}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let plugin = Process::start_with("sh", ["-c", script]).await.unwrap();
        let plugins = Plugins::default();
        plugins.insert(Live::new(plugin)).await;
        let mut call = 2;
        let content = super::transcribe_media(
            &plugins,
            "telegram",
            vec![Content::Audio {
                uri: format!("file://{}", voice.display()),
                mime: Some("audio/ogg".into()),
            }],
            &media,
            &mut call,
        )
        .await;

        assert_eq!(content, vec![Content::Text { text: "recognized words".into() }]);
        assert!(!voice.exists());
        plugins.all().await.into_iter().next().unwrap().1.stop().await.unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn omits_unsafe_voice_files_without_leaking_their_paths() {
        let root = test_root("voice-unsafe");
        fs::create_dir_all(&root).unwrap();
        let outside = root.join("outside.ogg");
        fs::write(&outside, b"voice").unwrap();
        let plugins = Plugins::default();
        let mut call = 2;
        let content = super::transcribe_media(
            &plugins,
            "telegram",
            vec![Content::Audio { uri: format!("file://{}", outside.display()), mime: None }],
            &root.join("media"),
            &mut call,
        )
        .await;

        assert_eq!(content, vec![super::unavailable_voice()]);
        assert!(!content[0].render().contains(&outside.display().to_string()));
        assert_eq!(call, 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bounds_cached_media_files() {
        let root = test_root("media-size");
        fs::create_dir_all(&root).unwrap();
        let large = root.join("large.bin");
        fs::File::create(&large).unwrap().set_len(super::MEDIA_LIMIT + 1).unwrap();

        assert!(super::media_file(&format!("file://{}", large.display()), &root).is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pins_image_media_outside_the_expiring_cache() {
        let root = test_root("media-pin");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("photo.bin");
        fs::write(&source, b"\x89PNG\r\n\x1a\nimage").unwrap();
        let content = super::pin_media(
            vec![Content::Image {
                uri: format!("file://{}", source.display()),
                alt: Some("photo".into()),
            }],
            &root,
        );
        let Content::Image { uri, alt } = &content[0] else {
            panic!("expected pinned image");
        };

        let uri = uri.clone();
        let alt = alt.clone();
        assert_eq!(alt.as_deref(), Some("photo"));
        let pinned = PathBuf::from(uri.strip_prefix("file://").unwrap());
        assert!(pinned.starts_with(root.join("pinned")));
        assert!(pinned.is_file());

        let again = super::pin_media(content, &root);
        assert_eq!(again[0], Content::Image { uri, alt: Some("photo".into()) });
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepares_only_bounded_confined_images() {
        let root = test_root("image-media");
        fs::create_dir_all(&root).unwrap();
        let first = root.join("first.png");
        let second = root.join("second.png");
        let mut first_bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        first_bytes.resize(3 * 1024 * 1024, 7);
        let second_bytes = first_bytes.clone();
        fs::write(&first, &first_bytes).unwrap();
        fs::write(&second, &second_bytes).unwrap();
        let mut messages = vec![Message {
            id: "image-message".into(),
            session: "image-session".into(),
            role: Role::User,
            sender: None,
            content: vec![
                Content::Image { uri: format!("file://{}", first.display()), alt: None },
                Content::Image { uri: format!("file://{}", second.display()), alt: None },
                Content::Image { uri: "file:///outside/photo.png".into(), alt: None },
                Content::Image {
                    uri: "file://unsupported".into(),
                    alt: Some("description".into()),
                },
            ],
        }];

        super::prepare_images(&mut messages, &root);

        let Content::Image { uri, .. } = &messages[0].content[0] else {
            panic!("The confined image should remain an image.");
        };

        let (header, encoded) = uri.split_once(',').unwrap();
        assert_eq!(header, "data:image/png;base64");
        assert_eq!(super::STANDARD.decode(encoded).unwrap(), first_bytes);
        assert!(messages[0].content[1].render().contains("omitted"));
        assert!(messages[0].content[2].render().contains("omitted"));
        assert_eq!(
            messages[0].content[3],
            Content::Text {
                text: "[Image attachment unavailable. Description: description]".into()
            }
        );
        assert!(
            !messages[0]
                .content
                .iter()
                .any(|content| content.render().contains(&root.display().to_string()))
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn handles_discord_audio_without_speech_plugin() {
        let content = vec![Content::Audio { uri: "file://voice.ogg".into(), mime: None }];
        let plugins = Plugins::default();
        let mut call = 2;

        let result = super::transcribe_media(
            &plugins,
            "discord",
            content.clone(),
            Path::new("."),
            &mut call,
        )
        .await;

        assert_eq!(
            result[0].render(),
            "[Voice message omitted because transcription is unavailable.]"
        );
        assert_eq!(call, 2);
    }

    #[test]
    fn expands_only_bounded_text_attachments() {
        let root = test_root("text-media");
        fs::create_dir_all(&root).unwrap();
        let note = root.join("note.md");
        let secret = root.join("secret.pem");
        let large = root.join("large.txt");
        fs::write(&note, "A short note.").unwrap();
        fs::write(&secret, "PRIVATE KEY").unwrap();
        fs::File::create(&large).unwrap().set_len(super::FILE_LIMIT + 1).unwrap();
        let content = vec![
            Content::File {
                uri: format!("file://{}", note.display()),
                name: "note.md".into(),
                mime: Some("text/markdown".into()),
            },
            Content::File {
                uri: format!("file://{}", secret.display()),
                name: "secret.pem".into(),
                mime: Some("application/octet-stream".into()),
            },
            Content::File {
                uri: format!("file://{}", large.display()),
                name: "large.txt".into(),
                mime: Some("text/plain".into()),
            },
        ];

        let result = super::expand_media_files("telegram", content, &root);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Content::Text { text: "[File: note.md]\nA short note.".into() });
        assert!(result[1].render().contains("unsafe or unsupported"));
        assert!(result[2].render().contains("unsafe or unsupported"));
        assert!(!result.iter().any(|item| item.render().contains(&root.display().to_string())));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runs_session_commands_without_daemon() {
        let root = test_root("session");
        let _ = fs::remove_dir_all(&root);
        session_at(
            SessionCommand::New(SessionNew { id: "main".into(), model: "first".into() }),
            &root,
        )
        .await
        .unwrap();
        session_at(SessionCommand::List(Output { json: false }), &root).await.unwrap();
        session_at(SessionCommand::List(Output { json: true }), &root).await.unwrap();
        session_at(SessionCommand::Show(Id { id: "main".into() }), &root).await.unwrap();
        session_at(
            SessionCommand::Fork(Fork { source: "main".into(), target: "copy".into() }),
            &root,
        )
        .await
        .unwrap();
        session_at(
            SessionCommand::Model(SessionModel { id: "copy".into(), model: "second".into() }),
            &root,
        )
        .await
        .unwrap();
        session_at(SessionCommand::Cancel(Id { id: "copy".into() }), &root).await.unwrap();
        assert!(
            session_at(SessionCommand::Show(Id { id: "missing".into() }), &root).await.is_err()
        );
        assert!(
            session_at(
                SessionCommand::Delete(SessionDelete { id: "main".into(), yes: false }),
                &root,
            )
            .await
            .is_err()
        );
        session_at(SessionCommand::Delete(SessionDelete { id: "main".into(), yes: true }), &root)
            .await
            .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn asks_a_local_plugin() {
        use std::os::unix::fs::PermissionsExt;

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |value| value.as_nanos());
        let root = std::env::temp_dir().join(format!("crabbot-ask-{}-{nonce}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        super::init_at(&root).unwrap();
        let bin = root.join("plugins/fake/bin/crabbot-plugin-fake");
        fs::create_dir_all(bin.parent().unwrap()).unwrap();
        fs::write(
            root.join("plugins/fake/crabbot-plugin.toml"),
            "id = 'fake'\nversion = '0.1.0'\nprotocol = { major = 0, minor = 1 }\ncapabilities = ['model']\n",
        )
        .unwrap();
        fs::write(
            &bin,
            "#!/bin/sh\nwhile IFS= read -r line; do\ncase \"$line\" in\n*hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"fake\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;;\n*generate*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"text\":\"done\",\"stop\":\"stop\",\"input\":null,\"output\":null}}' ;;\n*shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;;\nesac\ndone\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&bin).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&bin, permissions).unwrap();
        let mut lock = super::Lock::default();
        lock.plugins.insert(
            "fake".into(),
            super::Entry {
                source: "local".into(),
                revision: "local".into(),
                pinned: false,
                default: false,
                hash: super::digest(&root.join("plugins/fake/crabbot-plugin.toml"), &bin).unwrap(),
                version: "0.1.0".into(),
                protocol: Protocol { major: 0, minor: 1 },
                capabilities: vec!["model".into()],
                permissions: Vec::new(),
                secrets: Vec::new(),
                commands: Vec::new(),
                linked: false,
            },
        );
        super::save_lock_at(&root, &lock).unwrap();

        super::ask_at(
            super::Ask {
                plugin: Some("fake".into()),
                model: "test".into(),
                prompt: vec!["greet".into(), "world".into()],
            },
            &root,
        )
        .await
        .unwrap();
        let mut lock = super::load_lock_at(&root).unwrap();
        lock.plugins.get_mut("fake").unwrap().capabilities = vec!["tool".into()];
        super::save_lock_at(&root, &lock).unwrap();
        assert!(
            super::ask_at(
                super::Ask {
                    plugin: Some("fake".into()),
                    model: "test".into(),
                    prompt: vec!["again".into()],
                },
                &root,
            )
            .await
            .is_err()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn checks_plugin_readiness() {
        assert!(super::ready("ollama"));
        assert!(!super::ready("unknown"));
        assert!(!super::ready("telegram") || std::env::var_os("CRABBOT_TELEGRAM_TOKEN").is_some());
        assert!(!super::codex_auth() || super::ready("codex"));
    }

    #[tokio::test]
    async fn serves_a_temporary_daemon() {
        let root = std::env::temp_dir().join(format!("crabbot-daemon-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);

        if let Err(error) = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await {
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                return;
            }

            panic!("listener failed: {error}");
        }

        let task_root = root.clone();
        let task = tokio::spawn(async move { super::serve_at(&task_root).await });

        let mut ready = false;

        for _ in 0..100 {
            if root.join("ipc.port").is_file() {
                ready = true;
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(ready);
        let shutdown = super::ipc::call(&root, "shutdown", serde_json::json!({})).await;

        if let Err(error) = shutdown {
            let result = task.await.unwrap();
            panic!("IPC shutdown failed: {error}; daemon: {result:?}");
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn starts_configured_plugins_and_stops_cleanly() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("crabbot-start-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);

        for (id, capability) in [("channel", "channel"), ("model", "model")] {
            let bin = root.join(format!("plugins/{id}/bin/crabbot-plugin-{id}"));
            fs::create_dir_all(bin.parent().unwrap()).unwrap();
            fs::write(
                root.join(format!("plugins/{id}/crabbot-plugin.toml")),
                format!("id = '{id}'\nversion = '0.1.0'\nprotocol = {{ major = 0, minor = 1 }}\ncapabilities = ['{capability}']\n"),
            )
            .unwrap();
            let script = format!(
                "#!/bin/sh\nwhile IFS= read -r line; do case \"$line\" in *hello*) printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocol\":{{\"major\":0,\"minor\":1}},\"id\":\"{id}\",\"version\":\"0.1.0\",\"capabilities\":[\"{capability}\"]}}}}' ;; *poll*) printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":10,\"result\":{{\"events\":[]}}}}' ;; *shutdown*) printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{{\"ok\":true}}}}'; exit 0 ;; esac; done\n"
            );
            fs::write(&bin, script).unwrap();
            let mut permissions = fs::metadata(&bin).unwrap().permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(bin, permissions).unwrap();
        }

        let task_root = root.clone();
        let task = tokio::spawn(async move {
            super::serve_inner(&task_root, "channel", "model", "test", true).await
        });

        for _ in 0..100 {
            if root.join("ipc.port").is_file() {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        if !root.join("ipc.port").is_file() {
            let result = task.await.unwrap();

            if matches!(result, Err(ref error) if error.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied))
            {
                let _ = fs::remove_dir_all(root);
                return;
            }

            panic!("daemon stopped before IPC became ready: {result:?}");
        }

        let shutdown = super::ipc::call(&root, "shutdown", serde_json::json!({})).await;

        if let Err(error) = shutdown {
            let result = task.await.unwrap();
            panic!("IPC shutdown failed: {error}; daemon: {result:?}");
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn loads_channel_and_model_during_bridge() {
        let channel_script = r#"first=1; second=1; while IFS= read -r line; do case "$line" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"telegram\",\"version\":\"0.1.0\",\"capabilities\":[\"channel\"]}}' ;; *poll*) if [ \"$first\" = 1 ]; then first=0; printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":10,\"result\":{\"events\":[{\"id\":1,\"chat\":7,\"private\":true,\"sender\":8,\"text\":\"hi\",\"gateway_sequence\":7}]}}'; elif [ \"$second\" = 1 ]; then second=0; printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":14,\"result\":{\"events\":[{\"id\":1,\"chat\":7,\"private\":true,\"sender\":8,\"text\":\"hi\"}]}}'; else printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":15,\"result\":{\"events\":[]}}'; fi ;; *ack*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":11,\"result\":{\"acknowledged\":true}}' ;; *send*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":13,\"result\":{\"ok\":true}}' ;; *shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#;
        let channel_script = channel_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let provider_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"codex\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;; *generate*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":12,\"result\":{\"text\":\"world\",\"stop\":\"stop\"}}' ;; *shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#;
        let provider_script = provider_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let plugins = Plugins::default();
        let root = std::env::temp_dir().join(format!("crabbot-bridge-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            signal.signal();
        });

        let active_plugins = plugins.clone();
        let active_sessions = Arc::clone(&sessions);
        let active_stop = Arc::clone(&stop);
        let bridge = tokio::spawn(async move {
            super::bridge(
                &active_plugins,
                "telegram",
                "codex",
                "test",
                active_sessions,
                active_stop,
                Arc::new(Mutex::new(BTreeMap::new())),
                BTreeMap::new(),
                super::ApprovalMode::Off,
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        plugins.insert(Live::new(channel)).await;
        plugins.insert(Live::new(provider)).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), bridge)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        notifier.await.unwrap();
        let messages = {
            let store = sessions.lock().unwrap();
            store.sessions.get("telegram-7").unwrap().messages.clone()
        };

        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[1].content[0],
            crabbot_core::types::Content::Text { text: "world".into() }
        );
        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn drains_queued_channel_events() {
        let channel_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"telegram\",\"version\":\"0.1.0\",\"capabilities\":[\"channel\"]}}' ;; *poll*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":13,\"result\":{\"events\":[]}}' ;; *send*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":12,\"result\":{\"ok\":true}}' ;; *shutdown*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#;
        let channel_script = channel_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let provider_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"codex\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;; *generate*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":11,\"result\":{\"text\":\"world\",\"stop\":\"stop\"}}' ;; *shutdown*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#;
        let provider_script = provider_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let plugins = registry([channel, provider]).await;
        let root = std::env::temp_dir().join(format!("crabbot-queue-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        {
            let mut store = sessions.lock().unwrap();
            store.create("telegram-7", "test").unwrap();
            store.set_status("telegram-7", "working").unwrap();
            store
                .queue(
                    "telegram-7",
                    Message {
                        id: "queued".into(),
                        session: "telegram-7".into(),
                        role: Role::User,
                        sender: Some("8".into()),
                        content: vec![Content::Text { text: "hi @bot".into() }],
                    },
                )
                .unwrap();
        }

        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            signal.signal();
        });

        super::bridge(
            &plugins,
            "telegram",
            "codex",
            "test",
            Arc::clone(&sessions),
            Arc::clone(&stop),
            Arc::new(Mutex::new(BTreeMap::new())),
            BTreeMap::from([(
                "telegram".into(),
                super::ChannelConfig {
                    allow: Vec::new(),
                    mention: Some("@bot".into()),
                    ..Default::default()
                },
            )]),
            super::ApprovalMode::Off,
        )
        .await
        .unwrap();
        notifier.await.unwrap();
        {
            let store = sessions.lock().unwrap();
            assert!(store.sessions["telegram-7"].queued.is_empty());
            assert_eq!(store.sessions["telegram-7"].messages.len(), 2);
        }

        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_a_tool_turn_before_replying() {
        let channel_script = r#"first=1; while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *poll*) if [ "$first" = 1 ]; then first=0; printf '%s\n' '{"jsonrpc":"2.0","id":10,"result":{"events":[{"id":1,"chat":7,"private":true,"sender":8,"text":"read note"}]}}'; else printf '%s\n' '{"jsonrpc":"2.0","id":13,"result":{"events":[]}}'; fi ;; *send*) request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"message_id":99}}' ;; *edit*) request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"message_id":99}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let channel_script = channel_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let provider_script = r#"count=0; while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}' ;; *generate*) if [ "$count" = 0 ]; then count=1; printf '%s\n' '{"jsonrpc":"2.0","id":11,"result":{"text":"","stop":"tool","events":[{"kind":"tool","name":"read","args":{"path":"note.txt"},"approve":false}]}}'; else printf '%s\n' '{"jsonrpc":"2.0","id":13,"result":{"text":"note is ready","stop":"stop"}}'; fi ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let provider_script = provider_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let tool_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"tools","version":"0.1.0","capabilities":["tool"]}}' ;; *read*) printf '%s\n' '{"jsonrpc":"2.0","id":12,"result":{"text":"note contents"}}' ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let tool_script = tool_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let tool = Process::start_with("sh", ["-c", &tool_script]).await.unwrap();
        let plugins = registry([channel, provider, tool]).await;
        let root = std::env::temp_dir().join(format!("crabbot-tool-turn-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            signal.signal();
        });

        super::bridge(
            &plugins,
            "telegram",
            "codex",
            "test",
            Arc::clone(&sessions),
            Arc::clone(&stop),
            Arc::new(Mutex::new(BTreeMap::new())),
            BTreeMap::from([(
                "telegram".into(),
                super::ChannelConfig { allow: vec!["7".into()], tools: true, ..Default::default() },
            )]),
            super::ApprovalMode::Auto,
        )
        .await
        .unwrap();
        notifier.await.unwrap();
        {
            let store = sessions.lock().unwrap();
            assert_eq!(store.sessions["telegram-7"].messages.len(), 4);
            assert_eq!(store.sessions["telegram-7"].messages[1].role, Role::Assistant);
            assert_eq!(store.sessions["telegram-7"].messages[2].role, Role::Tool);
            assert_eq!(
                store.sessions["telegram-7"].messages[3].content[0],
                Content::Text { text: "note is ready".into() }
            );
        }

        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prompts_for_signed_approval_before_mutating_tools() {
        let root = test_root("prompt-approval");
        let marker = root.join("approved");
        let channel_script = r#"first=1; callback=0; approval=; while IFS= read -r line; do request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); case "$line" in *'"method":"hello"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *'"method":"poll"'*) if [ "$first" = 1 ]; then first=0; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"events":[{"id":1,"chat":7,"private":true,"sender":8,"text":"write a note"}]}}'; elif [ -n "$approval" ] && [ "$callback" = 0 ]; then callback=1; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"events":[{"kind":"callback","id":2,"chat":7,"private":true,"sender":9,"callback_id":"callback-1","data":"'"$approval"'"}]}}'; elif [ -n "$approval" ] && [ "$callback" = 1 ]; then callback=2; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"events":[{"kind":"callback","id":3,"chat":7,"private":true,"sender":8,"callback_id":"callback-2","data":"'"$approval"'"}]}}'; else sleep 0.01; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"events":[]}}'; fi ;; *'"method":"approval"'*) approval=$(printf '%s' "$line" | sed -n 's/.*"approve":"\([^"]*\)".*/\1/p'); printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"sent":true}}' ;; *'"method":"shutdown"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"ok":true}}'; exit 0 ;; *) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"message_id":99,"id":"message-99","acknowledged":true}}' ;; esac; done"#;
        let provider_script = r#"count=0; while IFS= read -r line; do request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); case "$line" in *'"method":"hello"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}' ;; *'"method":"generate"'*) if [ "$count" = 0 ]; then count=1; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"text":"","stop":"tool","events":[{"kind":"tool","name":"write","args":{"path":"note.txt","text":"approved content","approve":false}}]}}'; else printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"text":"The note was written.","stop":"stop"}}'; fi ;; *'"method":"shutdown"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let tool_script = r#"while IFS= read -r line; do request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p'); case "$line" in *'"method":"hello"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"protocol":{"major":0,"minor":1},"id":"tools","version":"0.1.0","capabilities":["tool"]}}' ;; *'"method":"write"'*) case "$line" in *'"approve":true'*) touch "__MARKER__"; printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"text":"The file was written."}}' ;; *) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"error":{"code":-32000,"message":"Approval was missing."}}' ;; esac ;; *'"method":"shutdown"'*) printf '%s\n' '{"jsonrpc":"2.0","id":'"$request_id"',"result":{"ok":true}}'; exit 0 ;; esac; done"#
            .replace("__MARKER__", &marker.display().to_string());
        let channel = Process::start_with("sh", ["-c", channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", provider_script]).await.unwrap();
        let tool = Process::start_with("sh", ["-c", &tool_script]).await.unwrap();
        let plugins = registry([channel, provider, tool]).await;
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let marker_for_signal = marker.clone();
        let sessions_for_signal = Arc::clone(&sessions);

        let notifier = tokio::spawn(async move {
            for _ in 0..500 {
                if marker_for_signal.is_file() && approval_finished(&sessions_for_signal) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    if approval_finished(&sessions_for_signal) {
                        break;
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            signal.signal();
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(6),
            super::bridge(
                &plugins,
                "telegram",
                "codex",
                "test",
                Arc::clone(&sessions),
                Arc::clone(&stop),
                Arc::new(Mutex::new(BTreeMap::new())),
                BTreeMap::from([(
                    "telegram".into(),
                    super::ChannelConfig {
                        allow: vec!["7".into()],
                        owner: Some("8".into()),
                        tools: true,
                        ..Default::default()
                    },
                )]),
                super::ApprovalMode::Prompt,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        notifier.await.unwrap();
        assert!(marker.is_file());
        {
            let store = sessions.lock().unwrap();
            assert_eq!(store.sessions["telegram-7"].status, "idle");
            assert!(store.sessions["telegram-7"].messages.iter().any(|message| {
                message.content.iter().any(|content| {
                    matches!(content, Content::Text { text } if text == "The note was written.")
                })
            }));
        }

        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn denied_prompt_never_dispatches_a_mutating_tool() {
        let approvals = Arc::new(super::AsyncMutex::new(super::approval::Gate::new().unwrap()));
        let sessions = Arc::new(Mutex::new(super::state::Store::default()));
        let (notices, mut events) = tokio::sync::mpsc::channel(1);
        let gate = Arc::clone(&approvals);
        let execution = tokio::spawn(async move {
            let mut failed_tool = None;
            super::execute_tool(
                &Plugins::default(),
                1,
                "write",
                serde_json::json!({"path": "note.txt", "text": "blocked"}),
                super::ApprovalMode::Prompt,
                gate,
                "telegram",
                "7",
                None,
                "session",
                &sessions,
                None,
                notices,
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                &mut failed_tool,
            )
            .await
        });

        let Some(super::StreamNotice::Approval { deny, .. }) = events.recv().await else {
            panic!("The tool did not request approval.");
        };

        assert_eq!(approvals.lock().await.resolve(&deny, "telegram", "7", None, true), Some(false));
        assert_eq!(
            execution.await.unwrap().unwrap(),
            "The request was denied. No changes were made."
        );
        assert!(!approvals.lock().await.has_pending());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn commits_incomplete_approval_callbacks_without_resolving_them() {
        let script = r#"
while IFS= read -r line; do
    request_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')

    case "$line" in
        *hello*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}\n' "$request_id"
            ;;
        *callback*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"acknowledged":true}}\n' "$request_id"
            ;;
        *shutdown*)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$request_id"
            exit 0
            ;;
    esac
done
"#;
        let channel = Live::new(Process::start_with("sh", ["-c", script]).await.unwrap());
        let sessions = Arc::new(Mutex::new(super::state::Store::default()));
        let approvals = Arc::new(super::AsyncMutex::new(super::approval::Gate::new().unwrap()));
        let policy =
            super::ChannelConfig { allow: vec!["7".into()], tools: true, ..Default::default() };
        let mut offset = 0;
        let mut call = 10;

        assert!(
            super::handle_callback(
                &channel,
                "telegram",
                &serde_json::json!({}),
                &policy,
                super::ApprovalMode::Prompt,
                &approvals,
                &sessions,
                &mut offset,
                &mut call,
            )
            .await
            .is_err()
        );

        for (id, event) in
            [(1, serde_json::json!({"id": 1})), (2, serde_json::json!({"id": 2, "chat": 7}))]
        {
            super::handle_callback(
                &channel,
                "telegram",
                &event,
                &policy,
                super::ApprovalMode::Prompt,
                &approvals,
                &sessions,
                &mut offset,
                &mut call,
            )
            .await
            .unwrap();
            assert_eq!(offset, id + 1);
        }

        super::handle_callback(
            &channel,
            "telegram",
            &serde_json::json!({
                "kind": "callback",
                "id": 3,
                "chat": 7,
                "private": true,
                "sender": 8,
                "callback_id": "callback-1",
                "data": "invalid",
            }),
            &policy,
            super::ApprovalMode::Prompt,
            &approvals,
            &sessions,
            &mut offset,
            &mut call,
        )
        .await
        .unwrap();

        assert_eq!(offset, 4);
        assert_eq!(call, 11);
        channel.stop().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn routes_plugin_tool_requests_through_host_policy() {
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"tools","version":"0.1.0","capabilities":["tool"]}}' ;; *read*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"text":"workspace content"}}' ;; *write*) case "$line" in *'"approve":false'*) printf '%s\n' '{"jsonrpc":"2.0","id":9,"error":{"code":-32000,"message":"Approval required."}}' ;; *) printf '%s\n' '{"jsonrpc":"2.0","id":10,"result":{"text":"Write executed."}}' ;; esac ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let process = Process::start_with("sh", ["-c", script]).await.unwrap();
        let plugins = registry([process]).await;
        let sessions = Arc::new(Mutex::new(super::state::Store::default()));
        {
            let mut store = sessions.lock().unwrap();
            store.create("room", "test").unwrap();
            store
                .begin(
                    "room",
                    Message {
                        id: "turn".into(),
                        session: "room".into(),
                        role: Role::User,
                        sender: None,
                        content: vec![Content::Text { text: "request".into() }],
                    },
                )
                .unwrap();
        }

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let approvals = Arc::new(super::AsyncMutex::new(super::approval::Gate::new().unwrap()));
        let (notices, _events) = tokio::sync::mpsc::channel(4);
        let response = super::host_tool(
            Request::call(
                7,
                "host/tool",
                serde_json::json!({"name": "read", "args": {"path": "note.txt"}}),
            ),
            &plugins,
            &sessions,
            "room",
            None,
            true,
            super::ApprovalMode::Off,
            Arc::clone(&approvals),
            "telegram",
            "8",
            None,
            notices.clone(),
            Arc::clone(&calls),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(response.result.unwrap()["output"], "workspace content");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);

        let mutation = super::host_tool(
            Request::call(
                9,
                "host/tool",
                serde_json::json!({
                    "name": "write",
                    "args": {"path": "note.txt", "text": "unapproved", "approve": true}
                }),
            ),
            &plugins,
            &sessions,
            "room",
            None,
            true,
            super::ApprovalMode::Off,
            Arc::clone(&approvals),
            "telegram",
            "8",
            None,
            notices.clone(),
            Arc::clone(&calls),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(mutation.error.unwrap().message.contains("Approval policy blocks"));
        assert_eq!(sessions.lock().unwrap().sessions["room"].phase, "safe");

        let disabled = super::host_tool(
            Request::call(
                8,
                "host/tool",
                serde_json::json!({"name": "read", "args": {"path": "note.txt"}}),
            ),
            &plugins,
            &sessions,
            "room",
            None,
            false,
            super::ApprovalMode::Off,
            Arc::clone(&approvals),
            "telegram",
            "8",
            None,
            notices.clone(),
            Arc::clone(&calls),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(disabled.error.unwrap().message.contains("disabled"));

        let approved = super::host_tool(
            Request::call(
                10,
                "host/tool",
                serde_json::json!({
                    "name": "write",
                    "args": {"path": "note.txt", "text": "approved"}
                }),
            ),
            &plugins,
            &sessions,
            "room",
            None,
            true,
            super::ApprovalMode::Auto,
            approvals,
            "telegram",
            "8",
            None,
            notices,
            calls,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(approved.result.unwrap()["output"], "Write executed.");
        assert_eq!(sessions.lock().unwrap().sessions["room"].phase, "unsafe");
        stop_registry(&plugins).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interrupts_a_stalled_tool_on_shutdown() {
        let provider_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"codex\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;; *generate*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":11,\"result\":{\"text\":\"\",\"stop\":\"tool\",\"events\":[{\"kind\":\"tool\",\"name\":\"read\",\"args\":{\"path\":\"note.txt\"},\"approve\":false}]}}' ;; *shutdown*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#
            .replace("\\\\n", "\\n")
            .replace("\\\"", "\"");
        let tool_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"tools\",\"version\":\"0.1.0\",\"capabilities\":[\"tool\"]}}' ;; *read*) sleep 5 ;; *shutdown*) printf '%s\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#
            .replace("\\\\n", "\\n")
            .replace("\\\"", "\"");
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let tool = Process::start_with("sh", ["-c", &tool_script]).await.unwrap();
        let plugins = registry([provider, tool]).await;
        let provider = plugins.get("codex").await.unwrap();
        let root = test_root("stalled-tool");
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        sessions.lock().unwrap().create("session", "test").unwrap();
        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            signal.signal();
        });

        let mut call = 10;
        let mut failed_tool = None;
        let (notices, mut events) = tokio::sync::mpsc::channel(32);
        let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            answer(
                &provider,
                &plugins,
                "test",
                vec![Message {
                    id: "message".into(),
                    session: "session".into(),
                    role: Role::User,
                    sender: Some("sender".into()),
                    content: vec![Content::Text { text: "read note".into() }],
                }],
                "session",
                "telegram",
                "7",
                None,
                &sessions,
                None,
                &root,
                true,
                super::ApprovalMode::Off,
                Arc::new(super::AsyncMutex::new(super::approval::Gate::new().unwrap())),
                &Cancellation::new(),
                &stop,
                tokio::time::Instant::now() + super::TURN_LIMIT,
                &mut call,
                &mut failed_tool,
                notices,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(result.to_string(), "The turn was interrupted.");
        notifier.await.unwrap();
        drain.await.unwrap();
        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restarts_a_failed_tool_process() {
        let marker =
            std::env::temp_dir().join(format!("crabbot-tool-restart-{}", std::process::id()));
        let _ = fs::remove_file(&marker);
        let script = format!(
            r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"protocol":{{"major":0,"minor":1}},"id":"tools","version":"0.1.0","capabilities":["tool"]}}}}' ;; *read*) if [ ! -e "{marker}" ]; then touch "{marker}"; exit 0; else printf '%s\n' '{{"jsonrpc":"2.0","id":7,"result":{{"text":"recovered"}}}}'; fi ;; *shutdown*) printf '%s\n' '{{"jsonrpc":"2.0","id":9999,"result":{{"ok":true}}}}'; exit 0 ;; esac; done"#,
            marker = marker.display()
        );
        let process = Process::start_with("sh", ["-c", &script]).await.unwrap();
        let plugins = registry([process]).await;
        let mut failed = None;
        let error = tool(
            &plugins,
            7,
            "read",
            serde_json::json!({"path": "note.txt"}),
            false,
            None,
            &mut failed,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("closed stdout"));
        assert_eq!(failed.as_deref(), Some("tools"));

        restart_tool(&plugins, failed.as_deref().unwrap()).await;
        let mut ignored = None;
        let output = tool(
            &plugins,
            7,
            "read",
            serde_json::json!({"path": "note.txt"}),
            false,
            None,
            &mut ignored,
        )
        .await
        .unwrap();
        assert_eq!(output, "recovered");
        stop_registry(&plugins).await;
        let _ = fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retries_failed_channel_delivery_and_resets_status() {
        let channel_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"telegram\",\"version\":\"0.1.0\",\"capabilities\":[\"channel\"]}}' ;; *poll*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":10,\"result\":{\"events\":[{\"id\":1,\"chat\":7,\"private\":true,\"sender\":8,\"text\":\"hi\"}]}}';; *send*) exit 0 ;; esac; done"#;
        let channel_script = channel_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let provider_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocol\":{\"major\":0,\"minor\":1},\"id\":\"codex\",\"version\":\"0.1.0\",\"capabilities\":[\"model\"]}}' ;; *generate*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":11,\"result\":{\"text\":\"world\",\"stop\":\"stop\"}}' ;; *shutdown*) printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":9999,\"result\":{\"ok\":true}}'; exit 0 ;; esac; done"#;
        let provider_script = provider_script.replace("\\\\n", "\\n").replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let plugins = registry([channel, provider]).await;
        let root = std::env::temp_dir().join(format!("crabbot-retry-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);

        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));

        let stop = Arc::new(Stop::new());
        let sessions_for_signal = Arc::clone(&sessions);
        let signal = Arc::clone(&stop);

        let notifier = tokio::spawn(async move {
            for _ in 0..200 {
                let ready = {
                    let store = sessions_for_signal.lock().unwrap();

                    store.sessions.get("telegram-7").is_some_and(|session| session.status == "idle")
                        && store.outbox.first().is_some_and(|delivery| {
                            delivery.status == super::state::DeliveryStatus::Uncertain
                        })
                };

                if ready {
                    signal.signal();
                    return;
                }

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            signal.signal();
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            super::bridge(
                &plugins,
                "telegram",
                "codex",
                "test",
                Arc::clone(&sessions),
                Arc::clone(&stop),
                Arc::new(Mutex::new(BTreeMap::new())),
                BTreeMap::new(),
                super::ApprovalMode::Off,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        notifier.await.unwrap();
        {
            let store = sessions.lock().unwrap();
            assert_eq!(store.sessions["telegram-7"].status, "idle");
            assert_eq!(store.outbox[0].status, super::state::DeliveryStatus::Uncertain);
        }

        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retries_channel_acknowledgement_after_restarting_plugin() {
        let root = test_root("ack");
        let marker = root.join("failed");
        let script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *ack*) if [ ! -e "__MARKER__" ]; then touch "__MARKER__"; printf '%s\n' '{"jsonrpc":"2.0","id":10,"error":{"code":-32000,"message":"retry"}}'; else printf '%s\n' '{"jsonrpc":"2.0","id":11,"result":{"acknowledged":true}}'; fi ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#
            .replace("__MARKER__", &marker.display().to_string())
            .replace("\\\\n", "\\n")
            .replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &script]).await.unwrap();
        let channel = Live::new(channel);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        let mut current = 0;
        let mut call = 10;
        commit_event(
            &channel,
            &sessions,
            "telegram",
            "event",
            Some(2),
            &mut current,
            Some(7),
            &mut call,
        )
        .await
        .unwrap();
        assert_eq!(current, 2);
        assert_eq!(call, 12);
        assert!(sessions.lock().unwrap().known("telegram", "event"));
        channel.stop().await.unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interrupts_working_sessions_on_shutdown() {
        let channel_script = r#"first=1; while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"telegram","version":"0.1.0","capabilities":["channel"]}}' ;; *poll*) if [ "$first" = 1 ]; then first=0; printf '%s\n' '{"jsonrpc":"2.0","id":10,"result":{"events":[{"id":1,"chat":7,"private":true,"sender":8,"text":"hi"}]}}'; fi ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let channel_script = channel_script.replace("\\n", "\n").replace("\\\"", "\"");
        let provider_script = r#"while IFS= read -r line; do case "$line" in *hello*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocol":{"major":0,"minor":1},"id":"codex","version":"0.1.0","capabilities":["model"]}}' ;; *generate*) sleep 5 ;; *shutdown*) printf '%s\n' '{"jsonrpc":"2.0","id":9999,"result":{"ok":true}}'; exit 0 ;; esac; done"#;
        let provider_script = provider_script.replace("\\n", "\n").replace("\\\"", "\"");
        let channel = Process::start_with("sh", ["-c", &channel_script]).await.unwrap();
        let provider = Process::start_with("sh", ["-c", &provider_script]).await.unwrap();
        let plugins = registry([channel, provider]).await;
        let root = std::env::temp_dir().join(format!("crabbot-interrupt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let sessions =
            Arc::new(Mutex::new(super::state::Store::load(root.join("sessions.json")).unwrap()));
        let stop = Arc::new(Stop::new());
        let signal = Arc::clone(&stop);
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            signal.signal();
        });

        super::bridge(
            &plugins,
            "telegram",
            "codex",
            "test",
            Arc::clone(&sessions),
            Arc::clone(&stop),
            Arc::new(Mutex::new(BTreeMap::new())),
            BTreeMap::new(),
            super::ApprovalMode::Off,
        )
        .await
        .unwrap();
        notifier.await.unwrap();
        assert_eq!(sessions.lock().unwrap().sessions["telegram-7"].status, "interrupted");
        stop_registry(&plugins).await;
        let _ = fs::remove_dir_all(root);
    }
}
