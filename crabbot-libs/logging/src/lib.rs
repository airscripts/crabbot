use std::{
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};

use tracing_subscriber::{
    EnvFilter,
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    layer::{Layer, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Command { verbosity: Verbosity, json: bool },
    Daemon,
    Plugin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verbosity {
    Quiet,
    Verbose,
    Debug,
}

struct JsonFormat;

impl<S, N> FormatEvent<S, N> for JsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let mut fields = serde_json::Map::new();
        event.record(&mut JsonFields(&mut fields));

        let mut value = serde_json::Map::from_iter([
            ("timestamp".into(), serde_json::json!(timestamp())),
            ("level".into(), serde_json::json!(metadata.level().as_str())),
            ("target".into(), serde_json::json!(metadata.target())),
        ]);

        value.extend(fields);
        writeln!(writer, "{}", serde_json::Value::Object(value))
    }
}

struct JsonFields<'a>(&'a mut serde_json::Map<String, serde_json::Value>);

impl Visit for JsonFields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().into(), serde_json::json!(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), serde_json::json!(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), serde_json::json!(value));
    }
}

fn timestamp() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Text,
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Settings {
    filter: String,
    format: Format,
    invalid_filter: bool,
    invalid_format: bool,
}

fn default_filter() -> &'static str {
    "trace"
}

fn terminal_filter(mode: Mode) -> Option<&'static str> {
    match mode {
        Mode::Command { verbosity: Verbosity::Debug, json: false } => Some("debug"),
        Mode::Command { .. } | Mode::Daemon | Mode::Plugin => None,
    }
}

fn settings(
    _mode: Mode,
    crabbot_filter: Option<&str>,
    rust_filter: Option<&str>,
    format: Option<&str>,
) -> Settings {
    let default = default_filter();
    let mut invalid_filter = false;

    let filter = [crabbot_filter, rust_filter]
        .into_iter()
        .flatten()
        .find(|value| match EnvFilter::try_new(*value) {
            Ok(_) => true,

            Err(_) => {
                invalid_filter = true;
                false
            }
        })
        .unwrap_or(default)
        .to_owned();

    let (format, invalid_format) = match format {
        None | Some("text") => (Format::Text, false),
        Some("json") => (Format::Json, false),
        Some(_) => (Format::Text, true),
    };

    Settings { filter, format, invalid_filter, invalid_format }
}

pub fn initialize(mode: Mode) {
    let _ = initialize_with_command_log_enabled(mode, false);
}

pub fn initialize_with_command_log(mode: Mode) -> Option<PathBuf> {
    initialize_with_command_log_enabled(mode, true)
}

fn initialize_with_command_log_enabled(mode: Mode, capture: bool) -> Option<PathBuf> {
    let settings = settings(
        mode,
        std::env::var("CRABBOT_LOG").ok().as_deref(),
        std::env::var("RUST_LOG").ok().as_deref(),
        std::env::var("CRABBOT_LOG_FORMAT").ok().as_deref(),
    );

    let file_filter =
        EnvFilter::try_new(&settings.filter).unwrap_or_else(|_| EnvFilter::new(default_filter()));

    let Some(writer) = LogFile::open(settings.format, mode, capture) else {
        initialize_terminal(mode);
        return None;
    };

    let command_path = writer.command_path.clone();

    let file_layer = match settings.format {
        Format::Text => tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_target(true)
            .with_ansi(false)
            .boxed(),

        Format::Json => {
            tracing_subscriber::fmt::layer().event_format(JsonFormat).with_writer(writer).boxed()
        }
    }
    .with_filter(file_filter);

    let initialized = if let Some(filter) = terminal_filter(mode) {
        tracing_subscriber::registry()
            .with(file_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_target(true)
                    .with_ansi(false)
                    .with_filter(EnvFilter::new(filter)),
            )
            .try_init()
            .is_ok()
    } else {
        tracing_subscriber::registry().with(file_layer).try_init().is_ok()
    };

    if initialized && (settings.invalid_filter || settings.invalid_format) {
        tracing::warn!(
            invalid_filter = settings.invalid_filter,
            invalid_format = settings.invalid_format,
            "Ignoring invalid logging configuration."
        );
    }

    initialized.then_some(command_path).flatten()
}

fn initialize_terminal(mode: Mode) {
    if let Some(filter) = terminal_filter(mode) {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(filter))
            .with_writer(std::io::stderr)
            .with_target(true)
            .with_ansi(false)
            .try_init();
    }
}

#[derive(Clone)]
struct LogFile {
    directory: PathBuf,
    extension: &'static str,
    command_path: Option<PathBuf>,
}

impl LogFile {
    fn open(format: Format, mode: Mode, capture: bool) -> Option<Self> {
        let home = std::env::var_os("CRABBOT_HOME").map(PathBuf::from).unwrap_or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    std::env::var_os("HOME")
                        .or_else(|| std::env::var_os("USERPROFILE"))
                        .map_or_else(|| PathBuf::from("."), PathBuf::from)
                        .join(".config")
                })
                .join("crabbot")
        });

        let directory = home.join("logs");
        create_private_dir(&directory).ok()?;
        let extension = match format {
            Format::Text => "log",
            Format::Json => "jsonl",
        };

        let command_path = if capture
            && let Mode::Command { verbosity, .. } = mode
            && verbosity != Verbosity::Quiet
        {
            let label = if verbosity == Verbosity::Debug { "debug" } else { "verbose" };

            let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
            let path = directory.join(format!(
                "{}-{label}-{stamp}-{}.{}",
                date_stamp(),
                std::process::id(),
                extension
            ));

            private_log_file(path.clone()).ok().map(|_| path)
        } else {
            None
        };

        Some(Self { directory, extension, command_path })
    }
}

impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for LogFile {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let filename = format!("{}.{}", date_stamp(), self.extension);

        LogWriter { path: self.directory.join(filename), command_path: self.command_path.clone() }
    }
}

struct LogWriter {
    path: PathBuf,
    command_path: Option<PathBuf>,
}

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut daily = private_log_file(self.path.clone())?;
        daily.write_all(bytes)?;
        daily.flush()?;

        if let Some(path) = &self.command_path {
            let mut command = private_log_file(path.clone())?;
            command.write_all(bytes)?;
            command.flush()?;
        }

        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn date_stamp() -> String {
    date_stamp_at(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs())
}

fn date_stamp_at(seconds: u64) -> String {
    crabbot_date::format_date(seconds)
}

#[cfg(unix)]
fn create_private_dir(path: &std::path::Path) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    fs::DirBuilder::new().recursive(true).mode(0o700).create(path).or_else(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists { Ok(()) } else { Err(error) }
    })?;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn create_private_dir(path: &std::path::Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

#[cfg(unix)]
fn private_log_file(path: PathBuf) -> io::Result<File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = OpenOptions::new().create(true).append(true).mode(0o600).open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;

    Ok(file)
}

#[cfg(not(unix))]
fn private_log_file(path: PathBuf) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

pub async fn run_plugin<F, E>(id: &'static str, run: F) -> ExitCode
where
    F: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    initialize(Mode::Plugin);
    tracing::info!(plugin = id, "Plugin process started.");

    match run.await {
        Ok(()) => {
            tracing::info!(plugin = id, "Plugin process stopped.");
            ExitCode::SUCCESS
        }

        Err(error) => {
            tracing::error!(plugin = id, error = %redact_diagnostic(error.to_string()), "Plugin process failed.");
            ExitCode::FAILURE
        }
    }
}

pub fn redact_diagnostic(value: impl AsRef<str>) -> String {
    redact_bytes(value.as_ref().as_bytes())
}

pub fn redact_bytes(value: &[u8]) -> String {
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
            let normalized = line.to_ascii_lowercase();

            if normalized.contains("authorization:") || normalized.contains("proxy-authorization:")
            {
                "Authorization: [redacted]".to_owned()
            } else if ["api_key", "api-key", "token", "secret", "password"]
                .iter()
                .any(|field| normalized.contains(field))
                && let Some((field, _)) = line.split_once(['=', ':'])
            {
                format!("{field}=[redacted]")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use super::{
        Format, LogWriter, Mode, Settings, Verbosity, date_stamp_at, default_filter, redact_bytes,
        settings, terminal_filter, timestamp,
    };

    #[test]
    fn application_logs_default_to_all_levels() {
        assert_eq!(default_filter(), "trace");
    }

    #[test]
    fn terminal_logs_require_a_verbosity_flag() {
        assert_eq!(
            terminal_filter(Mode::Command { verbosity: Verbosity::Quiet, json: false }),
            None
        );

        assert_eq!(
            terminal_filter(Mode::Command { verbosity: Verbosity::Verbose, json: false }),
            None
        );

        assert_eq!(
            terminal_filter(Mode::Command { verbosity: Verbosity::Debug, json: false }),
            Some("debug")
        );

        assert_eq!(
            terminal_filter(Mode::Command { verbosity: Verbosity::Debug, json: true }),
            None
        );

        assert_eq!(terminal_filter(Mode::Daemon), None);
        assert_eq!(terminal_filter(Mode::Plugin), None);
    }

    #[test]
    fn command_log_receives_the_raw_application_log_lines() {
        let root = std::env::temp_dir().join(format!(
            "crabbot-command-log-{}-{}",
            std::process::id(),
            timestamp()
        ));

        fs::create_dir_all(&root).unwrap();
        let daily = root.join("daily.log");
        let command = root.join("command.log");
        let mut writer = LogWriter { path: daily.clone(), command_path: Some(command.clone()) };

        writer.write_all(b"DEBUG crabbot_runtime: turn started\n").unwrap();

        assert_eq!(fs::read(&daily).unwrap(), b"DEBUG crabbot_runtime: turn started\n");
        assert_eq!(fs::read(&command).unwrap(), b"DEBUG crabbot_runtime: turn started\n");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(fs::metadata(command).unwrap().permissions().mode() & 0o777, 0o600);
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn json_commands_keep_application_logs_even_when_environment_filters_are_set() {
        assert_eq!(
            settings(
                Mode::Command { verbosity: Verbosity::Debug, json: true },
                Some("trace"),
                Some("debug"),
                Some("json")
            ),
            Settings {
                filter: "trace".into(),
                format: Format::Json,
                invalid_filter: false,
                invalid_format: false,
            }
        );
    }

    #[test]
    fn project_filter_precedes_rust_log_and_format_selects_json() {
        assert_eq!(
            settings(Mode::Daemon, Some("crabbot_runtime=debug"), Some("warn"), Some("json")),
            Settings {
                filter: "crabbot_runtime=debug".into(),
                format: Format::Json,
                invalid_filter: false,
                invalid_format: false,
            }
        );
    }

    #[test]
    fn invalid_environment_values_fall_back_with_a_warning_flag() {
        assert_eq!(
            settings(Mode::Plugin, Some("crabbot=notalevel"), Some("debug"), Some("yaml")),
            Settings {
                filter: "debug".into(),
                format: Format::Text,
                invalid_filter: true,
                invalid_format: true,
            }
        );
    }

    #[test]
    fn falls_back_to_all_level_logs_when_filters_are_invalid_or_absent() {
        assert_eq!(
            settings(Mode::Daemon, Some("crabbot=notalevel"), None, None),
            Settings {
                filter: "trace".into(),
                format: Format::Text,
                invalid_filter: true,
                invalid_format: false,
            }
        );
    }

    #[test]
    fn redacts_url_credentials_and_authorization_headers() {
        assert_eq!(
            redact_bytes(
                b"https://user:secret@example.com\nAuthorization: Bearer secret\nAPI_KEY=secret"
            ),
            "https://[redacted]@example.com\nAuthorization: [redacted]\nAPI_KEY=[redacted]"
        );
    }

    #[test]
    fn log_file_writer_appends_complete_records() {
        let path = std::env::temp_dir().join(format!(
            "crabbot-log-{}-{}.log",
            std::process::id(),
            timestamp()
        ));

        let mut writer = super::LogWriter { path: path.clone(), command_path: None };
        writer.write_all(b"first record\n").unwrap();
        writer.flush().unwrap();
        writer.write_all(b"second record\n").unwrap();
        writer.flush().unwrap();
        drop(writer);

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "first record\nsecond record\n");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn log_file_dates_sort_lexically_and_use_utc() {
        assert_eq!(date_stamp_at(0), "1970-01-01");
        assert_eq!(date_stamp_at(1_709_251_200), "2024-03-01");
    }

    #[test]
    fn json_format_emits_structured_application_log_fields() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Buffer {
            fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(value);
                Ok(value.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for Buffer {
            type Writer = Self;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let output = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .event_format(super::JsonFormat)
            .with_writer(Buffer(Arc::clone(&output)))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(plugin = "test", "Plugin process started.");
        });

        let output = output.lock().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&output).unwrap();

        assert_eq!(event["level"], "INFO");
        assert_eq!(event["plugin"], "test");
        assert_eq!(event["target"], "crabbot_log::tests");
        assert!(event["timestamp"].as_u64().is_some());
        assert!(event["message"].as_str().unwrap().contains("Plugin process started."));
    }

    #[tokio::test]
    async fn plugin_runner_preserves_exit_status() {
        assert_eq!(
            super::run_plugin("test", async { Ok::<(), String>(()) }).await,
            std::process::ExitCode::SUCCESS
        );

        assert_eq!(
            super::run_plugin("test", async { Err("failed".to_owned()) }).await,
            std::process::ExitCode::FAILURE
        );
    }
}
