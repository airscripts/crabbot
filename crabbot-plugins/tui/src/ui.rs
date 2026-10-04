use crabbot_core::{
    Result,
    types::{Message, Role},
};

#[cfg(not(windows))]
use crossterm::event::{
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers, MouseEventKind},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Widget, Wrap},
};

use std::{
    collections::VecDeque,
    fs::File,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    sync::mpsc,
    time::timeout,
};

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const ACCENT_COLOR: Color = Color::Rgb(255, 140, 0);
const TRANSCRIPT_LIMIT: usize = 512 * 1024;
const INTERACTION_LIMIT: usize = 12 * 1024;
const INPUT_HISTORY_LIMIT: usize = super::data::HISTORY_LIMIT;
const INPUT_MAX_ROWS: usize = 5;
const COMPOSER_GAP_ROWS: u16 = 1;
const MESSAGE_GAP_ROWS: usize = 2;
const GENERATION_FRAME_INTERVAL: Duration = Duration::from_millis(40);
const TYPEWRITER_INTERVAL: Duration = Duration::from_millis(16);
const SESSION_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const TYPEWRITER_BACKLOG: usize = 96;
const TYPEWRITER_BURST: usize = 2;
const TYPEWRITER_CATCHUP_BURST: usize = 12;
const STATUSLINE_ITEMS: [(&str, &str); 6] = [
    ("Title", "title"),
    ("Model", "model"),
    ("Context", "context"),
    ("Session", "session"),
    ("Workspace", "workspace"),
    ("Status", "status"),
];

const STATUSLINE_SCROLL_INTERVAL: Duration = Duration::from_millis(300);

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
struct TuiPreferences {
    #[serde(deserialize_with = "deserialize_statusline")]
    statusline: StatuslineOptions,
    #[serde(default = "default_typewriter")]
    typewriter: bool,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct StatuslineOptions {
    title: bool,
    model: bool,
    context: bool,
    session: bool,
    workspace: bool,
    status: bool,
}

impl Default for StatuslineOptions {
    fn default() -> Self {
        Self {
            title: true,
            model: true,
            context: true,
            session: true,
            workspace: true,
            status: false,
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum SavedStatusline {
    Legacy(String),
    Options(StatuslineOptions),
}

fn deserialize_statusline<'de, D>(
    deserializer: D,
) -> std::result::Result<StatuslineOptions, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let statusline = <SavedStatusline as serde::Deserialize>::deserialize(deserializer)?;

    match statusline {
        SavedStatusline::Legacy(value) => Ok(statusline_from_legacy(&value)),
        SavedStatusline::Options(options) => Ok(options),
    }
}

fn statusline_from_legacy(value: &str) -> StatuslineOptions {
    let mut options = StatuslineOptions {
        title: false,
        model: false,
        context: false,
        session: false,
        workspace: false,
        status: false,
    };

    options.title = value.contains("{name}");
    options.model = value.contains("{model}");
    options.context = value.contains("{context}");
    options.session = value.contains("{session}");
    options.workspace = value.contains("{workspace}");
    options.status = value.contains("{status}");

    if STATUSLINE_ITEMS.iter().all(|(_, item)| !statusline_item_enabled(options, item)) {
        StatuslineOptions::default()
    } else {
        options
    }
}

fn statusline_item_enabled(options: StatuslineOptions, item: &str) -> bool {
    match item {
        "title" => options.title,
        "model" => options.model,
        "context" => options.context,
        "session" => options.session,
        "workspace" => options.workspace,
        "status" => options.status,
        _ => false,
    }
}

fn toggle_statusline_item(options: &mut StatuslineOptions, item: &str) {
    match item {
        "title" => options.title = !options.title,
        "model" => options.model = !options.model,
        "context" => options.context = !options.context,
        "session" => options.session = !options.session,
        "workspace" => options.workspace = !options.workspace,
        "status" => options.status = !options.status,

        _ => {}
    }
}

fn default_typewriter() -> bool {
    true
}

impl Default for TuiPreferences {
    fn default() -> Self {
        Self { statusline: StatuslineOptions::default(), typewriter: true }
    }
}

#[derive(Default)]
struct App {
    transcript: String,
    transcript_generation: u64,
    transcript_layout: Option<TranscriptLayout>,
    reveal_queue: VecDeque<String>,
    reveal_bytes: usize,
    input: Vec<char>,
    cursor: usize,
    history: Vec<String>,
    history_path: Option<PathBuf>,
    history_index: Option<usize>,
    history_draft: String,
    transcript_scroll: ScrollState,
    mouse_position: Option<(u16, u16)>,
    input_scroll: ScrollState,
    input_cursor_needs_visibility: bool,
    input_area: Option<Rect>,
    command_selection: usize,
    approval_selection: usize,
    command_options: Vec<(&'static str, &'static str)>,
    status: String,
    session: String,
    model: String,
    model_available: bool,
    context_usage: Option<crabbot_core::types::ContextUsage>,
    workspace: String,
    default_workspace: String,
    name: String,
    statusline: StatuslineOptions,
    statusline_draft: StatuslineOptions,
    statusline_picker: bool,
    statusline_selection: usize,
    statusline_started: Option<Instant>,
    statusline_changed: bool,
    preferences_path: PathBuf,
    typewriter: bool,
    theme_enabled: bool,
    generation_active: bool,
    compaction_active: bool,
    session_working: bool,
    generation_started: Option<Instant>,
    compaction_started: Option<Instant>,
    generation_message: &'static str,
    compaction_message: &'static str,
    exiting: bool,
    interrupt_requested: bool,
    reply_pending: bool,
    reply_system: bool,
    approval_reply_pending: bool,
    pending_model: Option<String>,
    pending_workspace: Option<Option<String>>,
    pending_clear: bool,
    context_output: String,
    pending_interactions: VecDeque<PendingInteraction>,
    completed_interactions: Vec<SavedInteraction>,
    interaction_sequence: u64,
}

struct TranscriptLayout {
    generation: u64,
    width: u16,
    rows: Vec<Line<'static>>,
    message_rows: Vec<Range<usize>>,
    hovered_message: Option<usize>,
    hovered_rows: Option<Vec<Line<'static>>>,
}

struct TranscriptViewport<'a> {
    rows: &'a [Line<'static>],
}

impl Widget for TranscriptViewport<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        for (offset, row) in self.rows.iter().take(area.height as usize).enumerate() {
            buffer.set_line(area.x, area.y.saturating_add(offset as u16), row, area.width);
        }
    }
}

#[derive(Clone, Copy)]
struct ScrollState {
    offset: usize,
    content_rows: usize,
    viewport_rows: usize,
    follow_end: bool,
}

impl Default for ScrollState {
    fn default() -> Self {
        Self { offset: 0, content_rows: 0, viewport_rows: 0, follow_end: true }
    }
}

impl ScrollState {
    fn set_extent(&mut self, content_rows: usize, viewport_rows: usize) {
        self.content_rows = content_rows;
        self.viewport_rows = viewport_rows;
        let max_offset = self.max_offset();

        if self.follow_end {
            self.offset = max_offset;
        } else {
            self.offset = self.offset.min(max_offset);
            self.follow_end = self.offset == max_offset;
        }
    }

    fn scroll_up(&mut self, rows: usize) {
        if self.max_offset() > 0 {
            self.follow_end = false;
            self.offset = self.offset.saturating_sub(rows);
        }
    }

    fn scroll_down(&mut self, rows: usize) {
        self.offset = self.offset.saturating_add(rows).min(self.max_offset());
        self.follow_end = self.offset == self.max_offset();
    }

    fn page_up(&mut self) {
        self.scroll_up(self.page_rows());
    }

    fn page_down(&mut self) {
        self.scroll_down(self.page_rows());
    }

    fn follow_latest(&mut self) {
        self.follow_end = true;
        self.offset = self.max_offset();
    }

    fn ensure_visible(&mut self, row: usize) {
        if self.viewport_rows == 0 {
            self.follow_latest();
            return;
        }

        let viewport_end = self.offset.saturating_add(self.viewport_rows);

        if row < self.offset {
            self.offset = row;
        } else if row >= viewport_end {
            self.offset = row.saturating_add(1).saturating_sub(self.viewport_rows);
        }

        self.offset = self.offset.min(self.max_offset());
        self.follow_end = self.offset == self.max_offset();
    }

    fn page_rows(&self) -> usize {
        self.viewport_rows.saturating_sub(1).max(1)
    }

    fn max_offset(&self) -> usize {
        self.content_rows.saturating_sub(self.viewport_rows)
    }
}

struct PendingInteraction {
    session: String,
    input: String,
    output: String,
    system: bool,
}

struct SavedInteraction {
    session: String,
    input: String,
    output: String,
    sequence: u64,
    system: bool,
}

pub(super) struct ModelOptions {
    pub(super) plugin: String,
    pub(super) model: String,
    pub(super) model_override: Option<String>,
}

pub async fn run<C, F>(
    output: File,
    home: String,
    model_options: ModelOptions,
    session: String,
    name: String,
    host: C,
) -> crabbot_core::Result<()>
where
    C: Fn(String, String, serde_json::Value) -> F + Copy + Send + Sync + 'static,
    F: std::future::Future<Output = crabbot_core::Result<serde_json::Value>> + Send + 'static,
{
    let ModelOptions { plugin, model, model_override } = model_options;

    host(home.clone(), "session.ensure".into(), serde_json::json!({"id": session, "model": model}))
        .await?;

    if let Some(model) = model_override {
        host(
            home.clone(),
            "session.model".into(),
            serde_json::json!({"id": session, "model": model}),
        )
        .await?;
    }

    let initial_session = load_session(&home, &session, host).await?;
    let model_available = crate::has_capability(&home, "model");
    let command_options = command_options(&home, model_available);
    let mut terminal = Terminal::new(CrosstermBackend::new(output.try_clone()?))?;
    let _guard = TerminalGuard::enter(output)?;
    execute!(terminal.backend_mut(), terminal::Clear(terminal::ClearType::All))?;

    let (mut command_tx, command_rx) = duplex(16 * 1024);
    let (mut output_rx, response_tx) = duplex(64 * 1024);
    let preferences_path = super::data::plugin_file(&home, "tui", "preferences.toml");

    let preferences = crabbot_file::load(&preferences_path, 16 * 1024)
        .ok()
        .flatten()
        .and_then(|bytes| toml::from_str::<TuiPreferences>(&String::from_utf8_lossy(&bytes)).ok())
        .unwrap_or_default();

    let statusline = preferences.statusline;
    let typewriter = preferences.typewriter;
    let theme_enabled = std::env::var("CRABBOT_TUI_THEME").as_deref() != Ok("off");
    let history_path = super::data::plugin_file(&home, "tui", "history.json");
    let history = super::data::load_history(&history_path);

    let engine_model = model.clone();
    let (session_tx, mut session_rx) = mpsc::unbounded_channel();
    let (engine_event_tx, mut engine_event_rx) = mpsc::unbounded_channel();
    let (interrupt_tx, interrupt_rx) = mpsc::unbounded_channel();

    let mut engine = tokio::spawn(crate::run_with(
        command_rx,
        response_tx,
        crate::EngineConfig {
            home: home.clone(),
            plugin,
            model: engine_model,
            model_override: None,
            session,
        },
        host,
        crate::EngineEvents {
            session: Some(session_tx),
            engine: Some(engine_event_tx),
            streaming_host: Some(crate::daemon_streaming_control),
        },
        interrupt_rx,
    ));

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let event_cancelled = Arc::clone(&cancelled);

    let event_thread = thread::spawn(move || {
        while !event_cancelled.load(Ordering::Relaxed) {
            if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                match event::read() {
                    Ok(event) => {
                        if event_tx.send(event).is_err() {
                            break;
                        }
                    }

                    Err(_) => break,
                }
            }
        }
    });

    let default_workspace = crate::workspace_root(&home, None).display().to_string();
    let workspace = initial_session.workspace.clone().unwrap_or_else(|| default_workspace.clone());

    let mut app = App {
        status: "Ready".into(),
        transcript: render_messages(&initial_session.messages, &name),
        session: initial_session.id,
        model: display_model(&initial_session.model, model_available),
        model_available,
        context_usage: initial_session.context_usage,
        statusline_started: Some(Instant::now()),
        command_options,
        history,
        history_path: Some(history_path),
        workspace,
        session_working: initial_session.working,
        default_workspace,
        name,
        statusline,
        preferences_path,
        typewriter,
        theme_enabled,
        generation_message: random_generation_message(),
        ..App::default()
    };

    let mut buffer = [0_u8; 4096];
    let mut engine_finished = false;
    let mut session_events_open = true;
    let mut engine_events_open = true;
    let mut quit = false;
    let mut session_refresh = tokio::time::interval(SESSION_REFRESH_INTERVAL);
    let mut generation_refresh = tokio::time::interval(GENERATION_FRAME_INTERVAL);
    let mut statusline_refresh = tokio::time::interval(GENERATION_FRAME_INTERVAL);
    generation_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    statusline_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    session_refresh.tick().await;
    generation_refresh.tick().await;
    statusline_refresh.tick().await;

    while !quit {
        terminal.draw(|frame| draw(frame, &mut app))?;

        tokio::select! {
            event = event_rx.recv() => {
                if let Some(event) = event {
                    if !interrupt_generation(&event, &mut app, &interrupt_tx) {
                        quit = handle_event(event, &mut app, &mut command_tx, engine_finished).await?;
                    }

                    for interaction in app.take_persistable_interactions() {
                        persist_interaction(&home, interaction, host).await?;
                    }

                    if app.statusline_changed {
                        save_preferences(
                            &app.preferences_path,
                            app.statusline,
                            app.typewriter,
                        )?;

                        app.statusline_changed = false;
                    }
                }
            }

            session = session_rx.recv(), if session_events_open => {
                match session {
                    Some(session) => {
                        let renamed = app.pending_interactions.iter().any(
                            |interaction| interaction.input.starts_with("/session rename "),
                        );

                        let created_input = app
                            .pending_interactions
                            .iter()
                            .find(|interaction| {
                                interaction.input.starts_with("/session create ")
                                    || interaction.input.starts_with("/new ")
                            })
                            .map(|interaction| interaction.input.clone());

                        if let Some(interaction) = app.pending_interactions.iter_mut().find(|interaction| {
                            interaction.input.starts_with("/session rename ")
                                || interaction.input.starts_with("/session create ")
                                || interaction.input.starts_with("/new ")
                        }) && (renamed || created_input.is_some()) {
                            interaction.session = session.id.clone();
                        }

                        app.status = format!("Session {}", session_display_name(&session.id));

                        if renamed {
                            app.session = session.id;
                            app.model = display_model(&session.model, app.model_available);

                            app.workspace = session
                                .workspace
                                .unwrap_or_else(|| app.default_workspace.clone());
                        } else if created_input.is_some() || app.session != session.id {
                            app.restore_session(session);

                            if let Some(input) = created_input {
                                app.push_user(&input);
                            }
                        } else {
                            app.refresh_session(session);
                        }
                    }

                    None => session_events_open = false,
                }
            }

            _ = session_refresh.tick(), if !app.generation_active && !app.compaction_active => {
                let session_id = app.session.clone();

                if let Ok(session) = load_session_snapshot(&home, &session_id, host).await {
                    app.refresh_session(session);
                }
            }

            event = engine_event_rx.recv(), if engine_events_open => {
                match event {
                    Some(crate::EngineEvent::GenerationStarted) => {
                        app.generation_active = true;
                        app.generation_started = Some(Instant::now());
                        app.interrupt_requested = false;
                        app.generation_message = random_generation_message();
                    }

                    Some(crate::EngineEvent::CompactionStarted) => {
                        app.begin_compaction();
                    }

                    Some(crate::EngineEvent::CompactionFinished) => {
                        app.finish_compaction();
                    }

                    Some(crate::EngineEvent::GenerationFinished { interrupted }) => {
                        app.finish_generation(interrupted);

                        for interaction in app.take_persistable_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }

                    Some(crate::EngineEvent::AssistantText(text)) => {
                        let interactions = app.push_assistant_output(text);

                        for interaction in interactions {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }

                    Some(crate::EngineEvent::SystemText(text)) => {
                        app.push_system_output(&text);
                    }

                    Some(crate::EngineEvent::SystemNotice(text)) => {
                        app.push_system(&text);
                    }

                    Some(crate::EngineEvent::AssistantFinished) => {
                        app.finish_streamed_interactions();
                    }

                    None => engine_events_open = false,
                }
            }

            _ = tokio::time::sleep(TYPEWRITER_INTERVAL), if !app.reveal_queue.is_empty() => {
                app.reveal_next();
            }

            _ = generation_refresh.tick(), if app.generation_active || app.compaction_active => {
                // Keep the waiting-message glow moving smoothly.
            }

            _ = statusline_refresh.tick() => {}

            result = output_rx.read(&mut buffer), if !engine_finished => {
                match result {
                    Ok(0) => {
                        engine_finished = true;
                        app.generation_active = false;
                        app.compaction_active = false;
                        app.reply_pending = false;
                        app.reply_system = false;
                        app.pending_model = None;
                        app.pending_workspace = None;
                        app.pending_clear = false;
                        app.context_output.clear();

                        match (&mut engine).await {
                            Ok(Ok(())) => app.status = "Session ended".into(),

                            Ok(Err(error)) => {
                                app.status = "Session failed".into();
                                let message = format!("\n{error}");
                                app.capture_interaction_output(&message);
                                app.push_output(message);
                            }

                            Err(error) => {
                                app.status = "Session failed".into();
                                let message = format!("\n{error}");
                                app.capture_interaction_output(&message);
                                app.push_output(message);
                            }
                        }

                        for interaction in app.take_persistable_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }

                        for interaction in app.finish_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }

                    Ok(count) => {
                        let text = String::from_utf8_lossy(&buffer[..count]).into_owned();
                        let interactions = app.push_assistant_output(text);

                        for interaction in interactions {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }

                    Err(error) => {
                        let message = format!("\nOutput error: {error}");
                        app.capture_interaction_output(&message);
                        app.push_output(message);
                        engine.abort();
                        engine_finished = true;
                        app.generation_active = false;
                        app.compaction_active = false;
                        app.reply_pending = false;

                        for interaction in app.take_persistable_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }

                        for interaction in app.finish_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }
                }
            }

        }
    }

    if !engine_finished {
        app.exiting = true;
        terminal.draw(|frame| draw(frame, &mut app))?;
        let _ = command_tx.write_all(b"/quit\n").await;

        if timeout(Duration::from_secs(2), &mut engine).await.is_err() {
            engine.abort();
            let _ = engine.await;
        }

        while let Ok(event) = engine_event_rx.try_recv() {
            match event {
                crate::EngineEvent::AssistantText(text) => {
                    let interactions = app.push_assistant_output(text);

                    for interaction in interactions {
                        persist_interaction(&home, interaction, host).await?;
                    }
                }

                crate::EngineEvent::SystemText(text) => app.push_system_output(&text),

                crate::EngineEvent::AssistantFinished => {
                    app.finish_streamed_interactions();
                }

                crate::EngineEvent::SystemNotice(text) => app.push_system(&text),

                crate::EngineEvent::GenerationStarted => {}

                crate::EngineEvent::CompactionStarted | crate::EngineEvent::CompactionFinished => {}

                crate::EngineEvent::GenerationFinished { .. } => {}
            }
        }

        loop {
            let count = output_rx.read(&mut buffer).await?;

            if count == 0 {
                break;
            }

            let text = String::from_utf8_lossy(&buffer[..count]).into_owned();
            let interactions = app.push_assistant_output(text);

            for interaction in interactions {
                persist_interaction(&home, interaction, host).await?;
            }
        }

        app.finish_streamed_interactions();
    }

    cancelled.store(true, Ordering::Relaxed);
    let _ = tokio::task::spawn_blocking(move || event_thread.join()).await;

    Ok(())
}

async fn load_session<C, F>(home: &str, id: &str, host: C) -> Result<SessionView>
where
    C: Fn(String, String, serde_json::Value) -> F,
    F: std::future::Future<Output = Result<serde_json::Value>>,
{
    load_session_snapshot(home, id, host).await
}

async fn load_session_snapshot<C, F>(home: &str, id: &str, host: C) -> Result<SessionView>
where
    C: Fn(String, String, serde_json::Value) -> F,
    F: std::future::Future<Output = Result<serde_json::Value>>,
{
    let value = host(home.to_owned(), "session.get".into(), serde_json::json!({"id": id})).await?;

    let model = value["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("Session has no model configured.".into()))?
        .to_owned();

    let messages = serde_json::from_value(value["messages"].clone())?;
    let workspace = value["workspace"].as_str().map(str::to_owned);
    let context_usage = serde_json::from_value(value["context_usage"].clone()).ok().flatten();
    let working = value["status"] == "working" || value["inflight"] == true;

    Ok(SessionView { id: id.to_owned(), model, workspace, context_usage, messages, working })
}

async fn persist_interaction<C, F>(home: &str, interaction: SavedInteraction, host: C) -> Result<()>
where
    C: Fn(String, String, serde_json::Value) -> F + Copy,
    F: std::future::Future<Output = Result<serde_json::Value>>,
{
    let output = if interaction.output.is_empty() {
        "No response was produced."
    } else {
        interaction.output.as_str()
    };

    let entries = [
        ("user", Role::User, Some("tui".to_owned()), interaction.input.as_str()),
        ("assistant", Role::Assistant, None, output),
    ];

    for (role_name, role, sender, text) in entries {
        let text = bounded_interaction_text(text);
        let session = interaction.session.clone();
        let message = Message {
            id: crate::message_id(
                &format!(
                    "{}interaction-{role_name}",
                    if interaction.system && role_name == "assistant" { "system-" } else { "" }
                ),
                interaction.sequence,
            ),
            session: session.clone(),
            role,
            sender,
            content: vec![crabbot_core::types::Content::Text { text }],
        };

        host(
            home.to_owned(),
            "session.append".into(),
            serde_json::json!({"id": session, "message": message}),
        )
        .await?;
    }

    Ok(())
}

fn bounded_interaction_text(text: &str) -> String {
    if text.len() <= INTERACTION_LIMIT {
        return text.to_owned();
    }

    let mut end = INTERACTION_LIMIT - "\n[history entry truncated]".len();

    while !text.is_char_boundary(end) {
        end -= 1;
    }

    format!("{}\n[history entry truncated]", &text[..end])
}

pub(super) struct SessionView {
    pub(super) id: String,
    pub(super) model: String,
    pub(super) workspace: Option<String>,
    pub(super) context_usage: Option<crabbot_core::types::ContextUsage>,
    pub(super) messages: Vec<Message>,
    pub(super) working: bool,
}

fn display_model(model: &str, model_available: bool) -> String {
    if model_available { model.into() } else { "unset".into() }
}

fn render_messages(messages: &[Message], name: &str) -> String {
    let mut transcript = String::new();

    for message in messages {
        let timestamp = message_timestamp(&message.id);
        let terminal_system = message.id.starts_with("tui-system-");
        let label = match &message.role {
            Role::Assistant if terminal_system => format_role_label("System", timestamp.as_deref()),
            Role::User => format_role_label("You", timestamp.as_deref()),
            Role::Assistant => format_role_label(name, timestamp.as_deref()),

            Role::Tool => {
                format_role_label(message.sender.as_deref().unwrap_or("Tool"), timestamp.as_deref())
            }

            Role::System => format_role_label("System", timestamp.as_deref()),
        };

        let body = message
            .content
            .iter()
            .map(crabbot_core::types::Content::render)
            .filter(|content| !content.is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        if body.is_empty() {
            continue;
        }

        transcript.push_str(&label);
        transcript.push('\n');
        transcript.push_str(&body);
        transcript.push_str("\n\n");
    }

    transcript
}

fn format_role_label(label: &str, timestamp: Option<&str>) -> String {
    timestamp.map_or_else(|| label.to_owned(), |timestamp| format!("{label} | {timestamp}"))
}

fn message_timestamp(id: &str) -> Option<String> {
    let (timestamp, digits) = id.split('-').find_map(|part| {
        (part.len() >= 13 && part.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| part.parse::<u128>().ok().map(|timestamp| (timestamp, part.len())))
            .flatten()
    })?;

    let seconds = match digits {
        19.. => timestamp / 1_000_000_000,
        16..=18 => timestamp / 1_000_000,
        13..=15 => timestamp / 1_000,
        _ => return None,
    };

    super::date::format_datetime(u64::try_from(seconds).ok()?)
}

fn current_timestamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());

    super::date::format_datetime(seconds).unwrap_or_else(|| "time unavailable".into())
}

const GENERATION_MESSAGES: &[&str] = &[
    "The Crabbot is consulting the tide charts...",
    "The Crabbot is pinching the problem from both sides...",
    "The Crabbot is rearranging its tiny thoughts...",
    "The Crabbot is checking its shell for inspiration...",
    "The Crabbot is scuttling toward an answer...",
    "The Crabbot is asking the sea what it thinks...",
    "The Crabbot is weighing the evidence...",
    "The Crabbot is tracing the thread...",
    "The Crabbot is lining up the possibilities...",
    "The Crabbot is turning the question over...",
    "The Crabbot is checking each assumption...",
    "The Crabbot is sorting signal from noise...",
    "The Crabbot is mapping the way forward...",
    "The Crabbot is testing a few ideas...",
    "The Crabbot is untangling the tricky part...",
    "The Crabbot is taking a closer look...",
    "The Crabbot is connecting the scattered clues...",
    "The Crabbot is comparing possible answers...",
    "The Crabbot is giving the details another pass...",
    "The Crabbot is following the logic through...",
    "The Crabbot is thinking through the edge cases...",
    "The Crabbot is finding the shape of the answer...",
    "The Crabbot is checking that the pieces fit...",
    "The Crabbot is narrowing things down...",
    "The Crabbot is preparing a thoughtful reply...",
];

const COMPACTION_MESSAGES: &[&str] = &[
    "The Crabbot is tidying up the conversation...",
    "The Crabbot is folding the earlier turns into a summary...",
    "The Crabbot is packing away the older messages...",
    "The Crabbot is keeping the useful bits close...",
    "The Crabbot is making room for what comes next...",
];

fn random_generation_message() -> &'static str {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as usize);

    GENERATION_MESSAGES[seed % GENERATION_MESSAGES.len()]
}

fn random_compaction_message(previous: &str) -> &'static str {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as usize);

    let mut index = seed % COMPACTION_MESSAGES.len();

    if COMPACTION_MESSAGES[index] == previous {
        index = (index + 1) % COMPACTION_MESSAGES.len();
    }

    COMPACTION_MESSAGES[index]
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();

    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    }
}

fn is_escape(event: &Event) -> bool {
    matches!(event, Event::Key(key) if key.kind == KeyEventKind::Press && key.code == KeyCode::Esc)
}

fn interrupt_generation(
    event: &Event,
    app: &mut App,
    interrupt_tx: &mpsc::UnboundedSender<()>,
) -> bool {
    if !is_escape(event) || !app.generation_active {
        return false;
    }

    if !app.interrupt_requested {
        let _ = interrupt_tx.send(());
        app.interrupt_requested = true;
        app.status = "Interrupting generation".into();
    }

    true
}

const TUI_COMMANDS: &[(&str, &str)] = &[
    ("/help", "Show commands available in this session."),
    ("/history list", "List recent TUI input history."),
    ("/history clear", "Clear saved TUI input history."),
    ("/history help", "Show input history commands."),
    ("/status", "Show whether the background runtime is running."),
    ("/plugin", "Browse installed plugins; optionally choose a page."),
    ("/session help", "Show session commands."),
    ("/session list", "List saved sessions; optionally choose a page."),
    ("/session create <id>", "Create and switch to a session."),
    ("/session switch <id>", "Switch to a saved session."),
    ("/session rename <new-id>", "Rename the active session."),
    ("/session archive <id>...|--all", "Archive saved sessions."),
    ("/session unarchive <id>...|--all", "Restore archived sessions."),
    ("/session delete <id>...|--all", "Permanently delete sessions; optionally pass -y or --deep."),
    ("/new <id>", "Create and switch to a session."),
    (
        "/workspace",
        "Show or change this session's filesystem root; optionally set a path or reset it.",
    ),
    ("/clear", "Clear this session's conversation."),
    ("/statusline", "Choose which details appear in the statusline; optionally reset it."),
    ("/animation", "Show or configure typewriter animation; optionally turn it on or off."),
    ("/quit", "Leave the TUI."),
    ("/exit", "Leave the TUI."),
];

fn command_options(home: &str, has_model: bool) -> Vec<(&'static str, &'static str)> {
    let mut commands = TUI_COMMANDS.to_vec();

    if has_model {
        commands.push(("/model <help|list|show|set>", "Manage the selected model."));
        commands.push(("/compact", "Summarize older turns and keep recent conversation."));
    }

    if crate::has_capability(home, "tool") {
        commands.extend([
            ("/approval", "Show approval policy."),
            ("/approvals", "List pending tool approvals."),
            ("/approve <id>", "Approve a pending tool action."),
            ("/deny <id>", "Deny a pending tool action."),
        ]);
    }

    if crate::has_capability(home, "channel") {
        commands.extend([
            ("/deliveries", "List pending channel deliveries."),
            ("/retry <id>", "Retry a channel delivery."),
            ("/drop <id>", "Drop a channel delivery."),
        ]);
    }

    if crate::has_capability(home, "timer") {
        commands.push(("/timer <list|add|remove>", "Manage timers."));
    }

    if crate::has_capability(home, "memory") {
        commands.push(("/memory <list|remember|forget>", "Manage memories."));
    }

    commands
}

fn command_suggestions(
    input: &str,
    commands: &[(&'static str, &'static str)],
) -> Option<Vec<(&'static str, &'static str)>> {
    if !input.starts_with('/') || input.contains('\n') {
        return None;
    }

    let matches = commands
        .iter()
        .copied()
        .filter(|(command, _)| command.starts_with(input))
        .collect::<Vec<_>>();

    let exact_command_exists = matches.iter().any(|(command, _)| *command == input);

    (!matches.is_empty() && (!exact_command_exists || input == "/")).then_some(matches)
}

fn approval_prompts(transcript: &str) -> Option<Vec<String>> {
    let mut requests = Vec::new();

    for line in transcript.lines() {
        if let Some(id) = line.strip_prefix("Approve: /approve ") {
            requests.push(id.trim().to_owned());
        } else if matches!(line, "Approval accepted." | "Approval denied.") && !requests.is_empty()
        {
            requests.remove(0);
        }
    }

    (!requests.is_empty()).then_some(requests)
}

fn approval_summary(transcript: &str, id: &str) -> Option<String> {
    let lines = transcript.lines().collect::<Vec<_>>();

    let approval_line =
        lines.iter().rposition(|line| *line == format!("Approve: /approve {id}"))?;

    lines[..approval_line].iter().rev().take(4).find_map(|line| {
        line.strip_prefix("Command: ")
            .or_else(|| line.strip_prefix("Action: "))
            .or_else(|| line.strip_prefix("Tool: "))
            .map(str::to_owned)
    })
}

fn approval_picker(
    id: &str,
    summary: Option<String>,
    selected: usize,
    theme_enabled: bool,
) -> Vec<Line<'static>> {
    let command = if selected == 0 { "/approve" } else { "/deny" };

    let title_style =
        if theme_enabled { Style::default().fg(Color::DarkGray) } else { Style::default() };

    let title = Line::styled(" Approval needed ", title_style);
    let details = Line::raw(format!("  {}", summary.unwrap_or_else(|| format!("Request {id}"))));

    let choices = Line::from(vec![
        Span::raw("  "),
        Span::styled(
            if selected == 0 { "[Approve]" } else { " Approve " },
            if selected == 0 && theme_enabled {
                Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD)
            } else if selected == 0 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        ),
        Span::raw("   "),
        Span::styled(
            if selected == 1 { "[Deny]" } else { " Deny " },
            if selected == 1 && theme_enabled {
                Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD)
            } else if selected == 1 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        ),
        Span::raw(format!("   ←/→ move | Enter: {command} {id}")),
    ]);

    vec![title, details, choices]
}

async fn handle_event<W>(
    event: Event,
    app: &mut App,
    command_tx: &mut W,
    engine_finished: bool,
) -> crabbot_core::Result<bool>
where
    W: AsyncWrite + Unpin,
{
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            app.input_cursor_needs_visibility = true;

            if app.statusline_picker {
                match key.code {
                    KeyCode::Up => {
                        app.statusline_selection = app.statusline_selection.saturating_sub(1);
                    }

                    KeyCode::Down => {
                        app.statusline_selection = app
                            .statusline_selection
                            .saturating_add(1)
                            .min(STATUSLINE_ITEMS.len().saturating_sub(1));
                    }

                    KeyCode::Char(' ') => {
                        let item = STATUSLINE_ITEMS[app.statusline_selection].1;

                        if !statusline_item_enabled(app.statusline_draft, item)
                            || STATUSLINE_ITEMS
                                .iter()
                                .filter(|(_, name)| {
                                    statusline_item_enabled(app.statusline_draft, name)
                                })
                                .count()
                                > 1
                        {
                            toggle_statusline_item(&mut app.statusline_draft, item);
                        }
                    }

                    KeyCode::Enter => {
                        app.statusline = app.statusline_draft;
                        app.statusline_changed = true;
                        app.statusline_picker = false;
                        app.push_bot("Statusline updated.\n".into());
                        app.complete_local_interaction("Statusline updated.".into());
                    }

                    KeyCode::Esc => {
                        app.statusline_picker = false;
                        app.statusline_draft = app.statusline;
                        app.push_bot("Statusline unchanged.\n".into());
                        app.complete_local_interaction("Statusline unchanged.".into());
                    }

                    _ => {}
                }

                return Ok(false);
            }

            if let Some(approvals) = approval_prompts(&app.transcript) {
                match key.code {
                    KeyCode::Left => {
                        app.approval_selection = 0;
                        return Ok(false);
                    }

                    KeyCode::Right => {
                        app.approval_selection = 1;
                        return Ok(false);
                    }

                    KeyCode::Up | KeyCode::Down => return Ok(false),

                    KeyCode::Enter => {
                        if let Some(id) = approvals.first() {
                            let command =
                                if app.approval_selection == 0 { "approve" } else { "deny" };

                            app.input = format!("/{command} {id}").chars().collect();
                            app.cursor = app.input.len();
                        }
                    }

                    KeyCode::Esc => return Ok(false),

                    _ => {}
                }
            }

            if let Some(suggestions) =
                command_suggestions(&app.input.iter().collect::<String>(), &app.command_options)
            {
                match key.code {
                    KeyCode::Up => {
                        app.command_selection = app.command_selection.saturating_sub(1);
                        return Ok(false);
                    }

                    KeyCode::Down => {
                        app.command_selection = app
                            .command_selection
                            .saturating_add(1)
                            .min(suggestions.len().saturating_sub(1));

                        return Ok(false);
                    }

                    KeyCode::Enter => {
                        if let Some((command, _)) = suggestions.get(app.command_selection) {
                            app.input = command.chars().collect();
                            app.cursor = app.input.len();
                            app.input_scroll.follow_latest();
                            return Ok(false);
                        }
                    }

                    _ => {}
                }
            }

            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                    return Ok(true);
                }

                KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if !app.session_working || app.input.first() == Some(&'/') {
                        app.input.insert(app.cursor, '\n');
                        app.cursor += 1;
                        app.input_scroll.follow_latest();
                        app.history_index = None;
                    }
                }

                KeyCode::Char(character)
                    if (app.generation_active || app.compaction_active || app.session_working)
                        && app.input.first() != Some(&'/')
                        && character != '/' =>
                {
                    app.status = if app.generation_active {
                        "A response is running; use approval commands or Esc to interrupt.".into()
                    } else if app.compaction_active {
                        "Crabbot is compacting this conversation. Wait for it to finish.".into()
                    } else {
                        "This session is busy; wait for Crabbot's turn to finish.".into()
                    };
                }

                KeyCode::Char(character) => {
                    app.input.insert(app.cursor, character);
                    app.cursor += 1;
                    app.input_scroll.follow_latest();
                    app.history_index = None;
                }

                KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    if !app.session_working || app.input.first() == Some(&'/') {
                        app.input.insert(app.cursor, '\n');
                        app.cursor += 1;
                        app.input_scroll.follow_latest();
                        app.history_index = None;
                    }
                }

                KeyCode::Backspace if app.cursor > 0 => {
                    app.cursor -= 1;
                    app.input.remove(app.cursor);
                    app.input_scroll.follow_latest();
                }

                KeyCode::Delete if app.cursor < app.input.len() => {
                    app.input.remove(app.cursor);
                    app.input_scroll.follow_latest();
                }

                KeyCode::Left if app.cursor > 0 => {
                    app.cursor -= 1;
                    app.input_scroll.follow_latest();
                }

                KeyCode::Right if app.cursor < app.input.len() => {
                    app.cursor += 1;
                    app.input_scroll.follow_latest();
                }

                KeyCode::Home => {
                    app.cursor = 0;
                    app.input_scroll.follow_latest();
                }

                KeyCode::End => {
                    app.cursor = app.input.len();
                    app.input_scroll.follow_latest();
                }

                KeyCode::Up if !app.input.contains(&'\n') => app.history_up(),
                KeyCode::Down if !app.input.contains(&'\n') => app.history_down(),

                KeyCode::PageUp => {
                    app.transcript_scroll.page_up();
                }

                KeyCode::PageDown => {
                    app.transcript_scroll.page_down();
                }

                KeyCode::Enter => {
                    let line = app.input.iter().collect::<String>();

                    let command = line.trim();
                    let is_live_control =
                        matches!(command, "/approvals" | "/approve" | "/deny" | "/quit" | "/exit")
                            || command.starts_with("/approve ")
                            || command.starts_with("/deny ")
                            || command == "/animation"
                            || command.starts_with("/animation ")
                            || command == "/statusline"
                            || command.starts_with("/statusline ");

                    if (app.generation_active
                        || app.compaction_active
                        || app.session_working
                        || !app.pending_interactions.is_empty())
                        && !is_live_control
                    {
                        app.status = if app.generation_active {
                            "A response is still running. Use /approvals, /approve, /deny, or Esc."
                                .into()
                        } else if app.compaction_active {
                            "Crabbot is compacting this conversation. Wait for it to finish.".into()
                        } else {
                            "Crabbot is working in this session. Wait for the turn to finish."
                                .into()
                        };

                        if !app.generation_active && !app.compaction_active {
                            app.push_system(&app.status.clone());
                        }

                        return Ok(false);
                    }

                    app.input.clear();
                    app.cursor = 0;
                    app.input_scroll.follow_latest();
                    app.history_index = None;

                    if line.trim().is_empty() {
                        return Ok(false);
                    }

                    if line != "/clear"
                        && ((line.starts_with('/') && line != "/quit" && line != "/exit")
                            || !app.model_available)
                    {
                        app.begin_interaction(&line);
                    }

                    app.transcript_scroll.follow_latest();
                    app.push_user(&line);
                    app.remember_input(&line);

                    if line == "/history" || line.starts_with("/history ") {
                        let output = app.history_command(command);
                        app.push_bot(format!("{output}\n"));
                        app.complete_local_interaction(output);
                        return Ok(false);
                    }

                    if let Some(model) = line
                        .strip_prefix("/model set ")
                        .or_else(|| {
                            line.strip_prefix("/model ").filter(|value| {
                                !matches!(value.trim(), "help" | "list" | "show" | "set")
                            })
                        })
                        .map(str::trim)
                        && crate::valid_model(model)
                    {
                        app.pending_model = Some(model.to_owned());
                    }

                    if let Some(workspace) = line.strip_prefix("/workspace ").map(str::trim)
                        && !workspace.is_empty()
                    {
                        app.pending_workspace = crate::resolve_workspace(workspace).ok();
                    }

                    if line == "/clear" {
                        app.pending_clear = true;
                    }

                    if let Some(workspace) = line.strip_prefix("/workspace ").map(str::trim)
                        && !workspace.is_empty()
                        && let Ok(workspace) = crate::resolve_workspace(workspace)
                    {
                        app.workspace = workspace.unwrap_or_else(|| app.default_workspace.clone());
                    }

                    if line == "/statusline" {
                        app.statusline_draft = app.statusline;
                        app.statusline_selection = 0;
                        app.statusline_picker = true;
                        app.push_bot("Choose which details appear in the statusline.\n".into());
                        return Ok(false);
                    }

                    if let Some(value) = line.strip_prefix("/statusline ") {
                        let value = value.trim();

                        if value == "reset" {
                            app.statusline = StatuslineOptions::default();
                            app.statusline_changed = true;
                            app.push_bot("Statusline restored to its default.\n".into());

                            app.complete_local_interaction(
                                "Statusline restored to its default.".into(),
                            );
                        } else {
                            let error =
                                "Use /statusline to choose visible details, or /statusline reset.";
                            app.push_bot(format!("{error}\n"));
                            app.complete_local_interaction(error.into());
                        }

                        return Ok(false);
                    }

                    if line == "/animation" {
                        let state = if app.typewriter { "on" } else { "off" };

                        app.push_bot(format!("Typewriter animation is {state}. Use /animation on or /animation off.\n"));
                        app.complete_local_interaction(format!("Typewriter animation is {state}."));
                        return Ok(false);
                    }

                    if let Some(value) = line.strip_prefix("/animation ") {
                        match value.trim() {
                            "on" => app.typewriter = true,

                            "off" => {
                                app.typewriter = false;
                                app.reveal_all();
                            }

                            _ => {
                                let message = "Usage: /animation [on|off].";
                                app.push_bot(format!("{message}\n"));
                                app.complete_local_interaction(message.into());
                                return Ok(false);
                            }
                        }

                        app.statusline_changed = true;
                        let state = if app.typewriter { "enabled" } else { "disabled" };

                        let message = format!("Typewriter animation {state}.");
                        app.push_bot(format!("{message}\n"));
                        app.complete_local_interaction(message);
                        return Ok(false);
                    }

                    if line == "/quit" || line == "/exit" {
                        app.exiting = true;
                        return Ok(true);
                    }

                    if !engine_finished {
                        if command == "/compact" {
                            app.begin_compaction();
                        }

                        command_tx.write_all(serde_json::to_string(&line)?.as_bytes()).await?;
                        command_tx.write_all(b"\n").await?;

                        if !app.generation_active {
                            app.reply_pending = true;
                            app.reply_system = line.starts_with('/') || line.starts_with('!');
                        }
                    } else {
                        app.push_bot("The session engine is no longer running.".into());
                    }
                }

                KeyCode::Esc if app.input.is_empty() => {
                    app.exiting = true;
                    return Ok(true);
                }

                _ => {}
            }
        }

        Event::Mouse(mouse) => {
            app.mouse_position = Some((mouse.column, mouse.row));

            match mouse.kind {
                MouseEventKind::Moved | MouseEventKind::Drag(_) => {}

                MouseEventKind::ScrollUp => {
                    if app
                        .input_area
                        .is_some_and(|area| area.contains((mouse.column, mouse.row).into()))
                    {
                        app.input_scroll.scroll_up(1);
                    } else {
                        app.transcript_scroll.scroll_up(3);
                    }
                }

                MouseEventKind::ScrollDown => {
                    if app
                        .input_area
                        .is_some_and(|area| area.contains((mouse.column, mouse.row).into()))
                    {
                        app.input_scroll.scroll_down(1);
                    } else {
                        app.transcript_scroll.scroll_down(3);
                    }
                }

                _ => {}
            }
        }

        _ => {}
    }

    Ok(false)
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut App) {
    let input_text = app.input.iter().collect::<String>();
    let padded_input = input_text
        .split('\n')
        .enumerate()
        .map(|(index, line)| if index == 0 { format!("|> {line}") } else { line.to_owned() })
        .collect::<Vec<_>>()
        .join("\n");

    let input_content_rows = wrapped_rows(&padded_input, frame.area().width);
    let input_rows = input_content_rows.clamp(1, INPUT_MAX_ROWS);
    app.input_scroll.set_extent(input_content_rows, input_rows);

    let approvals = approval_prompts(&app.transcript).unwrap_or_default();
    let suggestions = command_suggestions(&input_text, &app.command_options);
    let popup_rows = if app.statusline_picker {
        STATUSLINE_ITEMS.len().min(u16::MAX as usize) as u16
    } else if !approvals.is_empty() {
        3
    } else {
        suggestions.as_ref().map_or(0, |items| items.len().min(7) as u16)
    };

    let is_working = app.generation_active || app.compaction_active || app.session_working;
    let composer_gap = if is_working { 0 } else { COMPOSER_GAP_ROWS };

    let picker_gap = if popup_rows > 0 { COMPOSER_GAP_ROWS } else { 0 };

    if app.input_cursor_needs_visibility {
        let (_, cursor_row) = input_cursor_position(&app.input, app.cursor, frame.area().width);
        app.input_scroll.ensure_visible(usize::from(cursor_row));
        app.input_cursor_needs_visibility = false;
    }

    let input_scroll = app.input_scroll.offset.min(u16::MAX as usize) as u16;

    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(composer_gap),
            Constraint::Length(popup_rows),
            Constraint::Length(picker_gap),
            Constraint::Length(input_rows as u16 + 2),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let transcript_width = areas[0].width;
    let content_height = areas[0].height;
    let indicator_height = if is_working { content_height.min(2) } else { 0 };

    let transcript_height = content_height.saturating_sub(indicator_height) as usize;
    let layout_is_current = app.transcript_layout.as_ref().is_some_and(|layout| {
        layout.generation == app.transcript_generation && layout.width == transcript_width
    });

    if !layout_is_current {
        let (mut rows, message_rows) =
            wrap_transcript_layout(&app.transcript, &app.name, transcript_width, None);

        if !app.theme_enabled {
            strip_transcript_colors(&mut rows);
        }

        app.transcript_layout = Some(TranscriptLayout {
            generation: app.transcript_generation,
            width: transcript_width,
            rows,
            message_rows,
            hovered_message: None,
            hovered_rows: None,
        });
    }

    let statusline_text = render_statusline(app);
    let footer_style =
        if app.theme_enabled { Style::default().fg(Color::DarkGray) } else { Style::default() };

    let layout = app.transcript_layout.as_mut().expect("layout was refreshed");
    let hovered_message = hovered_message_at(
        app.mouse_position,
        Rect::new(areas[0].x, areas[0].y, areas[0].width, transcript_height as u16),
        app.transcript_scroll.offset,
        &layout.message_rows,
    );

    if layout.hovered_message != hovered_message {
        layout.hovered_rows = hovered_message.map(|message| {
            let (mut rows, _) =
                wrap_transcript_layout(&app.transcript, &app.name, transcript_width, Some(message));

            if !app.theme_enabled {
                strip_transcript_colors(&mut rows);
            }

            rows
        });

        layout.hovered_message = hovered_message;
    }

    let rows = layout.hovered_rows.as_deref().unwrap_or(&layout.rows);
    app.transcript_scroll.set_extent(rows.len(), transcript_height);
    let start = app.transcript_scroll.offset;
    let end = start.saturating_add(transcript_height).min(rows.len());
    let visible_rows = &rows[start..end];
    let transcript = TranscriptViewport { rows: visible_rows };

    let input = Paragraph::new(padded_input)
        .scroll((input_scroll, 0))
        .wrap(Wrap { trim: false })
        .block(
        Block::default()
            .border_type(BorderType::Plain)
            .borders(Borders::TOP | Borders::BOTTOM)
            .title(if app.exiting {
                "Exiting Crabbot… | Please wait "
            } else if app.statusline_picker {
                "Statusline elements  |  ↑/↓ move  |  Space toggle  |  Enter save  |  Esc cancel "
            } else if app.compaction_active {
                "Compacting conversation  |  Please wait "
            } else if app.generation_active {
                "Approval commands only  |  /approvals  |  /approve <id>  |  Esc interrupt "
            } else if app.session_working {
                "Session busy  |  /approvals  |  /approve <id>  |  /deny <id> "
            } else {
                "Message  |  Enter send  |  Esc quit "
            }),
    );

    let footer_text =
        format!(" | {} v{} | /help for commands", app.name, env!("CARGO_PKG_VERSION"));

    let footer_width = UnicodeWidthStr::width(footer_text.as_str()).min(u16::MAX as usize) as u16;

    let footer_areas = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(footer_width)])
        .split(areas[5]);

    let statusline_elapsed =
        app.statusline_started.map_or(Duration::ZERO, |started| started.elapsed());

    let statusline_offset =
        (statusline_elapsed.as_millis() / STATUSLINE_SCROLL_INTERVAL.as_millis()) as usize;

    let statusline = Paragraph::new(marquee_statusline(
        &statusline_text,
        footer_areas[0].width as usize,
        statusline_offset,
    ))
    .style(footer_style);

    let footer = Paragraph::new(footer_text).alignment(Alignment::Right).style(footer_style);

    frame.render_widget(
        transcript,
        Rect::new(areas[0].x, areas[0].y, areas[0].width, transcript_height as u16),
    );

    if is_working && content_height > 0 {
        let text = if app.compaction_active {
            let elapsed = app
                .compaction_started
                .map(|started| format_elapsed(started.elapsed()))
                .unwrap_or_else(|| "0s".into());

            format!("{}  |  {elapsed}", app.compaction_message)
        } else if app.generation_active {
            let indicator =
                if app.interrupt_requested { "Stopping…" } else { "Esc to interrupt" };

            let elapsed = app
                .generation_started
                .map(|started| format_elapsed(started.elapsed()))
                .unwrap_or_else(|| "0s".into());

            format!("{}  |  {indicator}  |  {elapsed}", app.generation_message)
        } else {
            "Crabbot is working in this session  |  waiting for the turn to finish".into()
        };

        let started =
            if app.compaction_active { app.compaction_started } else { app.generation_started };

        let elapsed = started.map_or_else(
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
            },
            |started| started.elapsed(),
        );

        let line = generation_status_line(&text, elapsed);

        let indicator_area = Rect::new(
            areas[0].x,
            areas[0].y.saturating_add(areas[0].height.saturating_sub(1)),
            areas[0].width,
            1,
        );

        frame.render_widget(Paragraph::new(line), indicator_area);
    }

    if popup_rows > 0 {
        let popup_area = areas[2];

        if app.statusline_picker {
            let selected = app.statusline_selection.min(STATUSLINE_ITEMS.len().saturating_sub(1));
            app.statusline_selection = selected;

            let items = STATUSLINE_ITEMS
                .iter()
                .enumerate()
                .map(|(index, (label, item))| {
                    let style = if index == selected && app.theme_enabled {
                        Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD)
                    } else if index == selected {
                        Style::default().add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };

                    let rail_style = if app.theme_enabled {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default()
                    };

                    let checked = if statusline_item_enabled(app.statusline_draft, item) {
                        "[x]"
                    } else {
                        "[ ]"
                    };

                    ListItem::new(Line::from(vec![
                        Span::styled("▌ ", rail_style),
                        Span::styled(format!("{checked} {label}"), style),
                    ]))
                })
                .collect::<Vec<_>>();

            frame.render_widget(List::new(items), popup_area);
        } else if !approvals.is_empty() {
            let summary = approval_summary(&app.transcript, &approvals[0]);
            let rows =
                approval_picker(&approvals[0], summary, app.approval_selection, app.theme_enabled);

            let rail_style = if app.theme_enabled {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };

            let rows = rows
                .into_iter()
                .map(|mut row| {
                    row.spans.insert(0, Span::styled("▌ ", rail_style));

                    row
                })
                .collect::<Vec<_>>();

            let popup = Paragraph::new(rows);

            frame.render_widget(popup, popup_area);
        } else if let Some(suggestions) = suggestions {
            let selected = app.command_selection.min(suggestions.len().saturating_sub(1));
            app.command_selection = selected;
            let first = selected.saturating_sub(6);

            let command_width = app
                .command_options
                .iter()
                .map(|(command, _)| UnicodeWidthStr::width(*command))
                .max()
                .unwrap_or(26)
                .max(26);

            let items = suggestions
                .iter()
                .enumerate()
                .skip(first)
                .take(7)
                .map(|(index, (command, description))| {
                    let style = if index == selected && app.theme_enabled {
                        Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD)
                    } else if index == selected {
                        Style::default().add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };

                    let rail_style = if app.theme_enabled {
                        Style::default().fg(Color::DarkGray)
                    } else {
                        Style::default()
                    };

                    ListItem::new(Line::from(vec![
                        Span::styled("▌ ", rail_style),
                        Span::styled(format!("{command:<command_width$}  "), style),
                        Span::styled(
                            *description,
                            if app.theme_enabled {
                                Style::default().fg(Color::DarkGray)
                            } else {
                                Style::default()
                            },
                        ),
                    ]))
                })
                .collect::<Vec<_>>();

            let popup = List::new(items);

            frame.render_widget(popup, popup_area);
        }
    }

    frame.render_widget(input, areas[4]);
    frame.render_widget(statusline, footer_areas[0]);
    frame.render_widget(footer, footer_areas[1]);

    let input_area = areas[4];
    app.input_area = Some(input_area);

    if !is_working {
        let (cursor_x, cursor_y) = input_cursor_position(&app.input, app.cursor, input_area.width);
        let cursor_position = (
            input_area.x.saturating_add(3).saturating_add(cursor_x),
            input_area.y.saturating_add(1).saturating_add(
                cursor_y.saturating_sub(input_scroll).min(input_rows.saturating_sub(1) as u16),
            ),
        );

        if input_area.contains(cursor_position.into()) {
            frame.set_cursor_position(cursor_position);
        }
    }
}

fn generation_status_line(text: &str, elapsed: Duration) -> Line<'static> {
    let (message, suffix) = text.split_once("  |  ").unwrap_or((text, ""));
    let base_style = Style::default().fg(ACCENT_COLOR);
    let chars = message.chars().collect::<Vec<_>>();
    let phase = (elapsed.as_millis() / 40) as usize;
    let glow_center = phase as isize % (chars.len() + 32) as isize - 16;

    let mut spans = chars
        .into_iter()
        .enumerate()
        .map(|(index, character)| {
            let distance = (index as isize - glow_center).unsigned_abs();

            let green = if distance <= 16 { 140 + (45 * (16 - distance) / 16) as u8 } else { 140 };

            let color = Color::Rgb(255, green, 0);
            let style = if distance <= 16 { Style::default().fg(color) } else { base_style };

            Span::styled(character.to_string(), style)
        })
        .collect::<Vec<_>>();

    if !suffix.is_empty() {
        spans.push(Span::styled(format!("  |  {suffix}"), base_style));
    }

    Line::from(spans)
}

fn wrapped_rows(text: &str, width: u16) -> usize {
    if width == 0 {
        return 1;
    }

    text.split('\n')
        .map(|line| {
            let mut rows = 1_usize;

            let mut columns = 0_usize;
            let mut last_width = 0_usize;

            for character in line.chars() {
                let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
                last_width = character_width;

                if columns > 0 && columns + character_width > width as usize {
                    rows += 1;
                    columns = 0;
                }

                columns += character_width;

                if columns >= width as usize {
                    rows += 1;
                    columns = 0;
                }
            }

            if columns == 0 && last_width > 0 { rows.saturating_sub(1) } else { rows }
        })
        .sum()
}

fn input_cursor_position(input: &[char], cursor: usize, width: u16) -> (u16, u16) {
    let width = width.saturating_sub(3).max(1) as usize;

    let mut row = 0_usize;
    let mut column = 0_usize;

    for character in input.iter().take(cursor) {
        if *character == '\n' {
            row += 1;
            column = 0;
            continue;
        }

        let character_width = UnicodeWidthChar::width(*character).unwrap_or(0);

        if column > 0 && column + character_width > width {
            row += 1;
            column = 0;
        }

        column += character_width;

        if column >= width {
            row += 1;
            column = 0;
        }
    }

    (column.min(u16::MAX as usize) as u16, row.min(u16::MAX as usize) as u16)
}

fn render_statusline(app: &App) -> String {
    let options = app.statusline;
    let mut values = Vec::new();

    if options.title {
        values.push(app.name.clone());
    }

    if options.model {
        values.push(format!("model: {}", app.model));
    }

    if options.context {
        values.push(format_context_usage(app.context_usage));
    }

    if options.session {
        values.push(format!("session: {}", session_display_name(&app.session)));
    }

    if options.workspace {
        values.push(workspace_label(&app.workspace));
    }

    if options.status {
        values.push(format!("status: {}", app.status));
    }

    values.join(" | ")
}

fn marquee_statusline(text: &str, width: usize, offset: usize) -> String {
    if width == 0 || text.is_empty() {
        return String::new();
    }

    if UnicodeWidthStr::width(text) <= width {
        return text.to_owned();
    }

    let mut cycle = text.to_owned();
    cycle.push_str("   ");
    let graphemes = cycle.graphemes(true).collect::<Vec<_>>();
    let mut result = String::new();
    let mut columns = 0;

    for index in 0..graphemes.len() {
        let grapheme = graphemes[(offset + index) % graphemes.len()];
        let grapheme_width = UnicodeWidthStr::width(grapheme);

        if columns + grapheme_width > width {
            break;
        }

        result.push_str(grapheme);
        columns += grapheme_width;
    }

    result
}

fn format_context_usage(usage: Option<crabbot_core::types::ContextUsage>) -> String {
    let Some(usage) = usage.filter(|usage| usage.limit > 0) else {
        return "context: unavailable".into();
    };

    let tenths_percent = (u128::from(usage.used) * 1000 / u128::from(usage.limit)) as u64;
    let used = grouped_count(usage.used);
    let limit = grouped_count(usage.limit);

    format!("context: {used}/{limit} ({}.{:01}%)", tenths_percent / 10, tenths_percent % 10)
}

fn grouped_count(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, digit) in digits.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped.push(',');
        }

        grouped.push(digit);
    }

    grouped.chars().rev().collect()
}

fn workspace_label(path: &str) -> String {
    if path.is_empty() {
        return "workspace: unset".into();
    }

    let workspace_path = PathBuf::from(path);
    let name = workspace_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path);

    format!("workspace: {name}")
}

fn save_preferences(
    path: &std::path::Path,
    statusline: StatuslineOptions,
    typewriter: bool,
) -> crabbot_core::Result<()> {
    let preferences = TuiPreferences { statusline, typewriter };
    let content = toml::to_string(&preferences)
        .map_err(|error| crabbot_core::Error::Denied(error.to_string()))?;

    crabbot_file::save(path, content).map_err(crabbot_core::Error::from)
}

#[cfg(test)]
fn wrap_transcript(transcript: &str, name: &str, width: u16) -> Vec<Line<'static>> {
    wrap_transcript_layout(transcript, name, width, None).0
}

fn wrap_transcript_layout(
    transcript: &str,
    name: &str,
    width: u16,
    hovered_message: Option<usize>,
) -> (Vec<Line<'static>>, Vec<Range<usize>>) {
    if width == 0 {
        return (Vec::new(), Vec::new());
    }

    if transcript.is_empty() {
        return (
            vec![Line::styled(
                "No messages in this session yet.",
                Style::default().fg(Color::DarkGray),
            )],
            Vec::new(),
        );
    }

    let width = width as usize;
    let mut rows = Vec::new();
    let mut message_rows = Vec::new();
    let lines = transcript.lines().collect::<Vec<_>>();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];

        if let Some((title, is_user, is_system)) = message_title(line, name) {
            index += 1;
            let body_start = index;
            let mut open_fence = None;

            while index < lines.len() {
                if let Some((character, length)) = open_fence {
                    if is_closing_code_fence(lines[index], character, length) {
                        open_fence = None;
                    } else if is_timestamped_message_title(lines[index], name) {
                        break;
                    }

                    index += 1;
                    continue;
                }

                if let Some((character, length, _)) = opening_code_fence(lines[index]) {
                    open_fence = Some((character, length));
                    index += 1;
                    continue;
                }

                if message_title(lines[index], name).is_some() {
                    break;
                }

                index += 1;
            }

            let mut body_end = index;

            while body_end > body_start && lines[body_end - 1].is_empty() {
                body_end -= 1;
            }

            let message_index = message_rows.len();
            let start = rows.len();
            rows.extend(wrap_rail_message(
                title,
                &lines[body_start..body_end],
                name,
                width,
                is_user,
                is_system,
                hovered_message == Some(message_index),
            ));

            message_rows.push(start..rows.len());

            if index < lines.len() {
                rows.extend((0..MESSAGE_GAP_ROWS).map(|_| Line::raw("")));

                while index < lines.len() && lines[index].is_empty() {
                    index += 1;
                }
            }

            continue;
        }

        rows.extend(wrap_transcript_line(line, name, width));
        index += 1;
    }

    (rows, message_rows)
}

fn hovered_message_at(
    position: Option<(u16, u16)>,
    area: Rect,
    scroll_offset: usize,
    message_rows: &[Range<usize>],
) -> Option<usize> {
    let (column, row) = position?;

    if !area.contains((column, row).into()) {
        return None;
    }

    let transcript_row = scroll_offset.saturating_add(usize::from(row.saturating_sub(area.y)));

    message_rows.iter().position(|rows| rows.contains(&transcript_row))
}

fn strip_transcript_colors(rows: &mut [Line<'static>]) {
    for row in rows {
        for span in &mut row.spans {
            span.style = Style { fg: None, bg: None, underline_color: None, ..span.style };
        }
    }
}

fn message_title<'a>(line: &'a str, name: &str) -> Option<(&'a str, bool, bool)> {
    if line == "You" || line.starts_with("You | ") {
        Some((line, true, false))
    } else if line == "System" || line.starts_with("System | ") {
        Some((line, false, true))
    } else if line == name || line.starts_with(&format!("{name} | ")) {
        Some((line, false, false))
    } else {
        None
    }
}

fn is_timestamped_message_title(line: &str, name: &str) -> bool {
    let Some((title, _, _)) = message_title(line, name) else {
        return false;
    };

    let Some((_, timestamp)) = title.split_once(" | ") else {
        return false;
    };

    let Some((date, time)) = timestamp.split_once(" at ") else {
        return false;
    };

    date.len() == 10
        && date.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 4 | 7) { byte == b'-' } else { byte.is_ascii_digit() }
        })
        && time.len() == 5
        && time
            .bytes()
            .enumerate()
            .all(|(index, byte)| if index == 2 { byte == b':' } else { byte.is_ascii_digit() })
}

fn actor_heading(
    title: &str,
    is_user: bool,
    is_system: bool,
    show_timestamp: bool,
) -> Vec<(String, Style)> {
    let (actor, timestamp) = title.split_once(" | ").unwrap_or((title, ""));

    let actor_color = if is_system {
        Color::DarkGray
    } else if is_user {
        Color::Reset
    } else {
        ACCENT_COLOR
    };

    let actor_style = if is_user {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(actor_color).add_modifier(Modifier::BOLD)
    };

    let actor_style = actor_style.add_modifier(Modifier::ITALIC);
    let metadata_style = Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC);

    let mut heading = vec![(actor.to_owned(), actor_style)];

    if show_timestamp && !timestamp.is_empty() {
        heading.push((format!(", {timestamp}"), metadata_style));
    }

    heading
}

fn wrap_rail_message(
    title: &str,
    body: &[&str],
    name: &str,
    width: usize,
    is_user: bool,
    is_system: bool,
    show_timestamp: bool,
) -> Vec<Line<'static>> {
    if width < 5 {
        let (actor, timestamp) = title.split_once(" | ").unwrap_or((title, ""));
        let heading = if show_timestamp && !timestamp.is_empty() {
            format!("{actor}, {timestamp}")
        } else {
            actor.to_owned()
        };

        let mut rows = wrap_transcript_line(&heading, name, width);
        rows.extend(body.iter().flat_map(|line| wrap_transcript_line(line, name, width)));
        return rows;
    }

    let heading = actor_heading(title, is_user, is_system, show_timestamp);
    let heading_text = heading.iter().map(|(text, _)| text.as_str()).collect::<String>();
    let message_width = max_message_width(&heading_text, width);
    let content_width = message_width.saturating_sub(2).max(1);

    let rail_style = if is_system {
        Color::DarkGray
    } else if is_user {
        Color::Reset
    } else {
        ACCENT_COLOR
    };

    let rail_style = Style::default().fg(rail_style);
    let mut rows = Vec::new();

    let mut message_rows = wrap_styled_segments(&heading, content_width);
    message_rows.extend(wrap_message_body(body, name, content_width));

    for line in message_rows {
        let mut spans = vec![Span::styled("▌ ", rail_style)];

        spans.extend(line.spans);
        rows.push(Line::from(spans));
    }

    rows
}

fn wrap_message_body(body: &[&str], name: &str, width: usize) -> Vec<Line<'static>> {
    let mut rows = Vec::new();
    let mut index = 0;

    while index < body.len() {
        if let Some((character, length, info)) = opening_code_fence(body[index]) {
            index += 1;
            let code_start = index;

            while index < body.len() && !is_closing_code_fence(body[index], character, length) {
                index += 1;
            }

            rows.extend(wrap_code_block(&body[code_start..index], &info, width));

            if index < body.len() {
                index += 1;
            }

            continue;
        }

        rows.extend(wrap_transcript_line(body[index], name, width));
        index += 1;
    }

    rows
}

fn opening_code_fence(line: &str) -> Option<(char, usize, String)> {
    let leading_spaces = line.bytes().take_while(|byte| *byte == b' ').count();

    if leading_spaces > 3 {
        return None;
    }

    let fence = &line[leading_spaces..];
    let character = fence.chars().next()?;

    if character != '`' && character != '~' {
        return None;
    }

    let length = fence.chars().take_while(|candidate| *candidate == character).count();

    if length < 3 {
        return None;
    }

    let info = fence[length..].trim();

    if character == '`' && info.contains('`') {
        return None;
    }

    Some((character, length, code_language(info)))
}

fn code_language(info: &str) -> String {
    let language = info.split_whitespace().next().unwrap_or_default();
    let language = language
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '+' | '.' | '#' | '-')
        })
        .take(24)
        .collect::<String>();

    if language.is_empty() { "code".into() } else { language }
}

fn is_closing_code_fence(line: &str, character: char, minimum_length: usize) -> bool {
    let leading_spaces = line.bytes().take_while(|byte| *byte == b' ').count();

    if leading_spaces > 3 {
        return false;
    }

    let fence = &line[leading_spaces..];
    let length = fence.chars().take_while(|candidate| *candidate == character).count();

    length >= minimum_length && fence[length..].trim().is_empty()
}

fn wrap_code_block(lines: &[&str], language: &str, width: usize) -> Vec<Line<'static>> {
    if width < 8 {
        let mut rows = wrap_transcript_line(&format!("[{language}]"), "", width);
        rows.extend(lines.iter().flat_map(|line| wrap_transcript_line(line, "", width)));

        return rows;
    }

    let border_style = Style::default().fg(ACCENT_COLOR);
    let header_style = Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD);
    let code_style = Style::default().fg(Color::Gray);
    let inner_width = width.saturating_sub(4);
    let language = truncate_display_width(language, width.saturating_sub(5));
    let header = format!("┌─ {language} ");
    let header_width = UnicodeWidthStr::width(header.as_str());

    let mut rows = vec![Line::from(vec![
        Span::styled(header, header_style),
        Span::styled("─".repeat(width.saturating_sub(header_width + 1)), border_style),
        Span::styled("┐", border_style),
    ])];

    for line in lines {
        for fragment in wrap_code_line(line, inner_width) {
            let fragment_width = UnicodeWidthStr::width(fragment.as_str());
            let padding = " ".repeat(inner_width.saturating_sub(fragment_width));

            rows.push(Line::from(vec![
                Span::styled("│ ", border_style),
                Span::styled(fragment, code_style),
                Span::styled(padding, code_style),
                Span::styled(" │", border_style),
            ]));
        }
    }

    rows.push(Line::from(vec![Span::styled(format!("└{}┘", "─".repeat(width - 2)), border_style)]));
    rows
}

fn truncate_display_width(text: &str, width: usize) -> String {
    let mut result = String::new();
    let mut columns = 0;

    for grapheme in text.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);

        if columns + grapheme_width > width {
            break;
        }

        result.push_str(grapheme);
        columns += grapheme_width;
    }

    result
}

fn wrap_code_line(line: &str, width: usize) -> Vec<String> {
    if line.is_empty() {
        return vec![String::new()];
    }

    let mut rows = Vec::new();
    let mut fragment = String::new();
    let mut columns = 0;

    for grapheme in line.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);

        if columns > 0 && columns + grapheme_width > width {
            rows.push(std::mem::take(&mut fragment));
            columns = 0;
        }

        fragment.push_str(grapheme);
        columns += grapheme_width;
    }

    if !fragment.is_empty() {
        rows.push(fragment);
    }

    rows
}

fn max_message_width(title: &str, width: usize) -> usize {
    let title_width = UnicodeWidthStr::width(title);
    let available = if width >= 32 { width.saturating_mul(4) / 5 } else { width };

    available.max(title_width.saturating_add(2).min(width))
}

struct TranscriptLineBuilder {
    width: usize,
    columns: usize,
    just_wrapped: bool,
    rows: Vec<Line<'static>>,
    spans: Vec<Span<'static>>,
    fragment: String,
    fragment_style: Style,
}

impl TranscriptLineBuilder {
    fn new(width: usize) -> Self {
        Self {
            width: width.max(1),
            columns: 0,
            just_wrapped: false,
            rows: Vec::new(),
            spans: Vec::new(),
            fragment: String::new(),
            fragment_style: Style::default(),
        }
    }

    fn push(&mut self, text: &str, style: Style) {
        for grapheme in text.graphemes(true) {
            let grapheme_width = UnicodeWidthStr::width(grapheme);

            if self.columns > 0 && self.columns + grapheme_width > self.width {
                self.finish_row();
            }

            if !self.fragment.is_empty() && style != self.fragment_style {
                self.flush_fragment();
            }

            self.fragment_style = style;
            self.fragment.push_str(grapheme);
            self.columns += grapheme_width;
            self.just_wrapped = false;

            if self.columns >= self.width {
                self.finish_row();
            }
        }
    }

    fn flush_fragment(&mut self) {
        if !self.fragment.is_empty() {
            self.spans.push(Span::styled(std::mem::take(&mut self.fragment), self.fragment_style));
        }
    }

    fn finish_row(&mut self) {
        self.flush_fragment();
        self.rows.push(Line::from(std::mem::take(&mut self.spans)));
        self.columns = 0;
        self.just_wrapped = true;
    }

    fn finish(mut self) -> Vec<Line<'static>> {
        self.flush_fragment();

        if !self.spans.is_empty() || self.rows.is_empty() {
            self.rows.push(Line::from(self.spans));
        }

        self.rows
    }
}

fn wrap_styled_segments(segments: &[(String, Style)], width: usize) -> Vec<Line<'static>> {
    let mut builder = TranscriptLineBuilder::new(width);
    let mut spaces = Vec::<(String, Style)>::new();
    let mut word = Vec::<(String, Style)>::new();

    let flush_word = |builder: &mut TranscriptLineBuilder,
                      spaces: &mut Vec<(String, Style)>,

                      word: &mut Vec<(String, Style)>| {
        if word.is_empty() {
            return;
        }

        let spaces_width =
            spaces.iter().map(|(text, _)| UnicodeWidthStr::width(text.as_str())).sum::<usize>();

        let word_width =
            word.iter().map(|(text, _)| UnicodeWidthStr::width(text.as_str())).sum::<usize>();

        if builder.just_wrapped {
            spaces.clear();
        } else if builder.columns > 0 && builder.columns + spaces_width + word_width > builder.width
        {
            builder.finish_row();
            spaces.clear();
        } else {
            for (text, style) in spaces.drain(..) {
                builder.push(&text, style);
            }
        }

        for (text, style) in word.drain(..) {
            builder.push(&text, style);
        }
    };

    for (text, style) in segments {
        for grapheme in text.graphemes(true) {
            if grapheme.chars().all(char::is_whitespace) {
                flush_word(&mut builder, &mut spaces, &mut word);
                spaces.push((grapheme.to_owned(), *style));
            } else {
                word.push((grapheme.to_owned(), *style));
            }
        }
    }

    flush_word(&mut builder, &mut spaces, &mut word);

    for (text, style) in spaces {
        builder.push(&text, style);
    }

    builder.finish()
}

fn wrap_transcript_line(line: &str, name: &str, width: usize) -> Vec<Line<'static>> {
    if line.is_empty() {
        return vec![Line::raw("")];
    }

    let segments = if line == "You" || line.starts_with("You | ") {
        vec![(line.to_owned(), Style::default().add_modifier(Modifier::BOLD))]
    } else if line.starts_with(&format!("{name} | ")) || line == name {
        vec![(line.to_owned(), Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD))]
    } else if line == "System" || line.starts_with("System | ") {
        vec![(line.to_owned(), Style::default().fg(Color::DarkGray).add_modifier(Modifier::BOLD))]
    } else if let Some((command, description)) = help_command_segments(line) {
        return wrap_help_command(command, description, width);
    } else if line == "Commands:"
        || line.starts_with("Conditional Commands")
        || line.starts_with("Conditional commands")
        || line == "Session commands:"
        || line == "Sessions:"
        || line.starts_with("Sessions | ")
        || line.starts_with("Plugins | ")
    {
        vec![(line.to_owned(), Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD))]
    } else if line.starts_with("Session not found") {
        vec![(line.to_owned(), Style::default().fg(ACCENT_COLOR))]
    } else if let Some((label, value)) = line.split_once(": ")
        && matches!(label, "Tool" | "Arguments" | "Command" | "Action" | "Approve" | "Deny")
    {
        let value_style = match label {
            "Tool" => Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD),
            "Action" => Style::default(),
            _ => Style::default().fg(ACCENT_COLOR).add_modifier(Modifier::BOLD),
        };

        vec![
            (format!("{label}: "), Style::default().fg(Color::DarkGray)),
            (value.to_owned(), value_style),
        ]
    } else if line.starts_with("Crabbot terminal.") {
        vec![(line.to_owned(), Style::default().fg(Color::DarkGray))]
    } else {
        vec![(line.to_owned(), Style::default())]
    };

    wrap_styled_segments(&segments, width)
}

fn wrap_help_command(command: &str, description: &str, width: usize) -> Vec<Line<'static>> {
    let command_style = Style::default().add_modifier(Modifier::BOLD);
    let description_style = Style::default().fg(Color::DarkGray);
    let gap_width = description.chars().take_while(|character| character.is_whitespace()).count();

    let indent = UnicodeWidthStr::width(command).saturating_add(gap_width);

    if indent >= width {
        return wrap_styled_segments(
            &[(command.to_owned(), command_style), (description.to_owned(), description_style)],
            width,
        );
    }

    let description = description.trim_start();
    let mut rows = vec![Line::from(vec![
        Span::styled(command.to_owned(), command_style),
        Span::styled(" ".repeat(gap_width), description_style),
    ])];

    let mut row_width = indent;

    for word in description.split_whitespace() {
        let word_width = UnicodeWidthStr::width(word);

        if row_width > indent && row_width.saturating_add(1).saturating_add(word_width) <= width {
            rows.last_mut()
                .expect("help row exists")
                .spans
                .push(Span::styled(format!(" {word}"), description_style));

            row_width += word_width + 1;
        } else if row_width == indent && indent.saturating_add(word_width) <= width {
            rows.last_mut()
                .expect("help row exists")
                .spans
                .push(Span::styled(word.to_owned(), description_style));

            row_width += word_width;
        } else {
            rows.push(Line::from(vec![
                Span::raw(" ".repeat(indent)),
                Span::styled(word.to_owned(), description_style),
            ]));

            row_width = indent.saturating_add(word_width);
        }
    }

    rows
}

fn help_command_segments(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim_start();

    if !trimmed.starts_with('/') && !trimmed.starts_with("!<") {
        return None;
    }

    let indent = line.len() - trimmed.len();
    let gap = line[indent + 1..]
        .match_indices("  ")
        .map(|(index, _)| indent + 1 + index)
        .find(|index| *index > indent + 1)
        .or_else(|| line.find("> ").map(|index| index + 1))
        .or_else(|| {
            line.starts_with("  /statusline ")
                .then(|| line.find("] ").map(|index| index + 1))
                .flatten()
        })?;

    Some((&line[..gap], &line[gap..]))
}

struct TerminalGuard {
    output: File,
}

impl TerminalGuard {
    fn enter(mut output: File) -> std::io::Result<Self> {
        terminal::enable_raw_mode()?;

        #[cfg(not(windows))]
        let enter = execute!(
            output,
            EnterAlternateScreen,
            event::EnableMouseCapture,
            PushKeyboardEnhancementFlags(
                KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                    | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES
            )
        );

        #[cfg(windows)]
        let enter = execute!(output, EnterAlternateScreen, event::EnableMouseCapture);

        if let Err(error) = enter {
            let _ = terminal::disable_raw_mode();
            return Err(error);
        }

        Ok(Self { output })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        #[cfg(not(windows))]
        let _ = execute!(self.output, PopKeyboardEnhancementFlags);
        let _ = execute!(self.output, LeaveAlternateScreen, event::DisableMouseCapture);
        let _ = terminal::disable_raw_mode();
    }
}

impl App {
    fn begin_compaction(&mut self) {
        self.compaction_active = true;
        self.compaction_started = Some(Instant::now());
        self.compaction_message = random_compaction_message(self.compaction_message);
    }

    fn finish_compaction(&mut self) {
        self.compaction_active = false;
        self.compaction_started = None;
    }

    fn finish_generation(&mut self, interrupted: bool) {
        self.generation_active = false;
        self.generation_started = None;
        self.interrupt_requested = false;
        self.reply_pending = false;
        self.reply_system = false;
        self.approval_reply_pending = false;
        self.status = if interrupted { "Generation interrupted" } else { "Ready" }.into();

        if interrupted {
            self.push_system("Generation interrupted.");
        }
    }

    fn take_persistable_interactions(&mut self) -> Vec<SavedInteraction> {
        if self.generation_active || self.compaction_active || self.reply_pending {
            return Vec::new();
        }

        std::mem::take(&mut self.completed_interactions)
    }

    fn begin_interaction(&mut self, input: &str) {
        self.pending_interactions.push_back(PendingInteraction {
            session: self.session.clone(),
            input: input.to_owned(),
            output: String::new(),
            system: input.starts_with('/') || input.starts_with('!'),
        });
    }

    fn complete_local_interaction(&mut self, output: String) {
        let Some(mut interaction) = self.pending_interactions.pop_back() else {
            return;
        };

        interaction.output = output;
        self.interaction_sequence = self.interaction_sequence.saturating_add(1);
        self.completed_interactions.push(self.saved_interaction(interaction));
    }

    fn capture_interaction_output(&mut self, value: &str) -> Vec<SavedInteraction> {
        let mut remaining = value.to_owned();
        let mut completed = Vec::new();

        while let Some(interaction) = self.pending_interactions.front_mut() {
            let available = INTERACTION_LIMIT.saturating_sub(interaction.output.len());
            let mut end = remaining.len().min(available);

            while !remaining.is_char_boundary(end) {
                end -= 1;
            }

            interaction.output.push_str(&remaining[..end]);
            remaining.drain(..end);

            let delimiter = interaction.output.find("\n> ").map(|index| (index, 3));

            let rest = if let Some((index, length)) = delimiter {
                let rest = interaction.output.split_off(index + length);
                interaction.output.truncate(index);
                rest
            } else if let Some(index) = remaining.find("\n> ") {
                let rest = remaining[index + 3..].to_owned();
                remaining.truncate(index);
                interaction.output.push_str("\n[history entry truncated]");
                rest
            } else {
                break;
            };

            remaining.insert_str(0, &rest);
            let interaction = self.pending_interactions.pop_front().expect("front exists");
            self.interaction_sequence = self.interaction_sequence.saturating_add(1);
            completed.push(self.saved_interaction(interaction));
        }

        completed
    }

    fn finish_interactions(&mut self) -> Vec<SavedInteraction> {
        let mut completed = Vec::new();

        while let Some(interaction) = self.pending_interactions.pop_front() {
            self.interaction_sequence = self.interaction_sequence.saturating_add(1);
            completed.push(self.saved_interaction(interaction));
        }

        completed
    }

    fn finish_streamed_interactions(&mut self) {
        self.reply_pending = false;
        self.pending_interactions.clear();
        self.completed_interactions.clear();
    }

    fn saved_interaction(&self, mut interaction: PendingInteraction) -> SavedInteraction {
        if interaction.input.starts_with("/session rename ")
            && let Some(target) = interaction
                .output
                .strip_prefix("Renamed session ")
                .and_then(|text| text.split_once(" to "))
                .map(|(_, target)| target.trim_end_matches('.'))
        {
            interaction.session = target.to_owned();
        }

        SavedInteraction {
            session: interaction.session,
            input: interaction.input,
            output: interaction.output.trim_matches('\n').to_owned(),
            sequence: self.interaction_sequence,
            system: interaction.system,
        }
    }

    fn restore_session(&mut self, session: SessionView) {
        self.session = session.id;
        self.model = display_model(&session.model, self.model_available);
        self.workspace = session.workspace.unwrap_or_else(|| self.default_workspace.clone());
        self.context_usage = session.context_usage;
        self.session_working = session.working;
        self.transcript = render_messages(&session.messages, &self.name);
        self.invalidate_transcript_layout();
        self.transcript_scroll.follow_latest();
    }

    fn refresh_session(&mut self, session: SessionView) {
        // A snapshot can lag behind locally streamed output until its delimiter is saved.
        if self.generation_active
            || self.reply_pending
            || !self.pending_interactions.is_empty()
            || !self.completed_interactions.is_empty()
            || !self.reveal_queue.is_empty()
        {
            return;
        }

        let transcript = render_messages(&session.messages, &self.name);
        let model = display_model(&session.model, self.model_available);
        let workspace = session.workspace.unwrap_or_else(|| self.default_workspace.clone());
        self.context_usage = session.context_usage;
        self.session_working = session.working;

        if self.session != session.id
            || self.model != model
            || self.workspace != workspace
            || self.transcript != transcript
        {
            self.restore_session(SessionView {
                id: session.id,
                model: session.model,
                workspace: Some(workspace),
                context_usage: session.context_usage,
                messages: session.messages,
                working: session.working,
            });
        }
    }

    fn observe_context_output(&mut self, value: &str) {
        if self.pending_model.is_none() && self.pending_workspace.is_none() && !self.pending_clear {
            return;
        }

        self.context_output.push_str(value);

        if self.context_output.len() > 512 {
            let mut excess = self.context_output.len() - 512;

            while !self.context_output.is_char_boundary(excess) {
                excess += 1;
            }

            self.context_output.drain(..excess);
        }

        if let Some(model) = self.pending_model.clone() {
            let confirmation = format!("Using model {model}.");

            if self.context_output.contains(&confirmation) {
                self.model = model;
                self.pending_model = None;
            }
        }

        if let Some(workspace) = self.pending_workspace.clone()
            && (self.context_output.contains("Workspace changed to:")
                || self.context_output.contains("Workspace reset to the configured default:")
                || self.context_output.contains("Current workspace:")
                || self.context_output.contains("Workspace was not changed:"))
        {
            if !self.context_output.contains("Workspace was not changed:") {
                self.workspace = workspace.unwrap_or_else(|| self.default_workspace.clone());
            }

            self.pending_workspace = None;
        }

        if self.pending_clear && self.context_output.contains("Conversation cleared.") {
            self.transcript.clear();
            self.invalidate_transcript_layout();
            self.transcript_scroll.follow_latest();
            self.pending_clear = false;
        }

        if self.pending_model.is_none() && self.pending_workspace.is_none() && !self.pending_clear {
            self.context_output.clear();
        }
    }

    fn push_output(&mut self, value: String) {
        self.show_output(normalize_output(value));
    }

    fn show_output(&mut self, value: String) {
        if self.typewriter {
            for grapheme in value.graphemes(true) {
                if self.reveal_bytes.saturating_add(grapheme.len()) > TRANSCRIPT_LIMIT {
                    self.reveal_all();
                }

                self.reveal_bytes = self.reveal_bytes.saturating_add(grapheme.len());
                self.reveal_queue.push_back(grapheme.to_owned());
            }
        } else {
            self.append_visible(&value);
        }
    }

    fn reveal_next(&mut self) {
        let count = if self.reveal_queue.len() > TYPEWRITER_BACKLOG {
            TYPEWRITER_CATCHUP_BURST
        } else {
            TYPEWRITER_BURST
        };

        let mut visible = String::new();

        for _ in 0..count {
            let Some(grapheme) = self.reveal_queue.pop_front() else {
                break;
            };

            self.reveal_bytes = self.reveal_bytes.saturating_sub(grapheme.len());
            visible.push_str(&grapheme);
        }

        self.append_visible(&visible);
    }

    fn reveal_all(&mut self) {
        if self.reveal_queue.is_empty() {
            return;
        }

        let visible = self.reveal_queue.drain(..).collect::<String>();
        self.reveal_bytes = 0;
        self.append_visible(&visible);
    }

    fn append_visible(&mut self, value: &str) {
        if value.is_empty() {
            return;
        }

        self.transcript.push_str(value);

        if self.transcript.len() > TRANSCRIPT_LIMIT {
            let remove = self.transcript.len() - TRANSCRIPT_LIMIT;
            let mut boundary = remove;

            while !self.transcript.is_char_boundary(boundary) {
                boundary += 1;
            }

            self.transcript.drain(..boundary);
        }

        self.invalidate_transcript_layout();
    }

    fn invalidate_transcript_layout(&mut self) {
        self.transcript_generation = self.transcript_generation.wrapping_add(1);
        self.transcript_layout = None;
    }

    fn push_bot(&mut self, value: String) {
        if self.pending_interactions.back().is_some_and(|interaction| interaction.system) {
            self.push_system_label();
        } else {
            self.push_assistant_label();
        }

        self.push_output(value);
    }

    fn push_assistant_output(&mut self, value: String) -> Vec<SavedInteraction> {
        if self.approval_reply_pending {
            self.approval_reply_pending = false;

            if value.trim() == "." {
                return Vec::new();
            }
        }

        let value =
            if self.reply_pending { value.trim_start_matches('\n').to_owned() } else { value };

        if self.reply_pending {
            if self.reply_system {
                self.push_system_label();
            } else {
                self.push_assistant_label();
            }

            self.reply_pending = false;
        }

        let interactions = self.capture_interaction_output(&value);
        self.observe_context_output(&value);
        self.push_output(value);

        interactions
    }

    fn push_user(&mut self, value: &str) {
        let label = format_role_label("You", Some(&current_timestamp()));
        self.push_label(&label);
        self.append_visible(&normalize_output(format!("{value}\n")));
    }

    fn push_system(&mut self, value: &str) {
        self.push_system_label();
        self.append_visible(&normalize_output(format!("{value}\n")));

        if self.generation_active {
            self.reply_pending = true;
            self.reply_system = false;
            self.approval_reply_pending = value == "Approval accepted.";
        }
    }

    fn push_system_output(&mut self, value: &str) {
        if self.reply_pending {
            self.push_system_label();
            self.reply_pending = false;
        }

        let value = normalize_output(value.to_owned());
        let _ = self.capture_interaction_output(&value);
        self.observe_context_output(&value);
        self.show_output(value);
    }

    fn push_system_label(&mut self) {
        let label = format_role_label("System", Some(&current_timestamp()));
        self.push_label(&label);
    }

    fn push_assistant_label(&mut self) {
        let label = format_role_label(&self.name, Some(&current_timestamp()));
        self.push_label(&label);
    }

    fn push_label(&mut self, label: &str) {
        self.reveal_all();

        while self.transcript.ends_with('\n') {
            self.transcript.pop();
        }

        if !self.transcript.is_empty() {
            self.transcript.push_str("\n\n");
        }

        self.transcript.push_str(label);
        self.transcript.push('\n');
    }

    fn remember_input(&mut self, value: &str) {
        self.history.push(value.to_owned());

        if self.history.len() > INPUT_HISTORY_LIMIT {
            self.history.remove(0);
        }

        self.history_draft.clear();

        if let Some(path) = &self.history_path
            && let Err(error) = super::data::save_history(path, &self.history)
        {
            self.status = format!("Input history was not saved: {error}");
        }
    }

    fn history_command(&mut self, command: &str) -> String {
        let mut parts = command.split_whitespace();
        let _ = parts.next();
        let action = parts.next().unwrap_or("help");

        if parts.next().is_some() {
            return "Usage: /history [list|clear|help].".into();
        }

        match action {
            "list" => {
                if self.history.is_empty() {
                    return "Input history is empty.".into();
                }

                let entries = self
                    .history
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| {
                        let entry = entry.replace('\n', " ↵ ").replace('\r', "");
                        let mut clipped = entry.chars().take(160).collect::<String>();

                        if clipped.chars().count() < entry.chars().count() {
                            clipped.push('…');
                        }

                        format!("{:>3}  {clipped}", index + 1)
                    })
                    .collect::<Vec<_>>();

                format!("Input history ({}):\n{}", entries.len(), entries.join("\n"))
            }

            "clear" => {
                if let Some(path) = &self.history_path
                    && let Err(error) = super::data::save_history(path, &[])
                {
                    return format!("Input history could not be cleared: {error}");
                }

                self.history.clear();
                self.history_index = None;
                self.history_draft.clear();
                "Input history cleared.".into()
            }

            "help" => "Input history commands:\n  /history list   List saved inputs.\n  /history clear  Clear saved input history.\n  /history help   Show these commands.".into(),
            _ => "Usage: /history [list|clear|help].".into(),
        }
    }

    fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let index = self.history_index.unwrap_or_else(|| {
            self.history_draft = self.input.iter().collect();
            self.history.len()
        });

        let index = index.saturating_sub(1);
        self.history_index = Some(index);
        self.input = self.history[index].chars().collect();
        self.cursor = self.input.len();
    }

    fn history_down(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };

        if index + 1 >= self.history.len() {
            self.history_index = None;
            self.input = self.history_draft.chars().collect();
        } else {
            self.history_index = Some(index + 1);
            self.input = self.history[index + 1].chars().collect();
        }

        self.cursor = self.input.len();
    }
}

fn session_display_name(id: &str) -> &str {
    id.strip_prefix("tui-").unwrap_or(id)
}

fn normalize_output(value: String) -> String {
    value.replace("\n> ", "\n").trim_start_matches("> ").trim_end_matches("> ").to_owned()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use unicode_width::UnicodeWidthStr;

    use super::{
        App, GENERATION_MESSAGES, INPUT_HISTORY_LIMIT, STATUSLINE_SCROLL_INTERVAL, ScrollState,
        StatuslineOptions, TuiPreferences, draw, generation_status_line, handle_event,
        input_cursor_position, interrupt_generation, marquee_statusline, render_statusline,
        save_preferences, wrapped_rows,
    };

    #[test]
    fn assistant_text_stays_separate_when_generation_finishes_before_pipe_output() {
        let mut app = App { name: "Crabbot".into(), reply_pending: true, ..App::default() };

        app.push_user("Hey there!");
        app.generation_active = false;
        app.push_assistant_output("Hey! What can I help you with?".into());

        let user = app.transcript.find("Hey there!").unwrap();
        let assistant = app.transcript.find("Crabbot |").unwrap();

        assert!(assistant > user);
        assert!(app.transcript[assistant..].contains("Hey! What can I help you with?"));
        assert!(!app.reply_pending);
    }

    #[test]
    fn streamed_completion_keeps_the_visible_reply_without_client_side_reappending() {
        let mut app = App { name: "Crabbot".into(), reply_pending: true, ..App::default() };
        app.begin_interaction("Tell me about this project.");
        app.push_user("Tell me about this project.");
        app.push_assistant_output("It is a Rust workspace.".into());

        app.finish_streamed_interactions();

        assert!(app.transcript.contains("It is a Rust workspace."));
        assert!(app.pending_interactions.is_empty());
        assert!(app.completed_interactions.is_empty());
        assert!(!app.reply_pending);
    }

    #[test]
    fn live_approval_notice_stays_system_and_following_model_reply_is_crabbot() {
        let mut app = App {
            name: "Crabbot".into(),
            generation_active: true,
            reply_pending: true,
            ..App::default()
        };

        app.push_assistant_output("I’ll create the file.".into());
        app.push_system("Approval needed.");
        app.begin_interaction("/approve abc");
        app.push_user("/approve abc");
        app.push_system("Approval accepted.");
        app.push_assistant_output("\n\nCreated the empty file foo.".into());

        let first_assistant = app.transcript.find("Crabbot |").unwrap();
        let approval = app.transcript.find("System |").unwrap();
        let accepted = app.transcript.find("Approval accepted.").unwrap();
        let assistant = app.transcript.rfind("Crabbot |").unwrap();
        let result = app.transcript.find("Created the empty file foo.").unwrap();

        assert!(first_assistant < approval);
        assert!(approval < accepted);
        assert!(accepted < assistant);
        assert!(assistant < result);
        assert!(!app.transcript.contains("\n\nCreated the empty file foo."));
    }

    #[test]
    fn hides_orphan_period_before_the_reply_after_an_approval() {
        let mut app = App { generation_active: true, reply_pending: true, ..App::default() };

        app.push_system("Approval accepted.");
        app.push_assistant_output(".".into());
        app.push_assistant_output("\n\nCreated the empty file foo.".into());

        assert!(!app.transcript.contains("\n.\n"));
        assert!(app.transcript.contains("Created the empty file foo."));
    }

    #[test]
    fn terminal_command_stream_keeps_system_role_and_typewriter_animation() {
        let mut app = App {
            name: "Crabbot".into(),
            reply_pending: true,
            reply_system: true,
            typewriter: true,
            ..App::default()
        };

        app.begin_interaction("!pwd");
        app.push_user("!pwd");
        app.push_system_output("/work");
        app.push_system_output("space\n");

        assert_eq!(app.reveal_queue.iter().cloned().collect::<String>(), "/workspace\n");
        app.reveal_all();

        assert!(app.transcript.contains("System | "));
        assert!(!app.transcript.contains("Crabbot | "));
        assert!(app.transcript.contains("/workspace"));
        assert!(app.reveal_queue.is_empty());
    }

    #[test]
    fn approval_prompt_parser_drops_resolved_requests_in_order() {
        let transcript = "System | now\nApproval needed.\nApprove: /approve first\nDeny: /deny first\n\
            Approval needed.\nApprove: /approve second\nDeny: /deny second\nApproval accepted.";

        assert_eq!(super::approval_prompts(transcript), Some(vec!["second".into()]));
        assert_eq!(
            super::approval_prompts("Approval needed.\nApprove: /approve id"),
            Some(vec!["id".into()])
        );

        assert_eq!(
            super::approval_prompts(
                "Approval accepted.\nApproval denied.\nApproval needed.\n\
                 Approve: /approve current"
            ),
            Some(vec!["current".into()])
        );

        assert_eq!(super::approval_prompts("Approval accepted."), None);
    }

    #[tokio::test]
    async fn command_picker_fills_composer_without_submitting() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App {
            input: vec!['/'],
            cursor: 1,
            command_options: super::TUI_COMMANDS.to_vec(),
            ..App::default()
        };

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.input.iter().collect::<String>(), "/help");
        assert!(app.transcript.is_empty());
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                command_rx.read(&mut [0; 32]),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn command_picker_omits_optional_argument_placeholders() {
        let cases = [
            ("/pl", "/plugin"),
            ("/session l", "/session list"),
            ("/work", "/workspace"),
            ("/statusl", "/statusline"),
            ("/anim", "/animation"),
        ];

        for (input, expected) in cases {
            let (mut command_tx, _command_rx) = duplex(1024);
            let mut app = App {
                input: input.chars().collect(),
                cursor: input.len(),
                command_options: super::TUI_COMMANDS.to_vec(),
                ..App::default()
            };

            handle_event(
                Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
                &mut app,
                &mut command_tx,
                false,
            )
            .await
            .unwrap();

            assert_eq!(app.input.iter().collect::<String>(), expected);
        }

        assert!(super::TUI_COMMANDS.iter().all(|(command, _)| !command.contains('[')));
    }

    #[test]
    fn history_commands_list_help_and_clear_persisted_inputs() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let root = std::env::temp_dir()
            .join(format!("crabbot-tui-history-{}-{nonce}", std::process::id()));

        let history_path = super::super::data::plugin_file(&root, "tui", "history.json");
        let mut app = App {
            history: vec!["first input".into(), "second\ninput".into()],
            history_path: Some(history_path.clone()),
            ..App::default()
        };

        let listed = app.history_command("/history list");

        assert!(listed.contains("first input"));
        assert!(listed.contains("second ↵ input"));
        assert!(app.history_command("/history help").contains("/history clear"));
        assert_eq!(app.history_command("/history clear"), "Input history cleared.");
        assert!(app.history.is_empty());
        assert!(super::super::data::load_history(&history_path).is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn compact_submission_shows_the_wait_state_before_engine_response() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App {
            input: "/compact".chars().collect(),
            cursor: "/compact".len(),
            command_options: vec![(
                "/compact",
                "Summarize older turns and keep recent conversation.",
            )],
            ..App::default()
        };

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert!(app.compaction_active);
        assert!(app.compaction_started.is_some());

        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("The Crabbot is"));

        let mut submitted = [0; 32];
        let count = command_rx.read(&mut submitted).await.unwrap();

        assert_eq!(&submitted[..count], b"\"/compact\"\n");

        app.finish_compaction();

        assert!(!app.compaction_active);
        assert!(app.compaction_started.is_none());
    }

    #[test]
    fn compact_appears_in_command_picker_when_a_model_is_available() {
        let commands = super::command_options("missing-home", true);

        assert!(commands.iter().any(|(command, _)| *command == "/compact"));

        let commands = super::command_options("missing-home", false);

        assert!(!commands.iter().any(|(command, _)| *command == "/compact"));
    }

    #[test]
    fn command_picker_includes_every_session_action_and_alias() {
        let commands = super::command_options("missing-home", false);
        let names = commands.iter().map(|(command, _)| *command).collect::<Vec<_>>();

        for command in [
            "/session archive <id>...|--all",
            "/session unarchive <id>...|--all",
            "/session delete <id>...|--all",
            "/exit",
        ] {
            assert!(names.contains(&command), "missing command: {command}");
        }
    }

    #[test]
    fn command_picker_includes_installed_capability_commands() {
        let home =
            std::env::temp_dir().join(format!("crabbot-tui-commands-{}", std::process::id()));

        for (id, capability) in
            [("tools", "tool"), ("channel", "channel"), ("timer", "timer"), ("memory", "memory")]
        {
            let plugin = home.join("plugins").join(id);
            let binary = crabbot_core::plugin::binary_name(id);

            std::fs::create_dir_all(plugin.join("bin")).unwrap();
            std::fs::write(
                plugin.join("crabbot-plugin.toml"),
                format!("capabilities = ['{capability}']\n"),
            )
            .unwrap();
            std::fs::write(plugin.join("bin").join(binary), "test").unwrap();
        }

        let commands = super::command_options(home.to_str().unwrap(), false);
        let names = commands.iter().map(|(command, _)| *command).collect::<Vec<_>>();

        for command in [
            "/approval",
            "/approvals",
            "/approve <id>",
            "/deny <id>",
            "/deliveries",
            "/retry <id>",
            "/drop <id>",
            "/timer <list|add|remove>",
            "/memory <list|remember|forget>",
        ] {
            assert!(names.contains(&command), "missing command: {command}");
        }

        let _ = std::fs::remove_dir_all(home);
    }

    #[tokio::test]
    async fn approval_picker_submits_the_selected_decision() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App {
            generation_active: true,
            transcript: "System | now\nApproval needed.\nTool: shell\nCommand: touch foo\n\
                Approve: /approve approval-1\nDeny: /deny approval-1\n"
                .into(),
            ..App::default()
        };

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.approval_selection, 1);

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        let mut bytes = [0; 128];
        let count = command_rx.read(&mut bytes).await.unwrap();

        assert_eq!(&bytes[..count], b"\"/deny approval-1\"\n");

        app.approval_selection = 1;
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.approval_selection, 0);

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        let count = command_rx.read(&mut bytes).await.unwrap();

        assert_eq!(&bytes[..count], b"\"/approve approval-1\"\n");
    }

    #[test]
    fn draws_full_width_command_and_approval_pickers_above_the_prompt() {
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        let mut app = App {
            input: vec!['/'],
            cursor: 1,
            command_options: super::TUI_COMMANDS.to_vec(),
            ..App::default()
        };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let commands = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(commands.contains("Show commands available"));
        assert!(commands.contains("/help"));
        assert!(commands.contains("|> /"));
        assert!(commands.contains("▌ "));
        assert!(!commands.contains("╭"));
        let rows = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>();

        let input_row = rows.iter().position(|row| row.contains("Message")).unwrap();

        assert!(input_row > 0);
        assert!(rows[input_row - 1].trim().is_empty());

        app.generation_active = true;
        app.transcript = "System | now\nApproval needed.\nTool: shell\nCommand: touch foo\n\
            Approve: /approve approval-1\nDeny: /deny approval-1\n"
            .into();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let approval = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(approval.contains("Approval needed"));
        assert!(approval.contains("touch foo"));
        assert!(approval.contains("[Approve]"));
        assert!(approval.contains("/approve approval-1"));
        assert!(approval.contains("←/→ move"));
        assert!(approval.contains("▌ "));
        assert!(!approval.contains("╭"));

        let buffer = terminal.backend().buffer();
        let input_area = app.input_area.unwrap();
        let gap_row = input_area.y - super::COMPOSER_GAP_ROWS;

        assert!((0..buffer.area.width).all(|x| buffer[(x, gap_row)].symbol() == " "));

        app.generation_active = false;
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let approval = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(approval.contains("Approval needed"));
        assert!(approval.contains("[Approve]"));
    }

    #[test]
    fn command_picker_keeps_descriptions_clear_of_long_commands() {
        let mut terminal = Terminal::new(TestBackend::new(100, 12)).unwrap();
        let mut app = App {
            input: vec!['/'],
            cursor: 1,
            command_options: vec![
                ("/help", "Show commands."),
                ("/model <help|list|show|set>", "Manage the selected model."),
            ],
            ..App::default()
        };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("/model <help|list|show|set>  Manage the selected model."));
    }

    #[test]
    fn streamed_messages_keep_their_role_rail_fixed() {
        let mut app = App { name: "Crabbot".into(), typewriter: true, ..App::default() };
        app.begin_interaction("!ls");
        app.push_bot("a long command output that should not resize while it is revealed".into());

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let first_rail = app.transcript_layout.as_ref().unwrap().rows[0].spans[0].content.clone();

        app.reveal_next();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        assert_eq!(first_rail, "▌ ");
        assert_eq!(app.transcript_layout.as_ref().unwrap().rows[0].spans[0].content, "▌ ");

        while !app.reveal_queue.is_empty() {
            app.reveal_next();
        }
    }

    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::{Terminal, backend::TestBackend, layout::Rect, style::Color, text::Line};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[test]
    fn hides_models_when_no_intelligence_plugin_is_installed() {
        assert_eq!(super::display_model(crate::DEFAULT_MODEL, false), "unset");
        assert_eq!(super::display_model("foo", false), "unset");
        assert_eq!(super::display_model(crate::DEFAULT_MODEL, true), crate::DEFAULT_MODEL);
        assert_eq!(super::display_model("provider/model", true), "provider/model");

        let app = App {
            name: "Crabbot".into(),
            model: super::display_model("foo", false),
            session: "default".into(),
            workspace: String::new(),
            statusline: StatuslineOptions::default(),
            ..App::default()
        };

        assert_eq!(
            render_statusline(&app),
            "Crabbot | model: unset | context: unavailable | session: default | workspace: unset"
        );
    }

    #[test]
    fn generation_status_uses_a_suspense_phrase_and_escape_requests_interrupt() {
        let mut app = App { generation_active: true, ..App::default() };
        let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        let escape = Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));

        assert!(GENERATION_MESSAGES.contains(&super::random_generation_message()));
        assert_eq!(GENERATION_MESSAGES.len(), 25);
        assert!(
            GENERATION_MESSAGES
                .iter()
                .all(|message| { message.starts_with("The Crabbot ") && message.ends_with("...") })
        );

        assert!(interrupt_generation(&escape, &mut app, &interrupt_tx));
        assert!(app.interrupt_requested);
        assert_eq!(interrupt_rx.try_recv(), Ok(()));
        assert!(interrupt_generation(&escape, &mut app, &interrupt_tx));
        assert!(interrupt_rx.try_recv().is_err());
        assert!(!interrupt_generation(
            &Event::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)),
            &mut app,
            &interrupt_tx
        ));
    }

    #[test]
    fn compaction_status_uses_crabbot_phrases_without_consecutive_repeats() {
        assert_eq!(super::COMPACTION_MESSAGES.len(), 5);
        assert!(
            super::COMPACTION_MESSAGES.iter().all(|message| {
                message.starts_with("The Crabbot is ") && message.ends_with("...")
            })
        );

        for previous in super::COMPACTION_MESSAGES {
            assert_ne!(super::random_compaction_message(previous), *previous);
        }
    }

    #[test]
    fn interrupted_generation_is_reported_by_system() {
        let mut app = App { generation_active: true, reply_pending: true, ..App::default() };

        app.finish_generation(true);

        assert!(app.transcript.contains("System | "));
        assert!(app.transcript.contains("Generation interrupted."));
        assert!(!app.transcript.contains("Crabbot | "));
        assert!(!app.reply_pending);
    }

    #[test]
    fn live_system_and_crabbot_headers_include_local_time() {
        let mut system = App { reply_pending: true, reply_system: true, ..App::default() };
        system.push_system_output("command output");

        let mut assistant = App { reply_pending: true, ..App::default() };
        assistant.push_assistant_output("Reply".into());

        for transcript in [&system.transcript, &assistant.transcript] {
            let header = transcript.lines().next().unwrap();

            assert!(header.contains(" | "));
            assert!(header.contains(" at "));
        }
    }

    #[test]
    fn saved_message_timestamps_support_milliseconds_and_nanoseconds() {
        let milliseconds = super::message_timestamp("tui-user-1700000000000-1");
        let nanoseconds = super::message_timestamp("tui-assistant-1700000000000000000-1");

        assert_eq!(nanoseconds, milliseconds);
        assert!(nanoseconds.is_some());
    }

    #[test]
    fn drawing_generation_status_shows_the_phrase_and_interrupt_key() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App {
            generation_active: true,
            generation_message: "The Crabbot is scuttling toward an answer...",
            transcript: "Crabbot | now\nWorking on it.".into(),
            theme_enabled: true,
            ..App::default()
        };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains(app.generation_message));
        assert!(rendered.contains("Esc to interrupt"));

        let row_text = |row: usize| {
            terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .skip(row * 80)
                .take(80)
                .map(|cell| cell.symbol())
                .collect::<String>()
        };

        let status_row =
            (0..12).find(|row| row_text(*row).contains(app.generation_message)).unwrap();

        assert!(row_text(status_row - 1).trim().is_empty());
        assert!(row_text(status_row + 1).contains("Approval commands only"));
        let input_area = app.input_area.unwrap();

        assert_eq!(terminal.backend().buffer()[(input_area.x, input_area.y)].symbol(), "A");

        let phrase_start = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .find(|cell| cell.symbol() == "T")
            .expect("generation phrase is rendered");

        assert!(matches!(
            phrase_start.fg,
            Color::Rgb(255, green, 0) if (140..=185).contains(&green)
        ));

        assert!(!terminal.backend().cursor_visible());

        app.generation_active = false;
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let input_area = app.input_area.unwrap();
        let (cursor_x, cursor_y) = input_cursor_position(&app.input, app.cursor, input_area.width);

        assert!(terminal.backend().cursor_visible());
        terminal.backend_mut().assert_cursor_position((
            input_area.x.saturating_add(3).saturating_add(cursor_x),
            input_area.y.saturating_add(1).saturating_add(cursor_y),
        ));
    }

    #[test]
    fn drawing_compaction_status_shows_a_dedicated_glowing_message() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App {
            compaction_active: true,
            compaction_started: Some(std::time::Instant::now()),
            transcript: "You | now\n/compact".into(),
            theme_enabled: true,
            ..App::default()
        };

        app.begin_compaction();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains(app.compaction_message));
        assert!(rendered.contains("Compacting conversation  |  Please wait"));

        let indicator =
            terminal.backend().buffer().content().iter().find(|cell| cell.symbol() == "C").unwrap();

        assert_eq!(indicator.fg, Color::Rgb(255, 140, 0));
    }

    #[test]
    fn generation_status_glow_moves_across_the_message_only() {
        let text = "The Crabbot is thinking...  |  Esc to interrupt  |  2s";
        let first = generation_status_line(text, Duration::from_secs(1));
        let later = generation_status_line(text, Duration::from_secs(3));
        let bright = Color::Rgb(255, 185, 0);
        let bright_positions = |line: &Line<'_>| {
            line.spans
                .iter()
                .enumerate()
                .filter_map(|(index, span)| (span.style.fg == Some(bright)).then_some(index))
                .collect::<Vec<_>>()
        };

        assert_ne!(bright_positions(&first), bright_positions(&later));
        assert!(first.spans.iter().all(|span| matches!(
            span.style.fg,
            Some(Color::Rgb(255, green, 0)) if (140..=185).contains(&green)
        )));

        assert_eq!(first.spans.iter().map(|span| span.content.as_ref()).collect::<String>(), text);
        assert_eq!(later.spans.iter().map(|span| span.content.as_ref()).collect::<String>(), text);
        assert!(later.spans.last().unwrap().style.fg == Some(super::ACCENT_COLOR));
    }

    #[test]
    fn completed_local_interactions_wait_for_the_session_reservation() {
        let mut app = App { generation_active: true, ..App::default() };
        app.begin_interaction("/statusline");
        app.complete_local_interaction("Statusline shown.".into());

        assert!(app.take_persistable_interactions().is_empty());
        assert_eq!(app.completed_interactions.len(), 1);

        app.generation_active = false;
        app.reply_pending = true;

        assert!(app.take_persistable_interactions().is_empty());
        assert_eq!(app.completed_interactions.len(), 1);

        app.reply_pending = false;

        let interactions = app.take_persistable_interactions();

        assert_eq!(interactions.len(), 1);
        assert_eq!(interactions[0].input, "/statusline");
        assert!(app.completed_interactions.is_empty());
    }

    #[test]
    fn completed_local_interactions_wait_for_compaction_to_finish() {
        let mut app = App { compaction_active: true, ..App::default() };
        app.begin_interaction("/compact");
        app.complete_local_interaction("Compacted the conversation.".into());

        assert!(app.take_persistable_interactions().is_empty());
        assert_eq!(app.completed_interactions.len(), 1);

        app.compaction_active = false;

        assert_eq!(app.take_persistable_interactions().len(), 1);
    }

    #[tokio::test]
    async fn typewriter_reveals_whole_graphemes_and_can_be_disabled_mid_reply() {
        let (mut command_tx, _) = duplex(1024);
        let mut app = App {
            input: "/animation off".chars().collect(),
            cursor: "/animation off".len(),
            generation_active: true,
            typewriter: true,
            ..App::default()
        };

        app.push_output("Ae\u{301}B".into());

        assert!(app.transcript.is_empty());
        assert_eq!(app.reveal_queue.len(), 3);
        app.reveal_next();

        assert_eq!(app.transcript, "Ae\u{301}");

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert!(!app.typewriter);
        assert!(app.reveal_queue.is_empty());
        assert!(app.transcript.contains("Ae\u{301}B"));
        assert!(app.statusline_changed);
        assert_eq!(app.completed_interactions[0].output, "Typewriter animation disabled.");
    }

    #[test]
    fn typewriter_reveals_system_command_replies() {
        let mut app = App { typewriter: true, ..App::default() };

        app.push_user("/help");
        app.push_bot("Commands:\n  /statusline [reset]   Choose statusline details.\n".into());

        assert!(!app.generation_active);
        assert!(!app.transcript.contains("Show or reset it."));
        assert!(!app.reveal_queue.is_empty());

        app.reveal_all();

        assert!(app.transcript.contains("Choose statusline details."));
    }

    #[test]
    fn transcript_uses_left_rails_and_separates_turns() {
        let rows = super::wrap_transcript(
            "You\nhello\n\nCrabbot\nNo intelligence plugin is installed.",
            "Crabbot",
            40,
        );

        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        assert!(row_text(&rows[0]).starts_with("▌ "));
        assert!(rows.iter().any(|row| row_text(row).contains("You")));
        assert!(rows.iter().any(|row| row_text(row).contains("Crabbot")));
        assert!(rows.iter().any(|row| row_text(row).contains("No intelligence plugin")));
        assert!(rows.iter().any(|row| row.spans.is_empty()));
        assert!(!rows.iter().any(|row| {
            row.spans
                .iter()
                .any(|span| span.content.starts_with("> ") || span.content.starts_with("# "))
        }));
    }

    #[test]
    fn transcript_uses_role_colored_rails_on_the_left() {
        let rows =
            super::wrap_transcript("You | now\nHello\n\nCrabbot | now\nHi there.", "Crabbot", 50);

        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        let user_top = row_text(&rows[0]);
        let assistant_header = rows.iter().find(|row| row_text(row).contains("Crabbot")).unwrap();

        assert!(user_top.starts_with("▌ "));
        assert!(assistant_header.spans[0].content.starts_with("▌"));
        assert!(rows.iter().any(|row| row_text(row).starts_with("▌ ")));
        let user_title = rows[0].spans.iter().find(|span| span.content == "You").unwrap();
        let assistant_title = rows
            .iter()
            .flat_map(|row| row.spans.iter())
            .find(|span| span.content == "Crabbot")
            .unwrap();

        assert_eq!(user_title.style.fg, None);
        assert_eq!(assistant_title.style.fg, Some(super::ACCENT_COLOR));
        assert!(user_title.style.add_modifier.contains(super::Modifier::ITALIC));
        assert!(assistant_title.style.add_modifier.contains(super::Modifier::ITALIC));
        assert!(!row_text(assistant_header).contains("now"));
        let user_body_row = rows.iter().position(|row| row_text(row).contains("Hello")).unwrap();
        let assistant_row = rows.iter().position(|row| row_text(row).contains("Crabbot")).unwrap();

        assert_eq!(assistant_row - user_body_row - 1, super::MESSAGE_GAP_ROWS);
        let system = super::wrap_transcript("System | now\nA status update.", "Crabbot", 50);
        let system_text = system.iter().map(row_text).collect::<String>();

        assert!(system_text.contains("System"));
        assert!(!system_text.contains("now"));
        assert!(!system_text.contains("NOTICE"));
        assert_eq!(rows[0].spans[0].style.fg, Some(Color::Reset));
        assert_eq!(assistant_header.spans[0].style.fg, Some(super::ACCENT_COLOR));
    }

    #[test]
    fn fenced_code_blocks_show_a_language_panel_and_preserve_code_lines() {
        let transcript = concat!(
            "Crabbot\nBefore the example.\n```js\n",
            "const speaker = \"System\";\nSystem\n",
            "const answer = 42;\n```\nAfter the example."
        );
        let (rows, message_ranges) = super::wrap_transcript_layout(transcript, "Crabbot", 60, None);
        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();
        let rendered = rows.iter().map(row_text).collect::<Vec<_>>();

        assert_eq!(message_ranges.len(), 1);
        assert!(rendered.iter().any(|row| row.starts_with("▌ ┌─ js ─")));
        assert!(rendered.iter().any(|row| row.contains("const speaker = \"System\";")));
        assert!(rendered.iter().any(|row| row.contains("▌ │ System")));
        assert!(rendered.iter().any(|row| row.contains("const answer = 42;")));
        assert!(rendered.iter().any(|row| row.contains("After the example.")));
        assert!(!rendered.iter().any(|row| row.contains("```")));

        let code_row = rows
            .iter()
            .find(|row| row.spans.iter().any(|span| span.content.contains("const answer")))
            .unwrap();

        let code =
            code_row.spans.iter().find(|span| span.content.contains("const answer")).unwrap();

        assert_eq!(code.style.bg, None);

        let header_row = rows.iter().find(|row| row_text(row).contains("┌─ js ")).unwrap();

        assert!(header_row.spans.iter().all(|span| span.style.bg.is_none()));
        assert!(header_row.spans.iter().all(|span| span.style.fg == Some(super::ACCENT_COLOR)));
    }

    #[test]
    fn code_panels_render_open_streams_and_wrap_long_unicode_lines() {
        let transcript = "Crabbot\n```rust\nlet greeting = \"こんにちは世界\";";
        let rows = super::wrap_transcript(transcript, "Crabbot", 28);

        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        let rendered = rows.iter().map(row_text).collect::<Vec<_>>();
        let code = rows
            .iter()
            .flat_map(|row| &row.spans)
            .filter(|span| {
                span.style.fg == Some(Color::Gray)
                    && !span.content.trim().is_empty()
                    && !span.content.contains('│')
            })
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert!(rendered.iter().any(|row| row.starts_with("▌ ┌─ rust")));
        assert!(code.contains("こんにちは世界"));
        assert!(rendered.iter().any(|row| row.starts_with("▌ └")));
        assert!(rendered.iter().all(|row| UnicodeWidthStr::width(row.as_str()) <= 28));
    }

    #[test]
    fn code_fence_parser_supports_tildes_and_sanitizes_the_language_label() {
        assert_eq!(
            super::opening_code_fence("  ~~~~ c++ title=sample"),
            Some(('~', 4, "c++".into()))
        );

        assert_eq!(super::opening_code_fence("```"), Some(('`', 3, "code".into())));
        assert!(super::opening_code_fence("    ```js").is_none());
        assert!(!super::is_closing_code_fence("  ```", '~', 3));
        assert!(super::is_closing_code_fence("  ~~~~  ", '~', 3));
        assert!(!super::is_closing_code_fence("~~~ trailing", '~', 3));
    }

    #[test]
    fn timestamped_message_boundaries_recover_after_an_unclosed_code_fence() {
        let transcript = concat!(
            "Crabbot | 2026-10-03 at 12:00\n```js\n",
            "const title = \"System\";\n",
            "You | 2026-10-03 at 12:01\nNext turn."
        );

        let (_, message_ranges) = super::wrap_transcript_layout(transcript, "Crabbot", 60, None);

        assert_eq!(message_ranges.len(), 2);
    }

    #[test]
    fn message_timestamps_appear_only_on_the_hovered_message() {
        let transcript = "You | 2026-10-01 at 12:26\nping\n\nCrabbot | 2026-10-01 at 12:27\npong";
        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        let rows = super::wrap_transcript_layout(transcript, "Crabbot", 80, None).0;
        let visible = rows.iter().map(row_text).collect::<String>();

        assert!(visible.contains("You"));
        assert!(visible.contains("Crabbot"));
        assert!(!visible.contains("2026-10-01"));

        let hovered = super::wrap_transcript_layout(transcript, "Crabbot", 80, Some(1)).0;
        let hovered = hovered.iter().map(row_text).collect::<String>();

        assert!(!hovered.contains("You, 2026-10-01"));
        assert!(hovered.contains("Crabbot, 2026-10-01 at 12:27"));

        let header = super::wrap_transcript_layout(transcript, "Crabbot", 80, Some(1))
            .0
            .into_iter()
            .find(|row| row.spans.iter().any(|span| span.content == "Crabbot"))
            .unwrap();

        let actor = header.spans.iter().find(|span| span.content == "Crabbot").unwrap();
        let timestamp =
            header.spans.iter().find(|span| span.content.starts_with(", 2026-10-01")).unwrap();

        assert_eq!(actor.style.fg, Some(super::ACCENT_COLOR));
        assert!(actor.style.add_modifier.contains(super::Modifier::BOLD | super::Modifier::ITALIC));
        assert_eq!(timestamp.style.fg, Some(Color::DarkGray));
        assert!(timestamp.style.add_modifier.contains(super::Modifier::ITALIC));
        assert!(!timestamp.style.add_modifier.contains(super::Modifier::BOLD));
    }

    #[test]
    fn hover_coordinates_map_to_visible_message_rows_only() {
        let area = Rect::new(5, 3, 40, 8);
        let message_rows = [2..5, 7..9];

        assert_eq!(super::hovered_message_at(Some((6, 3)), area, 2, &message_rows), Some(0));
        assert_eq!(super::hovered_message_at(Some((6, 8)), area, 2, &message_rows), Some(1));
        assert_eq!(super::hovered_message_at(Some((6, 6)), area, 2, &message_rows), None);
        assert_eq!(super::hovered_message_at(Some((50, 3)), area, 2, &message_rows), None);
        assert_eq!(super::hovered_message_at(None, area, 2, &message_rows), None);
    }

    #[tokio::test]
    async fn moving_the_mouse_over_and_away_from_a_message_toggles_its_timestamp() {
        let (mut command_tx, _) = duplex(1024);
        let mut app = App {
            transcript: "Crabbot | 2026-10-01 at 12:27\npong".into(),
            name: "Crabbot".into(),
            ..App::default()
        };

        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Moved,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("Crabbot, 2026-10-01 at 12:27"));

        let input_area = app.input_area.unwrap();

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Moved,
                column: input_area.x,
                row: input_area.y,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("Crabbot"));
        assert!(!rendered.contains("2026-10-01"));
    }

    #[test]
    fn monochrome_transcript_clears_foreground_and_background_colors() {
        let mut rows = super::wrap_transcript("You\nhello", "Crabbot", 40);

        super::strip_transcript_colors(&mut rows);

        assert!(rows.iter().flat_map(|row| &row.spans).all(|span| {
            span.style.fg.is_none()
                && span.style.bg.is_none()
                && span.style.underline_color.is_none()
        }));
    }

    #[test]
    fn wrapped_help_descriptions_hang_indent_under_the_description_column() {
        let line = "  /workspace [path|reset]  Show or change this session's filesystem root.";
        let (command, description) = super::help_command_segments(line).unwrap();
        let rows = super::wrap_help_command(command, description, 40);
        let indent = UnicodeWidthStr::width(command)
            + description.chars().take_while(|character| character.is_whitespace()).count();

        let continuation = rows.get(1).unwrap();
        let continuation_text =
            continuation.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        assert_eq!(
            continuation_text.chars().take_while(|character| *character == ' ').count(),
            indent
        );
    }

    #[test]
    fn transcript_wraps_at_word_boundaries_and_hides_soft_wrap_spaces() {
        let rows = super::wrap_transcript_line("alpha workspace beta", "Crabbot", 9);
        let row_text =
            |row: &Line<'_>| row.spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        let rendered = rows.iter().map(row_text).collect::<Vec<_>>();

        assert_eq!(rendered, ["alpha", "workspace", "beta"]);
        assert!(rendered.iter().all(|row| !row.starts_with(' ')));
    }

    #[test]
    fn exit_feedback_is_visible_during_shutdown() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App { exiting: true, ..App::default() };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("Exiting Crabbot… | Please wait"));
    }

    #[test]
    fn transcript_rails_continue_across_message_paragraphs() {
        let rows = super::wrap_transcript(
            "You | now\nFirst paragraph.\n\nSecond paragraph.\n\nCrabbot | now\nReply.",
            "Crabbot",
            60,
        );

        let second_paragraph = rows
            .iter()
            .position(|row| row.spans.iter().any(|span| span.content == "Second paragraph."));

        let paragraph_gap = second_paragraph.unwrap().saturating_sub(1);

        assert!(rows[paragraph_gap].spans.iter().any(|span| span.content == "▌ "));
    }

    #[test]
    fn labels_saved_messages_with_their_recorded_times_and_role_rails() {
        let messages = [
            crabbot_core::types::Message {
                id: "tui-user-1709164800000-1".into(),
                session: "work".into(),
                role: crabbot_core::types::Role::User,
                sender: None,
                content: vec![crabbot_core::types::Content::Text { text: "List files".into() }],
            },
            crabbot_core::types::Message {
                id: "tui-assistant-1709164860000-1".into(),
                session: "work".into(),
                role: crabbot_core::types::Role::Assistant,
                sender: None,
                content: vec![crabbot_core::types::Content::Text { text: "Done.".into() }],
            },
        ];

        let transcript = super::render_messages(&messages, "Crabbot");

        let user_time = crate::date::format_datetime(1_709_164_800).unwrap();
        let assistant_time = crate::date::format_datetime(1_709_164_860).unwrap();

        assert!(transcript.contains(&format!("You | {user_time}\nList files")));
        assert!(transcript.contains(&format!("Crabbot | {assistant_time}\nDone.")));

        let rows = super::wrap_transcript(&transcript, "Crabbot", 50);

        assert!(rows.iter().any(|row| row.spans.iter().any(|span| span.content == "▌ ")));
        assert!(rows.iter().any(|row| row.spans.iter().any(|span| span.content == "▌ ")));
    }

    #[test]
    fn elapsed_generation_time_is_compact_and_human_readable() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(9)), "9s");
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(62)), "1m 02s");
    }

    #[test]
    fn notice_and_key_hint_use_the_default_foreground() {
        for text in [
            "No intelligence plugin is installed. Install a model plugin to send messages.",
            "Use Ctrl+O for a new message line.",
        ] {
            let row = super::wrap_transcript_line(text, "Crabbot", 100);

            assert_eq!(row[0].spans[0].style.fg, None);
        }
    }

    #[test]
    fn list_items_keep_regular_transcript_styling() {
        for line in [
            "- Enter `q` to quit.",
            "– Other input is ignored.",
            "* A regular list item.",
            "o Another regular list item.",
        ] {
            let row = super::wrap_transcript_line(line, "Crabbot", 100);

            assert_eq!(row.len(), 1);
            assert_eq!(row[0].spans.len(), 1);
            assert_eq!(row[0].spans[0].content, line);
            assert_eq!(row[0].spans[0].style, super::Style::default());
        }
    }

    #[test]
    fn help_commands_and_descriptions_use_distinct_colors() {
        let row = super::wrap_transcript_line(
            "  /statusline [reset]   Choose which details appear in the bottom statusline.",
            "Crabbot",
            100,
        );

        assert!(
            row[0]
                .spans
                .iter()
                .any(|span| { span.content.contains("/statusline") && span.style.fg.is_none() })
        );

        let rendered = row[0].spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        assert!(rendered.contains("Choose which details"));
        assert!(row[0].spans.iter().skip(1).all(|span| span.style.fg == Some(Color::DarkGray)));

        let row = super::wrap_transcript_line(
            "  /model <help|list|show|set> Manage the selected model.",
            "Crabbot",
            100,
        );

        assert!(row[0].spans.iter().any(|span| {
            span.content.contains("/model <help|list|show|set>") && span.style.fg.is_none()
        }));

        let rendered = row[0].spans.iter().map(|span| span.content.as_ref()).collect::<String>();

        assert!(rendered.contains("Manage the selected model"));
        assert!(row[0].spans.iter().skip(1).all(|span| span.style.fg == Some(Color::DarkGray)));

        let row = super::wrap_transcript_line(
            "  !<command>  Run a shell command directly.",
            "Crabbot",
            80,
        );

        assert!(
            row[0]
                .spans
                .iter()
                .any(|span| { span.content.contains("!<command>") && span.style.fg.is_none() })
        );

        assert!(row[0].spans.iter().skip(1).all(|span| span.style.fg == Some(Color::DarkGray)));
    }

    #[test]
    fn migrates_legacy_statusline_placeholders_to_fixed_options() {
        let options = super::statusline_from_legacy("{name} / {session}");

        assert!(options.title);
        assert!(options.session);
        assert!(!options.model);
        assert!(!options.context);
        assert!(!options.workspace);
        assert!(!options.status);

        assert_eq!(super::statusline_from_legacy("old custom text"), StatuslineOptions::default());
    }

    #[test]
    fn statusline_shows_workspace_label_and_only_its_basename() {
        let app = App {
            name: "Crabbot".into(),
            model: "unset".into(),
            session: "default".into(),
            workspace: "/home/airscript/repos/personal/crabbot".into(),
            context_usage: Some(crabbot_core::types::ContextUsage { used: 12345, limit: 128000 }),
            statusline: StatuslineOptions::default(),
            ..App::default()
        };

        assert_eq!(
            render_statusline(&app),
            "Crabbot | model: unset | context: 12,345/128,000 (9.6%) | session: default | workspace: crabbot"
        );
    }

    #[test]
    fn statusline_reports_unavailable_context_for_providers_without_usage() {
        let app = App {
            statusline: StatuslineOptions {
                title: false,
                model: false,
                context: true,
                session: false,
                workspace: false,
                status: false,
            },
            ..App::default()
        };

        assert_eq!(render_statusline(&app), "context: unavailable");
    }

    #[test]
    fn restores_session_context_with_the_correct_model_label() {
        let mut app = App {
            name: "Ada".into(),
            session: "old".into(),
            model: "old-model".into(),
            statusline: StatuslineOptions {
                title: true,
                model: true,
                context: false,
                session: true,
                workspace: false,
                status: false,
            },
            model_available: false,
            ..App::default()
        };

        app.transcript_scroll.set_extent(30, 10);
        app.transcript_scroll.scroll_up(12);

        app.restore_session(super::SessionView {
            id: "fresh".into(),
            model: crate::DEFAULT_MODEL.into(),
            workspace: Some("/work".into()),
            context_usage: None,
            messages: Vec::new(),
            working: false,
        });

        assert_eq!(app.session, "fresh");
        assert_eq!(app.model, "unset");
        assert_eq!(app.workspace, "/work");
        assert!(app.transcript_scroll.follow_end);
        assert_eq!(render_statusline(&app), "Ada | model: unset | session: fresh");

        app.model_available = true;
        app.restore_session(super::SessionView {
            id: "configured".into(),
            model: "provider/model".into(),
            workspace: None,
            context_usage: None,
            messages: Vec::new(),
            working: false,
        });

        assert_eq!(app.model, "provider/model");
        assert!(app.workspace.is_empty());
        assert_eq!(render_statusline(&app), "Ada | model: provider/model | session: configured");
    }

    #[test]
    fn refreshes_a_shared_session_snapshot_without_requiring_a_manual_reload() {
        let mut app = App {
            name: "Crabbot".into(),
            session: "shared".into(),
            model: "model".into(),
            default_workspace: "/work".into(),
            ..App::default()
        };

        app.refresh_session(super::SessionView {
            id: "shared".into(),
            model: "model".into(),
            workspace: Some("/work".into()),
            context_usage: None,
            messages: vec![crabbot_core::types::Message {
                id: "new-user-message".into(),
                session: "shared".into(),
                role: crabbot_core::types::Role::User,
                sender: Some("tui".into()),
                content: vec![crabbot_core::types::Content::Text {
                    text: "message from the other TUI".into(),
                }],
            }],
            working: true,
        });

        assert!(app.transcript.contains("message from the other TUI"));
        assert!(app.session_working);
    }

    #[test]
    fn compaction_snapshot_waits_for_the_local_system_reply_to_be_saved() {
        let mut app = App {
            name: "Crabbot".into(),
            session: "work".into(),
            model: "model".into(),
            ..App::default()
        };

        app.begin_interaction("/compact");
        app.push_user("/compact");
        app.reply_pending = true;
        app.reply_system = true;

        app.refresh_session(super::SessionView {
            id: "work".into(),
            model: "model".into(),
            workspace: None,
            context_usage: None,
            messages: vec![crabbot_core::types::Message {
                id: "compact-summary".into(),
                session: "work".into(),
                role: crabbot_core::types::Role::Assistant,
                sender: Some("compaction".into()),
                content: vec![crabbot_core::types::Content::Text {
                    text: "Earlier conversation summary: Keep the project goals.".into(),
                }],
            }],
            working: false,
        });

        assert!(app.transcript.contains("/compact"));
        assert!(!app.transcript.contains("Earlier conversation summary"));

        let reply = "Compacted 11 older messages into a summary; kept 5 recent messages.";
        let completed = app.push_assistant_output(format!("{reply}\n> "));

        assert_eq!(completed.len(), 1);

        app.refresh_session(super::SessionView {
            id: "work".into(),
            model: "model".into(),
            workspace: None,
            context_usage: None,
            messages: vec![
                crabbot_core::types::Message {
                    id: "tui-interaction-user-1709164800000-1".into(),
                    session: "work".into(),
                    role: crabbot_core::types::Role::User,
                    sender: Some("tui".into()),
                    content: vec![crabbot_core::types::Content::Text { text: "/compact".into() }],
                },
                crabbot_core::types::Message {
                    id: "tui-system-interaction-assistant-1709164860000-1".into(),
                    session: "work".into(),
                    role: crabbot_core::types::Role::Assistant,
                    sender: None,
                    content: vec![crabbot_core::types::Content::Text { text: reply.into() }],
                },
            ],
            working: false,
        });

        assert_eq!(app.transcript.matches(reply).count(), 1);
        assert!(app.transcript.contains("System | "));
    }

    #[test]
    fn shared_session_refresh_does_not_replace_an_animating_local_reply() {
        let mut app = App {
            name: "Crabbot".into(),
            session: "shared".into(),
            model: "model".into(),
            typewriter: true,
            reply_pending: true,
            reply_system: true,
            ..App::default()
        };

        let reply = "Commands:\n  /status  Show runtime status.\n";

        app.begin_interaction("/help");
        app.push_user("/help");
        let completed = app.push_assistant_output(format!("{reply}> "));

        assert_eq!(completed.len(), 1);
        assert!(app.transcript.contains("System | "));

        for _ in 0..4 {
            app.reveal_next();
            let transcript = app.transcript.clone();
            let queued = app.reveal_queue.len();

            app.refresh_session(super::SessionView {
                id: "shared".into(),
                model: "model".into(),
                workspace: None,
                context_usage: None,
                messages: Vec::new(),
                working: false,
            });

            assert_eq!(app.transcript, transcript);
            assert_eq!(app.reveal_queue.len(), queued);
        }

        app.reveal_all();

        assert!(app.transcript.contains("Commands:"));
        assert!(app.transcript.contains("Show runtime status."));
    }

    #[test]
    fn separates_live_replies_once_after_session_restore_and_on_empty_session() {
        let mut app = App { name: "Crabbot".into(), ..App::default() };

        app.push_bot("Created session temp.\n".into());

        assert!(app.transcript.starts_with("Crabbot | "));
        assert!(app.transcript.ends_with("\nCreated session temp.\n"));

        app.restore_session(super::SessionView {
            id: "work".into(),
            model: "model".into(),
            workspace: None,
            context_usage: None,
            messages: vec![crabbot_core::types::Message {
                id: "old-reply".into(),
                session: "work".into(),
                role: crabbot_core::types::Role::Assistant,
                sender: None,
                content: vec![crabbot_core::types::Content::Text { text: "Previous reply".into() }],
            }],
            working: false,
        });

        app.push_bot("Using session work.\n".into());

        assert!(app.transcript.contains("Crabbot\nPrevious reply"));
        assert!(app.transcript.ends_with("Using session work.\n"));
        assert!(
            super::wrap_transcript(&app.transcript, "Crabbot", 80)
                .iter()
                .any(|row| row.spans.iter().any(|span| span.content == "▌ "))
        );
    }

    #[test]
    fn renders_an_unboxed_conversation_and_horizontal_prompt() {
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = App {
            transcript: "Welcome\nAnswer".into(),
            input: "next".chars().collect(),
            cursor: 4,
            status: "Local session".into(),
            session: "main".into(),
            ..App::default()
        };

        app.name = "Crabbot".into();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(!rendered.contains("Conversation"));
        assert!(!rendered.contains("Agent"));
        assert!(rendered.contains("Welcome"));
        assert!(rendered.contains("Message"));
        assert!(rendered.contains("|> next"));
        assert!(rendered.contains("Esc quit ─"));

        let buffer = terminal.backend().buffer();
        let input_area = app.input_area.unwrap();
        let gap_row = input_area.y - super::COMPOSER_GAP_ROWS;

        assert_eq!(buffer[(input_area.x, input_area.y)].symbol(), "M");
        assert!((0..buffer.area.width).all(|x| buffer[(x, gap_row)].symbol() == " "));

        assert!(
            (input_area.x..input_area.x + input_area.width)
                .any(|x| buffer[(x, input_area.y)].symbol() == "─")
        );

        assert_eq!(buffer[(input_area.x, input_area.y + input_area.height - 1)].symbol(), "─");
        assert!((input_area.y..input_area.y + input_area.height).all(|y| {
            buffer[(input_area.x, y)].symbol() != "│"
                && buffer[(input_area.x + input_area.width - 1, y)].symbol() != "│"
        }));

        assert!(
            rendered
                .contains(&format!("Crabbot v{} | /help for commands", env!("CARGO_PKG_VERSION")))
        );

        assert!(!rendered.contains("Ready | /help for commands"));

        app.push_output("Again".into());
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        assert!(app.transcript.ends_with("Again"));
        assert!(
            terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
                .contains("Again")
        );
    }

    #[test]
    fn explains_when_a_session_has_no_saved_messages() {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let mut app = App::default();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("No messages in this session yet."));
    }

    #[tokio::test]
    async fn persists_command_and_no_model_interactions_for_session_restore() {
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-interactions-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let home = root.to_string_lossy().into_owned();

        crate::local_aware(
            home.clone(),
            "session.ensure".into(),
            serde_json::json!({"id": "tui-work", "model": "model-work"}),
        )
        .await
        .unwrap();

        let mut app = App { session: "tui-work".into(), model_available: false, ..App::default() };
        app.begin_interaction("/status");

        assert!(app.capture_interaction_output("Background runtime: ").is_empty());
        assert!(app.capture_interaction_output("stopped.\n").is_empty());
        let saved = app.capture_interaction_output("> ").pop().unwrap();
        super::persist_interaction(&home, saved, crate::local_aware).await.unwrap();

        app.begin_interaction("hello without a model");
        let saved = app
            .capture_interaction_output(
                "No intelligence plugin is installed. Install a model plugin to send messages; session and help commands remain available.\n> ",
            )
            .pop()
            .unwrap();

        super::persist_interaction(&home, saved, crate::local_aware).await.unwrap();

        app.begin_interaction("/session missing");
        app.capture_interaction_output("\nSession was not found.");
        let saved = app.finish_interactions().pop().unwrap();
        super::persist_interaction(&home, saved, crate::local_aware).await.unwrap();

        let session = super::load_session(&home, "tui-work", crate::local_aware).await.unwrap();
        let transcript = super::render_messages(&session.messages, "Crabbot");

        assert_eq!(session.messages.len(), 6);
        assert!(transcript.contains("You | "));
        assert!(transcript.contains("System | "));
        assert!(transcript.contains("/status"));
        assert!(transcript.contains("Background runtime: stopped."));
        assert!(transcript.contains("hello without a model"));
        assert!(transcript.contains("No intelligence plugin is installed."));
        assert!(transcript.contains("/session missing"));
        assert!(transcript.contains("Session was not found."));
        assert!(!transcript.contains("Error:"));

        let bounded =
            super::bounded_interaction_text(&format!("{}é", "x".repeat(super::INTERACTION_LIMIT)));

        assert!(bounded.len() <= super::INTERACTION_LIMIT);
        assert!(bounded.ends_with("[history entry truncated]"));

        app.begin_interaction("/session rename old-name");
        let saved = app
            .capture_interaction_output("Renamed session old-name to new-name.\n> ")
            .pop()
            .unwrap();

        assert_eq!(saved.session, "new-name");

        app.begin_interaction("/first");
        app.begin_interaction("/second");
        let saved = app.capture_interaction_output("first reply\n> second reply\n> ");

        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].output, "first reply");
        assert_eq!(saved[1].output, "second reply");

        app.begin_interaction("/large");
        let saved = app
            .capture_interaction_output(&format!(
                "{}\n> ",
                "x".repeat(super::INTERACTION_LIMIT + 10)
            ))
            .pop()
            .unwrap();

        assert!(saved.output.ends_with("[history entry truncated]"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn active_turn_rejects_chat_and_shows_command_reply_as_system() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App { generation_active: true, ..App::default() };
        app.input = "please wait".chars().collect();
        app.cursor = app.input.len();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.input.iter().collect::<String>(), "please wait");
        assert!(app.transcript.is_empty());
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                command_rx.read(&mut [0; 32]),
            )
            .await
            .is_err()
        );

        app.input.clear();
        app.cursor = 0;

        for character in "/approvals".chars() {
            handle_event(
                Event::Key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE)),
                &mut app,
                &mut command_tx,
                false,
            )
            .await
            .unwrap();
        }

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        let mut submitted = [0; 32];
        let count = command_rx.read(&mut submitted).await.unwrap();

        assert_eq!(&submitted[..count], b"\"/approvals\"\n");
        assert!(app.transcript.contains("You | "));
        assert!(!app.transcript.contains("System | "));

        app.push_system("Pending approvals: none.");

        assert!(app.transcript.contains("System | "));
    }

    #[tokio::test]
    async fn shared_working_session_locks_chat_and_reports_the_state_as_system() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App { session_working: true, ..App::default() };
        app.input = "hello from the second TUI".chars().collect();
        app.cursor = app.input.len();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.input.iter().collect::<String>(), "hello from the second TUI");
        assert!(app.transcript.contains("System | "));
        assert!(app.transcript.contains("Crabbot is working in this session."));
        assert!(!app.transcript.contains("You | "));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                command_rx.read(&mut [0; 32]),
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn system_rail_uses_the_statusline_gray() {
        let rows = super::wrap_transcript("System | now\nApproval needed.", "Crabbot", 60);
        let title = rows[0].spans.iter().find(|span| span.content == "System").unwrap();
        let rail = rows[0].spans.iter().find(|span| span.content == "▌ ").unwrap();

        assert_eq!(title.style.fg, Some(Color::DarkGray));
        assert_eq!(rail.style.fg, Some(Color::DarkGray));
    }

    #[test]
    fn approval_actions_and_command_are_highlighted() {
        for line in [
            "Tool: shell",
            "Arguments: {\"command\":\"rm foo\"}",
            "Command: rm foo",
            "Approve: /approve id",
            "Deny: /deny id",
        ] {
            let row = super::wrap_transcript_line(line, "Crabbot", 80);

            assert_eq!(row[0].spans.len(), 2);
            assert_eq!(row[0].spans[0].style.fg, Some(Color::DarkGray));
            assert!(row[0].spans[1].style.add_modifier.contains(ratatui::style::Modifier::BOLD));

            let expected = super::ACCENT_COLOR;

            assert_eq!(row[0].spans[1].style.fg, Some(expected));
        }
    }

    #[test]
    fn tree_listings_use_branch_connectors_instead_of_status_glyphs() {
        let entry = super::super::format_tree_entry(
            "codex | v1.0.0",
            &["health: ready".into(), "permissions: none".into()],
            false,
        );

        assert_eq!(entry, "├─ codex | v1.0.0\n│  ├─ health: ready\n│  └─ permissions: none");
        assert!(!entry.contains('◆'));
        assert!(!entry.contains('◇'));
    }

    #[tokio::test]
    async fn edits_submits_scrolls_and_quits_from_terminal_events() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let mut app = App {
            input: "ac".chars().collect(),
            cursor: 1,
            model_available: true,
            ..App::default()
        };

        app.transcript_scroll.set_extent(100, 10);

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Delete, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.transcript_scroll.offset, 84);
        assert_eq!(app.input.iter().collect::<String>(), "a");
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.transcript_scroll.offset, 90);

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.transcript_scroll.offset, 87);

        app.input = "/help".chars().collect();
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        let mut submitted = [0; 16];
        let count = command_rx.read(&mut submitted).await.unwrap();

        assert_eq!(&submitted[..count], b"\"/help\"\n");
        assert!(app.transcript.contains("You | "));
        assert!(app.transcript.contains("/help"));
        assert_eq!(app.capture_interaction_output("Help response.\n> ").len(), 1);

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("You"));
        assert!(rendered.contains("/help"));

        app.model_available = false;
        app.input = "/clear".chars().collect();
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert!(app.pending_clear);
        assert!(app.pending_interactions.is_empty());
        assert!(app.capture_interaction_output("Conversation cleared.\n> ").is_empty());
        app.observe_context_output("Conversation cleared.\n> ");

        assert!(app.transcript.is_empty());
        app.model_available = true;

        app.input = "/again".chars().collect();
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            true,
        )
        .await
        .unwrap();

        assert!(app.transcript.contains("no longer running"));

        assert!(
            handle_event(
                Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
                &mut app,
                &mut command_tx,
                false,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn mouse_wheel_scrolls_the_message_box_when_pointer_is_over_it() {
        let (mut command_tx, _) = duplex(1024);
        let mut app = App {
            input: "one\ntwo\nthree\nfour\nfive\nsix".chars().collect(),
            input_area: Some(Rect::new(10, 10, 30, 5)),
            ..App::default()
        };

        app.input_scroll.set_extent(6, 3);

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 12,
                row: 12,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.input_scroll.offset, 2);
        assert_eq!(app.transcript_scroll.offset, 0);

        handle_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 12,
                row: 12,
                modifiers: KeyModifiers::NONE,
            }),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.input_scroll.offset, 3);
    }

    #[test]
    fn scroll_state_pages_predictably_and_follows_only_from_the_end() {
        let mut scroll = ScrollState::default();
        scroll.set_extent(100, 10);

        assert_eq!(scroll.offset, 90);

        scroll.page_up();

        assert_eq!(scroll.offset, 81);
        assert!(!scroll.follow_end);

        scroll.set_extent(110, 10);

        assert_eq!(scroll.offset, 81);

        scroll.scroll_down(100);

        assert_eq!(scroll.offset, 100);
        assert!(scroll.follow_end);

        scroll.set_extent(120, 10);

        assert_eq!(scroll.offset, 110);
    }

    #[test]
    fn input_scroll_keeps_the_moving_cursor_visible() {
        let mut scroll = ScrollState::default();
        scroll.set_extent(20, 5);
        scroll.ensure_visible(2);

        assert_eq!(scroll.offset, 2);
        assert!(!scroll.follow_end);

        scroll.ensure_visible(19);

        assert_eq!(scroll.offset, 15);
        assert!(scroll.follow_end);
    }

    #[test]
    fn bounds_transcript_and_removes_protocol_prompts() {
        let mut app = App::default();
        app.push_output("> hello\n> world\n> ".into());

        assert_eq!(app.transcript, "hello\nworld\n");

        app.push_output("x".repeat(super::TRANSCRIPT_LIMIT + 1));

        assert_eq!(app.transcript.len(), super::TRANSCRIPT_LIMIT);
    }

    #[tokio::test]
    async fn keeps_terminal_history_and_restores_the_draft() {
        let (mut command_tx, _) = duplex(1024);
        let mut app = App {
            input: "draft".chars().collect(),
            cursor: 5,
            history: vec!["first".into(), "second".into()],
            ..App::default()
        };

        for code in [KeyCode::Up, KeyCode::Up, KeyCode::Down, KeyCode::Down] {
            handle_event(
                Event::Key(KeyEvent::new(code, KeyModifiers::NONE)),
                &mut app,
                &mut command_tx,
                false,
            )
            .await
            .unwrap();
        }

        assert_eq!(app.input.iter().collect::<String>(), "draft");
        assert_eq!(app.cursor, 5);
    }

    #[tokio::test]
    async fn accepts_multiline_input_and_persists_statusline() {
        let (mut command_tx, mut command_rx) = duplex(1024);
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-preferences-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let mut app = App {
            input: "first".chars().collect(),
            cursor: 5,
            name: "Ada".into(),
            model: "model-1".into(),
            model_available: true,
            session: "work".into(),
            typewriter: true,
            statusline: StatuslineOptions::default(),
            preferences_path: root
                .join("data")
                .join("plugins")
                .join("tui")
                .join("preferences.toml"),
            ..App::default()
        };

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        app.input.extend("second".chars());
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        app.input.extend("third".chars());
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        let mut submitted = Vec::new();
        command_rx.read_buf(&mut submitted).await.unwrap();

        assert_eq!(
            serde_json::from_slice::<String>(&submitted[..submitted.len() - 1]).unwrap(),
            "first\nsecond\nthird"
        );

        app.input = "/statusline".chars().collect();
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert!(app.statusline_picker);
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert!(!app.statusline_picker);
        assert!(!app.statusline.model);
        save_preferences(&app.preferences_path, app.statusline, app.typewriter).unwrap();
        let saved = crabbot_file::load(&app.preferences_path, 16 * 1024).unwrap().unwrap();
        let preferences: TuiPreferences =
            toml::from_str(&String::from_utf8(saved).unwrap()).unwrap();

        assert_eq!(preferences.statusline, app.statusline);
        assert_eq!(
            render_statusline(&app),
            "Ada | context: unavailable | session: work | workspace: unset"
        );

        assert_eq!(wrapped_rows("one\ntwo", 80), 2);

        app.input = "/statusline reset".chars().collect();
        app.cursor = app.input.len();
        handle_event(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut app,
            &mut command_tx,
            false,
        )
        .await
        .unwrap();

        assert_eq!(app.statusline, StatuslineOptions::default());
        save_preferences(&app.preferences_path, app.statusline, app.typewriter).unwrap();
        let saved = crabbot_file::load(&app.preferences_path, 16 * 1024).unwrap().unwrap();

        let preferences: TuiPreferences =
            toml::from_str(&String::from_utf8(saved).unwrap()).unwrap();

        assert_eq!(preferences.statusline, StatuslineOptions::default());
        assert!(preferences.typewriter);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn switching_sessions_delivers_saved_history_to_the_conversation_view() {
        let root =
            std::env::temp_dir().join(format!("crabbot-tui-session-view-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let home = root.to_string_lossy().into_owned();

        crate::offline::control(
            &home,
            "session.ensure",
            serde_json::json!({"id": "tui-work", "model": "model-work"}),
        )
        .await
        .unwrap();

        for index in 0..20 {
            let role = if index % 2 == 0 { "user" } else { "assistant" };

            let sender = if role == "user" { Some("tui") } else { None };

            let content = format!("saved history message {index}");
            crate::offline::control(
                &home,
                "session.append",
                serde_json::json!({
                    "id": "tui-work",
                    "message": {
                        "id": format!("message-{index}"),
                        "session": "tui-work",
                        "role": role,
                        "sender": sender,
                        "content": [{"kind": "text", "text": content}]
                    }
                }),
            )
            .await
            .unwrap();
        }

        let (mut input_tx, input_rx) = duplex(4096);
        input_tx
            .write_all(b"/session switch missing\n/session switch work\n/quit\n")
            .await
            .unwrap();

        drop(input_tx);
        let (output_tx, mut output_rx) = duplex(16 * 1024);
        let (session_tx, mut session_rx) = tokio::sync::mpsc::unbounded_channel();
        let (interrupt_tx, interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(interrupt_tx);

        let engine = tokio::spawn(crate::run_with(
            input_rx,
            output_tx,
            crate::EngineConfig {
                home: home.clone(),
                plugin: "missing-model".into(),
                model: "model-main".into(),
                model_override: None,
                session: "tui-main".into(),
            },
            crate::local_aware,
            crate::EngineEvents { session: Some(session_tx), ..crate::EngineEvents::default() },
            interrupt_rx,
        ));

        let output = async {
            let mut output = String::new();
            output_rx.read_to_string(&mut output).await.unwrap();
            output
        };

        let (result, output) = tokio::join!(engine, output);
        result.unwrap().unwrap();

        let mut app = App {
            session: "tui-main".into(),
            model: "model-main".into(),
            model_available: true,
            statusline: StatuslineOptions {
                title: false,
                model: false,
                context: false,
                session: true,
                workspace: false,
                status: false,
            },
            ..App::default()
        };

        app.name = "Crabbot".into();
        app.restore_session(session_rx.recv().await.unwrap());

        assert_eq!(app.session, "tui-work");
        assert_eq!(app.model, "model-work");
        assert!(app.transcript.contains("You\nsaved history message 0"));
        assert!(app.transcript.contains("saved history message 19"));
        assert!(super::wrap_transcript(&app.transcript, "Crabbot", 60).iter().any(|row| {
            row.spans.iter().any(|span| span.content.contains("saved history message 19"))
        }));

        assert!(super::wrap_transcript(&app.transcript, "Crabbot", 60).len() > 10);
        assert!(output.contains("Using session work."));
        assert!(output.contains("Session not found"));
        assert!(!app.transcript.contains("main"));
        assert_eq!(render_statusline(&app), "session: work");

        app.pending_model = Some("model-next".into());
        app.observe_context_output("Using mod");
        app.observe_context_output("el model-next.\n> ");

        assert_eq!(app.model, "model-next");

        app.pending_workspace = Some(None);
        app.observe_context_output("Workspace: not configured.\n> ");

        assert!(app.workspace.is_empty());

        app.transcript = "old conversation".into();
        app.pending_clear = true;
        app.observe_context_output("Conversation cleared.\n> ");

        assert!(app.transcript.is_empty());

        app.model_available = false;
        app.restore_session(super::SessionView {
            id: "plain".into(),
            model: crate::DEFAULT_MODEL.into(),
            workspace: None,
            context_usage: None,
            messages: Vec::new(),
            working: false,
        });

        assert_eq!(app.model, "unset");
        assert!(app.transcript.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn marquee_scrolls_long_statusline_and_keeps_short_statusline_still() {
        assert_eq!(marquee_statusline("short", 10, 0), "short");
        assert_eq!(marquee_statusline("abcdefghij", 4, 0), "abcd");
        assert_eq!(marquee_statusline("abcdefghij", 4, 1), "bcde");
        assert_eq!(marquee_statusline("a界bc", 4, 0), "a界b");
        assert_eq!(input_cursor_position(&['a', 'b', 'c', 'd'], 4, 6), (1, 1));
        assert_eq!(input_cursor_position(&['界'], 1, 8), (2, 0));
    }

    #[test]
    fn marquee_moves_slowly_without_glow_or_cursor_on_its_row() {
        assert_eq!(STATUSLINE_SCROLL_INTERVAL, Duration::from_millis(300));

        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App {
            name: "Crabbot".into(),
            model: "provider/model-with-a-long-name".into(),
            session: "session-with-a-long-name".into(),
            workspace: "workspace-with-a-long-name".into(),
            statusline_started: Some(std::time::Instant::now()),
            ..App::default()
        };

        for offset in [0, 1, 2] {
            app.statusline_started =
                Some(std::time::Instant::now() - STATUSLINE_SCROLL_INTERVAL * offset);
            terminal.draw(|frame| draw(frame, &mut app)).unwrap();

            let statusline_area = app.input_area.unwrap();
            let statusline_y = terminal.backend().buffer().area().bottom() - 1;
            let statusline = (0..terminal.backend().buffer().area().width)
                .map(|x| terminal.backend().buffer()[(x, statusline_y)].symbol())
                .collect::<String>();

            let cursor = terminal.backend().cursor_position();

            assert!(terminal.backend().cursor_visible());
            assert_ne!(cursor.y, statusline_y);
            assert!(statusline_area.contains(cursor));
            assert!(!statusline.contains('█'));
        }

        assert_ne!(marquee_statusline("abcdefghij", 4, 0), marquee_statusline("abcdefghij", 4, 1));
    }

    #[test]
    fn marquee_cursor_stays_inside_the_composer() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App {
            name: "Crabbot".into(),
            model: "provider/model-with-a-long-name".into(),
            session: "session-with-a-long-name".into(),
            workspace: "workspace-with-a-long-name".into(),
            statusline_started: Some(std::time::Instant::now()),
            ..App::default()
        };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let position = terminal.backend().cursor_position();
        let input_area = app.input_area.unwrap();

        assert!(terminal.backend().cursor_visible());
        assert!(input_area.contains(position));
        assert!(position.y < 11);
    }

    #[test]
    fn bounds_history_and_handles_empty_navigation() {
        let mut app = App::default();
        app.history_up();
        app.history_down();

        for index in 0..=INPUT_HISTORY_LIMIT {
            app.remember_input(&format!("line-{index}"));
        }

        assert_eq!(app.history.len(), INPUT_HISTORY_LIMIT);
        assert_eq!(app.history.first().map(String::as_str), Some("line-1"));
    }

    #[test]
    fn keeps_new_output_visible_and_supports_scrollback() {
        let transcript = (0..40).map(|line| format!("line {line}\n")).collect::<String>();
        let backend = TestBackend::new(80, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = App { transcript, ..App::default() };

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("line 39"));

        app.transcript_scroll.scroll_up(10);
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(!rendered.contains("line 39"));
        assert!(rendered.contains("line 29"));
    }
}
