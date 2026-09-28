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
    widgets::{Block, Borders, Paragraph, Widget, Wrap},
};

use std::{
    collections::VecDeque,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    sync::mpsc,
    time::timeout,
};

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const TRANSCRIPT_LIMIT: usize = 512 * 1024;
const INTERACTION_LIMIT: usize = 12 * 1024;
const INPUT_HISTORY_LIMIT: usize = 100;
const INPUT_MAX_ROWS: usize = 5;
const TYPEWRITER_INTERVAL: Duration = Duration::from_millis(16);
const TYPEWRITER_BACKLOG: usize = 96;
const TYPEWRITER_BURST: usize = 2;
const TYPEWRITER_CATCHUP_BURST: usize = 12;
const DEFAULT_STATUSLINE: &str = "{name} · model: {model} · session: {session} · {workspace}";
const LEGACY_DEFAULT_STATUSLINE: &str = "{name} · {model} · {session} · {workspace}";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
struct TuiPreferences {
    statusline: String,
    #[serde(default = "default_typewriter")]
    typewriter: bool,
}

fn default_typewriter() -> bool {
    true
}

impl Default for TuiPreferences {
    fn default() -> Self {
        Self { statusline: String::new(), typewriter: true }
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
    history_index: Option<usize>,
    history_draft: String,
    transcript_scroll: ScrollState,
    input_scroll: ScrollState,
    input_cursor_needs_visibility: bool,
    input_area: Option<Rect>,
    status: String,
    session: String,
    model: String,
    model_available: bool,
    workspace: String,
    name: String,
    statusline: String,
    statusline_changed: bool,
    statusline_reset: bool,
    preferences_path: PathBuf,
    typewriter: bool,
    generation_active: bool,
    generation_message: &'static str,
    interrupt_requested: bool,
    reply_pending: bool,
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
}

struct SavedInteraction {
    session: String,
    input: String,
    output: String,
    sequence: u64,
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

    let statusline = upgraded_statusline(&preferences.statusline);
    let typewriter = preferences.typewriter;

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
        Some(session_tx),
        Some(engine_event_tx),
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

    let mut app = App {
        status: "Ready".into(),
        transcript: render_messages(&initial_session.messages, &name),
        session: initial_session.id,
        model: display_model(&initial_session.model, model_available),
        model_available,
        workspace: initial_session.workspace.unwrap_or_default(),
        name,
        statusline,
        preferences_path,
        typewriter,
        generation_message: random_generation_message(),
        ..App::default()
    };

    let mut buffer = [0_u8; 4096];
    let mut engine_finished = false;
    let mut session_events_open = true;
    let mut engine_events_open = true;
    let mut quit = false;

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
                        if app.statusline_reset {
                            save_preferences(&app.preferences_path, "", app.typewriter)?;
                            app.statusline_reset = false;
                        } else {
                            save_preferences(&app.preferences_path, &app.statusline, app.typewriter)?;
                        }

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

                        app.status = format!("Session {}", session.id);

                        if renamed {
                            app.session = session.id;
                            app.model = display_model(&session.model, app.model_available);
                            app.workspace = session.workspace.unwrap_or_default();
                        } else {
                            app.restore_session(session);

                            if let Some(input) = created_input {
                                app.push_user(&input);
                            }
                        }
                    }

                    None => session_events_open = false,
                }
            }

            event = engine_event_rx.recv(), if engine_events_open => {
                match event {
                    Some(crate::EngineEvent::GenerationStarted) => {
                        app.generation_active = true;
                        app.interrupt_requested = false;
                        app.generation_message = random_generation_message();
                    }

                    Some(crate::EngineEvent::GenerationFinished { interrupted }) => {
                        app.generation_active = false;
                        app.reply_pending = false;
                        app.interrupt_requested = false;
                        app.status = if interrupted { "Generation interrupted" } else { "Ready" }.into();

                        for interaction in app.take_persistable_interactions() {
                            persist_interaction(&home, interaction, host).await?;
                        }
                    }

                    None => engine_events_open = false,
                }
            }

            _ = tokio::time::sleep(TYPEWRITER_INTERVAL), if !app.reveal_queue.is_empty() => {
                app.reveal_next();
            }

            result = output_rx.read(&mut buffer), if !engine_finished => {
                match result {
                    Ok(0) => {
                        engine_finished = true;
                        app.generation_active = false;
                        app.reply_pending = false;
                        app.pending_model = None;
                        app.pending_workspace = None;
                        app.pending_clear = false;
                        app.context_output.clear();

                        match (&mut engine).await {
                            Ok(Ok(())) => app.status = "Session ended".into(),

                            Ok(Err(error)) => {
                                app.status = "Session failed".into();
                                let message = format!("\nError: {error}");
                                app.capture_interaction_output(&message);
                                app.push_output(message);
                            }

                            Err(error) => {
                                app.status = "Session failed".into();
                                let message = format!("\nError: {error}");
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
                        if app.reply_pending {
                            let speaker = app.name.clone();
                            app.push_label(&speaker);
                            app.reply_pending = false;
                        }

                        let text = String::from_utf8_lossy(&buffer[..count]).into_owned();
                        let interactions = app.capture_interaction_output(&text);
                        app.observe_context_output(&text);
                        app.push_output(text);

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
        let _ = command_tx.write_all(b"/quit\n").await;
        let _ = timeout(Duration::from_secs(2), &mut engine).await;

        if !engine.is_finished() {
            engine.abort();
        }
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
    let value = host(home.to_owned(), "session.get".into(), serde_json::json!({"id": id})).await?;

    if value["status"] == "working" || value["inflight"] == true {
        return Err(crabbot_core::Error::Denied("Session is already working.".into()));
    }

    let model = value["model"]
        .as_str()
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("Session has no model configured.".into()))?
        .to_owned();

    let messages = serde_json::from_value(value["messages"].clone())?;
    let workspace = value["workspace"].as_str().map(str::to_owned);

    Ok(SessionView { id: id.to_owned(), model, workspace, messages })
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
            id: crate::message_id(&format!("interaction-{role_name}"), interaction.sequence),
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
    pub(super) messages: Vec<Message>,
}

fn display_model(model: &str, model_available: bool) -> String {
    if model_available { model.into() } else { "unset".into() }
}

fn upgraded_statusline(statusline: &str) -> String {
    if statusline.is_empty() || statusline == LEGACY_DEFAULT_STATUSLINE {
        DEFAULT_STATUSLINE.to_owned()
    } else {
        statusline.to_owned()
    }
}

fn render_messages(messages: &[Message], name: &str) -> String {
    let mut transcript = String::new();

    for message in messages {
        let label = match &message.role {
            Role::User => "You",
            Role::Assistant => name,
            Role::Tool => message.sender.as_deref().unwrap_or("Tool"),
            Role::System => "System",
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

        transcript.push_str(label);
        transcript.push('\n');
        transcript.push_str(&body);
        transcript.push_str("\n\n");
    }

    transcript
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

fn random_generation_message() -> &'static str {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as usize);

    GENERATION_MESSAGES[seed % GENERATION_MESSAGES.len()]
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

            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                    return Ok(true);
                }

                KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    app.input.insert(app.cursor, '\n');
                    app.cursor += 1;
                    app.input_scroll.follow_latest();
                    app.history_index = None;
                }

                KeyCode::Char(character) => {
                    app.input.insert(app.cursor, character);
                    app.cursor += 1;
                    app.input_scroll.follow_latest();
                    app.history_index = None;
                }

                KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                    app.input.insert(app.cursor, '\n');
                    app.cursor += 1;
                    app.input_scroll.follow_latest();
                    app.history_index = None;
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
                    if !app.pending_interactions.is_empty() {
                        app.status = "Wait for the current response to finish.".into();
                        return Ok(false);
                    }

                    let line = app.input.iter().collect::<String>();
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

                    if let Some(model) = line.strip_prefix("/model ").map(str::trim)
                        && crate::valid_model(model)
                    {
                        app.pending_model = Some(model.to_owned());
                    }

                    if let Some(workspace) = line.strip_prefix("/workspace ").map(str::trim)
                        && !workspace.is_empty()
                    {
                        app.pending_workspace =
                            Some((workspace != "reset").then(|| workspace.to_owned()));
                    }

                    if line == "/clear" {
                        app.pending_clear = true;
                    }

                    if let Some(workspace) = line.strip_prefix("/workspace ").map(str::trim)
                        && !workspace.is_empty()
                    {
                        app.workspace = selected_workspace(workspace);
                    }

                    if line == "/statusline" {
                        let response = format!(
                            "Statusline: {}\nSet it with /statusline <format>. Placeholders: {{name}}, {{model}}, {{session}}, {{workspace}}, {{status}}.",
                            app.statusline
                        );

                        app.push_bot(format!(
                        "Statusline: {}\nSet it with /statusline <format>. Placeholders: {{name}}, {{model}}, {{session}}, {{workspace}}, {{status}}.\n",
                        app.statusline
                    ));
                        app.complete_local_interaction(response);
                        return Ok(false);
                    }

                    if let Some(value) = line.strip_prefix("/statusline ") {
                        let value = value.trim();

                        if value == "reset" {
                            app.statusline = DEFAULT_STATUSLINE.to_owned();
                            app.statusline_changed = true;
                            app.statusline_reset = true;
                            app.push_bot("Statusline restored to its default.\n".into());

                            app.complete_local_interaction(
                                "Statusline restored to its default.".into(),
                            );
                            return Ok(false);
                        }

                        match validate_statusline(value) {
                            Ok(value) => {
                                app.statusline = value;
                                app.statusline_changed = true;
                                app.push_bot("Statusline updated.\n".into());
                                app.complete_local_interaction("Statusline updated.".into());
                            }

                            Err(error) => {
                                app.push_bot(format!("{error}\n"));
                                app.complete_local_interaction(error.to_owned());
                            }
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
                        return Ok(true);
                    }

                    if !engine_finished {
                        command_tx.write_all(serde_json::to_string(&line)?.as_bytes()).await?;
                        command_tx.write_all(b"\n").await?;
                        app.reply_pending = true;
                    } else {
                        app.push_bot("The session engine is no longer running.".into());
                    }
                }

                KeyCode::Esc if app.input.is_empty() => return Ok(true),

                _ => {}
            }
        }

        Event::Mouse(mouse) => match mouse.kind {
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
        },

        _ => {}
    }

    Ok(false)
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut App) {
    let input_text = app.input.iter().collect::<String>();
    let input_content_rows = wrapped_rows(&input_text, frame.area().width.saturating_sub(4));
    let input_rows = input_content_rows.clamp(1, INPUT_MAX_ROWS);
    app.input_scroll.set_extent(input_content_rows, input_rows);

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
            Constraint::Length(input_rows as u16 + 2),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let transcript_width = areas[0].width.saturating_sub(2);
    let content_height = areas[0].height.saturating_sub(2);
    let indicator_height = u16::from(app.generation_active);
    let transcript_height = content_height.saturating_sub(indicator_height) as usize;
    let layout_is_current = app.transcript_layout.as_ref().is_some_and(|layout| {
        layout.generation == app.transcript_generation && layout.width == transcript_width
    });

    if !layout_is_current {
        app.transcript_layout = Some(TranscriptLayout {
            generation: app.transcript_generation,
            width: transcript_width,
            rows: wrap_transcript(&app.transcript, &app.name, transcript_width),
        });
    }

    let rows = &app.transcript_layout.as_ref().expect("layout was refreshed").rows;
    app.transcript_scroll.set_extent(rows.len(), transcript_height);
    let start = app.transcript_scroll.offset;
    let end = start.saturating_add(transcript_height).min(rows.len());
    let visible_rows = &rows[start..end];
    let transcript = TranscriptViewport { rows: visible_rows };
    let conversation = Block::default().borders(Borders::ALL).title("Conversation");

    let input = Paragraph::new(input_text)
        .scroll((input_scroll, 0))
        .wrap(Wrap { trim: false })
        .block(Block::default().borders(Borders::ALL).title("Message  ·  Enter send  ·  Esc quit"));

    let footer_text = format!("{} v{} · /help for commands", app.name, env!("CARGO_PKG_VERSION"));

    let footer_width = UnicodeWidthStr::width(footer_text.as_str()).min(u16::MAX as usize) as u16;

    let footer_areas = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(footer_width)])
        .split(areas[2]);

    let statusline =
        Paragraph::new(render_statusline(app)).style(Style::default().fg(Color::DarkGray));

    let footer = Paragraph::new(footer_text)
        .alignment(Alignment::Right)
        .style(Style::default().fg(Color::DarkGray));

    frame.render_widget(conversation, areas[0]);
    frame.render_widget(
        transcript,
        Rect::new(
            areas[0].x.saturating_add(1),
            areas[0].y.saturating_add(1),
            areas[0].width.saturating_sub(2),
            transcript_height.min(u16::MAX as usize) as u16,
        ),
    );

    if app.generation_active && content_height > 0 {
        let indicator = if app.interrupt_requested { "Stopping…" } else { "Esc to interrupt" };

        let text = format!("{}  ·  {indicator}", app.generation_message);
        let line = Line::styled(text, Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM));

        let indicator_area = Rect::new(
            areas[0].x.saturating_add(1),
            areas[0].y.saturating_add(areas[0].height.saturating_sub(2)),
            areas[0].width.saturating_sub(2),
            1,
        );
        frame.render_widget(Paragraph::new(line), indicator_area);
    }

    frame.render_widget(input, areas[1]);
    frame.render_widget(statusline, footer_areas[0]);
    frame.render_widget(footer, footer_areas[1]);

    let input_area = areas[1];
    app.input_area = Some(input_area);
    let (cursor_x, cursor_y) = input_cursor_position(&app.input, app.cursor, input_area.width);
    frame.set_cursor_position((
        input_area.x.saturating_add(1).saturating_add(cursor_x),
        input_area.y.saturating_add(1).saturating_add(
            cursor_y.saturating_sub(input_scroll).min(input_rows.saturating_sub(1) as u16),
        ),
    ));
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
    let width = width.saturating_sub(2).max(1) as usize;
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

fn validate_statusline(value: &str) -> std::result::Result<String, &'static str> {
    const TOKENS: &[&str] = &["{name}", "{model}", "{session}", "{workspace}", "{status}"];

    if value.is_empty() || value.len() > 120 || value.chars().any(char::is_control) {
        return Err("Statusline must be 1–120 visible characters.");
    }

    let mut remainder = value;

    while let Some(start) = remainder.find('{') {
        let Some(end) = remainder[start..].find('}') else {
            return Err("Statusline has an unclosed placeholder.");
        };

        let token = &remainder[start..=start + end];

        if !TOKENS.contains(&token) {
            return Err(
                "Supported placeholders: {name}, {model}, {session}, {workspace}, {status}.",
            );
        }

        remainder = &remainder[start + end + 1..];
    }

    Ok(value.to_owned())
}

fn selected_workspace(value: &str) -> String {
    if value == "reset" { String::new() } else { value.to_owned() }
}

fn render_statusline(app: &App) -> String {
    let workspace = if app.workspace.is_empty() { "workspace: unset" } else { &app.workspace };

    app.statusline
        .replace("{name}", &app.name)
        .replace("{model}", &app.model)
        .replace("{session}", &app.session)
        .replace("{workspace}", workspace)
        .replace("{status}", &app.status)
}

fn save_preferences(
    path: &std::path::Path,
    statusline: &str,
    typewriter: bool,
) -> crabbot_core::Result<()> {
    let preferences = TuiPreferences { statusline: statusline.to_owned(), typewriter };
    let content = toml::to_string(&preferences)
        .map_err(|error| crabbot_core::Error::Denied(error.to_string()))?;
    crabbot_file::save(path, content).map_err(crabbot_core::Error::from)
}

fn wrap_transcript(transcript: &str, name: &str, width: u16) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }

    if transcript.is_empty() {
        return vec![Line::styled(
            "No messages in this session yet.",
            Style::default().fg(Color::DarkGray),
        )];
    }

    transcript.lines().flat_map(|line| wrap_transcript_line(line, name, width as usize)).collect()
}

fn wrap_transcript_line(line: &str, name: &str, width: usize) -> Vec<Line<'static>> {
    if line.is_empty() {
        return vec![Line::raw("")];
    }

    let mut segments = if line == "You" {
        vec![
            ("> ".to_owned(), Style::default().fg(Color::DarkGray)),
            (line.to_owned(), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        ]
    } else if line == name {
        vec![
            ("# ".to_owned(), Style::default().fg(Color::Cyan)),
            (line.to_owned(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        ]
    } else if line == "System" {
        vec![
            ("! ".to_owned(), Style::default().fg(Color::Yellow)),
            (line.to_owned(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        ]
    } else if let Some((command, description)) = help_command_segments(line) {
        vec![
            (command.to_owned(), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            (description.to_owned(), Style::default().fg(Color::DarkGray)),
        ]
    } else if line == "Commands:"
        || line.starts_with("Conditional Commands")
        || line == "Session commands:"
        || line == "Sessions:"
    {
        vec![(line.to_owned(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))]
    } else if line.starts_with("* ") {
        vec![(line.to_owned(), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))]
    } else if line.starts_with("o ") {
        vec![(line.to_owned(), Style::default().fg(Color::Gray).add_modifier(Modifier::BOLD))]
    } else if line.starts_with("- ") {
        vec![(line.to_owned(), Style::default().fg(Color::DarkGray).add_modifier(Modifier::BOLD))]
    } else if line.starts_with("Error:") || line.starts_with("Session not found") {
        vec![(line.to_owned(), Style::default().fg(Color::Red))]
    } else if line.starts_with("Crabbot terminal.") {
        vec![(line.to_owned(), Style::default().fg(Color::DarkGray))]
    } else {
        vec![(line.to_owned(), Style::default())]
    };

    let width = width.max(1);
    let mut rows = Vec::new();
    let mut spans = Vec::new();
    let mut fragment = String::new();
    let mut fragment_style = Style::default();
    let mut columns = 0;

    for (text, style) in segments.drain(..) {
        for grapheme in text.graphemes(true) {
            let grapheme_width = UnicodeWidthStr::width(grapheme);

            if columns > 0 && columns + grapheme_width > width {
                if !fragment.is_empty() {
                    spans.push(Span::styled(std::mem::take(&mut fragment), fragment_style));
                }

                rows.push(Line::from(std::mem::take(&mut spans)));
                columns = 0;
            }

            if !fragment.is_empty() && style != fragment_style {
                spans.push(Span::styled(std::mem::take(&mut fragment), fragment_style));
            }

            fragment_style = style;
            fragment.push_str(grapheme);
            columns += grapheme_width;

            if columns >= width {
                spans.push(Span::styled(std::mem::take(&mut fragment), fragment_style));
                rows.push(Line::from(std::mem::take(&mut spans)));
                columns = 0;
            }
        }
    }

    if !fragment.is_empty() {
        spans.push(Span::styled(fragment, fragment_style));
    }

    if !spans.is_empty() || rows.is_empty() {
        rows.push(Line::from(spans));
    }

    rows
}

fn help_command_segments(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim_start();

    if !trimmed.starts_with('/') {
        return None;
    }

    let indent = line.len() - trimmed.len();
    let gap = line[indent + 1..]
        .match_indices("  ")
        .map(|(index, _)| indent + 1 + index)
        .find(|index| *index > indent + 1)
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
    fn take_persistable_interactions(&mut self) -> Vec<SavedInteraction> {
        if self.generation_active || self.reply_pending {
            return Vec::new();
        }

        std::mem::take(&mut self.completed_interactions)
    }

    fn begin_interaction(&mut self, input: &str) {
        self.pending_interactions.push_back(PendingInteraction {
            session: self.session.clone(),
            input: input.to_owned(),
            output: String::new(),
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
        }
    }

    fn restore_session(&mut self, session: SessionView) {
        self.session = session.id;
        self.model = display_model(&session.model, self.model_available);
        self.workspace = session.workspace.unwrap_or_default();
        self.transcript = render_messages(&session.messages, &self.name);
        self.invalidate_transcript_layout();
        self.transcript_scroll.follow_latest();
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
                || self.context_output.contains("Current workspace:"))
        {
            self.workspace = workspace.unwrap_or_default();
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
        let value = normalize_output(value);

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
        let speaker = self.name.clone();
        self.push_label(&speaker);
        self.push_output(value);
    }

    fn push_user(&mut self, value: &str) {
        self.push_label("You");
        self.append_visible(&normalize_output(format!("{value}\n")));
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

fn normalize_output(value: String) -> String {
    value.replace("\n> ", "\n").trim_start_matches("> ").trim_end_matches("> ").to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        App, DEFAULT_STATUSLINE, GENERATION_MESSAGES, INPUT_HISTORY_LIMIT, ScrollState,
        TuiPreferences, draw, handle_event, input_cursor_position, interrupt_generation,
        render_statusline, save_preferences, validate_statusline, wrapped_rows,
    };

    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::{Terminal, backend::TestBackend, layout::Rect, style::Color};
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
            statusline: DEFAULT_STATUSLINE.into(),
            ..App::default()
        };

        assert_eq!(
            render_statusline(&app),
            "Crabbot · model: unset · session: default · workspace: unset"
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
    fn drawing_generation_status_shows_the_phrase_and_interrupt_key() {
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut app = App {
            generation_active: true,
            generation_message: "The Crabbot is scuttling toward an answer...",
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
        app.push_bot("Commands:\n  /statusline [format|reset]   Show or reset it.\n".into());

        assert!(!app.generation_active);
        assert!(!app.transcript.contains("Show or reset it."));
        assert!(!app.reveal_queue.is_empty());

        app.reveal_all();

        assert!(app.transcript.contains("Show or reset it."));
    }

    #[test]
    fn transcript_uses_role_styles_and_separates_turns() {
        let rows = super::wrap_transcript(
            "You\nhello\n\nCrabbot\nNo intelligence plugin is installed.",
            "Crabbot",
            40,
        );

        assert!(rows[0].spans.iter().any(|span| span.content == "> "));
        assert!(rows[2].spans.is_empty());
        assert!(rows[3].spans.iter().any(|span| span.content == "# "));
        assert_eq!(rows[4].spans[0].style.fg, None);
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
    fn statusline_help_command_and_description_use_distinct_colors() {
        let row = super::wrap_transcript_line(
            "  /statusline [format|reset]   Show, configure, or reset the bottom statusline.",
            "Crabbot",
            100,
        );

        assert!(row[0].spans.iter().any(|span| {
            span.content.contains("/statusline") && span.style.fg == Some(Color::Green)
        }));

        assert!(row[0].spans.iter().any(|span| {
            span.content.contains("Show, configure") && span.style.fg == Some(Color::DarkGray)
        }));
    }

    #[test]
    fn upgrades_the_saved_builtin_statusline_but_preserves_custom_formats() {
        assert_eq!(super::upgraded_statusline(""), DEFAULT_STATUSLINE);
        assert_eq!(
            super::upgraded_statusline(super::LEGACY_DEFAULT_STATUSLINE),
            DEFAULT_STATUSLINE
        );

        assert_eq!(super::upgraded_statusline("{name} / {session}"), "{name} / {session}");
    }

    #[test]
    fn restores_session_context_with_the_correct_model_label() {
        let mut app = App {
            name: "Ada".into(),
            session: "old".into(),
            model: "old-model".into(),
            statusline: "{name} · {model} · {session}".into(),
            model_available: false,
            ..App::default()
        };

        app.transcript_scroll.set_extent(30, 10);
        app.transcript_scroll.scroll_up(12);

        app.restore_session(super::SessionView {
            id: "fresh".into(),
            model: crate::DEFAULT_MODEL.into(),
            workspace: Some("/work".into()),
            messages: Vec::new(),
        });

        assert_eq!(app.session, "fresh");
        assert_eq!(app.model, "unset");
        assert_eq!(app.workspace, "/work");
        assert!(app.transcript_scroll.follow_end);
        assert_eq!(render_statusline(&app), "Ada · unset · fresh");

        app.model_available = true;
        app.restore_session(super::SessionView {
            id: "configured".into(),
            model: "provider/model".into(),
            workspace: None,
            messages: Vec::new(),
        });

        assert_eq!(app.model, "provider/model");
        assert!(app.workspace.is_empty());
        assert_eq!(render_statusline(&app), "Ada · provider/model · configured");
    }

    #[test]
    fn separates_live_replies_once_after_session_restore_and_on_empty_session() {
        let mut app = App { name: "Crabbot".into(), ..App::default() };

        app.push_bot("Created session temp.\n".into());

        assert_eq!(app.transcript, "Crabbot\nCreated session temp.\n");

        app.restore_session(super::SessionView {
            id: "work".into(),
            model: "model".into(),
            workspace: None,
            messages: vec![crabbot_core::types::Message {
                id: "old-reply".into(),
                session: "work".into(),
                role: crabbot_core::types::Role::Assistant,
                sender: None,
                content: vec![crabbot_core::types::Content::Text { text: "Previous reply".into() }],
            }],
        });

        app.push_bot("Using session work.\n".into());

        assert_eq!(app.transcript, "Crabbot\nPrevious reply\n\nCrabbot\nUsing session work.\n");
    }

    #[test]
    fn renders_a_bordered_conversation_and_message_box() {
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

        assert!(rendered.contains("Conversation"));
        assert!(!rendered.contains("Agent"));
        assert!(rendered.contains("Welcome"));
        assert!(rendered.contains("Message"));
        assert!(rendered.contains("next"));
        assert!(
            rendered
                .contains(&format!("Crabbot v{} · /help for commands", env!("CARGO_PKG_VERSION")))
        );

        assert!(!rendered.contains("Ready · /help for commands"));

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
            serde_json::json!({"id": "work", "model": "model-work"}),
        )
        .await
        .unwrap();

        let mut app = App { session: "work".into(), model_available: false, ..App::default() };
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
        app.capture_interaction_output("\nError: Session was not found.");
        let saved = app.finish_interactions().pop().unwrap();
        super::persist_interaction(&home, saved, crate::local_aware).await.unwrap();

        let session = super::load_session(&home, "work", crate::local_aware).await.unwrap();
        let transcript = super::render_messages(&session.messages, "Crabbot");

        assert_eq!(session.messages.len(), 6);
        assert!(transcript.contains("You\n/status"));
        assert!(transcript.contains("Crabbot\nBackground runtime: stopped."));
        assert!(transcript.contains("You\nhello without a model"));
        assert!(transcript.contains("Crabbot\nNo intelligence plugin is installed."));
        assert!(transcript.contains("You\n/session missing"));
        assert!(transcript.contains("Crabbot\nError: Session was not found."));

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
        assert!(app.transcript.contains("You\n/help"));
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
            statusline: DEFAULT_STATUSLINE.into(),
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

        app.statusline = validate_statusline("{name} / {session}").unwrap();
        save_preferences(&app.preferences_path, &app.statusline, app.typewriter).unwrap();
        let saved = crabbot_file::load(&app.preferences_path, 16 * 1024).unwrap().unwrap();
        let preferences: TuiPreferences =
            toml::from_str(&String::from_utf8(saved).unwrap()).unwrap();

        assert_eq!(preferences.statusline, "{name} / {session}");
        assert_eq!(render_statusline(&app), "Ada / work");
        assert_eq!(wrapped_rows("one\ntwo", 80), 2);
        assert!(validate_statusline("{unknown}").is_err());

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

        assert_eq!(app.statusline, DEFAULT_STATUSLINE);
        assert!(app.statusline_reset);
        save_preferences(&app.preferences_path, "", app.typewriter).unwrap();
        let saved = crabbot_file::load(&app.preferences_path, 16 * 1024).unwrap().unwrap();

        let preferences: TuiPreferences =
            toml::from_str(&String::from_utf8(saved).unwrap()).unwrap();

        assert!(preferences.statusline.is_empty());
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
            serde_json::json!({"id": "work", "model": "model-work"}),
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
                    "id": "work",
                    "message": {
                        "id": format!("message-{index}"),
                        "session": "work",
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
                session: "main".into(),
            },
            crate::local_aware,
            Some(session_tx),
            None,
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
            session: "main".into(),
            model: "model-main".into(),
            model_available: true,
            statusline: "{session}".into(),
            ..App::default()
        };

        app.name = "Crabbot".into();
        app.restore_session(session_rx.recv().await.unwrap());

        assert_eq!(app.session, "work");
        assert_eq!(app.model, "model-work");
        assert!(app.transcript.contains("You\nsaved history message 0"));
        assert!(app.transcript.contains("Crabbot\nsaved history message 19"));
        assert!(super::wrap_transcript(&app.transcript, "Crabbot", 60).len() > 10);
        assert!(output.contains("Using session work."));
        assert!(output.contains("Session not found"));
        assert!(!app.transcript.contains("main"));
        assert_eq!(render_statusline(&app), "work");

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
            messages: Vec::new(),
        });

        assert_eq!(app.model, "unset");
        assert!(app.transcript.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn validates_statusline_bounds_and_input_cursor_wrapping() {
        assert!(validate_statusline("").is_err());
        assert!(validate_statusline("line\nnext").is_err());
        assert!(validate_statusline("{name").is_err());
        assert!(validate_statusline(&"x".repeat(121)).is_err());
        assert_eq!(input_cursor_position(&['a', 'b', 'c', 'd'], 4, 6), (0, 1));
        assert_eq!(input_cursor_position(&['界'], 1, 8), (2, 0));
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
