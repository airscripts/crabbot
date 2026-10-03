use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Stdio,
    thread,
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

use crabbot_core::types::{ContextUsage, Message, Role};
use crabbot_file::{load as load_file, private as private_file, save as save_file};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const LIMIT: usize = 100;
const SEEN_LIMIT: usize = 10_000;
const SEEN_KEY_LIMIT: usize = 4 * 1024;
const BYTE_LIMIT: u64 = 32 * 1024 * 1024;
pub const TUI_RESERVATION_TTL: u64 = crabbot_core::session::RESERVATION_TTL_SECONDS;

pub fn compact_messages(messages: &mut Vec<Message>) {
    while messages.len() > crabbot_core::session::MESSAGE_HISTORY_LIMIT {
        if !remove_oldest_message_group(messages, None) {
            break;
        }
    }
}

pub fn remove_oldest_message_group(messages: &mut Vec<Message>, protected: Option<usize>) -> bool {
    let mut index = 0;

    while index < messages.len() {
        if messages[index].role == Role::System {
            index += 1;
            continue;
        }

        let (start, end) = message_group(messages, index);

        if protected.is_none_or(|protected| protected < start || protected > end) {
            messages.drain(start..=end);
            return true;
        }

        index = end + 1;
    }

    false
}

fn message_group(messages: &[Message], index: usize) -> (usize, usize) {
    let mut start = index;

    if messages[index].role == Role::Tool {
        while start > 0 && messages[start - 1].role == Role::Tool {
            start -= 1;
        }

        if start > 0 && messages[start - 1].role == Role::Assistant {
            start -= 1;
        }
    }

    let mut end = start;

    if messages[start].role == Role::Assistant {
        while messages.get(end + 1).is_some_and(|message| message.role == Role::Tool) {
            end += 1;
        }
    }

    (start, end)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Session {
    pub id: String,
    pub model: String,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub chat: Option<String>,
    #[serde(default)]
    pub thread: Option<String>,
    #[serde(default = "direct")]
    pub private: bool,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub compacted_through: Option<String>,
    #[serde(default)]
    pub compacted_context: Option<Vec<Message>>,
    #[serde(default)]
    pub context_usage: Option<ContextUsage>,
    #[serde(default)]
    pub queued: Vec<Message>,
    #[serde(default)]
    pub queue_roles: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub inflight: Option<Message>,
    #[serde(default)]
    pub reservation_until: Option<u64>,
    #[serde(default)]
    pub reservation_owner: Option<String>,
    #[serde(default)]
    pub inflight_roles: Vec<String>,
    #[serde(default)]
    pub stream_delivery: Option<String>,
    #[serde(default = "unsafe_phase")]
    pub phase: String,
    pub status: String,
    pub created: u64,
    pub updated: u64,
}

impl Session {
    pub fn has_live_tui_reservation(&self) -> bool {
        self.status == "working"
            && self.inflight.is_none()
            && !reservation_expired(self.reservation_until, self.updated, now())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Delivery {
    pub id: String,
    pub channel: String,
    pub chat: String,
    #[serde(default = "direct")]
    pub private: bool,
    #[serde(default)]
    pub thread: Option<String>,
    pub text: String,
    pub attempts: u32,
    pub created: u64,
    #[serde(default)]
    pub status: DeliveryStatus,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub updated: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    #[default]
    Pending,
    Sending,
    Streaming,
    Uncertain,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Store {
    pub sessions: BTreeMap<String, Session>,
    #[serde(default)]
    deferred: BTreeMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    deferred_acknowledged: BTreeSet<String>,
    #[serde(default)]
    pub outbox: Vec<Delivery>,
    #[serde(default)]
    pub dead: Vec<Delivery>,
    #[serde(default)]
    pub offsets: BTreeMap<String, i64>,
    #[serde(default)]
    pub seen: BTreeMap<String, u64>,
    #[serde(skip)]
    path: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SessionIndex {
    version: u8,
    sessions: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct ClientState {
    deferred: BTreeMap<String, Vec<serde_json::Value>>,
    deferred_acknowledged: BTreeSet<String>,
    offsets: BTreeMap<String, i64>,
    seen: BTreeMap<String, u64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct DeliveryIndex {
    outbox: Vec<String>,
    dead: Vec<String>,
}

impl Store {
    pub fn load(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();

        if session_catalog(&path) {
            return load_catalog(&path);
        }

        let bytes = match load_file(&path, BYTE_LIMIT) {
            Ok(bytes) => bytes,

            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                private_file(&path)?;
                load_file(&path, BYTE_LIMIT)?
            }

            Err(error) => return Err(error),
        };

        let Some(bytes) = bytes else {
            return Ok(Self { path: Some(path), ..Self::default() });
        };

        let text = String::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

        let document: serde_json::Value =
            serde_json::from_str(&text).map_err(std::io::Error::other)?;

        let mut store: Self =
            serde_json::from_value(document.clone()).map_err(std::io::Error::other)?;

        let mut changed = migrate_delivery_routes(
            &mut store.outbox,
            document.get("outbox"),
            &store.sessions,
            true,
        )?;

        changed |=
            migrate_delivery_routes(&mut store.dead, document.get("dead"), &store.sessions, false)?;

        for (id, session) in &mut store.sessions {
            if !valid(id) || session.id != *id {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Session state contains an invalid ID.",
                ));
            }

            let message_count = session.messages.len();
            compact_messages(&mut session.messages);
            changed |= session.messages.len() != message_count;

            if session.inflight.is_some() && session.queued.len() > LIMIT {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Session recovery queue exceeds the temporary lease bound.",
                ));
            }

            if session.queued.len() > LIMIT && session.inflight.is_none() {
                session.queued.drain(..session.queued.len() - LIMIT);
            }

            session
                .queue_roles
                .retain(|id, _| session.queued.iter().any(|message| &message.id == id));

            let tui_reservation = session.status == "working"
                && session.inflight.is_none()
                && session.reservation_until.is_some();

            if !tui_reservation && (session.status == "working" || session.inflight.is_some()) {
                let roles = std::mem::take(&mut session.inflight_roles);
                session.stream_delivery = None;

                if let Some(message) = session.inflight.take()
                    && session.status != "cancelled"
                    && session.phase == "safe"
                {
                    if !roles.is_empty() {
                        session.queue_roles.insert(message.id.clone(), roles);
                    }

                    session.queued.insert(0, message);
                }

                session.phase = safe();

                session.status = "interrupted".into();
                session.reservation_until = None;
                session.reservation_owner = None;
            }
        }

        for delivery in &mut store.outbox {
            if delivery.updated == 0 {
                delivery.updated = delivery.created;
                changed = true;
            }

            if matches!(delivery.status, DeliveryStatus::Sending | DeliveryStatus::Streaming) {
                delivery.status = DeliveryStatus::Uncertain;
                delivery.last_error = Some("The daemon stopped during channel delivery.".into());
                delivery.updated = now();
                changed = true;
            }
        }

        if store.sessions.len() > LIMIT {
            let excess = store.sessions.len() - LIMIT;
            let ids = {
                let mut values = store.sessions.values().collect::<Vec<_>>();
                values.sort_by_key(|session| session.updated);
                values
                    .into_iter()
                    .take(excess)
                    .map(|session| session.id.clone())
                    .collect::<Vec<_>>()
            };

            for id in ids {
                store.sessions.remove(&id);
            }
        }

        if store.outbox.len() > LIMIT {
            store.outbox.drain(..store.outbox.len() - LIMIT);
        }

        if store.dead.len() > LIMIT {
            store.dead.drain(..store.dead.len() - LIMIT);
        }

        if store.offsets.len() > LIMIT {
            let excess = store.offsets.len() - LIMIT;
            let keys = store.offsets.keys().take(excess).cloned().collect::<Vec<_>>();

            for key in keys {
                store.offsets.remove(&key);
            }
        }

        trim_seen(&mut store.seen, None);
        store.path = Some(path);

        if changed {
            store.save()?;
        }

        Ok(store)
    }

    pub fn save(&self) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };

        if session_catalog(path) {
            return save_catalog(path, self);
        }

        let bytes = serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?;

        if bytes.len() as u64 > BYTE_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "Session state exceeds the size limit.",
            ));
        }

        save_file(path, bytes)
    }

    pub fn create(
        &mut self,
        id: impl Into<String>,
        model: impl Into<String>,
    ) -> std::io::Result<()> {
        let id = id.into();
        let model = model.into();

        if !valid(&id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Session ID must contain lowercase letters, digits, or hyphens.",
            ));
        }

        if model.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Model must not be empty.",
            ));
        }

        if self.sessions.contains_key(&id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Session already exists.",
            ));
        }

        if self.sessions.len() >= LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session limit reached; remove an old session first.",
            ));
        }

        let now = now();
        self.change(|store| {
            store.sessions.insert(
                id.clone(),
                Session {
                    id,
                    model,
                    workspace: None,
                    archived: false,
                    channel: None,
                    chat: None,
                    thread: None,
                    private: true,
                    messages: Vec::new(),
                    compacted_through: None,
                    compacted_context: None,
                    context_usage: None,
                    queued: Vec::new(),
                    queue_roles: BTreeMap::new(),
                    inflight: None,
                    reservation_until: None,
                    reservation_owner: None,
                    inflight_roles: Vec::new(),
                    stream_delivery: None,
                    phase: safe(),
                    status: "idle".into(),
                    created: now,
                    updated: now,
                },
            );
        })
    }

    pub fn ensure(
        &mut self,
        id: impl Into<String>,
        model: impl Into<String>,
    ) -> std::io::Result<()> {
        let id = id.into();

        if !self.sessions.contains_key(&id) {
            self.create(id, model)?;
        }

        Ok(())
    }

    pub fn rename_session(&mut self, source: &str, target: &str) -> std::io::Result<()> {
        if !valid(source) || !valid(target) || source == target {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Session ID is invalid.",
            ));
        }

        if self.sessions.contains_key(target) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Session already exists.",
            ));
        }

        let mut session = self.sessions.get(source).cloned().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Session was not found.")
        })?;

        if session.channel.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "A channel-backed session cannot be renamed.",
            ));
        }

        if session.status == "working" || session.inflight.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "A working session cannot be renamed.",
            ));
        }

        session.id = target.to_owned();
        session.updated = now();

        for message in session.messages.iter_mut().chain(session.queued.iter_mut()) {
            message.session = target.to_owned();
        }

        if let Some(message) = &mut session.inflight {
            message.session = target.to_owned();
        }

        self.change(|store| {
            store.sessions.remove(source);
            store.sessions.insert(target.to_owned(), session);
        })
    }

    pub fn archive_session(&mut self, id: &str, archived: bool) -> std::io::Result<()> {
        let session = self.sessions.get(id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Session was not found.")
        })?;

        if session.status == "working" || session.inflight.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "A working session cannot be archived.",
            ));
        }

        self.change(|store| {
            let session = store.sessions.get_mut(id).expect("session was checked above");
            session.archived = archived;
            session.updated = now();
        })
    }

    pub fn route(
        &mut self,
        id: &str,
        channel: &str,
        chat: &str,
        thread: Option<&str>,
        private: bool,
    ) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.channel = Some(channel.into());
                session.chat = Some(chat.into());
                session.thread = thread.map(str::to_owned);
                session.private = private;
            }
        })
    }

    pub fn push(&mut self, id: &str, message: Message) -> std::io::Result<()> {
        self.push_with_status(id, message, false, false)
    }

    pub fn append(&mut self, id: &str, message: Message) -> std::io::Result<()> {
        self.push_with_status(id, message, true, false)
    }

    pub fn append_reserved(
        &mut self,
        id: &str,
        owner: &str,
        message: Message,
    ) -> std::io::Result<()> {
        let session = self.sessions.get(id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Session was not found.")
        })?;

        if session.inflight.is_some()
            || session.status != "working"
            || session.reservation_owner.as_deref() != Some(owner)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session reservation is unavailable.",
            ));
        }

        self.push_with_status(id, message, true, true)
    }

    pub fn set_compacted_context_reserved(
        &mut self,
        id: &str,
        owner: &str,
        context: Vec<Message>,
    ) -> std::io::Result<()> {
        let session = self.sessions.get(id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Session was not found.")
        })?;

        if session.inflight.is_some()
            || session.status != "working"
            || session.reservation_owner.as_deref() != Some(owner)
            || context.iter().any(|message| message.session != id)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session reservation is unavailable.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.compacted_through =
                    session.messages.last().map(|message| message.id.clone());
                session.compacted_context = Some(context);
                session.context_usage = None;
                session.updated = now();
            }
        })
    }

    fn push_with_status(
        &mut self,
        id: &str,
        message: Message,
        resume_cancelled: bool,
        preserve_working: bool,
    ) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            session.messages.push(message);

            compact_messages(&mut session.messages);

            session.updated = now();

            if preserve_working {
                session.status = "working".into();

                if session.reservation_until.is_some() {
                    session.reservation_until = Some(now().saturating_add(TUI_RESERVATION_TTL));
                }
            } else if session.status != "cancelled" || resume_cancelled {
                session.status = "idle".into();
                session.reservation_until = None;
                session.reservation_owner = None;
            }
        })
    }

    pub fn clear_history(&mut self, id: &str) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.messages.clear();
                session.compacted_through = None;
                session.compacted_context = None;
                session.context_usage = None;
                session.updated = now();
            }
        })
    }

    #[cfg(test)]
    pub fn queue(&mut self, id: &str, message: Message) -> std::io::Result<()> {
        self.queue_with_roles(id, message, Vec::new())
    }

    pub fn queue_with_roles(
        &mut self,
        id: &str,
        message: Message,
        roles: Vec<String>,
    ) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        if self
            .sessions
            .get(id)
            .is_some_and(|session| session.queued.iter().any(|queued| queued.id == message.id))
        {
            return Ok(());
        }

        if self.sessions.get(id).is_some_and(|session| {
            session.queued.len() + usize::from(session.inflight.is_some()) >= LIMIT
        }) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session queue is full; no message was discarded.",
            ));
        }

        self.change(|store| {
            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            if roles.is_empty() {
                session.queue_roles.remove(&message.id);
            } else {
                session.queue_roles.insert(message.id.clone(), roles);
            }

            session.queued.push(message);

            if session.status == "cancelled" {
                session.status = "idle".into();
                session.reservation_until = None;
                session.reservation_owner = None;
            }

            session.updated = now();
        })
    }

    #[cfg(test)]
    pub fn take(&mut self, id: &str) -> std::io::Result<Option<Message>> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        let mut message = None;

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id)
                && !session.queued.is_empty()
            {
                message = Some(session.queued.remove(0));

                if let Some(message) = &message {
                    session.queue_roles.remove(&message.id);
                }

                session.updated = now();
            }
        })?;

        Ok(message)
    }

    #[cfg(test)]
    pub fn begin(&mut self, id: &str, message: Message) -> std::io::Result<()> {
        self.begin_with_roles(id, message, Vec::new())
    }

    pub fn begin_with_roles(
        &mut self,
        id: &str,
        message: Message,
        roles: Vec<String>,
    ) -> std::io::Result<()> {
        self.begin_with_roles_clearing_deferred(id, message, roles, None)
    }

    pub fn begin_queued_with_roles(
        &mut self,
        id: &str,
        message: Message,
        roles: Vec<String>,
        channel: &str,
    ) -> std::io::Result<()> {
        self.begin_with_roles_clearing_deferred(id, message, roles, Some(channel))
    }

    fn begin_with_roles_clearing_deferred(
        &mut self,
        id: &str,
        message: Message,
        roles: Vec<String>,
        channel: Option<&str>,
    ) -> std::io::Result<()> {
        let Some(session) = self.sessions.get(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        if session.has_live_tui_reservation()
            || session.inflight.is_some()
            || (session.status == "working"
                && !session.queued.iter().any(|item| item.id == message.id))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session is already working.",
            ));
        }

        self.change(|store| {
            if let Some(channel) = channel
                && let Some(events) = store.deferred.get_mut(channel)
            {
                events.retain(|event| {
                    event["id"].as_i64().is_none_or(|value| value.to_string() != message.id)
                        && event["id"].as_str() != Some(&message.id)
                });

                if events.is_empty() {
                    store.deferred.remove(channel);
                }
            }

            if let Some(channel) = channel {
                store.deferred_acknowledged.remove(&seen_key(channel, &message.id));
            }

            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            if let Some(index) = session.queued.iter().position(|item| item.id == message.id) {
                session.queued.remove(index);
            }

            let queued_roles = session.queue_roles.remove(&message.id).unwrap_or_default();

            if !session.messages.iter().any(|item| item.id == message.id) {
                session.messages.push(message.clone());

                compact_messages(&mut session.messages);
            }

            session.inflight = Some(message);
            session.inflight_roles = if roles.is_empty() { queued_roles } else { roles };

            session.stream_delivery = None;
            session.phase = safe();
            session.status = "working".into();
            session.reservation_until = None;
            session.reservation_owner = None;
            session.updated = now();
        })
    }

    pub fn clear(&mut self, id: &str, status: &str) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            let delivery_id = session.stream_delivery.take();
            session.inflight = None;
            session.inflight_roles.clear();

            if status == "cancelled" {
                session.queued.clear();
                session.queue_roles.clear();
            }

            session.phase = safe();
            session.status = status.into();
            session.reservation_until = None;
            session.reservation_owner = None;
            session.updated = now();

            if let Some(delivery_id) = delivery_id
                && let Some(delivery) =
                    store.outbox.iter_mut().find(|delivery| delivery.id == delivery_id)
                && matches!(delivery.status, DeliveryStatus::Sending | DeliveryStatus::Streaming)
            {
                delivery.status = DeliveryStatus::Uncertain;
                delivery.last_error = Some("The turn ended before streaming completed.".into());
                delivery.updated = now();
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reply(
        &mut self,
        session: &str,
        message: Message,
        id: impl Into<String>,
        channel: impl Into<String>,
        chat: impl Into<String>,
        thread: Option<String>,
        text: impl Into<String>,
    ) -> std::io::Result<()> {
        if !self.sessions.contains_key(session) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        let id = id.into();

        let channel = channel.into();
        let chat = chat.into();
        let text = text.into();
        let private = self.sessions.get(session).is_some_and(|session| session.private);
        let uncertain_delivery = self.outbox.iter().any(|delivery| {
            delivery.id == id
                && matches!(delivery.status, DeliveryStatus::Sending | DeliveryStatus::Uncertain)
        });

        let completing_stream = self
            .sessions
            .get(session)
            .and_then(|session| session.stream_delivery.as_deref())
            .is_some_and(|stream_id| stream_id == id);

        let message_id = completing_stream
            .then(|| self.outbox.iter().find(|delivery| delivery.id == id))
            .flatten()
            .and_then(|delivery| delivery.message_id.clone());

        if !self.outbox.iter().any(|delivery| delivery.id == id) && self.outbox.len() >= LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Delivery outbox is full; no message was discarded.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(session) {
                session.messages.push(message);

                compact_messages(&mut session.messages);

                session.updated = now();
                session.status = "idle".into();
                session.reservation_until = None;
                session.reservation_owner = None;
                session.inflight = None;
                session.inflight_roles.clear();
                session.stream_delivery = None;
                session.phase = safe();
            }

            if let Some(delivery) = store.outbox.iter_mut().find(|delivery| delivery.id == id) {
                if completing_stream {
                    delivery.channel = channel;
                    delivery.chat = chat;
                    delivery.private = private;
                    delivery.thread = thread;
                    delivery.text = text;
                    delivery.status = if uncertain_delivery {
                        DeliveryStatus::Uncertain
                    } else {
                        DeliveryStatus::Pending
                    };

                    if !uncertain_delivery {
                        delivery.last_error = None;
                    }

                    delivery.message_id = message_id;
                    delivery.updated = now();
                }
            } else {
                store.outbox.push(Delivery {
                    id,
                    channel,
                    chat,
                    private,
                    thread,
                    text,
                    attempts: 0,
                    created: now(),
                    status: DeliveryStatus::Pending,
                    last_error: None,
                    message_id: None,
                    updated: now(),
                });
            }
        })
    }

    pub fn start_stream(
        &mut self,
        session: &str,
        id: impl Into<String>,
        channel: impl Into<String>,
        chat: impl Into<String>,
        thread: Option<String>,
        text: impl Into<String>,
    ) -> std::io::Result<()> {
        let id = id.into();

        let channel = channel.into();
        let chat = chat.into();
        let text = text.into();
        let Some(current) = self.sessions.get(session) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        let private = current.private;

        if current.inflight.is_none() || current.stream_delivery.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session cannot start a streamed delivery.",
            ));
        }

        if self.outbox.iter().any(|delivery| delivery.id == id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Streamed delivery already exists.",
            ));
        }

        if self.outbox.len() >= LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Delivery outbox is full; no message was discarded.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(session) {
                session.phase = "unsafe".into();
                session.stream_delivery = Some(id.clone());
                session.updated = now();
            }

            store.outbox.push(Delivery {
                id,
                channel,
                chat,
                private,
                thread,
                text,
                attempts: 0,
                created: now(),
                status: DeliveryStatus::Sending,
                last_error: None,
                message_id: None,
                updated: now(),
            });
        })
    }

    pub fn stream_sending(&mut self, id: &str, text: impl Into<String>) -> std::io::Result<()> {
        let text = text.into();

        self.update_delivery(id, |delivery| {
            delivery.text = text;
            delivery.status = DeliveryStatus::Sending;
            delivery.last_error = None;
            delivery.updated = now();
        })
    }

    pub fn stream_started(
        &mut self,
        id: &str,
        message_id: impl Into<String>,
    ) -> std::io::Result<()> {
        let message_id = message_id.into();

        self.update_delivery(id, |delivery| {
            delivery.message_id = Some(message_id);
            delivery.status = DeliveryStatus::Streaming;
            delivery.last_error = None;
            delivery.updated = now();
        })
    }

    pub fn stream_updated(&mut self, id: &str) -> std::io::Result<()> {
        self.update_delivery(id, |delivery| {
            delivery.status = DeliveryStatus::Streaming;
            delivery.last_error = None;
            delivery.updated = now();
        })
    }

    pub fn set_status(&mut self, id: &str, status: &str) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            if session.status == "cancelled" && session.inflight.is_some() && status != "cancelled"
            {
                return;
            }

            if status == "cancelled" {
                session.queued.clear();
                session.queue_roles.clear();
            }

            session.status = status.into();

            if status != "working" {
                session.reservation_until = None;
                session.reservation_owner = None;
            }

            session.updated = now();
        })
    }

    pub fn reserve(&mut self, id: &str, owner: String) -> std::io::Result<()> {
        let Some(session) = self.sessions.get(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        if session.inflight.is_some() || session.status == "working" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session is already working.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.status = "working".into();
                session.reservation_until = Some(now().saturating_add(TUI_RESERVATION_TTL));
                session.reservation_owner = Some(owner);
                session.updated = now();
            }
        })
    }

    pub fn recover_reservations(&mut self) -> std::io::Result<()> {
        let current = now();

        let expired = self.sessions.values().any(|session| {
            session.status == "working"
                && session.inflight.is_none()
                && reservation_expired(session.reservation_until, session.updated, current)
        });

        if !expired {
            return Ok(());
        }

        self.change(|store| {
            for session in store.sessions.values_mut() {
                if session.status == "working"
                    && session.inflight.is_none()
                    && reservation_expired(session.reservation_until, session.updated, current)
                {
                    session.status = "idle".into();
                    session.reservation_until = None;
                    session.reservation_owner = None;
                    session.updated = current;
                }
            }
        })
    }

    pub fn renew_reservation(&mut self, id: &str, owner: &str) -> std::io::Result<()> {
        let Some(session) = self.sessions.get(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        if session.status != "working"
            || session.inflight.is_some()
            || session.reservation_until.is_none()
            || session.reservation_owner.as_deref() != Some(owner)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session reservation is unavailable.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.reservation_until = Some(now().saturating_add(TUI_RESERVATION_TTL));
            }
        })
    }

    pub fn set_model(&mut self, id: &str, model: &str) -> std::io::Result<()> {
        if model.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Model must not be empty.",
            ));
        }

        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            let Some(session) = store.sessions.get_mut(id) else {
                return;
            };

            session.model = model.into();
            session.context_usage = None;
            session.updated = now();
        })
    }

    pub fn set_context_usage(&mut self, id: &str, usage: ContextUsage) -> std::io::Result<()> {
        if usage.limit == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Context usage is invalid.",
            ));
        }

        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.context_usage = Some(usage);
                session.updated = now();
            }
        })
    }

    pub fn set_workspace(&mut self, id: &str, workspace: Option<&str>) -> std::io::Result<()> {
        if workspace.is_some_and(|workspace| workspace.trim().is_empty() || workspace.len() > 4096)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Workspace path is invalid.",
            ));
        }

        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        if self
            .sessions
            .get(id)
            .is_some_and(|session| session.inflight.is_some() || session.status == "working")
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "A working session cannot change its workspace.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.workspace = workspace.map(str::to_owned);
                session.updated = now();
            }
        })
    }

    pub fn fork(&mut self, source: &str, target: &str) -> std::io::Result<()> {
        if !valid(target) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Session ID must contain lowercase letters, digits, or hyphens.",
            ));
        }

        if self.sessions.contains_key(target) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Session already exists.",
            ));
        }

        let source = self.sessions.get(source).cloned().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Source session was not found.")
        })?;

        if self.sessions.len() >= LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session capacity is full.",
            ));
        }

        let now = now();
        let target = target.to_owned();
        self.change(|store| {
            let messages = source
                .messages
                .into_iter()
                .map(|mut message| {
                    message.session = target.clone();
                    message
                })
                .collect();

            let compacted_context = source.compacted_context.map(|messages| {
                messages
                    .into_iter()
                    .map(|mut message| {
                        message.session = target.clone();
                        message
                    })
                    .collect()
            });

            store.sessions.insert(
                target.clone(),
                Session {
                    id: target,
                    model: source.model,
                    workspace: source.workspace,
                    archived: false,
                    channel: source.channel,
                    chat: source.chat,
                    thread: source.thread,
                    private: source.private,
                    messages,
                    compacted_through: source.compacted_through,
                    compacted_context,
                    context_usage: source.context_usage,
                    queued: Vec::new(),
                    queue_roles: BTreeMap::new(),
                    inflight: None,
                    reservation_until: None,
                    reservation_owner: None,
                    inflight_roles: Vec::new(),
                    stream_delivery: None,
                    phase: safe(),
                    status: "idle".into(),
                    created: now,
                    updated: now,
                },
            );
        })
    }

    pub fn cancel(&mut self, id: &str) -> std::io::Result<()> {
        let active = self.sessions.get(id).is_some_and(|session| session.inflight.is_some());

        if active { self.set_status(id, "cancelled") } else { self.clear(id, "cancelled") }
    }

    pub fn phase(&mut self, id: &str, phase: &str) -> std::io::Result<()> {
        if !matches!(phase, "safe" | "unsafe") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Lease phase is invalid.",
            ));
        }

        let Some(session) = self.sessions.get(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        if session.inflight.is_none() && !session.has_live_tui_reservation() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Session has no active turn lease.",
            ));
        }

        self.change(|store| {
            if let Some(session) = store.sessions.get_mut(id) {
                session.phase = phase.into();
                session.updated = now();
            }
        })
    }

    pub fn remove(&mut self, id: &str) -> std::io::Result<()> {
        if !self.sessions.contains_key(id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        }

        if self
            .sessions
            .get(id)
            .is_some_and(|session| session.status == "working" || session.inflight.is_some())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session is working; cancel it and wait before deleting it.",
            ));
        }

        self.change(|store| {
            store.sessions.remove(id);
        })
    }

    pub fn remove_deep(&mut self, id: &str) -> std::io::Result<()> {
        self.remove(id)?;

        let Some(path) = self.path.as_deref().filter(|path| session_catalog(path)) else {
            return Ok(());
        };

        let records = path.parent().expect("catalog has parent").join("records");
        remove_session_records(&records, id)
    }

    pub fn purge(
        &mut self,
        sessions: bool,
        deliveries: bool,
    ) -> std::io::Result<(Vec<String>, usize)> {
        if sessions
            && self
                .sessions
                .values()
                .any(|session| session.status == "working" || session.inflight.is_some())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "A session is working; cancel it and wait before purging sessions.",
            ));
        }

        let session_ids =
            if sessions { self.sessions.keys().cloned().collect() } else { Vec::new() };

        let delivery_count = if deliveries { self.outbox.len() + self.dead.len() } else { 0 };

        if session_ids.is_empty() && delivery_count == 0 {
            return Ok((session_ids, delivery_count));
        }

        self.change(|store| {
            if sessions {
                store.sessions.clear();
            }

            if deliveries {
                store.outbox.clear();
                store.dead.clear();
            }
        })?;

        Ok((session_ids, delivery_count))
    }

    pub fn ack(&mut self, id: &str) -> std::io::Result<()> {
        if let Some(index) = self.outbox.iter().position(|delivery| delivery.id == id) {
            self.change(|store| {
                store.outbox.remove(index);
            })?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn retry(&mut self, id: &str) -> std::io::Result<()> {
        if let Some(index) = self.outbox.iter().position(|delivery| delivery.id == id) {
            self.change(|store| {
                store.outbox[index].attempts = store.outbox[index].attempts.saturating_add(1);
                store.outbox[index].status = DeliveryStatus::Pending;
                store.outbox[index].updated = now();
            })?;
        }

        Ok(())
    }

    pub fn sending(&mut self, id: &str) -> std::io::Result<()> {
        self.update_delivery(id, |delivery| {
            delivery.status = DeliveryStatus::Sending;
            delivery.last_error = None;
            delivery.updated = now();
        })
    }

    pub fn uncertain(&mut self, id: &str, error: impl Into<String>) -> std::io::Result<()> {
        let error = error.into();

        self.update_delivery(id, |delivery| {
            delivery.status = DeliveryStatus::Uncertain;
            delivery.last_error = Some(error);
            delivery.updated = now();
        })
    }

    pub fn retry_delivery(&mut self, id: &str) -> std::io::Result<()> {
        let Some(delivery) = self.outbox.iter().find(|delivery| delivery.id == id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Delivery was not found.",
            ));
        };

        if delivery.status != DeliveryStatus::Uncertain {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Only uncertain deliveries can be retried.",
            ));
        }

        self.update_delivery(id, |delivery| {
            delivery.status = DeliveryStatus::Pending;
            delivery.last_error = None;
            delivery.updated = now();
        })
    }

    pub fn drop_delivery(&mut self, id: &str) -> std::io::Result<()> {
        if !self.outbox.iter().any(|delivery| delivery.id == id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Delivery was not found.",
            ));
        }

        self.dead(id)
    }

    pub fn dead(&mut self, id: &str) -> std::io::Result<()> {
        if !self.outbox.iter().any(|delivery| delivery.id == id) {
            return Ok(());
        }

        self.change(|store| {
            if let Some(index) = store.outbox.iter().position(|item| item.id == id) {
                let delivery = store.outbox.remove(index);
                store.dead.push(delivery);

                if store.dead.len() > LIMIT {
                    store.dead.drain(..store.dead.len() - LIMIT);
                }
            }
        })
    }

    #[cfg(test)]
    pub fn attempts(&self, id: &str) -> Option<u32> {
        self.outbox.iter().find(|delivery| delivery.id == id).map(|delivery| delivery.attempts)
    }

    pub fn offset(&self, channel: &str) -> i64 {
        self.offsets.get(channel).copied().unwrap_or_default()
    }

    pub fn known(&self, channel: &str, id: &str) -> bool {
        self.seen.contains_key(&seen_key(channel, id))
    }

    pub fn deferred_event(&self, channel: &str) -> Option<serde_json::Value> {
        self.deferred.get(channel).and_then(|events| events.first()).cloned()
    }

    pub fn has_deferred_event(&self, channel: &str, id: &str) -> bool {
        self.deferred.get(channel).is_some_and(|events| {
            events.iter().any(|event| {
                event["id"].as_i64().is_some_and(|value| value.to_string() == id)
                    || event["id"].as_str() == Some(id)
            })
        })
    }

    pub fn deferred_acknowledged(&self, channel: &str, id: &str) -> bool {
        self.deferred_acknowledged.contains(&seen_key(channel, id))
    }

    pub fn mark_deferred_acknowledged(&mut self, channel: &str, id: &str) -> std::io::Result<()> {
        let key = seen_key(channel, id);

        if self.deferred_acknowledged.contains(&key) || !self.has_deferred_event(channel, id) {
            return Ok(());
        }

        self.change(|store| {
            store.deferred_acknowledged.insert(key);
        })
    }

    pub fn defer_event(&mut self, channel: &str, event: serde_json::Value) -> std::io::Result<()> {
        let already_deferred = self
            .deferred
            .get(channel)
            .is_some_and(|events| events.iter().any(|current| current["id"] == event["id"]));

        if already_deferred {
            return Ok(());
        }

        if self.deferred.get(channel).map_or(0, Vec::len) >= LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Deferred channel event limit reached.",
            ));
        }

        let mut candidate = serde_json::to_value(&*self).map_err(std::io::Error::other)?;
        let deferred = candidate
            .get_mut("deferred")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| std::io::Error::other("Session state has invalid deferred storage."))?;

        let events = deferred.entry(channel).or_insert_with(|| serde_json::json!([]));
        events
            .as_array_mut()
            .ok_or_else(|| std::io::Error::other("Session state has invalid deferred events."))?
            .push(event.clone());

        let bytes = serde_json::to_vec_pretty(&candidate).map_err(std::io::Error::other)?;

        if bytes.len() as u64 > BYTE_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Deferred channel event storage limit reached.",
            ));
        }

        self.change(|store| {
            let events = store.deferred.entry(channel.into()).or_default();
            events.push(event);
        })
    }

    pub fn commit(&mut self, channel: &str, id: &str, offset: Option<i64>) -> std::io::Result<()> {
        self.commit_event(channel, id, offset, false)
    }

    pub fn commit_retained(
        &mut self,
        channel: &str,
        id: &str,
        offset: Option<i64>,
    ) -> std::io::Result<()> {
        self.commit_event(channel, id, offset, true)
    }

    fn commit_event(
        &mut self,
        channel: &str,
        id: &str,
        offset: Option<i64>,
        retained: bool,
    ) -> std::io::Result<()> {
        let key = seen_key(channel, id);

        self.change(|store| {
            store.seen.insert(key.clone(), seen_now());

            if !retained && let Some(events) = store.deferred.get_mut(channel) {
                events.retain(|event| {
                    event["id"].as_i64().is_none_or(|value| value.to_string() != id)
                        && event["id"].as_str() != Some(id)
                });

                if events.is_empty() {
                    store.deferred.remove(channel);
                }
            }

            if !retained {
                store.deferred_acknowledged.remove(&key);
            }

            if let Some(offset) = offset {
                store
                    .offsets
                    .entry(channel.into())
                    .and_modify(|current| *current = (*current).max(offset))
                    .or_insert(offset);
            }

            trim_seen(&mut store.seen, Some(&key));
        })
    }

    fn change(&mut self, update: impl FnOnce(&mut Self)) -> std::io::Result<()> {
        let previous = self.sessions.clone();

        let outbox = self.outbox.clone();
        let dead = self.dead.clone();
        let offsets = self.offsets.clone();
        let seen = self.seen.clone();
        let deferred = self.deferred.clone();
        let deferred_acknowledged = self.deferred_acknowledged.clone();
        let path = self.path.clone();
        update(self);

        if let Err(error) = self.save() {
            self.sessions = previous;
            self.outbox = outbox;
            self.dead = dead;
            self.offsets = offsets;
            self.seen = seen;
            self.deferred = deferred;
            self.deferred_acknowledged = deferred_acknowledged;
            self.path = path;
            return Err(error);
        }

        Ok(())
    }

    fn update_delivery(
        &mut self,
        id: &str,
        update: impl FnOnce(&mut Delivery),
    ) -> std::io::Result<()> {
        if !self.outbox.iter().any(|delivery| delivery.id == id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Delivery was not found.",
            ));
        }

        self.change(|store| {
            if let Some(delivery) = store.outbox.iter_mut().find(|delivery| delivery.id == id) {
                update(delivery);
            }
        })
    }
}

pub fn valid(id: &str) -> bool {
    !id.is_empty()
        && id.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn trim_seen(seen: &mut BTreeMap<String, u64>, keep: Option<&str>) {
    if seen.len() <= SEEN_LIMIT {
        return;
    }

    let excess = seen.len() - SEEN_LIMIT;

    let mut entries = seen
        .iter()
        .filter(|(key, _)| Some(key.as_str()) != keep)
        .map(|(key, timestamp)| (key.clone(), *timestamp))
        .collect::<Vec<_>>();

    entries.sort_by(|(left_key, left_time), (right_key, right_time)| {
        left_time.cmp(right_time).then_with(|| left_key.cmp(right_key))
    });

    for (key, _) in entries.into_iter().take(excess) {
        seen.remove(&key);
    }
}

fn seen_key(channel: &str, id: &str) -> String {
    let key = format!("{channel}\0{id}");

    if key.len() <= SEEN_KEY_LIMIT {
        return key;
    }

    let mut hash = Sha256::new();
    hash.update(key.as_bytes());
    format!("#oversized:{:x}", hash.finalize())
}

pub fn remove_worktree(root: &Path, id: &str) -> std::io::Result<()> {
    if !valid(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Session ID is invalid.",
        ));
    }

    if !root.exists() {
        return Ok(());
    }

    let root = std::fs::canonicalize(root)?;
    let path = root.join(".crabbot/worktrees").join(id);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "The worktree path cannot be a symbolic link.",
        ));
    }

    let path = std::fs::canonicalize(path)?;

    if !path.starts_with(&root) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "The worktree path leaves the configured root.",
        ));
    }

    let mut command = super::git_command();
    command
        .args(["-C", &root.display().to_string(), "worktree", "remove"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = command.spawn()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }

        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git worktree removal exceeded the execution time limit.",
            ));
        }

        thread::sleep(Duration::from_millis(25));
    };

    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other("Git could not remove the session worktree."))
    }
}

pub fn rename_worktree(root: &Path, source: &str, target: &str) -> std::io::Result<()> {
    if !valid(source) || !valid(target) || source == target {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Session ID is invalid.",
        ));
    }

    if !root.exists() {
        return Ok(());
    }

    let root = std::fs::canonicalize(root)?;
    let worktrees = root.join(".crabbot/worktrees");
    let old_path = worktrees.join(source);
    let new_path = worktrees.join(target);

    let metadata = match std::fs::symlink_metadata(&old_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "The worktree path cannot be a symbolic link.",
        ));
    }

    match std::fs::symlink_metadata(&new_path) {
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "The target worktree already exists.",
            ));
        }

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let worktrees = std::fs::canonicalize(worktrees)?;
    let old_path = std::fs::canonicalize(old_path)?;

    if !worktrees.starts_with(&root) || !old_path.starts_with(&worktrees) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "The worktree path leaves the configured root.",
        ));
    }

    let mut command = super::git_command();
    command
        .args(["-C", &root.display().to_string(), "worktree", "move"])
        .arg(&old_path)
        .arg(&new_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = command.spawn()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }

        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();

            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git worktree rename exceeded the execution time limit.",
            ));
        }

        thread::sleep(Duration::from_millis(25));
    };

    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other("Git could not rename the session worktree."))
    }
}

fn direct() -> bool {
    true
}

fn migrate_delivery_routes(
    deliveries: &mut [Delivery],
    raw: Option<&serde_json::Value>,
    sessions: &BTreeMap<String, Session>,
    require_match: bool,
) -> std::io::Result<bool> {
    let Some(raw) = raw.and_then(serde_json::Value::as_array) else {
        return Ok(false);
    };

    let mut changed = false;

    for (index, delivery) in deliveries.iter_mut().enumerate() {
        let missing = raw
            .get(index)
            .and_then(serde_json::Value::as_object)
            .is_some_and(|value| !value.contains_key("private"));

        if !missing {
            continue;
        }

        let matching = sessions.values().filter(|session| {
            session.channel.as_deref() == Some(delivery.channel.as_str())
                && session.chat.as_deref() == Some(delivery.chat.as_str())
                && session.thread.as_deref() == delivery.thread.as_deref()
        });

        let mut matching = matching.peekable();

        match matching.next() {
            Some(session) if matching.peek().is_none() => {
                delivery.private = session.private;
                changed = true;
            }

            Some(_) if require_match => {
                delivery.status = DeliveryStatus::Uncertain;
                delivery.last_error = Some(
                    "The legacy delivery route could not be matched to exactly one session; verify the channel before retrying.".into(),
                );

                changed = true;
            }

            Some(_) => {}

            None if require_match => {
                delivery.status = DeliveryStatus::Uncertain;
                delivery.last_error = Some(
                    "The legacy delivery route could not be matched to exactly one session; verify the channel before retrying.".into(),
                );

                changed = true;
            }

            None => {}
        }
    }

    Ok(changed)
}

fn safe() -> String {
    "safe".into()
}

fn unsafe_phase() -> String {
    "unsafe".into()
}

fn session_catalog(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "index.json")
        && path.parent().and_then(Path::file_name).is_some_and(|name| name == "sessions")
}

fn load_catalog(path: &Path) -> std::io::Result<Store> {
    let Some(bytes) = load_file(path, BYTE_LIMIT)? else {
        let home =
            path.parent().and_then(Path::parent).and_then(Path::parent).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid session catalog path.",
                )
            })?;

        let legacy = home.join("sessions.json");
        let mut store = if legacy.exists() { Store::load(legacy)? } else { Store::default() };

        store.path = Some(path.to_path_buf());

        let offline = home.join("data/plugins/tui/sessions.json");

        if offline.exists() {
            let bytes = load_file(&offline, 8 * 1024 * 1024)?.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "TUI session source disappeared.",
                )
            })?;
            let value: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;

            if let Some(sessions) = value.get("sessions").and_then(serde_json::Value::as_object) {
                for (id, value) in sessions {
                    let imported_id = format!("tui-{id}");

                    if store.sessions.contains_key(&imported_id) {
                        continue;
                    }

                    if let Ok(mut session) = serde_json::from_value::<Session>(value.clone())
                        && session.id == *id
                        && valid(id)
                    {
                        session.id = imported_id.clone();

                        for message in &mut session.messages {
                            message.session = imported_id.clone();
                        }

                        for message in &mut session.queued {
                            message.session = imported_id.clone();
                        }

                        if let Some(message) = &mut session.inflight {
                            message.session = imported_id.clone();
                        }

                        store.sessions.insert(imported_id, session);
                    }
                }
            }
        }

        store.save()?;
        return Ok(store);
    };

    let index: SessionIndex = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;

    if index.version != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported session catalog version.",
        ));
    }

    let root = path.parent().expect("catalog has parent");
    let mut store = Store { path: Some(path.to_path_buf()), ..Store::default() };

    for (id, key) in index.sessions {
        if !valid(&id) || key.len() != 64 || key.bytes().any(|byte| !byte.is_ascii_hexdigit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Session catalog contains an invalid record key.",
            ));
        }

        let record_path = root.join("records").join(format!("{key}.json"));
        let record = load_file(&record_path, BYTE_LIMIT)?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Session record is missing.")
        })?;

        let session: Session = serde_json::from_slice(&record).map_err(std::io::Error::other)?;

        if session.id != id || session_record_key(&record) != key {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Session record ID does not match its catalog entry.",
            ));
        }

        store.sessions.insert(id, session);
    }

    if let Some(bytes) = load_file(root.join("state/clients/runtime.json"), BYTE_LIMIT)? {
        let client: ClientState = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        store.deferred = client.deferred;
        store.deferred_acknowledged = client.deferred_acknowledged;
        store.offsets = client.offsets;
        store.seen = client.seen;
    }

    if let Some(bytes) = load_file(root.join("state/deliveries/index.json"), BYTE_LIMIT)? {
        let index: DeliveryIndex = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;

        for (key, dead) in index
            .outbox
            .into_iter()
            .map(|key| (key, false))
            .chain(index.dead.into_iter().map(|key| (key, true)))
        {
            let record = load_file(
                root.join("state/deliveries/records").join(format!("{key}.json")),
                BYTE_LIMIT,
            )?
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Delivery record is missing.")
            })?;

            let delivery: Delivery =
                serde_json::from_slice(&record).map_err(std::io::Error::other)?;

            if session_record_key(&record) != key {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Delivery record key is invalid.",
                ));
            }

            if dead {
                store.dead.push(delivery);
            } else {
                store.outbox.push(delivery);
            }
        }
    }

    Ok(store)
}

fn save_catalog(path: &Path, store: &Store) -> std::io::Result<()> {
    let root = path.parent().expect("catalog has parent");
    let mut sessions = BTreeMap::new();

    for (id, session) in &store.sessions {
        if !valid(id) || session.id != *id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Session ID is invalid.",
            ));
        }

        let bytes = serde_json::to_vec(session).map_err(std::io::Error::other)?;

        if bytes.len() as u64 > BYTE_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "Session record exceeds the size limit.",
            ));
        }

        let key = session_record_key(&bytes);
        save_file(root.join("records").join(format!("{key}.json")), bytes)?;
        sessions.insert(id.clone(), key);
    }

    let state = ClientState {
        deferred: store.deferred.clone(),
        deferred_acknowledged: store.deferred_acknowledged.clone(),
        offsets: store.offsets.clone(),
        seen: store.seen.clone(),
    };

    save_json(&root.join("state/clients/runtime.json"), &state)?;

    let mut delivery_index = DeliveryIndex::default();
    let mut delivery_records = BTreeSet::new();

    for (delivery, dead) in store
        .outbox
        .iter()
        .map(|item| (item, false))
        .chain(store.dead.iter().map(|item| (item, true)))
    {
        let bytes = serde_json::to_vec(delivery).map_err(std::io::Error::other)?;

        let key = session_record_key(&bytes);
        save_file(root.join("state/deliveries/records").join(format!("{key}.json")), bytes)?;
        delivery_records.insert(key.clone());

        if dead {
            delivery_index.dead.push(key);
        } else {
            delivery_index.outbox.push(key);
        }
    }

    save_json(&root.join("state/deliveries/index.json"), &delivery_index)?;
    save_json(path, &SessionIndex { version: 1, sessions: sessions.clone() })?;

    let _ = cleanup_session_records(&root.join("records"), &sessions);
    let _ = cleanup_records(&root.join("state/deliveries/records"), delivery_records);

    Ok(())
}

fn cleanup_session_records(
    directory: &Path,
    sessions: &BTreeMap<String, String>,
) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;

        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(key) = name.strip_suffix(".json") else {
            continue;
        };

        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }

        if sessions.values().any(|referenced| referenced == key) {
            continue;
        }

        if entry.metadata()?.len() > BYTE_LIMIT {
            continue;
        }

        let bytes = std::fs::read(entry.path())?;
        let session = serde_json::from_slice::<Session>(&bytes).ok();

        if session.is_some_and(|session| !sessions.contains_key(&session.id)) {
            continue;
        }

        remove_record(entry.path())?;
    }

    Ok(())
}

fn remove_session_records(directory: &Path, id: &str) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;

        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(key) = name.strip_suffix(".json") else {
            continue;
        };

        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }

        if entry.metadata()?.len() > BYTE_LIMIT {
            continue;
        }

        let bytes = std::fs::read(entry.path())?;
        let Ok(session) = serde_json::from_slice::<Session>(&bytes) else {
            continue;
        };

        if session.id == id {
            remove_record(entry.path())?;
        }
    }

    Ok(())
}

fn cleanup_records(directory: &Path, referenced: BTreeSet<String>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !entry.file_type()?.is_file() {
            continue;
        }

        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };

        let Some(key) = name.strip_suffix(".json") else {
            continue;
        };

        if key.len() == 64
            && key.bytes().all(|byte| byte.is_ascii_hexdigit())
            && !referenced.contains(key)
        {
            remove_record(path)?;
        }
    }

    Ok(())
}

fn remove_record(path: PathBuf) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn save_json(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;

    if bytes.len() as u64 > BYTE_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "State record exceeds the size limit.",
        ));
    }

    save_file(path, bytes)
}

fn session_record_key(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |value| value.as_secs())
}

fn reservation_expired(until: Option<u64>, updated: u64, current: u64) -> bool {
    until.map_or_else(
        || updated.saturating_add(TUI_RESERVATION_TTL) <= current,
        |until| until <= current,
    )
}

fn seen_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_nanos().min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::{DeliveryStatus, LIMIT, Session, Store, compact_messages, save_file, valid};

    use crabbot_core::types::{Content, ContextUsage, Message, Role};
    use std::collections::BTreeMap;

    fn message(id: usize, session: &str) -> Message {
        Message {
            id: id.to_string(),
            session: session.into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "hello".into() }],
        }
    }

    #[test]
    fn stores_reserved_compacted_context_without_replacing_history() {
        let root = std::env::temp_dir().join(format!("crabbot-replace-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "local").unwrap();
        store.append("main", message(1, "main")).unwrap();
        store.reserve("main", "owner".into()).unwrap();

        assert_eq!(
            store.set_compacted_context_reserved("main", "other", Vec::new()).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        store.set_compacted_context_reserved("main", "owner", vec![message(2, "main")]).unwrap();

        assert_eq!(store.sessions["main"].messages, vec![message(1, "main")]);
        assert_eq!(store.sessions["main"].compacted_context, Some(vec![message(2, "main")]));

        drop(store);

        let restored = Store::load(&path).unwrap();

        assert_eq!(restored.sessions["main"].messages, vec![message(1, "main")]);
        assert_eq!(restored.sessions["main"].compacted_context, Some(vec![message(2, "main")]));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn sessions_persist_and_bound_history() {
        let root = std::env::temp_dir().join(format!("crabbot-state-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "local").unwrap();

        for id in 0..=LIMIT {
            store.push("main", message(id, "main")).unwrap();
        }

        assert_eq!(store.sessions["main"].messages.len(), LIMIT);

        let loaded = Store::load(path.clone()).unwrap();

        assert_eq!(loaded.sessions["main"].messages.len(), LIMIT);
        assert_eq!(loaded.sessions["main"].status, "idle");
        let mut store = loaded;
        store.set_status("main", "working").unwrap();
        let recovered = Store::load(path.clone()).unwrap();

        assert_eq!(recovered.sessions["main"].status, "interrupted");
        assert!(!std::fs::metadata(path).unwrap().permissions().readonly());
        let large = root.join("large.json");
        std::fs::write(&large, vec![b'x'; 32 * 1024 * 1024 + 1]).unwrap();

        assert!(Store::load(large).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn compacts_tool_call_groups_atomically() {
        let mut messages = vec![
            Message {
                id: "assistant".into(),
                session: "main".into(),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: "[Tool call read]: {}".into() }],
            },
            Message {
                id: "tool".into(),
                session: "main".into(),
                role: Role::Tool,
                sender: Some("read".into()),
                content: vec![Content::Text { text: "result".into() }],
            },
        ];

        messages.extend((0..LIMIT - 1).map(|id| message(id, "main")));
        compact_messages(&mut messages);

        assert_eq!(messages.len(), LIMIT - 1);
        assert!(messages.iter().all(|message| message.role != Role::Tool));
    }

    #[test]
    fn sessions_fork_and_validate_ids() {
        let root = std::env::temp_dir().join(format!("crabbot-state-fork-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut store = Store::load(root.join("sessions.json")).unwrap();
        store.create("main", "local").unwrap();
        store.push("main", message(1, "main")).unwrap();
        store.fork("main", "copy").unwrap();

        assert_eq!(store.sessions["copy"].messages.len(), 1);
        assert!(valid("copy-2"));
        assert!(!valid("../escape"));
        store.remove("copy").unwrap();

        for id in 0..LIMIT - 1 {
            store.create(format!("session-{id}"), "local").unwrap();
        }

        assert!(store.fork("main", "copy").is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn renames_sessions_and_updates_their_message_routes() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-rename-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let mut store = Store::load(root.join("sessions.json")).unwrap();
        store.create("old", "local").unwrap();
        store.push("old", message(1, "old")).unwrap();

        store.rename_session("old", "new").unwrap();

        assert!(!store.sessions.contains_key("old"));
        assert_eq!(store.sessions["new"].id, "new");
        assert_eq!(store.sessions["new"].messages[0].session, "new");
        assert!(store.rename_session("missing", "other").is_err());
        assert!(store.rename_session("new", "new").is_err());
        assert!(store.rename_session("new", "../escape").is_err());
        store.create("taken", "local").unwrap();

        assert!(store.rename_session("new", "taken").is_err());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn channel_backed_sessions_cannot_be_renamed() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-rename-channel-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let mut store = Store::load(root.join("sessions.json")).unwrap();
        store.create("telegram-123", "local").unwrap();
        store.route("telegram-123", "telegram", "123", None, true).unwrap();

        let error = store.rename_session("telegram-123", "renamed").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(store.sessions.contains_key("telegram-123"));
        assert!(!store.sessions.contains_key("renamed"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn archives_and_restores_sessions_without_deleting_them() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-archive-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("saved", "local").unwrap();

        store.archive_session("saved", true).unwrap();

        assert!(store.sessions["saved"].archived);
        let mut store = Store::load(&path).unwrap();
        store.archive_session("saved", false).unwrap();

        assert!(!store.sessions["saved"].archived);
        assert!(store.archive_session("missing", true).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn load_bounds_session_count() {
        let path =
            std::env::temp_dir().join(format!("crabbot-state-bound-{}.json", std::process::id()));

        let _ = std::fs::remove_file(&path);
        let mut sessions = serde_json::Map::new();

        for id in 0..=LIMIT {
            let id = format!("session-{id}");
            sessions.insert(
                id.clone(),
                serde_json::json!({
                    "id": id,
                    "model": "model",
                    "messages": [],
                    "status": "idle",
                    "created": 0,
                    "updated": 0
                }),
            );
        }

        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({"sessions": sessions})).unwrap(),
        )
        .unwrap();

        let store = Store::load(&path).unwrap();

        assert_eq!(store.sessions.len(), LIMIT);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_invalid_state_operations() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-errors-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let malformed = root.join("broken.json");
        std::fs::write(&malformed, "not json").unwrap();

        assert!(Store::load(malformed).is_err());

        let invalid = root.join("invalid.json");
        std::fs::write(
            &invalid,
            r#"{"sessions":{"Bad":{"id":"Bad","model":"model","messages":[],"status":"idle","created":0,"updated":0}}}"#,
        )
        .unwrap();

        assert!(Store::load(invalid).is_err());

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();

        assert!(store.create("Bad", "model").is_err());
        assert!(store.create("blank", "").is_err());
        store.ensure("main", "model").unwrap();
        store.ensure("main", "other").unwrap();
        store.set_model("main", "next").unwrap();

        assert_eq!(store.sessions["main"].model, "next");
        assert!(store.set_model("main", "").is_err());
        assert!(store.set_model("missing", "next").is_err());
        store.set_workspace("main", Some("/tmp")).unwrap();

        assert_eq!(store.sessions["main"].workspace.as_deref(), Some("/tmp"));
        store.fork("main", "workspace-copy").unwrap();

        assert_eq!(store.sessions["workspace-copy"].workspace.as_deref(), Some("/tmp"));
        store.set_workspace("main", None).unwrap();

        assert!(store.sessions["main"].workspace.is_none());
        assert!(store.set_workspace("main", Some(" ")).is_err());
        assert!(store.set_workspace("missing", None).is_err());
        assert!(store.create("main", "model").is_err());
        assert!(store.push("missing", message(1, "missing")).is_err());
        assert!(store.set_status("missing", "working").is_err());
        assert!(store.fork("missing", "copy").is_err());
        assert!(store.fork("main", "bad_id").is_err());
        store.fork("main", "copy").unwrap();

        assert!(store.fork("main", "copy").is_err());
        assert!(store.cancel("missing").is_err());
        assert!(store.remove("missing").is_err());
        store.remove("copy").unwrap();

        assert!(!store.sessions.contains_key("copy"));

        let blocker = root.join("blocker");
        std::fs::write(&blocker, "file").unwrap();
        let mut failed = Store::load(blocker.join("sessions.json")).unwrap();

        assert!(failed.create("safe", "model").is_err());
        failed.sessions.insert(
            "safe".into(),
            Session {
                id: "safe".into(),
                model: "model".into(),
                workspace: None,
                archived: false,
                channel: None,
                chat: None,
                thread: None,
                private: true,
                messages: Vec::new(),
                compacted_through: None,
                compacted_context: None,
                context_usage: None,
                queued: Vec::new(),
                queue_roles: BTreeMap::new(),
                inflight: None,
                reservation_until: None,
                reservation_owner: None,
                inflight_roles: Vec::new(),
                stream_delivery: None,
                phase: "safe".into(),
                status: "idle".into(),
                created: 0,
                updated: 0,
            },
        );

        assert!(
            failed
                .reply("safe", message(1, "safe"), "delivery", "telegram", "7", None, "hello")
                .is_err()
        );

        assert!(failed.sessions["safe"].messages.is_empty());
        assert!(failed.outbox.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn state_files_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("crabbot-state-mode-{}.json", std::process::id()));

        let _ = std::fs::remove_file(&path);
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();

        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn persists_context_usage_and_clears_it_when_model_changes() {
        let root =
            std::env::temp_dir().join(format!("crabbot-context-usage-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "codex/model").unwrap();
        store.set_context_usage("main", ContextUsage { used: 12345, limit: 128000 }).unwrap();

        let mut loaded = Store::load(&path).unwrap();

        assert_eq!(
            loaded.sessions["main"].context_usage,
            Some(ContextUsage { used: 12345, limit: 128000 })
        );

        loaded.set_model("main", "other/model").unwrap();

        assert_eq!(loaded.sessions["main"].context_usage, None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_persists_retries_and_acknowledgements() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-outbox-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store
            .reply("main", message(1, "main"), "delivery", "telegram", "7", None, "hello")
            .unwrap();

        store
            .reply("main", message(2, "main"), "delivery", "telegram", "7", None, "duplicate")
            .unwrap();

        assert_eq!(store.outbox[0].attempts, 0);
        assert_eq!(store.outbox.len(), 1);
        store.retry("delivery").unwrap();

        assert_eq!(store.outbox[0].attempts, 1);
        let loaded = Store::load(&path).unwrap();

        assert_eq!(loaded.outbox[0].text, "hello");
        let mut loaded = loaded;
        loaded.ack("missing").unwrap();
        loaded.ack("delivery").unwrap();

        assert!(loaded.outbox.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn migrates_legacy_delivery_routes_from_sessions() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-legacy-delivery-{}", std::process::id()));

        let path = root.join("sessions.json");
        let document = serde_json::json!({
            "sessions": {
                "signal-group": {
                    "id": "signal-group",
                    "model": "model",
                    "channel": "signal",
                    "chat": "group-id",
                    "private": false,
                    "messages": [],
                    "status": "idle",
                    "created": 0,
                    "updated": 0
                }
            },
            "outbox": [{
                "id": "signal-event",
                "channel": "signal",
                "chat": "group-id",
                "text": "hello",
                "attempts": 0,
                "created": 0
            }]
        });

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        let store = Store::load(&path).unwrap();

        assert!(!store.outbox[0].private);
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();

        assert_eq!(saved["outbox"][0]["private"], false);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn quarantines_unresolved_legacy_delivery_routes() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-unresolved-delivery-{}", std::process::id()));

        let path = root.join("sessions.json");
        let session = serde_json::json!({
            "id": "signal-group",
            "model": "model",
            "channel": "signal",
            "chat": "group-id",
            "private": false,
            "messages": [],
            "status": "idle",
            "created": 0,
            "updated": 0
        });

        let delivery = serde_json::json!({
            "id": "signal-event",
            "channel": "signal",
            "chat": "group-id",
            "text": "hello",
            "attempts": 0,
            "created": 0
        });

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "sessions": {},
                "outbox": [delivery.clone()]
            }))
            .unwrap(),
        )
        .unwrap();

        let store = Store::load(&path).unwrap();

        assert_eq!(store.outbox[0].status, DeliveryStatus::Uncertain);
        assert!(store.outbox[0].last_error.is_some());
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "sessions": {
                    "signal-group": session,
                    "signal-copy": {
                        "id": "signal-copy",
                        "model": "model",
                        "channel": "signal",
                        "chat": "group-id",
                        "private": false,
                        "messages": [],
                        "status": "idle",
                        "created": 0,
                        "updated": 0
                    }
                },
                "outbox": [delivery]
            }))
            .unwrap(),
        )
        .unwrap();

        let store = Store::load(&path).unwrap();

        assert_eq!(store.outbox[0].status, DeliveryStatus::Uncertain);
        assert!(store.outbox[0].last_error.is_some());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn streamed_reply_reuses_the_channel_message() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-stream-{}", std::process::id()));

        let mut store = Store::load(root.join("sessions.json")).unwrap();
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();
        store.start_stream("main", "telegram-1", "telegram", "7", None, "Working…").unwrap();
        store.stream_started("telegram-1", "9").unwrap();
        store.stream_sending("telegram-1", "partial").unwrap();
        store.stream_updated("telegram-1").unwrap();
        store
            .reply("main", message(2, "main"), "telegram-1", "telegram", "7", None, "complete")
            .unwrap();

        assert_eq!(store.outbox[0].message_id.as_deref(), Some("9"));
        assert_eq!(store.outbox[0].text, "complete");
        assert_eq!(store.outbox[0].status, DeliveryStatus::Pending);
        assert!(store.sessions["main"].stream_delivery.is_none());

        let loaded = Store::load(root.join("sessions.json")).unwrap();

        assert_eq!(loaded.outbox[0].message_id.as_deref(), Some("9"));
        assert_eq!(loaded.outbox[0].status, DeliveryStatus::Pending);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_streams_are_uncertain_and_not_replayed() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-stream-recovery-{}", std::process::id()));

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();
        store.start_stream("main", "discord-1", "discord", "7", None, "Working…").unwrap();
        store.stream_started("discord-1", "9").unwrap();

        let loaded = Store::load(&path).unwrap();

        assert_eq!(loaded.sessions["main"].status, "interrupted");
        assert!(loaded.sessions["main"].queued.is_empty());
        assert!(loaded.sessions["main"].stream_delivery.is_none());
        assert_eq!(loaded.outbox[0].status, DeliveryStatus::Uncertain);
        assert_eq!(loaded.outbox[0].message_id.as_deref(), Some("9"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn replies_persist_transcript_and_delivery_together() {
        let root = std::env::temp_dir().join(format!("crabbot-state-reply-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store
            .reply("main", message(1, "main"), "delivery", "telegram", "7", None, "hello")
            .unwrap();

        let loaded = Store::load(path).unwrap();

        assert_eq!(loaded.sessions["main"].messages.len(), 1);
        assert_eq!(loaded.outbox[0].text, "hello");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn retrying_a_missing_delivery_reports_not_found() {
        let mut store = Store::default();
        let error = store.retry_delivery("missing").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(error.to_string(), "Delivery was not found.");
    }

    #[test]
    fn recovers_sending_deliveries_as_uncertain() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-delivery-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store
            .reply("main", message(1, "main"), "delivery", "telegram", "7", None, "hello")
            .unwrap();

        store.sending("delivery").unwrap();
        let recovered = Store::load(&path).unwrap();

        assert_eq!(recovered.outbox[0].status, DeliveryStatus::Uncertain);
        assert!(recovered.outbox[0].last_error.is_some());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn queues_and_takes_messages_with_a_bound() {
        let root = std::env::temp_dir().join(format!("crabbot-state-queue-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.queue("main", message(1, "main")).unwrap();

        assert_eq!(store.sessions["main"].queued.len(), 1);
        assert_eq!(store.take("main").unwrap().unwrap().id, "1");
        assert!(store.take("main").unwrap().is_none());
        assert!(store.queue("missing", message(2, "missing")).is_err());
        assert!(store.take("missing").is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn starting_a_retained_queued_event_clears_it_atomically() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-queued-deferred-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.set_status("main", "working").unwrap();
        store.queue("main", message(7, "main")).unwrap();
        store
            .defer_event("telegram", serde_json::json!({"id": 7, "chat": 11, "text": "retry"}))
            .unwrap();

        let mut recovered = Store::load(&path).unwrap();

        recovered
            .begin_queued_with_roles("main", message(7, "main"), Vec::new(), "telegram")
            .unwrap();

        let recovered = Store::load(&path).unwrap();

        assert!(recovered.deferred_event("telegram").is_none());
        assert_eq!(recovered.sessions["main"].queued[0].id, "7");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn queued_events_do_not_take_over_a_live_tui_reservation() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-queued-reservation-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.reserve("main", "tui-owner".into()).unwrap();
        store.queue("main", message(7, "main")).unwrap();

        let error = store
            .begin_queued_with_roles("main", message(7, "main"), Vec::new(), "telegram")
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(store.sessions["main"].has_live_tui_reservation());
        assert_eq!(store.sessions["main"].reservation_owner.as_deref(), Some("tui-owner"));
        assert_eq!(store.sessions["main"].queued[0].id, "7");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn recovers_inflight_work_and_rejects_active_deletion() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-inflight-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.begin_with_roles("main", message(1, "main"), vec!["moderator".into()]).unwrap();

        assert_eq!(store.sessions["main"].messages[0].id, "1");
        assert!(store.remove("main").is_err());
        let recovered = Store::load(path).unwrap();

        assert_eq!(recovered.sessions["main"].status, "interrupted");
        assert_eq!(recovered.sessions["main"].queued[0].id, "1");
        assert_eq!(recovered.sessions["main"].queue_roles["1"], vec!["moderator"]);
        assert!(recovered.sessions["main"].inflight.is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn preserves_tui_reservations_during_store_load() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-reservation-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.reserve("main", "owner-token".into()).unwrap();

        let mut recovered = Store::load(path).unwrap();

        assert_eq!(recovered.sessions["main"].status, "working");
        assert_eq!(recovered.sessions["main"].reservation_owner.as_deref(), Some("owner-token"));
        recovered.append_reserved("main", "owner-token", message(2, "main")).unwrap();

        assert_eq!(recovered.sessions["main"].messages[0].id, "2");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn marks_mutating_tui_turns_unsafe_under_the_reservation_lease() {
        let mut store = Store::default();
        store.create("main", "model").unwrap();
        store.reserve("main", "owner-token".into()).unwrap();

        store.phase("main", "unsafe").unwrap();

        assert_eq!(store.sessions["main"].phase, "unsafe");
        assert!(store.sessions["main"].has_live_tui_reservation());
    }

    #[test]
    fn does_not_replay_unsafe_inflight_work() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-unsafe-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();
        store.phase("main", "unsafe").unwrap();
        let recovered = Store::load(path).unwrap();
        let session = &recovered.sessions["main"];

        assert!(session.queued.is_empty());
        assert!(session.inflight.is_none());
        assert_eq!(session.status, "interrupted");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn treats_legacy_leases_as_unsafe() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-legacy-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();
        let mut value = serde_json::to_value(&store).unwrap();
        value["sessions"]["main"].as_object_mut().unwrap().remove("phase");
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        let recovered = Store::load(path).unwrap();

        assert!(recovered.sessions["main"].queued.is_empty());
        assert!(recovered.sessions["main"].inflight.is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn preserves_full_queue_during_safe_recovery() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-recovery-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();

        for id in 0..LIMIT {
            store.queue("main", message(id, "main")).unwrap();
        }

        {
            let session = store.sessions.get_mut("main").unwrap();
            session.inflight = Some(message(999, "main"));
            session.phase = "safe".into();
            session.status = "working".into();
        }

        store.save().unwrap();

        let recovered = Store::load(path).unwrap();
        let session = &recovered.sessions["main"];

        assert_eq!(session.queued.len(), LIMIT + 1);
        assert_eq!(session.queued.first().unwrap().id, "999");
        assert_eq!(session.queued.last().unwrap().id, "99");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cancels_without_retrying_inflight_work() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-cancel-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();

        assert!(store.begin("missing", message(1, "missing")).is_err());
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();

        assert!(store.begin("main", message(2, "main")).is_err());
        store.queue_with_roles("main", message(2, "main"), vec!["moderator".into()]).unwrap();

        assert_eq!(store.sessions["main"].queue_roles["2"], vec!["moderator"]);
        store.cancel("main").unwrap();

        assert!(store.sessions["main"].inflight.is_some());
        assert!(store.sessions["main"].queued.is_empty());
        assert!(store.sessions["main"].queue_roles.is_empty());
        store.set_status("main", "working").unwrap();

        assert_eq!(store.sessions["main"].status, "cancelled");
        assert!(store.phase("main", "invalid").is_err());
        store.clear("main", "cancelled").unwrap();

        assert!(store.sessions["main"].queued.is_empty());
        assert!(store.sessions["main"].inflight.is_none());
        assert_eq!(store.sessions["main"].status, "cancelled");
        store.begin("main", message(2, "main")).unwrap();
        store.reply("main", message(3, "main"), "delivery", "telegram", "7", None, "done").unwrap();

        assert!(store.sessions["main"].inflight.is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn does_not_restore_a_canceled_lease() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-state-canceled-recovery-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.begin("main", message(1, "main")).unwrap();
        store.cancel("main").unwrap();
        let recovered = Store::load(path).unwrap();
        let session = &recovered.sessions["main"];

        assert_eq!(session.status, "interrupted");
        assert!(session.inflight.is_none());
        assert!(session.queued.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn validates_worktree_cleanup_inputs() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-worktree-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        assert!(super::remove_worktree(&root, "../escape").is_err());
        assert!(super::remove_worktree(&root, "safe").is_ok());
    }

    #[test]
    fn retains_deferred_events_across_offset_commits() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-deferred-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        let event = serde_json::json!({"id": 7, "chat": 11, "text": "keep me"});

        store.defer_event("telegram", event.clone()).unwrap();
        store.commit_retained("telegram", "7", Some(8)).unwrap();

        let mut recovered = Store::load(&path).unwrap();

        assert_eq!(recovered.offset("telegram"), 8);
        assert!(recovered.known("telegram", "7"));
        assert_eq!(recovered.deferred_event("telegram"), Some(event));

        recovered.commit("telegram", "7", Some(8)).unwrap();

        assert_eq!(Store::load(&path).unwrap().deferred_event("telegram"), None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn records_deferred_acknowledgements_until_the_event_is_committed() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-deferred-ack-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.defer_event("telegram", serde_json::json!({"id": "7"})).unwrap();
        store.commit_retained("telegram", "7", Some(8)).unwrap();
        store.mark_deferred_acknowledged("telegram", "7").unwrap();

        let mut recovered = Store::load(&path).unwrap();

        assert!(recovered.deferred_acknowledged("telegram", "7"));
        recovered.commit("telegram", "7", Some(8)).unwrap();

        let recovered = Store::load(&path).unwrap();

        assert!(!recovered.deferred_acknowledged("telegram", "7"));
        assert!(recovered.deferred_event("telegram").is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn defers_events_only_when_the_serialized_state_fits_the_byte_limit() {
        let mut store = Store::default();
        let event = serde_json::json!({
            "id": "large",
            "payload": "x".repeat(super::BYTE_LIMIT as usize)
        });

        let error = store.defer_event("telegram", event).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(store.deferred_event("telegram").is_none());
    }

    #[test]
    fn tracks_offsets_seen_and_dead_deliveries() {
        let root =
            std::env::temp_dir().join(format!("crabbot-state-tracking-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("sessions.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();

        assert_eq!(store.offset("telegram"), 0);
        store.commit("telegram", "first", Some(4)).unwrap();

        assert_eq!(store.offset("telegram"), 4);
        store.commit("telegram", "older", Some(2)).unwrap();

        assert_eq!(store.offset("telegram"), 4);
        store.commit("telegram", "accepted", Some(5)).unwrap();

        assert!(store.known("telegram", "accepted"));
        assert_eq!(store.offset("telegram"), 5);
        assert!(store.known("telegram", "first"));
        store
            .reply("main", message(1, "main"), "delivery", "telegram", "7", None, "hello")
            .unwrap();

        store.retry("delivery").unwrap();

        assert_eq!(store.attempts("delivery"), Some(1));
        store.dead("delivery").unwrap();

        assert!(store.outbox.is_empty());
        assert_eq!(store.dead.len(), 1);
        store.dead("missing").unwrap();

        for id in 0..LIMIT {
            store
                .reply(
                    "main",
                    message(id, "main"),
                    format!("delivery-{id}"),
                    "telegram",
                    "7",
                    None,
                    "hello",
                )
                .unwrap();
        }

        assert!(
            store
                .reply("main", message(999, "main"), "overflow", "telegram", "7", None, "hello")
                .is_err()
        );

        while store.sessions["main"].queued.len() < LIMIT {
            let id = store.sessions["main"].queued.len();
            store.queue("main", message(id, "main")).unwrap();
        }

        assert!(store.queue("main", message(10_001, "main")).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stores_sessions_in_opaque_records_and_round_trips_catalog_state() {
        let root = std::env::temp_dir().join(format!("crabbot-catalog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("data/sessions/index.json");
        let mut store = Store::load(&path).unwrap();
        store.create("private-session-name", "model").unwrap();
        store.append("private-session-name", message(1, "private-session-name")).unwrap();
        store.commit("telegram", "event", Some(9)).unwrap();
        store
            .reply(
                "private-session-name",
                message(2, "private-session-name"),
                "telegram-event",
                "telegram",
                "chat",
                None,
                "reply",
            )
            .unwrap();

        let index = std::fs::read_to_string(&path).unwrap();
        let catalog: serde_json::Value = serde_json::from_str(&index).unwrap();
        let digest = catalog["sessions"]["private-session-name"].as_str().unwrap();

        assert_eq!(digest.len(), 64);
        assert!(root.join(format!("data/sessions/records/{digest}.json")).is_file());

        let loaded = Store::load(&path).unwrap();

        assert_eq!(loaded.sessions["private-session-name"].messages.len(), 2);
        assert_eq!(loaded.offset("telegram"), 9);
        assert_eq!(loaded.outbox.len(), 1);
        assert!(root.join("data/sessions/state/deliveries/records").is_dir());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn catalog_rejects_oversized_session_records_without_replacing_saved_catalog() {
        let root =
            std::env::temp_dir().join(format!("crabbot-catalog-size-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        let path = root.join("data/sessions/index.json");
        let mut store = Store::load(&path).unwrap();
        store.create("main", "model").unwrap();
        store.sessions.get_mut("main").unwrap().messages.push(Message {
            id: "oversized".into(),
            session: "main".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "x".repeat(super::BYTE_LIMIT as usize) }],
        });

        let error = store.save().unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);

        let loaded = Store::load(&path).unwrap();

        assert!(loaded.sessions["main"].messages.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn catalog_save_succeeds_when_post_commit_record_cleanup_fails() {
        let root =
            std::env::temp_dir().join(format!("crabbot-catalog-cleanup-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("data/sessions/index.json");
        let mut store = Store::load(&path).unwrap();
        let records = path.parent().unwrap().join("records");

        std::fs::create_dir_all(records.parent().unwrap()).unwrap();
        std::fs::write(&records, b"not a directory").unwrap();

        store.commit("telegram", "event", Some(9)).unwrap();

        let index = std::fs::read_to_string(&path).unwrap();
        let catalog: serde_json::Value = serde_json::from_str(&index).unwrap();
        let loaded = Store::load(&path).unwrap();

        assert_eq!(catalog["version"], 1);
        assert_eq!(catalog["sessions"], serde_json::json!({}));
        assert_eq!(store.offset("telegram"), 9);
        assert_eq!(loaded.offset("telegram"), 9);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn catalog_saves_reclaim_superseded_and_unreferenced_records() {
        let root = std::env::temp_dir().join(format!("crabbot-deep-remove-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("data/sessions/index.json");
        let mut store = Store::load(&path).unwrap();

        store.create("regular", "model").unwrap();
        let regular_bytes = serde_json::to_vec(&store.sessions["regular"]).unwrap();
        let regular_key = super::session_record_key(&regular_bytes);
        let regular_record = root.join(format!("data/sessions/records/{regular_key}.json"));

        assert!(regular_record.is_file());
        store.append("regular", message(1, "regular")).unwrap();

        assert!(!regular_record.exists());

        let current_bytes = serde_json::to_vec(&store.sessions["regular"]).unwrap();
        let current_key = super::session_record_key(&current_bytes);
        let current_record = root.join(format!("data/sessions/records/{current_key}.json"));

        assert!(current_record.is_file());

        store.remove("regular").unwrap();

        assert!(current_record.is_file());

        store.create("deep", "model").unwrap();

        assert!(current_record.is_file());

        let deep_bytes = serde_json::to_vec(&store.sessions["deep"]).unwrap();
        let deep_key = super::session_record_key(&deep_bytes);
        let deep_record = root.join(format!("data/sessions/records/{deep_key}.json"));

        store.append("deep", message(2, "deep")).unwrap();

        assert!(!deep_record.exists());
        std::fs::write(&deep_record, deep_bytes).unwrap();

        let current_bytes = serde_json::to_vec(&store.sessions["deep"]).unwrap();
        let current_key = super::session_record_key(&current_bytes);
        let current_record = root.join(format!("data/sessions/records/{current_key}.json"));

        assert!(current_record.is_file());
        store.remove_deep("deep").unwrap();

        assert!(!deep_record.exists());
        assert!(!current_record.exists());
        assert!(!store.sessions.contains_key("deep"));

        store.create("delivery", "model").unwrap();
        store
            .reply("delivery", message(3, "delivery"), "delivery", "telegram", "chat", None, "text")
            .unwrap();

        let delivery_bytes = serde_json::to_vec(&store.outbox[0]).unwrap();
        let delivery_key = super::session_record_key(&delivery_bytes);
        let delivery_record =
            root.join(format!("data/sessions/state/deliveries/records/{delivery_key}.json"));

        assert!(delivery_record.is_file());

        store.set_status("delivery", "idle").unwrap();

        assert!(delivery_record.is_file());

        store.outbox.clear();
        store.save().unwrap();

        assert!(!delivery_record.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn imports_legacy_and_offline_sessions_once_without_overwriting_collisions() {
        let root = std::env::temp_dir().join(format!("crabbot-migrate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let legacy_path = root.join("sessions.json");
        let mut legacy = Store::load(&legacy_path).unwrap();
        legacy.create("shared", "daemon-model").unwrap();
        legacy.save().unwrap();

        let offline = root.join("data/plugins/tui/sessions.json");
        save_file(
            &offline,
            serde_json::to_vec(&serde_json::json!({
                "sessions": {
                    "shared": {
                        "id": "shared", "model": "offline-model", "status": "idle",
                        "messages": [], "created": 1, "updated": 1
                    },
                    "local-only": {
                        "id": "local-only", "model": "offline-model", "status": "idle",
                        "messages": [], "created": 1, "updated": 1
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let catalog = root.join("data/sessions/index.json");
        let imported = Store::load(&catalog).unwrap();

        assert_eq!(imported.sessions["shared"].model, "daemon-model");
        assert_eq!(imported.sessions["tui-shared"].model, "offline-model");
        assert_eq!(imported.sessions["tui-local-only"].model, "offline-model");
        assert!(legacy_path.is_file());
        assert!(offline.is_file());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn keeps_new_seen_keys_when_trimming_by_age() {
        let mut store = Store::default();

        for id in 1..=10_000 {
            store.seen.insert(format!("telegram\0{id:05}"), id as u64);
        }

        store.commit("telegram", "00000", None).unwrap();

        assert!(store.known("telegram", "00000"));
        assert_eq!(store.seen.len(), 10_000);
        assert!(!store.known("telegram", "00001"));
        assert!(store.known("telegram", "09999"));
    }
}
