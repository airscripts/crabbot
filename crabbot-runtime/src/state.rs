use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    thread,
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

use crabbot_core::types::{Message, Role};
use crabbot_file::{load as load_file, private as private_file, save as save_file};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const LIMIT: usize = 100;
const SEEN_LIMIT: usize = 10_000;
const SEEN_KEY_LIMIT: usize = 4 * 1024;
const BYTE_LIMIT: u64 = 32 * 1024 * 1024;

pub fn compact_messages(messages: &mut Vec<Message>) {
    while messages.len() > LIMIT {
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
    pub channel: Option<String>,
    #[serde(default)]
    pub chat: Option<String>,
    #[serde(default)]
    pub thread: Option<String>,
    #[serde(default = "direct")]
    pub private: bool,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub queued: Vec<Message>,
    #[serde(default)]
    pub queue_roles: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub inflight: Option<Message>,
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

impl Store {
    pub fn load(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
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

            if session.status == "working" || session.inflight.is_some() {
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
                    channel: None,
                    chat: None,
                    thread: None,
                    private: true,
                    messages: Vec::new(),
                    queued: Vec::new(),
                    queue_roles: BTreeMap::new(),
                    inflight: None,
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
        self.push_with_status(id, message, false)
    }

    pub fn append(&mut self, id: &str, message: Message) -> std::io::Result<()> {
        self.push_with_status(id, message, true)
    }

    fn push_with_status(
        &mut self,
        id: &str,
        message: Message,
        resume_cancelled: bool,
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

            if session.status != "cancelled" || resume_cancelled {
                session.status = "idle".into();
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
        let Some(session) = self.sessions.get(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Session was not found.",
            ));
        };

        if session.inflight.is_some()
            || (session.status == "working"
                && !session.queued.iter().any(|item| item.id == message.id))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Session is already working.",
            ));
        }

        self.change(|store| {
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
            session.updated = now();
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
            session.updated = now();
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

            store.sessions.insert(
                target.clone(),
                Session {
                    id: target,
                    model: source.model,
                    workspace: source.workspace,
                    channel: source.channel,
                    chat: source.chat,
                    thread: source.thread,
                    private: source.private,
                    messages,
                    queued: Vec::new(),
                    queue_roles: BTreeMap::new(),
                    inflight: None,
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

        if session.inflight.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Session has no active lease.",
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

    pub fn commit(&mut self, channel: &str, id: &str, offset: Option<i64>) -> std::io::Result<()> {
        let key = seen_key(channel, id);

        self.change(|store| {
            store.seen.insert(key.clone(), seen_now());

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
        let path = self.path.clone();
        update(self);

        if let Err(error) = self.save() {
            self.sessions = previous;
            self.outbox = outbox;
            self.dead = dead;
            self.offsets = offsets;
            self.seen = seen;
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

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |value| value.as_secs())
}

fn seen_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_nanos().min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::{DeliveryStatus, LIMIT, Session, Store, compact_messages, valid};

    use crabbot_core::types::{Content, Message, Role};
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
                channel: None,
                chat: None,
                thread: None,
                private: true,
                messages: Vec::new(),
                queued: Vec::new(),
                queue_roles: BTreeMap::new(),
                inflight: None,
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
