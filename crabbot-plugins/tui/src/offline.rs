use crabbot_core::{Error, Result, types::Message};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::PathBuf,
    time::SystemTime,
};

const STORE_LIMIT: u64 = 8 * 1024 * 1024;
const RESERVATION_TTL: u64 = crabbot_core::session::RESERVATION_TTL_SECONDS;

#[derive(Debug, Default, Deserialize, Serialize)]
struct Store {
    #[serde(default)]
    sessions: BTreeMap<String, Session>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Session {
    id: String,
    model: String,
    #[serde(default = "idle_status")]
    status: String,
    #[serde(default)]
    reservation_until: Option<u64>,
    #[serde(default)]
    reservation_owner: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    messages: Vec<Message>,
    created: u64,
    updated: u64,
}

pub async fn control(home: &str, method: &str, params: Value) -> Result<Value> {
    let home = PathBuf::from(home);
    let path = super::data::plugin_file(&home, "tui", "sessions.json");
    let _lock = lock(&path)?;
    let mut store = load(&path)?;

    if store.recover_reservations() {
        save(&path, &store)?;
    }

    let id = params["id"].as_str().unwrap_or_default();

    match method {
        "session.ensure" => {
            validate_id(id)?;

            if !store.sessions.contains_key(id) {
                let model = required_text(&params, "model")?;
                store.create(id, model)?;
                save(&path, &store)?;
            }

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.get" => {
            let session = store.session(id)?;

            Ok(json!({
                "id": session.id,
                "model": session.model,
                "workspace": session.workspace,
                "status": session.status,
                "inflight": session.status == "working",
                "messages": session.messages,
                "backend": "local"
            }))
        }

        "session.reserve" => {
            let owner = reservation_owner()?;

            let session = store.session_mut(id)?;

            if session.status == "working" {
                return Err(Error::Denied("Session is already working.".into()));
            }

            session.status = "working".into();
            session.reservation_until = Some(now().saturating_add(RESERVATION_TTL));
            session.reservation_owner = Some(owner.clone());
            session.updated = now();
            save(&path, &store)?;

            Ok(json!({"id": id, "reserved": true, "owner": owner, "backend": "local"}))
        }

        "session.renew" => {
            let owner = required_text(&params, "owner")?;
            let session = store.session_mut(id)?;

            if session.status != "working"
                || session.reservation_until.is_none()
                || session.reservation_owner.as_deref() != Some(owner)
            {
                return Err(Error::Denied("Session reservation is unavailable.".into()));
            }

            session.reservation_until = Some(now().saturating_add(RESERVATION_TTL));

            save(&path, &store)?;

            Ok(json!({"id": id, "renewed": true, "backend": "local"}))
        }

        "session.release" => {
            let owner = required_text(&params, "owner")?;
            let session = store.session_mut(id)?;

            if session.status != "working"
                || session.reservation_until.is_none()
                || session.reservation_owner.as_deref() != Some(owner)
            {
                return Err(Error::Denied("Session reservation is unavailable.".into()));
            }

            session.status = "idle".into();

            session.reservation_until = None;
            session.reservation_owner = None;
            session.updated = now();
            save(&path, &store)?;

            Ok(json!({"id": id, "released": true, "backend": "local"}))
        }

        "session.new" => {
            validate_id(id)?;

            let model = required_text(&params, "model")?;
            store.create(id, model)?;
            save(&path, &store)?;

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.append" | "session.append_reserved" => {
            let reserved = method == "session.append_reserved";
            let owner = if reserved { Some(required_text(&params, "owner")?) } else { None };

            let message: Message = serde_json::from_value(params["message"].clone())?;

            if message.session != id {
                return Err(Error::Denied("Message session does not match.".into()));
            }

            let session = store.session_mut(id)?;

            if reserved != (session.status == "working") {
                return Err(Error::Denied("Session is already working.".into()));
            }

            if reserved && session.reservation_owner.as_deref() != owner {
                return Err(Error::Denied("Session reservation is unavailable.".into()));
            }

            session.messages.push(message);

            if session.messages.len() > crabbot_core::session::MESSAGE_HISTORY_LIMIT {
                let excess = session.messages.len() - crabbot_core::session::MESSAGE_HISTORY_LIMIT;
                session.messages.drain(..excess);
            }

            session.updated = now();

            if reserved && session.reservation_until.is_some() {
                session.reservation_until = Some(now().saturating_add(RESERVATION_TTL));
            }

            save(&path, &store)?;

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.clear" => {
            let session = store.session_mut(id)?;

            if session.status == "working" {
                return Err(Error::Denied("A working session cannot be cleared.".into()));
            }

            session.messages.clear();
            session.updated = now();
            save(&path, &store)?;

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.archive" | "session.unarchive" => {
            let archived = method == "session.archive";
            let session = store.session_mut(id)?;
            let changed = session.archived != archived;

            if changed {
                session.archived = archived;
                session.updated = now();
                save(&path, &store)?;
            }

            Ok(json!({"id": id, "archived": archived, "changed": changed, "backend": "local"}))
        }

        "session.rename" => {
            validate_id(id)?;
            let target = required_text(&params, "target")?;
            validate_id(target)?;

            if id == target {
                return Err(Error::Denied("Session ID is unchanged.".into()));
            }

            if store.sessions.contains_key(target) {
                return Err(Error::Denied("Session already exists.".into()));
            }

            if store.session(id)?.status == "working" {
                return Err(Error::Denied("A working session cannot be renamed.".into()));
            }

            let mut session = store
                .sessions
                .remove(id)
                .ok_or_else(|| Error::Denied("Session was not found.".into()))?;

            session.id = target.to_owned();
            session.updated = now();

            for message in &mut session.messages {
                message.session = target.to_owned();
            }

            store.sessions.insert(target.to_owned(), session);
            save(&path, &store)?;

            Ok(json!({"id": target, "renamed_from": id, "backend": "local"}))
        }

        "session.delete" => {
            if store.session(id)?.status == "working" {
                return Err(Error::Denied("A working session cannot be deleted.".into()));
            }

            store
                .sessions
                .remove(id)
                .ok_or_else(|| Error::Denied("Session was not found.".into()))?;

            save(&path, &store)?;

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.model" => {
            let model = required_text(&params, "model")?;
            let session = store.session_mut(id)?;
            session.model = model.to_owned();
            session.updated = now();
            save(&path, &store)?;

            Ok(json!({"id": id, "backend": "local"}))
        }

        "session.workspace" => {
            let workspace = match params["workspace"].as_str() {
                Some(path) => {
                    let path = std::path::Path::new(path)
                        .canonicalize()
                        .map_err(|_| Error::Denied("Workspace directory is unavailable.".into()))?;

                    if !path.is_dir() {
                        return Err(Error::Denied("Workspace must be a directory.".into()));
                    }

                    Some(path.to_string_lossy().into_owned())
                }

                None => None,
            };

            let session = store.session_mut(id)?;
            session.workspace = workspace.clone();
            session.updated = now();
            save(&path, &store)?;

            Ok(json!({"id": id, "workspace": workspace, "backend": "local"}))
        }

        "session.list" => {
            let items = store
                .sessions
                .values()
                .map(|session| {
                    json!({
                        "id": session.id,
                        "model": session.model,
                        "archived": session.archived,
                        "status": session.status,
                        "messages": session.messages.len(),
                        "created": session.created,
                        "updated": session.updated
                    })
                })
                .collect::<Vec<_>>();

            Ok(json!({"items": items, "backend": "local"}))
        }

        _ => Err(Error::Denied(
            "The local TUI session store does not support this operation.".into(),
        )),
    }
}

fn lock(path: &std::path::Path) -> Result<File> {
    let parent =
        path.parent().ok_or_else(|| Error::Denied("Local TUI session path is invalid.".into()))?;

    std::fs::create_dir_all(parent)?;

    let lock_path = path.with_extension("lock");

    match std::fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(Error::Denied("Local TUI session lock cannot be a symbolic link.".into()));
        }

        Ok(_) => {}

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let file =
        OpenOptions::new().read(true).write(true).create(true).truncate(false).open(lock_path)?;

    file.lock()?;

    Ok(file)
}

fn load(path: &std::path::Path) -> Result<Store> {
    let bytes = crabbot_file::load(path, STORE_LIMIT)?;

    let Some(bytes) = bytes else {
        return Ok(Store::default());
    };

    Ok(serde_json::from_slice(&bytes)?)
}

fn save(path: &std::path::Path, store: &Store) -> Result<()> {
    let bytes = serde_json::to_vec(store)?;

    if bytes.len() > STORE_LIMIT as usize {
        return Err(Error::Denied("Local TUI session storage reached its size limit.".into()));
    }

    crabbot_file::save(path, bytes)?;
    Ok(())
}

fn required_text<'a>(params: &'a Value, name: &str) -> Result<&'a str> {
    params[name]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::Denied(format!("{name} cannot be empty.")))
}

fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(Error::Denied("Session ID is invalid.".into()));
    }

    Ok(())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn reservation_owner() -> Result<String> {
    let mut bytes = [0_u8; 32];

    getrandom::fill(&mut bytes).map_err(|error| Error::Denied(error.to_string()))?;

    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn idle_status() -> String {
    "idle".into()
}

impl Store {
    fn recover_reservations(&mut self) -> bool {
        let current = now();

        let mut recovered = false;

        for session in self.sessions.values_mut() {
            if session.status == "working"
                && session.reservation_until.map_or_else(
                    || session.updated.saturating_add(RESERVATION_TTL) <= current,
                    |until| until <= current,
                )
            {
                session.status = "idle".into();

                session.reservation_until = None;
                session.reservation_owner = None;
                session.updated = current;
                recovered = true;
            }
        }

        recovered
    }

    fn create(&mut self, id: &str, model: &str) -> Result<()> {
        if self.sessions.contains_key(id) {
            return Err(Error::Denied("Session already exists.".into()));
        }

        let created = now();
        self.sessions.insert(
            id.to_owned(),
            Session {
                id: id.to_owned(),
                model: model.to_owned(),
                status: idle_status(),
                reservation_until: None,
                reservation_owner: None,
                workspace: None,
                archived: false,
                messages: Vec::new(),
                created,
                updated: created,
            },
        );

        Ok(())
    }

    fn session(&self, id: &str) -> Result<&Session> {
        self.sessions.get(id).ok_or_else(|| Error::Denied("Session was not found.".into()))
    }

    fn session_mut(&mut self, id: &str) -> Result<&mut Session> {
        self.sessions.get_mut(id).ok_or_else(|| Error::Denied("Session was not found.".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::{control, load, save};
    use serde_json::json;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
        time::SystemTime,
    };

    static NEXT_HOME: AtomicU64 = AtomicU64::new(0);

    fn home() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());

        let sequence = NEXT_HOME.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir()
            .join(format!("crabbot-tui-offline-{}-{nonce}-{sequence}", std::process::id()))
    }

    #[tokio::test]
    async fn persists_local_sessions_and_bounds_their_history() {
        let root = home();

        let home = root.to_string_lossy().into_owned();

        control(&home, "session.ensure", json!({"id": "main", "model": "test"})).await.unwrap();

        for index in 0..101 {
            control(
                &home,
                "session.append",
                json!({
                    "id": "main",
                    "message": {
                        "id": format!("message-{index}"),
                        "session": "main",
                        "role": "user",
                        "sender": null,
                        "content": [{"kind": "text", "text": index.to_string()}]
                    }
                }),
            )
            .await
            .unwrap();
        }

        let session = control(&home, "session.get", json!({"id": "main"})).await.unwrap();

        assert_eq!(session["backend"], "local");
        assert_eq!(session["messages"].as_array().unwrap().len(), 100);
        let path = super::super::data::plugin_file(&root, "tui", "sessions.json");

        assert!(path.is_file());

        let saved: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();

        assert_eq!(saved["sessions"]["main"]["id"], "main");
        assert_eq!(saved["sessions"]["main"]["model"], "test");
        assert_eq!(saved["sessions"]["main"]["messages"].as_array().unwrap().len(), 100);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn serializes_concurrent_local_session_updates() {
        let root = home();
        let home = root.to_string_lossy().into_owned();
        let mut updates = Vec::new();

        for index in 0..32 {
            let home = home.clone();
            let runtime = tokio::runtime::Handle::current();

            updates.push(tokio::task::spawn_blocking(move || {
                runtime.block_on(control(
                    &home,
                    "session.new",
                    json!({"id": format!("session-{index}"), "model": "test"}),
                ))
            }));
        }

        for update in updates {
            update.await.unwrap().unwrap();
        }

        let sessions = control(&home, "session.list", json!({})).await.unwrap();

        assert_eq!(sessions["items"].as_array().unwrap().len(), 32);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn validates_local_session_commands_and_mutations() {
        let root = home();
        let home = root.to_string_lossy().into_owned();

        assert!(
            control(&home, "session.ensure", json!({"id": "../bad", "model": "test"}))
                .await
                .is_err()
        );

        control(&home, "session.new", json!({"id": "main", "model": "test"})).await.unwrap();

        let reservation = control(&home, "session.reserve", json!({"id": "main"})).await.unwrap();
        let previous = reservation["owner"].as_str().unwrap().to_owned();

        let path = super::super::data::plugin_file(&root, "tui", "sessions.json");
        let mut store = load(&path).unwrap();
        store.sessions.get_mut("main").unwrap().reservation_until = Some(0);
        save(&path, &store).unwrap();

        let reservation = control(&home, "session.reserve", json!({"id": "main"})).await.unwrap();
        let owner = reservation["owner"].as_str().unwrap().to_owned();

        assert!(
            control(&home, "session.renew", json!({"id": "main", "owner": previous}),)
                .await
                .is_err()
        );

        assert!(
            control(&home, "session.release", json!({"id": "main", "owner": previous}),)
                .await
                .is_err()
        );

        assert!(control(&home, "session.reserve", json!({"id": "main"})).await.is_err());
        let message = json!({
            "id": "main-user-1",
            "session": "main",
            "role": "user",
            "sender": "tui",
            "content": [{"kind": "text", "text": "hello"}]
        });

        assert!(
            control(&home, "session.append", json!({"id": "main", "message": message}))
                .await
                .is_err()
        );

        assert!(
            control(
                &home,
                "session.append_reserved",
                json!({"id": "main", "owner": previous, "message": message}),
            )
            .await
            .is_err()
        );

        control(
            &home,
            "session.append_reserved",
            json!({"id": "main", "owner": owner, "message": message}),
        )
        .await
        .unwrap();

        control(&home, "session.release", json!({"id": "main", "owner": owner})).await.unwrap();

        control(&home, "session.ensure", json!({"id": "main", "model": "ignored"})).await.unwrap();

        assert!(
            control(&home, "session.new", json!({"id": "main", "model": "test"})).await.is_err()
        );

        assert!(control(&home, "session.get", json!({"id": "missing"})).await.is_err());
        assert!(
            control(&home, "session.append", json!({"id": "main", "message": {}})).await.is_err()
        );

        assert!(
            control(
                &home,
                "session.append",
                json!({
                    "id": "main",
                    "message": {
                        "id": "wrong-session",
                        "session": "other",
                        "role": "user",
                        "content": []
                    }
                })
            )
            .await
            .is_err()
        );

        control(
            &home,
            "session.workspace",
            json!({"id": "main", "workspace": root.display().to_string()}),
        )
        .await
        .unwrap();

        control(&home, "session.model", json!({"id": "main", "model": "next"})).await.unwrap();

        let sessions = control(&home, "session.list", json!({})).await.unwrap();

        assert_eq!(sessions["items"][0]["model"], "next");
        control(&home, "session.clear", json!({"id": "main"})).await.unwrap();

        let session = control(&home, "session.get", json!({"id": "main"})).await.unwrap();

        assert_eq!(session["workspace"], root.canonicalize().unwrap().to_string_lossy().as_ref());

        assert_eq!(session["messages"].as_array().unwrap().len(), 0);
        assert!(control(&home, "timer.list", json!({})).await.is_err());
        assert!(control(&home, "unsupported", json!({})).await.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn renames_and_deletes_local_sessions_with_their_history() {
        let root = home();
        let home = root.to_string_lossy().into_owned();

        control(&home, "session.new", json!({"id": "before", "model": "test"})).await.unwrap();
        control(
            &home,
            "session.append",
            json!({
                "id": "before",
                "message": {
                    "id": "message-1",
                    "session": "before",
                    "role": "user",
                    "content": [{"kind": "text", "text": "history"}]
                }
            }),
        )
        .await
        .unwrap();

        control(&home, "session.rename", json!({"id": "before", "target": "after"})).await.unwrap();

        let session = control(&home, "session.get", json!({"id": "after"})).await.unwrap();

        assert_eq!(session["messages"][0]["session"], "after");
        assert!(control(&home, "session.get", json!({"id": "before"})).await.is_err());

        let archived_result =
            control(&home, "session.archive", json!({"id": "after"})).await.unwrap();

        let archived = control(&home, "session.list", json!({})).await.unwrap();

        assert_eq!(archived_result["changed"], true);
        assert_eq!(archived["items"][0]["archived"], true);
        assert!(archived["items"][0]["created"].as_u64().is_some());

        let unchanged = control(&home, "session.archive", json!({"id": "after"})).await.unwrap();

        assert_eq!(unchanged["changed"], false);

        let restored_result =
            control(&home, "session.unarchive", json!({"id": "after"})).await.unwrap();

        let restored = control(&home, "session.list", json!({})).await.unwrap();

        assert_eq!(restored_result["changed"], true);
        assert_eq!(restored["items"][0]["archived"], false);

        let unchanged = control(&home, "session.unarchive", json!({"id": "after"})).await.unwrap();

        assert_eq!(unchanged["changed"], false);

        control(&home, "session.delete", json!({"id": "after"})).await.unwrap();

        assert!(control(&home, "session.get", json!({"id": "after"})).await.is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejects_local_session_mutations_while_working() {
        let root = home();
        let home = root.to_string_lossy().into_owned();

        control(&home, "session.new", json!({"id": "main", "model": "test"})).await.unwrap();
        control(
            &home,
            "session.append",
            json!({
                "id": "main",
                "message": {
                    "id": "message-1",
                    "session": "main",
                    "role": "user",
                    "content": [{"kind": "text", "text": "history"}]
                }
            }),
        )
        .await
        .unwrap();

        control(&home, "session.reserve", json!({"id": "main"})).await.unwrap();

        assert!(control(&home, "session.clear", json!({"id": "main"})).await.is_err());
        assert!(
            control(&home, "session.rename", json!({"id": "main", "target": "renamed"}))
                .await
                .is_err()
        );

        assert!(control(&home, "session.delete", json!({"id": "main"})).await.is_err());

        let session = control(&home, "session.get", json!({"id": "main"})).await.unwrap();

        assert_eq!(session["status"], "working");
        assert_eq!(session["messages"].as_array().unwrap().len(), 1);
        assert!(control(&home, "session.get", json!({"id": "renamed"})).await.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn recovers_expired_local_reservations_before_session_mutations() {
        let root = home();
        let home = root.to_string_lossy().into_owned();

        control(&home, "session.new", json!({"id": "main", "model": "test"})).await.unwrap();
        control(&home, "session.reserve", json!({"id": "main"})).await.unwrap();

        let path = super::super::data::plugin_file(&root, "tui", "sessions.json");
        let mut store = load(&path).unwrap();
        store.sessions.get_mut("main").unwrap().reservation_until = Some(0);
        save(&path, &store).unwrap();

        control(&home, "session.delete", json!({"id": "main"})).await.unwrap();

        assert!(control(&home, "session.get", json!({"id": "main"})).await.is_err());

        control(&home, "session.new", json!({"id": "legacy", "model": "test"})).await.unwrap();
        control(&home, "session.reserve", json!({"id": "legacy"})).await.unwrap();
        let mut store = load(&path).unwrap();
        let legacy = store.sessions.get_mut("legacy").unwrap();
        legacy.reservation_until = None;
        legacy.updated = 0;
        save(&path, &store).unwrap();

        control(&home, "session.delete", json!({"id": "legacy"})).await.unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn ignores_legacy_session_data_and_uses_plugin_data_directory() {
        let root = home();
        fs::create_dir_all(&root).unwrap();
        crabbot_file::save(
            root.join("tui-sessions.json"),
            serde_json::to_vec(&json!({
                "sessions": {
                    "legacy": {
                        "id": "legacy",
                        "model": "legacy-model",
                        "workspace": null,
                        "messages": [],
                        "created": 1,
                        "updated": 2
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let home = root.to_string_lossy().into_owned();
        let sessions = control(&home, "session.list", json!({})).await.unwrap();

        assert!(sessions["items"].as_array().unwrap().is_empty());
        control(&home, "session.ensure", json!({"id": "current", "model": "test"})).await.unwrap();

        assert!(super::super::data::plugin_file(&root, "tui", "sessions.json").is_file());
        assert!(root.join("tui-sessions.json").is_file());

        let _ = fs::remove_dir_all(root);
    }
}
