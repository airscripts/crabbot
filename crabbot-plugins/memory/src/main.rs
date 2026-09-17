#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol, Request, Response},
};
use crabbot_file::{load as load_file, save as save_file};
use serde_json::json;

const LIMIT: usize = 1_000;
const VALUE_LIMIT: usize = 256 * 1024;
const BYTE_LIMIT: usize = 32 * 1024 * 1024;
const FRAME_HEADROOM: usize = 64 * 1024;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Entry {
    #[serde(default)]
    key: String,
    value: String,
    scope: String,
    created: u64,
    updated: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Audit {
    action: String,
    key: String,
    scope: String,
    at: u64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct State {
    items: BTreeMap<String, Entry>,
    audit: Vec<Audit>,
}

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let items = load();

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "memory".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Memory],
            commands: vec![],
        },
        move |request| {
            let items = Arc::clone(&items);
            async move { call(&items, request) }
        },
    )
    .await
}

fn call(items: &Arc<Mutex<State>>, request: Request) -> crabbot_core::Result<Option<Response>> {
    call_at(items, request, path().as_deref())
}

fn call_at(
    items: &Arc<Mutex<State>>,
    request: Request,
    location: Option<&Path>,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };
    let mut state =
        items.lock().map_err(|_| crabbot_core::Error::Denied("Memory lock is poisoned.".into()))?;

    let result = match method.as_str() {
        "remember" => {
            let key = params["key"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("remember.key is required.".into()))?;
            let value = params["value"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("remember.value is required.".into()))?;
            if key.trim().is_empty() {
                return Err(crabbot_core::Error::Denied("remember.key cannot be empty.".into()));
            }
            if key.contains('\0') {
                return Err(crabbot_core::Error::Denied("remember.key cannot contain NUL.".into()));
            }
            if key.len() > VALUE_LIMIT || value.len() > VALUE_LIMIT {
                return Err(crabbot_core::Error::Denied(
                    "Memory keys and values must be smaller than 256 KiB.".into(),
                ));
            }
            let mode = params["mode"].as_str().unwrap_or("suggest");
            if mode == "off" {
                return Err(crabbot_core::Error::Denied("Memory is disabled.".into()));
            }
            if !matches!(mode, "suggest" | "auto") {
                return Err(crabbot_core::Error::Denied("Memory mode is invalid.".into()));
            }
            if mode == "suggest" && params["approved"] != true {
                return Err(crabbot_core::Error::Denied("Memory approval is required.".into()));
            }
            let scope = params["scope"].as_str().unwrap_or("global");
            if scope.trim().is_empty() {
                return Err(crabbot_core::Error::Denied("remember.scope cannot be empty.".into()));
            }
            if scope.contains('\0') {
                return Err(crabbot_core::Error::Denied(
                    "remember.scope cannot contain NUL.".into(),
                ));
            }
            let at = now();
            let previous = state.clone();
            let storage = storage(scope, key);
            let created = state.items.get(&storage).map_or(at, |entry| entry.created);
            state.items.insert(
                storage,
                Entry {
                    key: key.into(),
                    value: value.into(),
                    scope: scope.into(),
                    created,
                    updated: at,
                },
            );
            if state.items.len() > LIMIT {
                let excess = state.items.len() - LIMIT;
                let mut keys = state
                    .items
                    .iter()
                    .map(|(key, entry)| (key.clone(), entry.updated))
                    .collect::<Vec<_>>();
                keys.sort_by_key(|(_, updated)| *updated);
                let keys = keys.into_iter().take(excess).map(|(key, _)| key).collect::<Vec<_>>();
                for key in keys {
                    state.items.remove(&key);
                }
            }
            state.audit.push(Audit {
                action: "remember".into(),
                key: key.into(),
                scope: scope.into(),
                at,
            });
            if state.audit.len() > LIMIT {
                let excess = state.audit.len() - LIMIT;
                state.audit.drain(..excess);
            }
            if let Err(error) = persist_at(location, &state) {
                *state = previous;
                return Err(error);
            }
            json!({"ok": true, "scope": scope, "mode": mode})
        }
        "list" => {
            let scope = params["scope"].as_str();
            json!({
                "items": state
                    .items
                    .iter()
                    .filter(|(_, entry)| scope.is_none_or(|value| value == entry.scope))
                    .map(|(_, entry)| json!({"key": entry.key, "value": entry.value, "scope": entry.scope, "created": entry.created, "updated": entry.updated}))
                    .collect::<Vec<_>>()
            })
        }
        "audit" => {
            let key = params["key"].as_str();
            json!({
                "items": state
                    .audit
                    .iter()
                    .filter(|entry| key.is_none_or(|value| value == entry.key))
                    .map(|entry| json!({"action": entry.action, "key": entry.key, "scope": entry.scope, "at": entry.at}))
                    .collect::<Vec<_>>()
            })
        }
        "forget" => {
            let key = params["key"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("forget.key is required.".into()))?;
            let scope = params["scope"].as_str().unwrap_or("global");
            if key.contains('\0') {
                return Err(crabbot_core::Error::Denied("forget.key cannot contain NUL.".into()));
            }
            if scope.contains('\0') {
                return Err(crabbot_core::Error::Denied("forget.scope cannot contain NUL.".into()));
            }
            let previous = state.clone();
            let storage = storage(scope, key);
            let deleted = state.items.contains_key(&storage);
            if deleted && let Some(entry) = state.items.remove(&storage) {
                state.audit.push(Audit {
                    action: "forget".into(),
                    key: key.into(),
                    scope: entry.scope,
                    at: now(),
                });
                if state.audit.len() > LIMIT {
                    let excess = state.audit.len() - LIMIT;
                    state.audit.drain(..excess);
                }
            }
            if let Err(error) = persist_at(location, &state) {
                *state = previous;
                return Err(error);
            }
            json!({"deleted": deleted})
        }
        _ => return Ok(None),
    };

    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1 + FRAME_HEADROOM)
        > crabbot_core::jsonl::MAX
    {
        return Err(crabbot_core::Error::Denied(
            "Memory response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(Some(response))
}

fn load() -> Arc<Mutex<State>> {
    load_at(path().as_deref())
}

fn load_at(path: Option<&std::path::Path>) -> Arc<Mutex<State>> {
    let Some(path) = path else {
        return Arc::new(Mutex::new(State::default()));
    };
    let bytes = match load_file(path, BYTE_LIMIT as u64) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Arc::new(Mutex::new(State::default())),
        Err(_) => {
            quarantine(path);
            return Arc::new(Mutex::new(State::default()));
        }
    };
    let state = String::from_utf8(bytes).ok().and_then(|text| {
        serde_json::from_str::<State>(&text).ok().or_else(|| {
            serde_json::from_str::<BTreeMap<String, String>>(&text).ok().map(|items| State {
                items: items
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            storage("global", &key),
                            Entry { key, value, scope: "global".into(), created: 0, updated: 0 },
                        )
                    })
                    .collect(),
                audit: Vec::new(),
            })
        })
    });
    if state.is_none() {
        quarantine(path);
    }
    let mut state = state.unwrap_or_default();
    for (storage, entry) in &mut state.items {
        if entry.key.is_empty() {
            entry.key = storage.split_once('\0').map_or(storage.as_str(), |(_, key)| key).into();
        }
    }
    if state.items.len() > LIMIT {
        let excess = state.items.len() - LIMIT;
        let mut values = state.items.into_iter().collect::<Vec<_>>();
        values.sort_by_key(|(_, entry)| entry.updated);
        state.items = values.into_iter().skip(excess).collect();
    }
    if state.audit.len() > LIMIT {
        state.audit.drain(..state.audit.len() - LIMIT);
    }
    Arc::new(Mutex::new(state))
}

fn path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_MEMORY").map(PathBuf::from)
}

fn storage(scope: &str, key: &str) -> String {
    format!("{scope}\0{key}")
}

fn quarantine(path: &Path) {
    if !path.is_file() {
        return;
    }
    let target = path.with_file_name(format!(
        ".{}.corrupt-{}",
        path.file_name().and_then(|value| value.to_str()).unwrap_or("memory"),
        nonce()
    ));
    let _ = std::fs::rename(path, target);
}

fn persist_at(path: Option<&std::path::Path>, state: &State) -> crabbot_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if path.parent().is_some_and(|parent| !parent.is_dir()) {
        return Err(crabbot_core::Error::Denied("Memory state directory is unavailable.".into()));
    }
    let text = serde_json::to_vec_pretty(state)?;
    if text.len() > BYTE_LIMIT {
        return Err(crabbot_core::Error::Denied("Memory state exceeds the size limit.".into()));
    }
    save_file(path, text).map_err(Into::into)
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs())
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::{
        BYTE_LIMIT, Entry, LIMIT, State, VALUE_LIMIT, call, call_at, load, load_at, path,
        persist_at, storage,
    };
    use crabbot_core::types::Request;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn items() -> Arc<Mutex<State>> {
        Arc::new(Mutex::new(State::default()))
    }

    #[test]
    fn memory_can_remember_list_and_forget() {
        let items = items();
        let put = call(
            &items,
            Request::call(1, "remember", json!({"key": "name", "value": "Ada", "approved": true})),
        )
        .unwrap()
        .unwrap();
        assert_eq!(put.result.unwrap()["ok"], true);

        let list = call(&items, Request::call(2, "list", json!({}))).unwrap().unwrap();
        assert_eq!(list.result.unwrap()["items"][0]["value"], "Ada");

        let audit =
            call(&items, Request::call(3, "audit", json!({"key": "name"}))).unwrap().unwrap();
        assert_eq!(audit.result.unwrap()["items"][0]["action"], "remember");

        let forget =
            call(&items, Request::call(4, "forget", json!({"key": "name"}))).unwrap().unwrap();
        assert_eq!(forget.result.unwrap()["deleted"], true);
    }

    #[test]
    fn memory_rejects_missing_values() {
        let error =
            call(&items(), Request::call(1, "remember", json!({"key": "name"}))).unwrap_err();
        assert!(error.to_string().contains("remember.value"));
    }

    #[test]
    fn memory_bounds_key_and_value_size() {
        let items = items();
        let value = "x".repeat(VALUE_LIMIT + 1);
        assert!(
            call(
                &items,
                Request::call(
                    1,
                    "remember",
                    json!({"key": "name", "value": value, "mode": "auto"})
                )
            )
            .is_err()
        );
        assert!(
            call(
                &items,
                Request::call(
                    0,
                    "remember",
                    json!({"key": "bad\u{0000}key", "value": "y", "mode": "auto"}),
                ),
            )
            .is_err()
        );
        assert!(call(
            &items,
            Request::call(
                0,
                "remember",
                json!({"key": "key", "value": "y", "scope": "bad\u{0000}scope", "mode": "auto"}),
            ),
        )
        .is_err());
    }

    #[test]
    fn memory_rejects_missing_keys_and_unknown_methods() {
        let items = items();
        assert!(call(&items, Request::call(1, "remember", json!({"value": "Ada"}))).is_err());
        assert!(call(&items, Request::call(2, "forget", json!({}))).is_err());
        assert!(call(&items, Request::call(3, "unknown", json!({}))).unwrap().is_none());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "list".into(), params: json!({}) };
        assert!(call(&items, note).unwrap().is_none());
        let forgotten =
            call(&items, Request::call(4, "forget", json!({"key": "missing"}))).unwrap().unwrap();
        assert_eq!(forgotten.result.unwrap()["deleted"], false);
    }

    #[test]
    fn memory_persists_and_recovers_items() {
        let path = std::env::temp_dir().join(format!("crabbot-memory-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut state = State::default();
        state.items.insert(
            storage("global", "key"),
            Entry {
                key: "key".into(),
                value: "value".into(),
                scope: "global".into(),
                created: 0,
                updated: 0,
            },
        );
        persist_at(Some(&path), &state).unwrap();
        assert_eq!(
            load_at(Some(&path)).lock().unwrap().items[&storage("global", "key")].value,
            "value"
        );
        std::fs::write(&path, r#"{"legacy":"value"}"#).unwrap();
        assert_eq!(
            load_at(Some(&path)).lock().unwrap().items[&storage("global", "legacy")].scope,
            "global"
        );
        std::fs::write(&path, "broken").unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().items.is_empty());
        std::fs::File::create(&path).unwrap().set_len(BYTE_LIMIT as u64 + 1).unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().items.is_empty());
        assert!(persist_at(Some(&path.with_file_name("missing-dir/item")), &state).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn handles_disabled_persistence() {
        let _ = path();
        assert!(load().lock().unwrap().items.is_empty());
        assert!(load_at(None).lock().unwrap().items.is_empty());
        persist_at(None, &State::default()).unwrap();
    }

    #[test]
    fn memory_modes_and_scopes_are_enforced() {
        let items = items();
        assert!(
            call(
                &items,
                Request::call(0, "remember", json!({"key": " ", "value": "y", "mode": "auto"}))
            )
            .is_err()
        );
        assert!(
            call(
                &items,
                Request::call(
                    0,
                    "remember",
                    json!({"key": "x", "value": "y", "scope": " ", "mode": "auto"})
                )
            )
            .is_err()
        );
        assert!(
            call(&items, Request::call(1, "remember", json!({"key": "x", "value": "y"}))).is_err()
        );
        assert!(
            call(
                &items,
                Request::call(2, "remember", json!({"key": "x", "value": "y", "mode": "off"}))
            )
            .is_err()
        );
        assert!(
            call(
                &items,
                Request::call(
                    3,
                    "remember",
                    json!({"key": "x", "value": "y", "mode": "bad", "approved": true})
                )
            )
            .is_err()
        );
        let remembered = call(
            &items,
            Request::call(
                4,
                "remember",
                json!({"key": "x", "value": "y", "scope": "room", "mode": "auto"}),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(remembered.result.unwrap()["scope"], "room");
        assert_eq!(
            call(&items, Request::call(5, "list", json!({"scope": "other"})))
                .unwrap()
                .unwrap()
                .result
                .unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            call(&items, Request::call(6, "forget", json!({"key": "x", "scope": "other"})))
                .unwrap()
                .unwrap()
                .result
                .unwrap()["deleted"],
            false
        );
        assert!(
            call(&items, Request::call(7, "forget", json!({"key": "bad\u{0000}key"})),).is_err()
        );
    }

    #[test]
    fn bounds_memory_history_and_items() {
        let items = items();
        for index in 0..=LIMIT {
            call(
                &items,
                Request::call(
                    index as u64,
                    "remember",
                    json!({"key": format!("key-{index}"), "value": "value", "mode": "auto"}),
                ),
            )
            .unwrap();
        }
        let state = items.lock().unwrap();
        assert_eq!(state.items.len(), LIMIT);
        assert_eq!(state.audit.len(), LIMIT);
        drop(state);

        let path =
            std::env::temp_dir().join(format!("crabbot-memory-bound-{}.json", std::process::id()));
        let mut state = State::default();
        for index in 0..=LIMIT {
            state.items.insert(
                storage("global", &format!("key-{index}")),
                Entry {
                    key: format!("key-{index}"),
                    value: "value".into(),
                    scope: "global".into(),
                    created: 0,
                    updated: 0,
                },
            );
            state.audit.push(super::Audit {
                action: "remember".into(),
                key: format!("key-{index}"),
                scope: "global".into(),
                at: index as u64,
            });
        }
        std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o600);
            std::fs::set_permissions(&path, permissions).unwrap();
        }
        let loaded = load_at(Some(&path)).lock().unwrap().clone();
        assert_eq!(loaded.items.len(), LIMIT);
        assert_eq!(loaded.audit.len(), LIMIT);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bounds_forget_audit_history() {
        let items = items();
        {
            let mut state = items.lock().unwrap();
            for index in 0..LIMIT {
                state.audit.push(super::Audit {
                    action: "remember".into(),
                    key: format!("key-{index}"),
                    scope: "global".into(),
                    at: index as u64,
                });
            }
            state.items.insert(
                storage("global", "last"),
                Entry {
                    key: "last".into(),
                    value: "value".into(),
                    scope: "global".into(),
                    created: 0,
                    updated: 0,
                },
            );
        }
        call(&items, Request::call(1, "forget", json!({"key": "last"}))).unwrap();
        let state = items.lock().unwrap();
        assert_eq!(state.audit.len(), LIMIT);
        assert_eq!(state.audit.last().unwrap().action, "forget");
    }

    #[test]
    fn restores_memory_when_forget_persistence_fails() {
        let root =
            std::env::temp_dir().join(format!("crabbot-memory-forget-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "file").unwrap();
        let state = items();
        call(
            &state,
            Request::call(1, "remember", json!({"key": "x", "value": "y", "mode": "auto"})),
        )
        .unwrap();
        assert!(
            call_at(
                &state,
                Request::call(2, "forget", json!({"key": "x"})),
                Some(&blocker.join("state"))
            )
            .is_err()
        );
        assert!(state.lock().unwrap().items.contains_key(&storage("global", "x")));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn restores_memory_when_persistence_fails() {
        let root =
            std::env::temp_dir().join(format!("crabbot-memory-blocker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "file").unwrap();
        let state = items();
        assert!(
            call_at(
                &state,
                Request::call(1, "remember", json!({"key": "x", "value": "y", "mode": "auto"})),
                Some(&blocker.join("state")),
            )
            .is_err()
        );
        assert!(state.lock().unwrap().items.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn ignores_shared_state_files() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("crabbot-memory-shared-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, r#"{"items":{},"audit":[]}"#).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().items.is_empty());
        let _ = std::fs::remove_file(path);
    }
}
