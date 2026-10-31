#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use krabb_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol, Request, Response},
};

use krabb_file::{load as load_file, save as save_file};
use serde_json::json;
use sha2::{Digest, Sha256};

const LIMIT: usize = 1_000;
const VALUE_LIMIT: usize = 256 * 1024;
const BYTE_LIMIT: usize = 32 * 1024 * 1024;
const FRAME_HEADROOM: usize = 64 * 1024;
const INDEX_LIMIT: usize = 32;
const GLOBAL_INDEX_RESERVE: usize = INDEX_LIMIT / 2;
const CONFLICT_LIMIT: usize = 32;
const LIST_PAGE_SIZE: usize = 5;

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Entry {
    #[serde(default)]
    key: String,
    value: String,
    scope: String,
    created: u64,
    updated: u64,
    #[serde(default)]
    record: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Audit {
    action: String,
    key: String,
    scope: String,
    at: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct State {
    items: BTreeMap<String, Entry>,
    audit: Vec<Audit>,
    #[serde(default = "guided")]
    learning: String,
}

fn guided() -> String {
    "guided".into()
}

impl Default for State {
    fn default() -> Self {
        Self { items: BTreeMap::new(), audit: Vec::new(), learning: guided() }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("memory", plugin_main()).await
}

async fn plugin_main() -> krabb_core::Result<()> {
    let items = Arc::new(Mutex::new(State::default()));

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "memory".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Memory],
            commands: vec![krabb_core::types::CommandSpec {
                name: "memory".into(),
                description: "Search and manage scoped agent memories.".into(),
                interactive: false,
            }],
        },
        move |request| {
            let items = Arc::clone(&items);
            async move {
                tokio::task::spawn_blocking(move || call(&items, request))
                    .await
                    .map_err(|error| krabb_core::Error::Denied(error.to_string()))?
            }
        },
    )
    .await
}

fn call(items: &Arc<Mutex<State>>, request: Request) -> krabb_core::Result<Option<Response>> {
    call_at(items, request, path()?.as_deref())
}

fn call_at(
    items: &Arc<Mutex<State>>,
    request: Request,
    location: Option<&Path>,
) -> krabb_core::Result<Option<Response>> {
    let operation = interaction_name(&request);

    let _lock = location.map(lock).transpose()?;

    if let Some(location) = location {
        *items
            .lock()
            .map_err(|_| krabb_core::Error::Denied("Memory lock is poisoned.".into()))? =
            load_at(Some(location))?.lock().unwrap().clone();
    }

    let result = call_inner_at(items, request, location);

    match &result {
        Ok(Some(response)) if response.error.is_none() => {
            tracing::info!(operation, "Memory interaction completed.");
        }

        Ok(None) => {}

        Ok(Some(_)) | Err(_) => {
            tracing::warn!(operation, "Memory interaction failed.");
        }
    }

    result
}

fn lock(path: &Path) -> std::io::Result<std::fs::File> {
    let lock = path.with_extension("lock");

    if let Some(parent) = lock.parent()
        && !parent.is_dir()
    {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }

    if std::fs::symlink_metadata(&lock).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Memory lock cannot be a symbolic link.",
        ));
    }

    let mut options = std::fs::OpenOptions::new();

    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }

    let file = options.open(&lock)?;

    krabb_file::private(&lock)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),

            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
fn command_at(
    items: &Arc<Mutex<State>>,
    id: u64,
    params: &serde_json::Value,
    location: Option<&Path>,
) -> krabb_core::Result<Option<Response>> {
    call_at(items, Request::call(id, "command", params.clone()), location)
}

fn interaction_name(request: &Request) -> &'static str {
    let Request::Call { method, params, .. } = request else {
        return "notification";
    };

    let operation = if method == "command" {
        params["args"].as_array().and_then(|args| args.first()).and_then(|value| value.as_str())
    } else {
        Some(method.as_str())
    };

    match operation {
        Some("status") => "status",
        Some("search") => "search",
        Some("list") => "list",
        Some("show" | "recall") => "show",
        Some("remember") => "remember",
        Some("edit") => "edit",
        Some("forget") => "forget",
        Some("audit") => "audit",
        Some("converge") => "converge",
        Some("learning") => "learning",
        Some("help") | None if method == "command" => "help",
        Some("index") => "index",
        _ => "unknown",
    }
}

fn call_inner_at(
    items: &Arc<Mutex<State>>,
    request: Request,
    location: Option<&Path>,
) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    if method == "command" {
        return command_inner_at(items, id, &params, location);
    }

    let mut state =
        items.lock().map_err(|_| krabb_core::Error::Denied("Memory lock is poisoned.".into()))?;

    let result = match method.as_str() {
        "remember" => {
            let key = params["key"]
                .as_str()
                .ok_or_else(|| krabb_core::Error::Denied("remember.key is required.".into()))?;
            let value = params["value"]
                .as_str()
                .ok_or_else(|| krabb_core::Error::Denied("remember.value is required.".into()))?;

            if key.trim().is_empty() {
                return Err(krabb_core::Error::Denied("remember.key cannot be empty.".into()));
            }

            if key.contains('\0') {
                return Err(krabb_core::Error::Denied("remember.key cannot contain NUL.".into()));
            }

            if key.len() > VALUE_LIMIT || value.len() > VALUE_LIMIT {
                return Err(krabb_core::Error::Denied(
                    "Memory keys and values must be smaller than 256 KiB.".into(),
                ));
            }

            let mode = params["mode"].as_str().unwrap_or(&state.learning);
            let mode = match mode {
                "suggest" => "guided",
                "auto" => "autonomous",
                value => value,
            }
            .to_owned();

            if mode == "off" {
                return Err(krabb_core::Error::Denied("Memory is disabled.".into()));
            }

            if !matches!(mode.as_str(), "guided" | "autonomous") {
                return Err(krabb_core::Error::Denied("Memory mode is invalid.".into()));
            }

            if mode == "guided" && params["approved"] != true {
                return Err(krabb_core::Error::Denied("Memory approval is required.".into()));
            }

            let scope = params["scope"].as_str().unwrap_or("global");

            if scope.trim().is_empty() {
                return Err(krabb_core::Error::Denied("remember.scope cannot be empty.".into()));
            }

            if scope.contains('\0') {
                return Err(krabb_core::Error::Denied("remember.scope cannot contain NUL.".into()));
            }

            let at = next_timestamp(&state);
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
                    record: record_ref(scope, key),
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

            remove_orphan_records(location, &previous, &state);

            json!({"ok": true, "scope": scope, "mode": mode})
        }

        "list" => list_memories(&state, &params)?,

        "index" => {
            let scope = params["scope"].as_str();
            let mut entries = if let Some(scope) = scope.filter(|scope| *scope != "global") {
                let mut scoped =
                    state.items.values().filter(|entry| entry.scope == scope).collect::<Vec<_>>();

                scoped.sort_by_key(|entry| std::cmp::Reverse(entry.updated));

                let mut global = state
                    .items
                    .values()
                    .filter(|entry| entry.scope == "global")
                    .collect::<Vec<_>>();
                global.sort_by_key(|entry| std::cmp::Reverse(entry.updated));

                let reserved_globals = global.len().min(GLOBAL_INDEX_RESERVE);
                let scoped_limit = INDEX_LIMIT - reserved_globals;
                scoped.truncate(scoped_limit);

                let mut entries = global.iter().take(reserved_globals).copied().collect::<Vec<_>>();

                entries.extend(scoped);
                entries.extend(
                    global.iter().skip(reserved_globals).take(INDEX_LIMIT - entries.len()).copied(),
                );

                entries
            } else if scope == Some("global") {
                let mut entries = state
                    .items
                    .values()
                    .filter(|entry| entry.scope == "global")
                    .collect::<Vec<_>>();

                entries.sort_by_key(|entry| std::cmp::Reverse(entry.updated));
                entries.truncate(INDEX_LIMIT);
                entries
            } else {
                let mut entries = state.items.values().collect::<Vec<_>>();
                entries.sort_by_key(|entry| std::cmp::Reverse(entry.updated));
                entries.truncate(INDEX_LIMIT);
                entries
            };

            entries.sort_by_key(|entry| std::cmp::Reverse(entry.updated));

            json!({
                "learning": state.learning,
                "items": entries
                    .into_iter()
                    .map(|entry| json!({
                        "key": summary(&entry.key),
                        "summary": summary(&entry.value),
                        "scope": entry.scope,
                        "updated": entry.updated,
                        "record": entry.record,
                    }))
                    .collect::<Vec<_>>()
            })
        }

        "search" => {
            let query = params["query"]
                .as_str()
                .filter(|query| !query.trim().is_empty())
                .ok_or_else(|| krabb_core::Error::Denied("search.query is required.".into()))?;
            let query = query.to_lowercase();
            let scope = params["scope"].as_str();
            let mut entries = state
                .items
                .values()
                .filter(|entry| {
                    scope.is_none_or(|value| {
                        value == entry.scope || (value != "global" && entry.scope == "global")
                    })
                })
                .filter(|entry| {
                    entry.key.to_lowercase().contains(&query)
                        || entry.value.to_lowercase().contains(&query)
                })
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.updated));
            entries.truncate(20);

            json!({
                "items": entries
                    .into_iter()
                    .map(|entry| json!({
                        "key": summary(&entry.key),
                        "value": summary(&entry.value),
                        "scope": entry.scope,
                    }))
                    .collect::<Vec<_>>()
            })
        }

        "recall" => {
            let key = params["key"]
                .as_str()
                .ok_or_else(|| krabb_core::Error::Denied("recall.key is required.".into()))?;
            let scope = params["scope"].as_str().unwrap_or("global");
            let item = state.items.get(&storage(scope, key));

            json!({
                "item": item.map(|entry| json!({"key": entry.key, "value": entry.value, "scope": entry.scope}))
            })
        }

        "learning" => {
            let mode = params["mode"]
                .as_str()
                .ok_or_else(|| krabb_core::Error::Denied("learning.mode is required.".into()))?;

            if !matches!(mode, "guided" | "autonomous") {
                return Err(krabb_core::Error::Denied(
                    "Learning mode must be guided or autonomous.".into(),
                ));
            }

            let previous = state.clone();
            state.learning = mode.into();

            if let Err(error) = persist_at(location, &state) {
                *state = previous;
                return Err(error);
            }

            json!({"learning": state.learning})
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
                .ok_or_else(|| krabb_core::Error::Denied("forget.key is required.".into()))?;
            let scope = params["scope"].as_str().unwrap_or("global");

            if key.contains('\0') {
                return Err(krabb_core::Error::Denied("forget.key cannot contain NUL.".into()));
            }

            if scope.contains('\0') {
                return Err(krabb_core::Error::Denied("forget.scope cannot contain NUL.".into()));
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

            remove_orphan_records(location, &previous, &state);

            json!({"deleted": deleted})
        }

        _ => return Ok(None),
    };

    let response = Response::ok(id, result);

    ensure_response_fits(&response)?;

    Ok(Some(response))
}

fn list_memories(
    state: &State,
    params: &serde_json::Value,
) -> krabb_core::Result<serde_json::Value> {
    let record_type = params
        .get("type")
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_str().ok_or_else(|| {
                krabb_core::Error::Denied("Memory type must be global or session.".into())
            })
        })
        .transpose()?;

    if record_type.is_some_and(|value| !matches!(value, "global" | "session")) {
        return Err(krabb_core::Error::Denied("Memory type must be global or session.".into()));
    }

    let scope = params
        .get("scope")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| krabb_core::Error::Denied("Memory scope must be text.".into()))
        })
        .transpose()?;

    let scopes = params
        .get("scopes")
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_array().ok_or_else(|| {
                krabb_core::Error::Denied("Memory scopes must be a list of text values.".into())
            })
        })
        .transpose()?;

    if scope.is_some() && scopes.is_some() {
        return Err(krabb_core::Error::Denied(
            "Use scope or scopes when listing memories, not both.".into(),
        ));
    }

    if let Some(scopes) = scopes
        && (scopes.is_empty() || scopes.len() > 8 || scopes.iter().any(|scope| !scope.is_string()))
    {
        return Err(krabb_core::Error::Denied(
            "Memory scopes must contain between one and eight text values.".into(),
        ));
    }

    let page = params
        .get("page")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|page| usize::try_from(page).ok())
                .filter(|page| *page > 0)
                .ok_or_else(|| {
                    krabb_core::Error::Denied("Memory page must be a positive number.".into())
                })
        })
        .transpose()?
        .unwrap_or(1);

    if page == 0 {
        return Err(krabb_core::Error::Denied("Page numbers start at 1.".into()));
    }

    let mut entries = state
        .items
        .values()
        .filter(|entry| {
            scope.is_none_or(|scope| entry.scope == scope)
                && scopes.is_none_or(|scopes| {
                    scopes.iter().any(|scope| scope.as_str() == Some(&entry.scope))
                })
                && record_type.is_none_or(|kind| {
                    if kind == "global" { entry.scope == "global" } else { entry.scope != "global" }
                })
        })
        .collect::<Vec<_>>();

    if let Some(scopes) = scopes {
        entries.sort_by_key(|entry| {
            scopes
                .iter()
                .position(|scope| scope.as_str() == Some(&entry.scope))
                .unwrap_or(usize::MAX)
        });

        let mut session_keys = BTreeSet::new();
        entries.retain(|entry| entry.scope == "global" || session_keys.insert(entry.key.clone()));
    }

    entries.sort_by(|left, right| {
        right.updated.cmp(&left.updated).then_with(|| left.key.cmp(&right.key))
    });

    let total = entries.len();
    let pages = total.div_ceil(LIST_PAGE_SIZE).max(1);

    if page > pages {
        return Err(krabb_core::Error::Denied(format!(
            "Page {page} is out of range. Choose a page from 1 to {pages} for memories."
        )));
    }

    let start = (page - 1) * LIST_PAGE_SIZE;
    let end = (start + LIST_PAGE_SIZE).min(total);
    let items = entries[start..end]
        .iter()
        .map(|entry| {
            json!({
                "key": entry.key,
                "value": summary(&entry.value),
                "scope": entry.scope,
                "type": if entry.scope == "global" { "global" } else { "session" },
                "created": entry.created,
                "updated": entry.updated,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({"items": items, "total": total, "page": page, "pages": pages}))
}

#[cfg(test)]
fn load() -> std::io::Result<Arc<Mutex<State>>> {
    let location = path()?;
    let _lock = location.as_deref().map(lock).transpose()?;

    load_at(location.as_deref())
}

fn load_at(path: Option<&std::path::Path>) -> std::io::Result<Arc<Mutex<State>>> {
    let Some(path) = path else {
        return Ok(Arc::new(Mutex::new(State::default())));
    };

    let Some(bytes) = load_file(path, BYTE_LIMIT as u64)? else {
        return Ok(Arc::new(Mutex::new(State::default())));
    };

    let state = String::from_utf8(bytes).ok().and_then(|text| {
        serde_json::from_str::<State>(&text).ok().or_else(|| {
            serde_json::from_str::<BTreeMap<String, String>>(&text).ok().map(|items| State {
                items: items
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            storage("global", &key),
                            Entry {
                                record: record_ref("global", &key),
                                key,
                                value,
                                scope: "global".into(),
                                created: 0,
                                updated: 0,
                            },
                        )
                    })
                    .collect(),
                audit: Vec::new(),
                learning: guided(),
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

        if entry.record.is_empty() {
            entry.record = record_ref(&entry.scope, &entry.key);
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

    Ok(Arc::new(Mutex::new(state)))
}

fn path() -> std::io::Result<Option<PathBuf>> {
    if let Some(path) = std::env::var_os("KRABB_MEMORY") {
        return Ok(Some(PathBuf::from(path)));
    }

    std::env::var_os("KRABB_HOME").map(PathBuf::from).map(default_path).transpose()
}

fn default_path(root: PathBuf) -> std::io::Result<PathBuf> {
    let current = root.join("data").join("memories").join("index.json");
    let _current = lock(&current)?;
    krabb_file::recover(&current)?;

    if current.is_file() {
        return Ok(current);
    }

    let legacy = root.join("memory").join("index.json");
    let _legacy = lock(&legacy)?;
    krabb_file::recover(&legacy)?;

    Ok(if legacy.is_file() { legacy } else { current })
}

fn storage(scope: &str, key: &str) -> String {
    format!("{scope}\0{key}")
}

fn record_ref(scope: &str, key: &str) -> String {
    let mut scope_hash = Sha256::new();

    scope_hash.update(scope.as_bytes());
    let mut key_hash = Sha256::new();
    key_hash.update(key.as_bytes());

    format!("records/{}/{}.md", hex_digest(scope_hash.finalize()), hex_digest(key_hash.finalize()))
}

fn summary(value: &str) -> String {
    let mut summary = value.split_whitespace().collect::<Vec<_>>().join(" ");

    if summary.len() > 240 {
        let mut boundary = 240;

        while !summary.is_char_boundary(boundary) {
            boundary -= 1;
        }

        summary.truncate(boundary);
        summary.push('…');
    }

    summary
}

fn command_inner_at(
    items: &Arc<Mutex<State>>,
    id: u64,
    params: &serde_json::Value,
    location: Option<&Path>,
) -> krabb_core::Result<Option<Response>> {
    let args = params["args"]
        .as_array()
        .ok_or_else(|| krabb_core::Error::Denied("Memory command arguments are required.".into()))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                krabb_core::Error::Denied("Memory command arguments must be text.".into())
            })
        })
        .collect::<krabb_core::Result<Vec<_>>>()?;

    let Some(command) = args.first().map(String::as_str) else {
        return Ok(Some(Response::ok(id, json!(memory_help()))));
    };

    let json_output = params["json"] == true;
    let mut scope = None;
    let mut record_type = None;
    let mut values = Vec::new();
    let mut index = 1;

    while index < args.len() {
        if args[index] == "--scope" {
            scope = Some(
                args.get(index + 1)
                    .ok_or_else(|| krabb_core::Error::Denied("--scope requires an ID.".into()))?
                    .clone(),
            );

            index += 2;
        } else if args[index] == "--type" {
            record_type = Some(
                args.get(index + 1)
                    .ok_or_else(|| {
                        krabb_core::Error::Denied("--type requires global or session.".into())
                    })?
                    .clone(),
            );

            index += 2;
        } else if args[index].starts_with('-') {
            return Err(krabb_core::Error::Denied(format!(
                "Unknown memory option {}.",
                args[index]
            )));
        } else {
            values.push(args[index].clone());
            index += 1;
        }
    }

    if record_type.as_deref().is_some_and(|value| !matches!(value, "global" | "session")) {
        return Err(krabb_core::Error::Denied("Memory type must be global or session.".into()));
    }

    if record_type.is_some() && command != "list" {
        return Err(krabb_core::Error::Denied("--type is available only with memory list.".into()));
    }

    if command == "converge" {
        let state = items
            .lock()
            .map_err(|_| krabb_core::Error::Denied("Memory lock is poisoned.".into()))?;

        let result = detect_conflicts(&state, scope.as_deref().unwrap_or("global"));
        let result = if json_output { result } else { json!(command_text(command, &result)) };

        let response = Response::ok(id, result);

        ensure_response_fits(&response)?;

        return Ok(Some(response));
    }

    let (method, operation) = match command {
        "help" => return Ok(Some(Response::ok(id, json!(memory_help())))),

        "status" => {
            let state = items
                .lock()
                .map_err(|_| krabb_core::Error::Denied("Memory lock is poisoned.".into()))?;
            let result = json!({"learning": state.learning, "records": state.items.len()});

            return Ok(Some(Response::ok(
                id,
                if json_output {
                    result
                } else {
                    json!(format!(
                        "{:<width$}{}\n{:<width$}{}",
                        "Learning:",
                        state.learning,
                        "Memories:",
                        state.items.len(),
                        width = "Learning:".len().max("Memories:".len()) + 1
                    ))
                },
            )));
        }

        "list" => {
            let page = match values.as_slice() {
                [] => 1,

                [value] => {
                    value.parse::<usize>().ok().filter(|page| *page > 0).ok_or_else(|| {
                        krabb_core::Error::Denied(
                            "Memory list page must be a positive number.".into(),
                        )
                    })?
                }

                _ => {
                    return Err(krabb_core::Error::Denied(
                        "Usage: memory list [page] [--type global|session] [--scope ID].".into(),
                    ));
                }
            };

            let mut params = json!({"page": page});

            if let Some(scope) = &scope {
                params["scope"] = json!(scope);
            }

            if let Some(record_type) = &record_type {
                params["type"] = json!(record_type);
            }

            ("list", params)
        }

        "search" => {
            if values.is_empty() {
                return Err(krabb_core::Error::Denied(
                    "Usage: memory search <text> [--scope ID].".into(),
                ));
            }

            (
                "search",
                json!({"query": values.join(" "), "scope": scope.as_deref().unwrap_or("global")}),
            )
        }

        "show" => {
            let key = values.first().ok_or_else(|| {
                krabb_core::Error::Denied("Usage: memory show <key> [--scope ID].".into())
            })?;

            ("recall", json!({"key": key, "scope": scope.as_deref().unwrap_or("global")}))
        }

        "remember" | "edit" => {
            if values.len() < 2 {
                return Err(krabb_core::Error::Denied(format!(
                    "Usage: memory {command} <key> <text> [--scope ID]."
                )));
            }

            (
                "remember",
                json!({"key": values[0], "value": values[1..].join(" "), "scope": scope.as_deref().unwrap_or("global"), "approved": true}),
            )
        }

        "forget" => {
            let key = values.first().ok_or_else(|| {
                krabb_core::Error::Denied("Usage: memory forget <key> [--scope ID].".into())
            })?;

            ("forget", json!({"key": key, "scope": scope.as_deref().unwrap_or("global")}))
        }

        "audit" => ("audit", json!({"key": values.first()})),

        "learning" => {
            let mode = values.first().ok_or_else(|| {
                krabb_core::Error::Denied("Usage: memory learning <guided|autonomous>.".into())
            })?;

            ("learning", json!({"mode": mode}))
        }

        _ => {
            return Err(krabb_core::Error::Denied(format!("Unknown memory command {command}.")));
        }
    };

    let response = call_inner_at(items, Request::call(id, method, operation), location)?
        .ok_or_else(|| krabb_core::Error::Denied("Memory command is unavailable.".into()))?;

    let result = response
        .result
        .ok_or_else(|| krabb_core::Error::Denied("Memory command returned no result.".into()))?;

    let result = if json_output { result } else { json!(command_text(command, &result)) };

    Ok(Some(Response::ok(id, result)))
}

fn memory_help() -> &'static str {
    r#"Manage scoped agent memories.

Usage: krabb memory <command> [arguments...]

Commands:
  status                       Show learning mode and memory count
  search <text> [OPTIONS]      Search saved memories
  list [PAGE] [OPTIONS]        List saved memories
  show <KEY> [OPTIONS]         Show one memory
  remember <KEY> <TEXT> [OPTIONS]
                               Save a memory
  edit <KEY> <TEXT> [OPTIONS]  Replace a memory
  forget <KEY> [OPTIONS]       Delete a memory
  audit [KEY]                  Show memory change history
  converge [OPTIONS]           Review likely conflicting memories
  learning <MODE>              Set the learning mode: guided or autonomous
  help                         Show this help

Options:
  --scope <ID>                 Select scope for supported commands
  --type <TYPE>                Filter list by global or session
  -h, --help                   Print this help

Notes:
  Use --scope with search, list, show, remember, edit, forget, or converge.
  Scope defaults to global for scoped commands. List shows both global and
  session memories unless filtered with --type. Converge uses text matching,
  reports candidates without changing memories, and suggests a forget command
  for the older record. Guided learning saves only when you explicitly ask
  Krabb to remember something."#
}

fn detect_conflicts(state: &State, scope: &str) -> serde_json::Value {
    let mut entries = state.items.values().filter(|entry| entry.scope == scope).collect::<Vec<_>>();

    entries.sort_by_key(|entry| std::cmp::Reverse(entry.updated));

    let words = entries.iter().map(|entry| preference_words(&entry.value)).collect::<Vec<_>>();
    let negated = entries.iter().map(|entry| has_negation(&entry.value)).collect::<Vec<_>>();

    let mut conflicts = Vec::new();
    let mut truncated = false;

    for newer_index in 0..entries.len() {
        for older_index in newer_index + 1..entries.len() {
            let newer = entries[newer_index];
            let older = entries[older_index];

            if !memories_conflict(
                newer,
                &words[newer_index],
                negated[newer_index],
                older,
                &words[older_index],
                negated[older_index],
            ) {
                continue;
            }

            if conflicts.len() == CONFLICT_LIMIT {
                truncated = true;
                break;
            }

            if newer.updated == older.updated {
                conflicts.push(json!({
                    "scope": summary(scope),
                    "ordering": "ambiguous",
                    "memories": [
                        {"key": summary(&newer.key), "value": summary(&newer.value), "updated": newer.updated},
                        {"key": summary(&older.key), "value": summary(&older.value), "updated": older.updated},
                    ],
                }));
            } else {
                conflicts.push(json!({
                    "scope": summary(scope),
                    "newer": {"key": summary(&newer.key), "value": summary(&newer.value), "updated": newer.updated},
                    "older": {"key": summary(&older.key), "value": summary(&older.value), "updated": older.updated},
                }));
            }
        }

        if truncated {
            break;
        }
    }

    json!({"items": conflicts, "truncated": truncated})
}

fn ensure_response_fits(response: &Response) -> krabb_core::Result<()> {
    if serde_json::to_vec(response)?.len().saturating_add(1 + FRAME_HEADROOM)
        > krabb_core::jsonl::MAX
    {
        return Err(krabb_core::Error::Denied(
            "Memory response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(())
}

fn memories_conflict(
    left: &Entry,
    left_words: &std::collections::BTreeSet<String>,
    left_negated: bool,
    right: &Entry,
    right_words: &std::collections::BTreeSet<String>,
    right_negated: bool,
) -> bool {
    if left.key == right.key && left.value != right.value {
        return true;
    }

    let shared_words = left_words.intersection(right_words).count();

    let polarity_differs = left_negated != right_negated;
    polarity_differs && shared_words >= 2
}

fn preference_words(value: &str) -> std::collections::BTreeSet<String> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|word| {
            word.len() > 2
                && !matches!(
                    word.as_str(),
                    "the"
                        | "and"
                        | "for"
                        | "with"
                        | "that"
                        | "this"
                        | "from"
                        | "into"
                        | "when"
                        | "every"
                        | "after"
                        | "before"
                        | "response"
                        | "answer"
                )
        })
        .collect()
}

fn has_negation(value: &str) -> bool {
    value
        .split(|character: char| !character.is_alphanumeric() && character != '\'')
        .map(str::to_lowercase)
        .any(|word| {
            matches!(word.as_str(), "not" | "never" | "don't" | "dont" | "avoid" | "without")
        })
}

fn forget_suggestion(scope: &str, key: &str) -> String {
    let mut command = format!("krabb memory forget {}", shell_word(key));

    if scope != "global" {
        command.push_str(&format!(" --scope {}", shell_word(scope)));
    }

    command
}

fn shell_word(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn command_text(command: &str, result: &serde_json::Value) -> String {
    match command {
        "list" => memory_list_text(result),

        "search" => {
            let items = result["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|item| {
                    format!(
                        "{}: {}",
                        item["key"].as_str().unwrap_or(""),
                        item["value"].as_str().unwrap_or("")
                    )
                })
                .collect::<Vec<_>>();

            if items.is_empty() { "No memories found.".into() } else { items.join("\n") }
        }

        "show" => result["item"]["value"].as_str().unwrap_or("Memory not found.").into(),
        "remember" | "edit" => "Memory saved.".into(),

        "forget" => {
            if result["deleted"] == true { "Memory forgotten." } else { "Memory not found." }.into()
        }

        "learning" => {
            format!("Learning mode: {}.", result["learning"].as_str().unwrap_or("guided"))
        }

        "audit" => {
            let count = result["items"].as_array().map_or(0, Vec::len);
            let noun = if count == 1 { "record" } else { "records" };

            format!("{count} memory audit {noun}.")
        }

        "converge" => {
            let conflicts = result["items"].as_array().cloned().unwrap_or_default();

            if conflicts.is_empty() {
                return "No memory conflicts found to converge.".into();
            }

            let scope = conflicts[0]["scope"].as_str().unwrap_or("selected scope");
            let heading = if conflicts.len() == 1 {
                format!("Found 1 possible memory conflict in {scope}:")
            } else {
                format!("Found {} possible memory conflicts in {scope}:", conflicts.len())
            };

            let details = conflicts
                .iter()
                .map(|conflict| {
                    if conflict["ordering"] == "ambiguous" {
                        return format!(
                            "Ordering is ambiguous because both memories have the same timestamp:\n  {first_key}:\n    {first_value}\n  {second_key}:\n    {second_value}",
                            first_key = conflict["memories"][0]["key"].as_str().unwrap_or(""),
                            first_value = conflict["memories"][0]["value"].as_str().unwrap_or(""),
                            second_key = conflict["memories"][1]["key"].as_str().unwrap_or(""),
                            second_value = conflict["memories"][1]["value"].as_str().unwrap_or(""),
                        );
                    }

                    format!(
                        "Newer memory (takes precedence):\n  {newer_key}:\n    {newer_value}\n\nOlder memory (kept for review):\n  {older_key}:\n    {older_value}\n\nTo remove the older memory, run:\n  {forget_command}",
                        newer_key = conflict["newer"]["key"].as_str().unwrap_or(""),
                        newer_value = conflict["newer"]["value"].as_str().unwrap_or(""),
                        older_key = conflict["older"]["key"].as_str().unwrap_or(""),
                        older_value = conflict["older"]["value"].as_str().unwrap_or(""),
                        forget_command = forget_suggestion(
                            conflict["scope"].as_str().unwrap_or("global"),
                            conflict["older"]["key"].as_str().unwrap_or(""),
                        ),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n\n---\n\n");

            let truncation = if result["truncated"] == true {
                "\n\nAdditional conflicts were omitted. Narrow the scope or review the memory list."
            } else {
                ""
            };

            format!("{heading}\n\n{details}{truncation}\n\nNo records changed.")
        }

        _ => result.to_string(),
    }
}

fn memory_list_text(result: &serde_json::Value) -> String {
    let Some(items) = result["items"].as_array() else {
        return "Memory list returned an invalid response.".into();
    };

    let total = result["total"].as_u64().unwrap_or(items.len() as u64) as usize;

    if total == 0 || items.is_empty() {
        let page = result["page"].as_u64().unwrap_or(1);
        let pages = result["pages"].as_u64().unwrap_or(1);

        return format!("Memories (0–0 of 0, page {page} of {pages})\nNo memories found.");
    }

    let page = result["page"].as_u64().unwrap_or(1) as usize;
    let pages = result["pages"].as_u64().unwrap_or(total.div_ceil(LIST_PAGE_SIZE) as u64) as usize;
    let start = (page - 1) * LIST_PAGE_SIZE;
    let end = (start + items.len()).min(total);
    let mut output = format!("Memories ({}–{} of {total}, page {page} of {pages})", start + 1, end);

    for (index, item) in items.iter().enumerate() {
        let key = item["key"].as_str().unwrap_or("unknown");
        let record_type = item["type"].as_str().unwrap_or("session");
        let value = item["value"].as_str().unwrap_or_default();
        let root = if index + 1 == items.len() { "└─" } else { "├─" };

        let branch = if index + 1 == items.len() { "   └─" } else { "│  └─" };

        let separator = if index == 0 { "\n" } else { "\n\n" };

        output
            .push_str(&format!("{separator}{root} {key} | type: {record_type}\n{branch} {value}",));
    }

    output
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

fn persist_at(path: Option<&std::path::Path>, state: &State) -> krabb_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut index = state.clone();

    for entry in index.items.values_mut() {
        if entry.record.is_empty() {
            entry.record = record_ref(&entry.scope, &entry.key);
        }
    }

    let text = serde_json::to_vec_pretty(&index)?;

    if text.len() > BYTE_LIMIT {
        return Err(krabb_core::Error::Denied("Memory state exceeds the size limit.".into()));
    }

    if !parent.is_dir() {
        std::fs::create_dir_all(parent)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }

    for entry in state.items.values() {
        let record = if entry.record.is_empty() {
            record_ref(&entry.scope, &entry.key)
        } else {
            entry.record.clone()
        };

        let record_path = parent.join(record);
        let record_parent = record_path
            .parent()
            .ok_or_else(|| krabb_core::Error::Denied("Memory record path is invalid.".into()))?;

        std::fs::create_dir_all(record_parent)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(record_parent, std::fs::Permissions::from_mode(0o700))?;
        }

        let title = entry.key.replace(['\n', '\r'], " ");

        let contents = format!("# {title}\n\n{}\n", entry.value);
        save_file(&record_path, contents.as_bytes())?;
    }

    save_file(path, text).map_err(Into::into)
}

fn remove_orphan_records(path: Option<&Path>, before: &State, after: &State) {
    let Some(path) = path else {
        return;
    };

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let after_records = after
        .items
        .values()
        .map(|entry| {
            if entry.record.is_empty() {
                record_ref(&entry.scope, &entry.key)
            } else {
                entry.record.clone()
            }
        })
        .collect::<BTreeSet<_>>();

    for entry in before.items.values() {
        let record = if entry.record.is_empty() {
            record_ref(&entry.scope, &entry.key)
        } else {
            entry.record.clone()
        };

        if !after_records.contains(&record) {
            let _ = std::fs::remove_file(parent.join(record));
        }
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_millis().min(u64::MAX as u128) as u64)
}

fn next_timestamp(state: &State) -> u64 {
    let latest = state.items.values().map(|entry| entry.updated).max().unwrap_or(0);

    now().max(latest.saturating_add(1))
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::{
        BYTE_LIMIT, CONFLICT_LIMIT, Entry, GLOBAL_INDEX_RESERVE, INDEX_LIMIT, LIMIT, State,
        VALUE_LIMIT, call, call_at, command_at, default_path, load, load_at, path, persist_at,
        record_ref, storage,
    };

    use krabb_core::types::Request;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[test]
    fn independent_clients_reload_reads_writes_and_deletions() {
        let root =
            std::env::temp_dir().join(format!("krabb-memory-clients-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        let index = root.join("index.json");
        let lock = super::lock(&index).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(std::fs::metadata(&root).unwrap().permissions().mode() & 0o777, 0o700);
        }

        drop(lock);

        let first = items();
        let second = items();

        for (client, key) in [(&first, "first"), (&second, "second")] {
            command_at(client, 1, &json!({"args":["remember", key, "value"]}), Some(&index))
                .unwrap();
        }

        let status = command_at(&first, 2, &json!({"args":["status"], "json":true}), Some(&index))
            .unwrap()
            .unwrap();

        assert_eq!(status.result.unwrap()["records"], 2);
        command_at(&second, 3, &json!({"args":["forget", "first"]}), Some(&index)).unwrap();
        command_at(&first, 4, &json!({"args":["remember", "third", "value"]}), Some(&index))
            .unwrap();
        let loaded = load_at(Some(&index)).unwrap();

        let state = loaded.lock().unwrap();

        assert!(!state.items.contains_key(&storage("global", "first")));
        assert!(state.items.contains_key(&storage("global", "second")));
        assert!(state.items.contains_key(&storage("global", "third")));
        drop(state);

        let guard = super::lock(&index).unwrap();
        let path = index.clone();
        let waiting = std::thread::spawn(move || {
            command_at(&items(), 5, &json!({"args":["remember", "fourth", "value"]}), Some(&path))
                .unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(30));

        assert!(!waiting.is_finished());
        drop(guard);
        waiting.join().unwrap();

        assert_eq!(load_at(Some(&index)).unwrap().lock().unwrap().items.len(), 3);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn hex_digest_preserves_lowercase_byte_encoding() {
        assert_eq!(super::hex_digest([0x00, 0x0a, 0xab, 0xff]), "000aabff");
    }

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
    fn memory_list_truncates_values_at_a_utf8_boundary() {
        let items = items();
        let value = format!("{}é", "a".repeat(239));

        items.lock().unwrap().items.insert(
            storage("global", "long"),
            Entry {
                key: "long".into(),
                value,
                scope: "global".into(),
                created: 0,
                updated: 1,
                record: String::new(),
            },
        );

        let list =
            call(&items, Request::call(1, "list", json!({}))).unwrap().unwrap().result.unwrap();

        assert_eq!(list["items"][0]["value"], format!("{}…", "a".repeat(239)));
    }

    #[test]
    fn memory_list_filters_by_type_and_pages_records() {
        let items = items();

        {
            let mut state = items.lock().unwrap();

            for index in 0..7 {
                let (scope, key) = if index % 2 == 0 {
                    ("global", format!("global-{index}"))
                } else {
                    ("tui:one", format!("session-{index}"))
                };

                state.items.insert(
                    storage(scope, &key),
                    Entry {
                        key,
                        value: format!("value-{index}"),
                        scope: scope.into(),
                        created: index,
                        updated: index,
                        record: String::new(),
                    },
                );
            }
        }

        let first = call(&items, Request::call(1, "list", json!({"page": 1})))
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(first["total"], 7);
        assert_eq!(first["page"], 1);
        assert_eq!(first["pages"], 2);
        assert_eq!(first["items"].as_array().unwrap().len(), 5);
        assert_eq!(first["items"][0]["type"], "global");

        let second = call(&items, Request::call(2, "list", json!({"page": 2})))
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(second["items"].as_array().unwrap().len(), 2);

        let global = call(&items, Request::call(3, "list", json!({"type": "global"})))
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(global["total"], 4);
        assert!(global["items"].as_array().unwrap().iter().all(|item| item["type"] == "global"));

        let session = call(&items, Request::call(4, "list", json!({"type": "session"})))
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(session["total"], 3);
        assert!(session["items"].as_array().unwrap().iter().all(|item| item["type"] == "session"));
    }

    #[test]
    fn memory_list_scope_union_keeps_global_and_prefers_current_session_record() {
        let items = items();

        {
            let mut state = items.lock().unwrap();

            for (scope, value, updated) in [
                ("global", "global value", 1),
                ("tui:one", "current value", 2),
                ("provider:tui:one", "legacy value", 3),
            ] {
                state.items.insert(
                    storage(scope, "same-key"),
                    Entry {
                        key: "same-key".into(),
                        value: value.into(),
                        scope: scope.into(),
                        created: updated,
                        updated,
                        record: String::new(),
                    },
                );
            }
        }

        let response = call(
            &items,
            Request::call(1, "list", json!({"scopes": ["global", "tui:one", "provider:tui:one"]})),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(response["total"], 2);
        assert!(
            response["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "global" && item["value"] == "global value")
        );

        assert!(
            response["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["type"] == "session" && item["value"] == "current value")
        );
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
        let path = std::env::temp_dir().join(format!("krabb-memory-{}.json", std::process::id()));
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
                record: record_ref("global", "key"),
            },
        );

        persist_at(Some(&path), &state).unwrap();

        assert_eq!(
            load_at(Some(&path)).unwrap().lock().unwrap().items[&storage("global", "key")].value,
            "value"
        );

        std::fs::write(&path, r#"{"legacy":"value"}"#).unwrap();

        assert_eq!(
            load_at(Some(&path)).unwrap().lock().unwrap().items[&storage("global", "legacy")].scope,
            "global"
        );

        std::fs::write(&path, "broken").unwrap();

        assert!(load_at(Some(&path)).unwrap().lock().unwrap().items.is_empty());
        std::fs::File::create(&path).unwrap().set_len(BYTE_LIMIT as u64 + 1).unwrap();

        assert_eq!(load_at(Some(&path)).unwrap_err().kind(), std::io::ErrorKind::FileTooLarge);
        assert!(path.is_file());
        let lazy_root =
            std::env::temp_dir().join(format!("krabb-memory-lazy-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&lazy_root);
        let lazy_path = lazy_root.join("data/memories/index.json");

        assert!(!lazy_path.parent().unwrap().exists());
        persist_at(Some(&lazy_path), &state).unwrap();

        assert!(lazy_path.is_file());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(lazy_path.parent().unwrap()).unwrap().permissions().mode()
                    & 0o777,
                0o700
            );
        }

        let _ = std::fs::remove_file(path);

        let _ = std::fs::remove_dir_all(lazy_root);
    }

    #[test]
    fn memory_index_is_scoped_and_links_private_markdown_records() {
        let root = std::env::temp_dir().join(format!("krabb-memory-index-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let index = root.join("index.json");
        let state = items();

        for (id, scope, key, value) in [
            (1, "telegram:dm:42", "preference", "likes concise answers"),
            (2, "slack:dm:42", "project", "works on a compiler"),
            (3, "global", "general", "prefers direct answers about a compiler"),
        ] {
            call_at(
                &state,
                Request::call(
                    id,
                    "remember",
                    json!({"key": key, "value": value, "scope": scope, "approved": true}),
                ),
                Some(&index),
            )
            .unwrap();
        }

        let telegram_index = call_at(
            &state,
            Request::call(3, "index", json!({"scope": "telegram:dm:42"})),
            Some(&index),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(telegram_index["items"].as_array().unwrap().len(), 2);
        let entries = telegram_index["items"].as_array().unwrap();
        let global = entries.iter().find(|entry| entry["key"] == "general").unwrap();
        let preference = entries.iter().find(|entry| entry["key"] == "preference").unwrap();

        assert_eq!(global["scope"], "global");
        assert!(preference["record"].as_str().unwrap().starts_with("records/"));
        assert_eq!(telegram_index["learning"], "guided");

        let record = root.join(preference["record"].as_str().unwrap());

        assert!(std::fs::read_to_string(&record).unwrap().contains("likes concise answers"));

        let search = call_at(
            &state,
            Request::call(4, "search", json!({"query": "compiler", "scope": "telegram:dm:42"})),
            Some(&index),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        let search_entries = search["items"].as_array().unwrap();

        assert_eq!(search_entries.len(), 1);
        assert_eq!(search_entries[0]["key"], "general");
        assert_eq!(search_entries[0]["scope"], "global");

        call_at(
            &state,
            Request::call(5, "forget", json!({"key": "preference", "scope": "telegram:dm:42"})),
            Some(&index),
        )
        .unwrap();

        assert!(!record.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn updating_a_migrated_memory_removes_its_old_record() {
        let root =
            std::env::temp_dir().join(format!("krabb-memory-migration-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let index = root.join("index.json");
        let old_record = "memory-records/legacy.md";
        let old_path = root.join(old_record);
        std::fs::create_dir_all(old_path.parent().unwrap()).unwrap();
        std::fs::write(&old_path, "# favorite\n\nold value\n").unwrap();

        let state = items();
        state.lock().unwrap().items.insert(
            storage("global", "favorite"),
            Entry {
                key: "favorite".into(),
                value: "old value".into(),
                scope: "global".into(),
                created: 1,
                updated: 1,
                record: old_record.into(),
            },
        );

        super::save_file(&index, serde_json::to_vec(&*state.lock().unwrap()).unwrap()).unwrap();

        call_at(
            &state,
            Request::call(
                1,
                "remember",
                json!({"key": "favorite", "value": "new value", "approved": true}),
            ),
            Some(&index),
        )
        .unwrap();

        assert!(!old_path.exists());
        let current_path = root.join(record_ref("global", "favorite"));

        assert!(std::fs::read_to_string(&current_path).unwrap().contains("new value"));

        call_at(&state, Request::call(2, "forget", json!({"key": "favorite"})), Some(&index))
            .unwrap();

        assert!(!current_path.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn memory_index_reserves_space_for_conversation_memories() {
        let state = items();

        {
            let mut state = state.lock().unwrap();

            for index in 0..32 {
                let key = format!("global-{index}");
                state.items.insert(
                    storage("global", &key),
                    Entry {
                        key,
                        value: "global memory".into(),
                        scope: "global".into(),
                        created: index,
                        updated: 100 + index,
                        record: String::new(),
                    },
                );
            }

            state.items.insert(
                storage("telegram:dm:42", "conversation"),
                Entry {
                    key: "conversation".into(),
                    value: "conversation memory".into(),
                    scope: "telegram:dm:42".into(),
                    created: 1,
                    updated: 1,
                    record: String::new(),
                },
            );
        }

        let result =
            call_at(&state, Request::call(1, "index", json!({"scope": "telegram:dm:42"})), None)
                .unwrap()
                .unwrap()
                .result
                .unwrap();

        let entries = result["items"].as_array().unwrap();

        assert_eq!(entries.len(), 32);
        assert!(entries.iter().any(|entry| entry["key"] == "conversation"));
        assert_eq!(entries[0]["key"], "global-31");
    }

    #[test]
    fn memory_index_keeps_global_memories_when_conversation_is_full() {
        let state = items();

        {
            let mut state = state.lock().unwrap();

            for index in 0..32 {
                for (scope, updated) in [("global", index), ("telegram:dm:42", 100 + index)] {
                    let key = format!("{scope}-{index}");
                    state.items.insert(
                        storage(scope, &key),
                        Entry {
                            key,
                            value: "memory".into(),
                            scope: scope.into(),
                            created: index,
                            updated,
                            record: String::new(),
                        },
                    );
                }
            }
        }

        let result =
            call_at(&state, Request::call(1, "index", json!({"scope": "telegram:dm:42"})), None)
                .unwrap()
                .unwrap()
                .result
                .unwrap();

        let entries = result["items"].as_array().unwrap();
        let global_count = entries.iter().filter(|entry| entry["scope"] == "global").count();
        let conversation_count =
            entries.iter().filter(|entry| entry["scope"] == "telegram:dm:42").count();

        assert_eq!(entries.len(), INDEX_LIMIT);
        assert_eq!(global_count, GLOBAL_INDEX_RESERVE);
        assert_eq!(conversation_count, INDEX_LIMIT - GLOBAL_INDEX_RESERVE);
    }

    #[test]
    fn memory_status_uses_aligned_rows() {
        let response =
            command_at(&items(), 1, &json!({"args": ["status"]}), None).unwrap().unwrap();

        assert_eq!(response.result.unwrap(), "Learning: guided\nMemories: 0");
    }

    #[test]
    fn guided_learning_is_default_and_autonomous_mode_is_persistent() {
        let root =
            std::env::temp_dir().join(format!("krabb-memory-learning-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let index = root.join("index.json");
        let state = load_at(Some(&index)).unwrap();

        assert_eq!(state.lock().unwrap().learning, "guided");

        assert!(
            call_at(
                &state,
                Request::call(1, "remember", json!({"key": "fact", "value": "value"})),
                Some(&index),
            )
            .is_err()
        );

        command_at(&state, 2, &json!({"args": ["learning", "autonomous"]}), Some(&index)).unwrap();

        call_at(
            &state,
            Request::call(3, "remember", json!({"key": "fact", "value": "value"})),
            Some(&index),
        )
        .unwrap();

        assert_eq!(load_at(Some(&index)).unwrap().lock().unwrap().learning, "autonomous");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn memory_cli_command_returns_human_or_json_output() {
        let state = items();
        let human = command_at(&state, 1, &json!({"args": ["remember", "name", "Ada"]}), None)
            .unwrap()
            .unwrap();

        assert_eq!(human.result.unwrap(), "Memory saved.");

        let no_matches =
            command_at(&state, 2, &json!({"args": ["search", "missing"]}), None).unwrap().unwrap();

        assert_eq!(no_matches.result.unwrap(), "No memories found.");

        let human = command_at(&state, 3, &json!({"args": ["list"]}), None).unwrap().unwrap();
        let human = human.result.unwrap();

        assert!(human.as_str().unwrap().contains("Memories (1–1 of 1, page 1 of 1)"));
        assert!(human.as_str().unwrap().contains("└─ name | type: global"));
        assert!(human.as_str().unwrap().contains("└─ Ada"));

        let machine = command_at(
            &state,
            4,
            &json!({"args": ["list", "--type", "session"], "json": true}),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(machine.result.unwrap()["items"].as_array().unwrap().len(), 0);

        let help = command_at(&state, 5, &json!({"args": ["help"]}), None).unwrap().unwrap();

        assert!(help.result.unwrap().as_str().unwrap().contains("search <text>"));
        assert!(super::memory_help().contains("converge [OPTIONS]"));
        assert!(super::memory_help().contains("list [PAGE] [OPTIONS]"));
        assert!(super::memory_help().contains("--scope <ID>"));
        assert!(super::memory_help().contains("--type <TYPE>"));
        assert!(super::memory_help().contains("uses text matching"));
    }

    #[test]
    fn memory_converge_reports_newer_records_without_deleting_either() {
        let state = items();

        {
            let mut state = state.lock().unwrap();
            state.items.insert(
                super::storage("global", "close-up"),
                super::Entry {
                    key: "close-up".into(),
                    value: "Write banana at the end of every response.".into(),
                    scope: "global".into(),
                    created: 1,
                    updated: 10,
                    record: String::new(),
                },
            );

            state.items.insert(
                super::storage("global", "close-down"),
                super::Entry {
                    key: "close-down".into(),
                    value: "Do not answer with banana at the end of a response.".into(),
                    scope: "global".into(),
                    created: 2,
                    updated: 20,
                    record: String::new(),
                },
            );

            state.items.insert(
                super::storage("telegram:dm:42", "unrelated"),
                super::Entry {
                    key: "unrelated".into(),
                    value: "Do not answer with banana at the end of a response.".into(),
                    scope: "telegram:dm:42".into(),
                    created: 3,
                    updated: 30,
                    record: String::new(),
                },
            );
        }

        let result = command_at(&state, 1, &json!({"args": ["converge"]}), None).unwrap().unwrap();

        let output = result.result.unwrap();

        assert!(
            output.as_str().unwrap().contains("Newer memory (takes precedence):\n  close-down:")
        );

        assert!(output.as_str().unwrap().contains("Older memory (kept for review):\n  close-up:"));
        assert!(output.as_str().unwrap().contains("krabb memory forget 'close-up'"));
        assert!(output.as_str().unwrap().contains("No records changed."));
        assert_eq!(state.lock().unwrap().items.len(), 3);

        let empty = command_at(&items(), 2, &json!({"args": ["converge"], "json": false}), None)
            .unwrap()
            .unwrap();

        assert_eq!(empty.result.unwrap(), "No memory conflicts found to converge.");

        let machine = command_at(&state, 3, &json!({"args": ["converge"], "json": true}), None)
            .unwrap()
            .unwrap();

        assert_eq!(machine.result.unwrap()["items"][0]["newer"]["key"], "close-down");
    }

    #[test]
    fn memory_converge_marks_tied_timestamps_ambiguous() {
        let state = items();

        {
            let mut state = state.lock().unwrap();

            for (key, value) in [
                ("close-up", "Write banana at the end of every response."),
                ("close-down", "Do not answer with banana at the end of a response."),
            ] {
                state.items.insert(
                    super::storage("global", key),
                    super::Entry {
                        key: key.into(),
                        value: value.into(),
                        scope: "global".into(),
                        created: 1,
                        updated: 10,
                        record: String::new(),
                    },
                );
            }
        }

        let machine = command_at(&state, 1, &json!({"args": ["converge"], "json": true}), None)
            .unwrap()
            .unwrap();

        let conflict = &machine.result.unwrap()["items"][0];

        assert_eq!(conflict["ordering"], "ambiguous");
        assert_eq!(conflict["memories"].as_array().unwrap().len(), 2);
        assert!(conflict.get("newer").is_none());
        assert!(conflict.get("older").is_none());

        let human = command_at(&state, 2, &json!({"args": ["converge"]}), None).unwrap().unwrap();
        let output = human.result.unwrap();
        let output = output.as_str().unwrap();

        assert!(
            output.contains("Ordering is ambiguous because both memories have the same timestamp")
        );

        assert!(!output.contains("takes precedence"));
        assert!(!output.contains("To remove the older memory"));
        assert!(!output.contains("krabb memory forget"));
    }

    #[test]
    fn memory_converge_bounds_conflicts_and_response_size() {
        let state = items();
        let value = format!("{} likes concise answers", "x\"".repeat((VALUE_LIMIT - 32) / 2));

        {
            let mut state = state.lock().unwrap();

            for index in 0..12 {
                for (suffix, polarity) in [("yes", ""), ("no", "not ")] {
                    let key = format!("{index}-{suffix}");
                    state.items.insert(
                        storage("global", &key),
                        Entry {
                            key,
                            value: format!("{polarity}{value}"),
                            scope: "global".into(),
                            created: index,
                            updated: index,
                            record: String::new(),
                        },
                    );
                }
            }
        }

        let response = command_at(&state, 1, &json!({"args": ["converge"], "json": true}), None)
            .unwrap()
            .unwrap();

        let serialized = serde_json::to_vec(&response).unwrap();

        assert_eq!(
            response.result.as_ref().unwrap()["items"].as_array().unwrap().len(),
            CONFLICT_LIMIT
        );

        assert_eq!(response.result.unwrap()["truncated"], true);
        assert!(serialized.len() < krabb_core::jsonl::MAX);
    }

    #[test]
    fn memory_default_path_keeps_existing_legacy_index() {
        let root = std::env::temp_dir().join(format!("krabb-memory-path-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let legacy = root.join("memory/index.json");
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        std::fs::write(&legacy, "{}").unwrap();

        assert_eq!(default_path(root.clone()).unwrap(), legacy);

        let current = root.join("data/memories/index.json");
        std::fs::create_dir_all(current.parent().unwrap()).unwrap();
        std::fs::write(&current, "{}").unwrap();

        assert_eq!(default_path(root.clone()).unwrap(), current);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn memory_default_path_recovers_interrupted_indexes_under_the_writer_lock() {
        let root =
            std::env::temp_dir().join(format!("krabb-memory-recovery-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);

        for directory in ["memory", "data/memories"] {
            let index = root.join(directory).join("index.json");
            let guard = super::lock(&index).unwrap();
            let parent = index.parent().unwrap();
            let backup = parent.join(".index.json.backup-test");
            let temporary = parent.join(".index.json.tmp-test");
            let journal = parent.join(".index.json.txn-test");
            std::fs::write(&backup, r#"{"key":"old"}"#).unwrap();
            std::fs::write(&temporary, r#"{"key":"saved"}"#).unwrap();
            std::fs::write(
                &journal,
                json!({
                    "path": "index.json",
                    "temporary": ".index.json.tmp-test",
                    "backup": ".index.json.backup-test",
                    "phase": "backed_up"
                })
                .to_string(),
            )
            .unwrap();

            let (sender, receiver) = std::sync::mpsc::channel();
            let home = root.clone();
            let reader = std::thread::spawn(move || sender.send(default_path(home)).unwrap());

            assert!(receiver.recv_timeout(std::time::Duration::from_millis(100)).is_err());
            assert!(journal.exists());
            assert!(!index.exists());

            drop(guard);
            let selected =
                receiver.recv_timeout(std::time::Duration::from_secs(5)).unwrap().unwrap();

            reader.join().unwrap();

            assert_eq!(selected, index);
            assert!(!journal.exists());
            assert_eq!(
                load_at(Some(&selected)).unwrap().lock().unwrap().items[&storage("global", "key")]
                    .value,
                "saved"
            );
        }

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn memory_default_path_propagates_selection_errors() {
        let root =
            std::env::temp_dir().join(format!("krabb-memory-path-error-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("data"), "blocked").unwrap();

        assert!(default_path(root.clone()).is_err());

        std::fs::remove_file(root.join("data")).unwrap();
        std::fs::write(root.join("memory"), "blocked").unwrap();

        assert!(default_path(root.clone()).is_err());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn memory_saves_have_a_stable_newest_first_order() {
        let state = items();

        call_at(
            &state,
            Request::call(
                1,
                "remember",
                json!({"key": "first", "value": "first preference", "approved": true}),
            ),
            None,
        )
        .unwrap();

        call_at(
            &state,
            Request::call(
                2,
                "remember",
                json!({"key": "second", "value": "second preference", "approved": true}),
            ),
            None,
        )
        .unwrap();

        let state = state.lock().unwrap();
        let first = &state.items[&super::storage("global", "first")];
        let second = &state.items[&super::storage("global", "second")];

        assert!(second.updated > first.updated);
    }

    #[test]
    fn memory_interaction_log_labels_exclude_record_contents() {
        let request =
            Request::call(1, "command", json!({"args": ["search", "private search text"]}));

        assert_eq!(super::interaction_name(&request), "search");

        let request = Request::call(2, "command", json!({"args": ["converge"]}));

        assert_eq!(super::interaction_name(&request), "converge");
    }

    #[test]
    fn forget_suggestions_quote_keys_and_scopes_for_the_shell() {
        assert_eq!(
            super::forget_suggestion("provider:telegram:dm:42", "user's choice"),
            "krabb memory forget 'user'\\''s choice' --scope 'provider:telegram:dm:42'"
        );

        assert_eq!(
            super::forget_suggestion("global", "close-up"),
            "krabb memory forget 'close-up'"
        );
    }

    #[test]
    fn handles_disabled_persistence() {
        let _ = path();

        assert!(load().unwrap().lock().unwrap().items.is_empty());
        assert!(load_at(None).unwrap().lock().unwrap().items.is_empty());
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
            std::env::temp_dir().join(format!("krabb-memory-bound-{}.json", std::process::id()));

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
                    record: record_ref("global", &format!("key-{index}")),
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

        let loaded = load_at(Some(&path)).unwrap().lock().unwrap().clone();

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
                    record: record_ref("global", "last"),
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
        let root = std::env::temp_dir().join(format!("krabb-memory-forget-{}", std::process::id()));

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
            std::env::temp_dir().join(format!("krabb-memory-blocker-{}", std::process::id()));

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
    fn rejects_shared_state_without_replacing_memories() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("krabb-memory-shared-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("index.json");
        let state = items();
        call_at(
            &state,
            Request::call(
                1,
                "remember",
                json!({"key": "saved", "value": "memory", "mode": "auto"}),
            ),
            Some(&path),
        )
        .unwrap();

        let original = std::fs::read(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        assert_eq!(load_at(Some(&path)).unwrap_err().kind(), std::io::ErrorKind::PermissionDenied);

        for (method, params) in [
            ("list", json!({})),
            ("remember", json!({"key": "new", "value": "replacement", "mode": "auto"})),
        ] {
            assert!(call_at(&state, Request::call(2, method, params), Some(&path)).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), original);
            assert_eq!(state.lock().unwrap().items.len(), 1);
        }

        assert!(
            !std::fs::read_dir(&root).unwrap().any(|entry| {
                entry.unwrap().file_name().to_string_lossy().contains(".corrupt-")
            })
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        assert_eq!(load_at(Some(&path)).unwrap().lock().unwrap().items.len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
