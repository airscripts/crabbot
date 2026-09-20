#![forbid(unsafe_code)]

#[cfg(not(test))]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crabbot_core::types::{Request, Response};
#[cfg(not(test))]
use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol},
};

use rusqlite::{Connection, OptionalExtension, params};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const LIMIT: i64 = 1_000;
const KEY_LIMIT: usize = 4 * 1024;
const VALUE_LIMIT: usize = 256 * 1024;
const FRAME_HEADROOM: usize = 64 * 1024;
const DB_LIMIT: i64 = 32 * 1024 * 1024;

#[tokio::main]
#[cfg(not(test))]
async fn main() -> crabbot_core::Result<()> {
    let path = std::env::var_os("CRABBOT_DB")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("CRABBOT_HOME").map(PathBuf::from).map(|home| home.join("crabbot.db"))
        })
        .ok_or_else(|| {
            crabbot_core::Error::Denied("CRABBOT_DB or CRABBOT_HOME is required.".into())
        })?;

    let db = Connection::open(&path)
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite open failed: {error}.")))?;

    private(&path)?;
    migrate(&db)?;
    private_sidecars(&path)?;
    let db = Arc::new(Mutex::new(db));
    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "sqlite".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Store],
            commands: vec![],
        },
        move |request| {
            let db = Arc::clone(&db);
            async move { store(&db, request) }
        },
    )
    .await
}

fn private(path: &std::path::Path) -> crabbot_core::Result<()> {
    #[cfg(unix)]
    {
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

#[cfg(not(test))]
fn private_sidecars(path: &std::path::Path) -> crabbot_core::Result<()> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));

        if sidecar.exists() {
            private(&sidecar)?;
        }
    }

    Ok(())
}

fn store(db: &Arc<Mutex<Connection>>, request: Request) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let db =
        db.lock().map_err(|_| crabbot_core::Error::Denied("SQLite lock is poisoned.".into()))?;

    let result = match method.as_str() {
        "put" => {
            let key = key(&params, "put.key")?;
            let value = value(&params["value"], "put.value")?;
            db.execute(
                "INSERT INTO item(key, value, updated) VALUES(?1, ?2, ?3) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated = excluded.updated",
                params![key, value, now()],
            )
            .map_err(|error| crabbot_core::Error::Denied(format!("SQLite put failed: {error}.")))?;
            serde_json::json!({"ok": true})
        }

        "get" => {
            let key = key(&params, "get.key")?;
            let value = db
                .query_row("SELECT value FROM item WHERE key = ?1", params![key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()
                .map_err(|error| {
                    crabbot_core::Error::Denied(format!("SQLite get failed: {error}."))
                })?;

            match value {
                Some(value) => {
                    serde_json::json!({"found": true, "value": serde_json::from_str::<serde_json::Value>(&value)?})
                }

                None => serde_json::json!({"found": false}),
            }
        }

        "delete" => {
            let key = key(&params, "delete.key")?;
            let count =
                db.execute("DELETE FROM item WHERE key = ?1", params![key]).map_err(|error| {
                    crabbot_core::Error::Denied(format!("SQLite delete failed: {error}."))
                })?;

            serde_json::json!({"deleted": count == 1})
        }

        "lease" => lease(&db, &params)?,
        "release" => release(&db, &params)?,
        "enqueue" => enqueue(&db, &params)?,
        "outbox" => outbox(&db, &params)?,
        "ack" => ack(&db, &params)?,
        "retry" => retry(&db, &params)?,
        "seen" => seen(&db, &params)?,
        _ => return Ok(None),
    };

    let response = Response::ok(id, result);

    if serde_json::to_vec(&response)?.len().saturating_add(1 + FRAME_HEADROOM)
        > crabbot_core::jsonl::MAX
    {
        return Err(crabbot_core::Error::Denied(
            "SQLite response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(Some(response))
}

fn migrate(db: &Connection) -> crabbot_core::Result<()> {
    db.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS item (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS lease (name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, dedupe TEXT NOT NULL UNIQUE, value TEXT NOT NULL, created INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, expires INTEGER NOT NULL);
         INSERT OR IGNORE INTO schema_version(version, applied) VALUES(1, strftime('%s', 'now'));",
    )
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite migration failed: {error}.")))?;

    let page_size =
        db.query_row("PRAGMA page_size;", [], |row| row.get::<_, i64>(0)).map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let page_count =
        db.query_row("PRAGMA page_count;", [], |row| row.get::<_, i64>(0)).map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let max_pages = DB_LIMIT / page_size.max(1);

    if page_count > max_pages {
        return Err(crabbot_core::Error::Denied("SQLite database exceeds the size limit.".into()));
    }

    db.query_row(&format!("PRAGMA max_page_count = {max_pages};"), [], |row| row.get::<_, i64>(0))
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let mut columns = db.prepare("PRAGMA table_info(item)").map_err(|error| {
        crabbot_core::Error::Denied(format!("SQLite migration failed: {error}."))
    })?;

    let has_updated = columns
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite migration failed: {error}.")))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite migration failed: {error}.")))?
        .iter()
        .any(|name| name == "updated");

    if !has_updated {
        db.execute_batch("ALTER TABLE item ADD COLUMN updated INTEGER NOT NULL DEFAULT 0;")
            .map_err(|error| {
                crabbot_core::Error::Denied(format!("SQLite migration failed: {error}."))
            })?;
    }

    Ok(())
}

fn key<'a>(params: &'a serde_json::Value, label: &str) -> crabbot_core::Result<&'a str> {
    let key = params["key"]
        .as_str()
        .ok_or_else(|| crabbot_core::Error::Denied(format!("{label} is required.")))?;

    if key.trim().is_empty() {
        return Err(crabbot_core::Error::Denied(format!("{label} cannot be empty.")));
    }

    if key.len() > KEY_LIMIT {
        return Err(crabbot_core::Error::Denied(format!("{label} exceeds the size limit.")));
    }

    Ok(key)
}

fn value(value: &serde_json::Value, label: &str) -> crabbot_core::Result<String> {
    let value = serde_json::to_string(value)?;

    if value.len() > VALUE_LIMIT {
        return Err(crabbot_core::Error::Denied(format!("{label} exceeds the size limit.")));
    }

    Ok(value)
}

fn lease(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let name = params["name"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("lease.name is required.".into()))?;

    let owner = params["owner"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("lease.owner is required.".into()))?;

    let ttl = params["ttl"].as_u64().unwrap_or(30).clamp(1, 86_400) as i64;
    let expires = now() + ttl;
    let count = db
        .execute(
            "INSERT INTO lease(name, owner, expires) VALUES(?1, ?2, ?3) ON CONFLICT(name) DO UPDATE SET owner = excluded.owner, expires = excluded.expires WHERE lease.expires <= ?4 OR lease.owner = excluded.owner",
            params![name, owner, expires, now()],
        )
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite lease failed: {error}.")))?;

    Ok(serde_json::json!({"acquired": count == 1, "expires": expires}))
}

fn release(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let name = params["name"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("release.name is required.".into()))?;

    let owner = params["owner"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crabbot_core::Error::Denied("release.owner is required.".into()))?;

    let count = db
        .execute("DELETE FROM lease WHERE name = ?1 AND owner = ?2", params![name, owner])
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite release failed: {error}.")))?;

    Ok(serde_json::json!({"released": count == 1}))
}

fn enqueue(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let dedupe = key(params, "enqueue.key")?;
    let value = value(&params["value"], "enqueue.value")?;
    let existing = db
        .query_row("SELECT id FROM outbox WHERE dedupe = ?1", params![dedupe], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite enqueue lookup failed: {error}."))
        })?;

    if let Some(id) = existing {
        return Ok(serde_json::json!({"id": id}));
    }

    let count: i64 =
        db.query_row("SELECT COUNT(*) FROM outbox", [], |row| row.get(0)).map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite outbox count failed: {error}."))
        })?;

    if count >= LIMIT {
        return Err(crabbot_core::Error::Denied(
            "SQLite outbox is full; no message was discarded.".into(),
        ));
    }

    db.execute(
        "INSERT INTO outbox(dedupe, value, created) VALUES(?1, ?2, ?3) ON CONFLICT(dedupe) DO NOTHING",
        params![dedupe, value, now()],
    )
    .map_err(|error| crabbot_core::Error::Denied(format!("SQLite enqueue failed: {error}.")))?;

    let id: i64 = db
        .query_row("SELECT id FROM outbox WHERE dedupe = ?1", params![dedupe], |row| row.get(0))
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite enqueue lookup failed: {error}."))
        })?;

    Ok(serde_json::json!({"id": id}))
}

fn outbox(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let limit = params["limit"].as_u64().unwrap_or(50).clamp(1, 100) as i64;
    let mut statement = db
        .prepare("SELECT id, dedupe, value, attempts FROM outbox ORDER BY id LIMIT ?1")
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite outbox failed: {error}.")))?;

    let rows = statement
        .query_map(params![limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite outbox read failed: {error}."))
        })?;

    let mut items = Vec::new();

    for row in rows {
        let (id, key, value, attempts) = row.map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite outbox row failed: {error}."))
        })?;

        items.push(serde_json::json!({
            "id": id,
            "key": key,
            "value": serde_json::from_str::<serde_json::Value>(&value)?,
            "attempts": attempts,
        }));
    }

    Ok(serde_json::json!({"items": items}))
}

fn ack(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let id = params["id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| crabbot_core::Error::Denied("ack.id is required.".into()))?;

    let count = db
        .execute("DELETE FROM outbox WHERE id = ?1", params![id])
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite ack failed: {error}.")))?;

    Ok(serde_json::json!({"acked": count == 1}))
}

fn retry(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let id = params["id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| crabbot_core::Error::Denied("retry.id is required.".into()))?;

    let count = db
        .execute("UPDATE outbox SET attempts = attempts + 1 WHERE id = ?1", params![id])
        .map_err(|error| crabbot_core::Error::Denied(format!("SQLite retry failed: {error}.")))?;

    Ok(serde_json::json!({"retried": count == 1}))
}

fn seen(db: &Connection, params: &serde_json::Value) -> crabbot_core::Result<serde_json::Value> {
    let value = key(params, "seen.key")?;
    let ttl = params["ttl"].as_u64().unwrap_or(86_400).clamp(1, 2_592_000) as i64;
    let now = now();
    db.execute("DELETE FROM seen WHERE expires <= ?1", params![now]).map_err(|error| {
        crabbot_core::Error::Denied(format!("SQLite deduplication failed: {error}."))
    })?;

    let count = db
        .execute(
            "INSERT INTO seen(key, expires) VALUES(?1, ?2) ON CONFLICT(key) DO NOTHING",
            params![value, now + ttl],
        )
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("SQLite deduplication failed: {error}."))
        })?;

    Ok(serde_json::json!({"new": count == 1}))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::{DB_LIMIT, migrate, private, store};

    use crabbot_core::types::Request;
    use rusqlite::Connection;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn db() -> Arc<Mutex<Connection>> {
        let db = Connection::open_in_memory().unwrap();
        migrate(&db).unwrap();
        Arc::new(Mutex::new(db))
    }

    #[test]
    fn applies_the_actual_page_size_to_the_database_cap() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA page_size = 65536;").unwrap();
        migrate(&db).unwrap();
        let page_size: i64 = db.query_row("PRAGMA page_size;", [], |row| row.get(0)).unwrap();
        let max_pages: i64 = db.query_row("PRAGMA max_page_count;", [], |row| row.get(0)).unwrap();
        assert!(max_pages.saturating_mul(page_size) <= DB_LIMIT);
    }

    #[test]
    fn store_can_put_get_and_delete() {
        let db = db();
        let put = store(&db, Request::call(1, "put", json!({"key": "answer", "value": 42})))
            .unwrap()
            .unwrap();

        assert_eq!(put.result.unwrap()["ok"], true);

        let get = store(&db, Request::call(2, "get", json!({"key": "answer"}))).unwrap().unwrap();
        assert_eq!(get.result.unwrap()["value"], 42);

        let deleted =
            store(&db, Request::call(3, "delete", json!({"key": "answer"}))).unwrap().unwrap();

        assert_eq!(deleted.result.unwrap()["deleted"], true);

        let missing =
            store(&db, Request::call(4, "get", json!({"key": "answer"}))).unwrap().unwrap();

        assert_eq!(missing.result.unwrap()["found"], false);
    }

    #[test]
    fn store_validates_requests() {
        let db = db();

        for method in ["put", "get", "delete"] {
            assert!(store(&db, Request::call(1, method, json!({}))).is_err());
        }

        assert!(store(&db, Request::call(2, "unknown", json!({}))).unwrap().is_none());
        let note = Request::Note { jsonrpc: "2.0".into(), method: "get".into(), params: json!({}) };
        assert!(store(&db, note).unwrap().is_none());
    }

    #[test]
    fn store_rejects_corrupt_values() {
        let db = db();
        db.lock()
            .unwrap()
            .execute("INSERT INTO item(key, value, updated) VALUES('broken', 'not json', 0)", [])
            .unwrap();

        assert!(store(&db, Request::call(1, "get", json!({"key": "broken"}))).is_err());
        let updated =
            store(&db, Request::call(2, "put", json!({"key": "broken", "value": {"ok": true}})))
                .unwrap()
                .unwrap();

        assert_eq!(updated.result.unwrap()["ok"], true);
        db.lock()
            .unwrap()
            .execute(
                "INSERT INTO outbox(dedupe, value, created) VALUES('broken', 'not json', 0)",
                [],
            )
            .unwrap();

        assert!(store(&db, Request::call(3, "outbox", json!({}))).is_err());
        let deleted =
            store(&db, Request::call(4, "delete", json!({"key": "missing"}))).unwrap().unwrap();

        assert_eq!(deleted.result.unwrap()["deleted"], false);
    }

    #[test]
    fn store_supports_leases_outbox_and_deduplication() {
        let db = db();
        let acquired = store(
            &db,
            Request::call(1, "lease", json!({"name": "bridge", "owner": "one", "ttl": 30})),
        )
        .unwrap()
        .unwrap();

        assert_eq!(acquired.result.unwrap()["acquired"], true);
        let contested =
            store(&db, Request::call(2, "lease", json!({"name": "bridge", "owner": "two"})))
                .unwrap()
                .unwrap();

        assert_eq!(contested.result.unwrap()["acquired"], false);
        let renewed =
            store(&db, Request::call(3, "lease", json!({"name": "bridge", "owner": "one"})))
                .unwrap()
                .unwrap();

        assert_eq!(renewed.result.unwrap()["acquired"], true);
        let released =
            store(&db, Request::call(4, "release", json!({"name": "bridge", "owner": "one"})))
                .unwrap()
                .unwrap();

        assert_eq!(released.result.unwrap()["released"], true);

        let first = store(
            &db,
            Request::call(5, "enqueue", json!({"key": "event-1", "value": {"ok": true}})),
        )
        .unwrap()
        .unwrap();

        let second = store(
            &db,
            Request::call(6, "enqueue", json!({"key": "event-1", "value": {"ok": false}})),
        )
        .unwrap()
        .unwrap();

        assert_eq!(first.result.unwrap()["id"], second.result.unwrap()["id"]);
        let listed = store(&db, Request::call(7, "outbox", json!({"limit": 10}))).unwrap().unwrap();
        assert_eq!(listed.result.as_ref().unwrap()["items"].as_array().unwrap().len(), 1);
        let id = listed.result.as_ref().unwrap()["items"][0]["id"].clone();
        let retried = store(&db, Request::call(8, "retry", json!({"id": id}))).unwrap().unwrap();
        assert_eq!(retried.result.unwrap()["retried"], true);
        let retried = store(&db, Request::call(9, "outbox", json!({}))).unwrap().unwrap();
        assert_eq!(retried.result.unwrap()["items"][0]["attempts"], 1);
        let acked = store(&db, Request::call(10, "ack", json!({"id": id}))).unwrap().unwrap();
        assert_eq!(acked.result.unwrap()["acked"], true);
        let seen =
            store(&db, Request::call(11, "seen", json!({"key": "event-1"}))).unwrap().unwrap();

        assert_eq!(seen.result.unwrap()["new"], true);
        let duplicate =
            store(&db, Request::call(12, "seen", json!({"key": "event-1"}))).unwrap().unwrap();

        assert_eq!(duplicate.result.unwrap()["new"], false);
    }

    #[test]
    fn store_rejects_reliability_requests_without_keys() {
        let db = db();

        for (method, params) in [
            ("lease", json!({"owner": "one"})),
            ("release", json!({"name": "bridge"})),
            ("enqueue", json!({"value": true})),
            ("ack", json!({"id": 0})),
            ("retry", json!({"id": 0})),
            ("seen", json!({})),
        ] {
            assert!(store(&db, Request::call(1, method, params)).is_err());
        }

        assert!(store(&db, Request::call(2, "put", json!({"key": ""}))).is_err());

        let missing =
            store(&db, Request::call(3, "release", json!({"name": "missing", "owner": "one"})))
                .unwrap()
                .unwrap();

        assert_eq!(missing.result.unwrap()["released"], false);
        let unacked = store(&db, Request::call(4, "ack", json!({"id": 42}))).unwrap().unwrap();
        assert_eq!(unacked.result.unwrap()["acked"], false);
    }

    #[test]
    fn migration_upgrades_the_original_item_table() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE item (key TEXT PRIMARY KEY, value TEXT NOT NULL);").unwrap();
        migrate(&db).unwrap();
        let db = Arc::new(Mutex::new(db));
        let result = store(&db, Request::call(1, "put", json!({"key": "old", "value": true})))
            .unwrap()
            .unwrap();

        assert_eq!(result.result.unwrap()["ok"], true);
    }

    #[cfg(unix)]
    #[test]
    fn database_files_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("crabbot-sqlite-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, b"").unwrap();
        private(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_file(path);
    }
}
