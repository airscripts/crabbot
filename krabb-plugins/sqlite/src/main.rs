#![forbid(unsafe_code)]

#[cfg(not(test))]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use krabb_core::types::{Request, Response};
#[cfg(not(test))]
use krabb_core::{
    plugin::serve_with,
    types::{Capability, CommandSpec, Hello, Protocol},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const LIMIT: i64 = 1_000;
const KEY_LIMIT: usize = 4 * 1024;
const VALUE_LIMIT: usize = 256 * 1024;
// Query JSON must fit the host's 256 KiB tool and model-message text budgets.
const QUERY_LIMIT: usize = 256 * 1024;
const RECORD_LIMIT: usize = QUERY_LIMIT - 1024;
const FRAME_HEADROOM: usize = 64 * 1024;
const DB_LIMIT: i64 = 32 * 1024 * 1024;

#[cfg(not(test))]
#[tokio::main]
async fn main() -> std::process::ExitCode {
    krabb_log::run_plugin("sqlite", plugin_main()).await
}

#[cfg(not(test))]
async fn plugin_main() -> krabb_core::Result<()> {
    let path = std::env::var_os("KRABB_DB")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("KRABB_HOME").map(PathBuf::from).map(|home| home.join("krabb.db"))
        })
        .ok_or_else(|| krabb_core::Error::Denied("KRABB_DB or KRABB_HOME is required.".into()))?;

    let db = Connection::open(&path)
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite open failed: {error}.")))?;

    db.busy_timeout(std::time::Duration::from_secs(5)).map_err(|error| {
        krabb_core::Error::Denied(format!("SQLite busy timeout failed: {error}."))
    })?;

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
            commands: vec![CommandSpec {
                name: "data".into(),
                description: "Explore and manage structured SQLite data.".into(),
                interactive: false,
            }],
        },
        move |request| {
            let db = Arc::clone(&db);
            async move { store(&db, request) }
        },
    )
    .await
}

fn private(path: &std::path::Path) -> krabb_core::Result<()> {
    #[cfg(unix)]
    {
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

#[cfg(not(test))]
fn private_sidecars(path: &std::path::Path) -> krabb_core::Result<()> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));

        if sidecar.exists() {
            private(&sidecar)?;
        }
    }

    Ok(())
}

fn store(db: &Arc<Mutex<Connection>>, request: Request) -> krabb_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let mut db =
        db.lock().map_err(|_| krabb_core::Error::Denied("SQLite lock is poisoned.".into()))?;

    let result = match method.as_str() {
        "command" if params["name"] == "data" => {
            let args = params["args"].as_array().cloned().unwrap_or_default();
            let mut args = args.iter().filter_map(serde_json::Value::as_str).collect::<Vec<_>>();
            let scope = if args.first() == Some(&"--scope") {
                if args.len() < 2 {
                    return Err(denied("`--scope` requires a scope name."));
                }

                let scope = args[1].to_owned();
                args.drain(..2);
                scope
            } else {
                "cli".to_owned()
            };

            if args.is_empty() || matches!(args.as_slice(), ["help"] | ["--help"] | ["-h"]) {
                return Ok(Some(Response::ok(id, serde_json::Value::String(data_help().into()))));
            }

            let request = data_command(&args)?;
            let mut request = request;
            request["scope"] = serde_json::json!(scope);
            data(&mut db, &request)?
        }

        "put" => {
            let key = key(&params, "put.key")?;
            let value = value(&params["value"], "put.value")?;
            db.execute(
                "INSERT INTO item(key, value, updated) VALUES(?1, ?2, ?3) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated = excluded.updated",
                params![key, value, now()],
            )
            .map_err(|error| krabb_core::Error::Denied(format!("SQLite put failed: {error}.")))?;
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
                    krabb_core::Error::Denied(format!("SQLite get failed: {error}."))
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
                    krabb_core::Error::Denied(format!("SQLite delete failed: {error}."))
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
        "data" => data(&mut db, &params)?,
        _ => return Ok(None),
    };

    let response = Response::ok(id, result);

    if serde_json::to_vec(&response)?.len().saturating_add(1 + FRAME_HEADROOM)
        > krabb_core::jsonl::MAX
    {
        return Err(krabb_core::Error::Denied(
            "SQLite response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(Some(response))
}

fn migrate(db: &Connection) -> krabb_core::Result<()> {
    db.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS item (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS lease (name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, dedupe TEXT NOT NULL UNIQUE, value TEXT NOT NULL, created INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, expires INTEGER NOT NULL);
         PRAGMA foreign_keys = ON;
         CREATE TABLE IF NOT EXISTS data_database (scope TEXT NOT NULL, name TEXT NOT NULL, created INTEGER NOT NULL, PRIMARY KEY(scope, name));
         CREATE TABLE IF NOT EXISTS data_table (scope TEXT NOT NULL, database_name TEXT NOT NULL, name TEXT NOT NULL, columns TEXT NOT NULL, created INTEGER NOT NULL, PRIMARY KEY(scope, database_name, name), FOREIGN KEY(scope, database_name) REFERENCES data_database(scope, name) ON DELETE CASCADE);
         CREATE TABLE IF NOT EXISTS data_record (id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, database_name TEXT NOT NULL, table_name TEXT NOT NULL, value TEXT NOT NULL, created INTEGER NOT NULL, updated INTEGER NOT NULL, FOREIGN KEY(scope, database_name, table_name) REFERENCES data_table(scope, database_name, name) ON DELETE CASCADE);
         CREATE INDEX IF NOT EXISTS data_record_scope_table ON data_record(scope, database_name, table_name, id);
         INSERT OR IGNORE INTO schema_version(version, applied) VALUES(1, strftime('%s', 'now'));
         INSERT OR IGNORE INTO schema_version(version, applied) VALUES(2, strftime('%s', 'now'));",
    )
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite migration failed: {error}.")))?;

    let page_size =
        db.query_row("PRAGMA page_size;", [], |row| row.get::<_, i64>(0)).map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let page_count =
        db.query_row("PRAGMA page_count;", [], |row| row.get::<_, i64>(0)).map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let max_pages = DB_LIMIT / page_size.max(1);

    if page_count > max_pages {
        return Err(krabb_core::Error::Denied("SQLite database exceeds the size limit.".into()));
    }

    db.query_row(&format!("PRAGMA max_page_count = {max_pages};"), [], |row| row.get::<_, i64>(0))
        .map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite size limit failed: {error}."))
        })?;

    let mut columns = db
        .prepare("PRAGMA table_info(item)")
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite migration failed: {error}.")))?;

    let has_updated = columns
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite migration failed: {error}.")))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite migration failed: {error}.")))?
        .iter()
        .any(|name| name == "updated");

    if !has_updated {
        db.execute_batch("ALTER TABLE item ADD COLUMN updated INTEGER NOT NULL DEFAULT 0;")
            .map_err(|error| {
                krabb_core::Error::Denied(format!("SQLite migration failed: {error}."))
            })?;
    }

    Ok(())
}

fn data(db: &mut Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let scope = params["scope"]
        .as_str()
        .filter(|scope| {
            !scope.trim().is_empty() && scope.len() <= 256 && !scope.chars().any(char::is_control)
        })
        .ok_or_else(|| denied("data.scope is required and must be at most 256 characters."))?;

    let operation =
        params["operation"].as_str().ok_or_else(|| denied("data.operation is required."))?;

    let database = optional_label(params, "database", "default", "data.database", 64)?;
    let table = if matches!(
        operation,
        "create_table"
            | "describe_table"
            | "add_column"
            | "drop_column"
            | "drop_table"
            | "insert"
            | "query"
            | "update"
            | "delete"
    ) {
        optional_label(params, "table", "", "data.table", 64)?
    } else {
        ""
    };

    let transaction = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| denied(format!("SQLite data transaction failed: {error}.")))?;

    let result = match operation {
        "create_database" => {
            let count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM data_database WHERE scope = ?1",
                    params![scope],
                    |row| row.get(0),
                )
                .map_err(|error| denied(format!("SQLite database count failed: {error}.")))?;
            let exists: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM data_database WHERE scope = ?1 AND name = ?2)",
                    params![scope, database],
                    |row| row.get(0),
                )
                .map_err(|error| denied(format!("SQLite database lookup failed: {error}.")))?;

            if !exists && count >= 100 {
                return Err(denied("A scope can contain at most 100 databases."));
            }

            transaction
                .execute(
                    "INSERT INTO data_database(scope, name, created) VALUES(?1, ?2, ?3) ON CONFLICT(scope, name) DO NOTHING",
                    params![scope, database, now()],
                )
                .map_err(|error| denied(format!("SQLite create database failed: {error}.")))?;
            serde_json::json!({"database": database, "created": !exists})
        }

        "list_databases" => {
            let mut statement = transaction
                .prepare("SELECT name FROM data_database WHERE scope = ?1 ORDER BY name LIMIT 100")
                .map_err(|error| denied(format!("SQLite list databases failed: {error}.")))?;
            let items = statement
                .query_map(params![scope], |row| row.get::<_, String>(0))
                .map_err(|error| denied(format!("SQLite list databases failed: {error}.")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| denied(format!("SQLite list databases failed: {error}.")))?;
            serde_json::json!({"databases": items})
        }

        "drop_database" => {
            let count = transaction
                .execute(
                    "DELETE FROM data_database WHERE scope = ?1 AND name = ?2",
                    params![scope, database],
                )
                .map_err(|error| denied(format!("SQLite drop database failed: {error}.")))?;
            serde_json::json!({"deleted": count == 1})
        }

        "create_table" => {
            let empty_columns = serde_json::json!([]);
            let columns = validate_columns(params.get("columns").unwrap_or(&empty_columns))?;
            let database_exists: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM data_database WHERE scope = ?1 AND name = ?2)",
                    params![scope, database],
                    |row| row.get(0),
                )
                .map_err(|error| denied(format!("SQLite database lookup failed: {error}.")))?;

            if !database_exists {
                let count: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM data_database WHERE scope = ?1",
                        params![scope],
                        |row| row.get(0),
                    )
                    .map_err(|error| denied(format!("SQLite database count failed: {error}.")))?;

                if count >= 100 {
                    return Err(denied("A scope can contain at most 100 databases."));
                }
            }

            let count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM data_table WHERE scope = ?1 AND database_name = ?2",
                    params![scope, database],
                    |row| row.get(0),
                )
                .map_err(|error| denied(format!("SQLite table count failed: {error}.")))?;

            if count >= 100 {
                return Err(denied("A database can contain at most 100 tables."));
            }

            transaction
                .execute(
                    "INSERT INTO data_database(scope, name, created) VALUES(?1, ?2, ?3) ON CONFLICT(scope, name) DO NOTHING",
                    params![scope, database, now()],
                )
                .map_err(|error| denied(format!("SQLite create database failed: {error}.")))?;
            transaction
                .execute(
                    "INSERT INTO data_table(scope, database_name, name, columns, created) VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![scope, database, table, serde_json::to_string(&columns)?, now()],
                )
                .map_err(|error| denied(format!("SQLite create table failed: {error}.")))?;
            serde_json::json!({"database": database, "table": table, "columns": columns})
        }

        "list_tables" => {
            let mut statement = transaction
                .prepare("SELECT name FROM data_table WHERE scope = ?1 AND database_name = ?2 ORDER BY name LIMIT 100")
                .map_err(|error| denied(format!("SQLite list tables failed: {error}.")))?;
            let items = statement
                .query_map(params![scope, database], |row| row.get::<_, String>(0))
                .map_err(|error| denied(format!("SQLite list tables failed: {error}.")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| denied(format!("SQLite list tables failed: {error}.")))?;
            serde_json::json!({"database": database, "tables": items})
        }

        "describe_table" => {
            let columns = table_columns(&transaction, scope, database, table)?;
            serde_json::json!({"database": database, "table": table, "columns": columns})
        }

        "add_column" => {
            let mut columns = table_columns(&transaction, scope, database, table)?;
            let new_column = validate_columns(&serde_json::json!([params["column"]]))?
                .pop()
                .ok_or_else(|| denied("data.column is required."))?;

            if new_column.required {
                let records: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3",
                        params![scope, database, table],
                        |row| row.get(0),
                    )
                    .map_err(|error| denied(format!("SQLite record count failed: {error}.")))?;

                if records > 0 {
                    return Err(denied(
                        "A required column can only be added before records exist.",
                    ));
                }
            }

            if columns.len() >= 64 || columns.iter().any(|column| column.name == new_column.name) {
                return Err(denied(
                    "The column already exists or the table has reached its 64-column limit.",
                ));
            }

            columns.push(new_column.clone());
            transaction
                .execute(
                    "UPDATE data_table SET columns = ?1 WHERE scope = ?2 AND database_name = ?3 AND name = ?4",
                    params![serde_json::to_string(&columns)?, scope, database, table],
                )
                .map_err(|error| denied(format!("SQLite add column failed: {error}.")))?;
            serde_json::json!({"database": database, "table": table, "column": new_column})
        }

        "drop_column" => {
            let column_name = params["column_name"]
                .as_str()
                .filter(|name| valid_identifier(name))
                .ok_or_else(|| denied("data.column_name must be a valid column name."))?;
            let mut columns = table_columns(&transaction, scope, database, table)?;

            let Some(index) = columns.iter().position(|column| column.name == column_name) else {
                return Err(denied("data.column_name was not found."));
            };

            columns.remove(index);
            let mut statement = transaction
                .prepare("SELECT id, value FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3")
                .map_err(|error| denied(format!("SQLite drop column failed: {error}.")))?;
            let rows = statement
                .query_map(params![scope, database, table], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| denied(format!("SQLite drop column failed: {error}.")))?;
            let mut records = Vec::new();

            for row in rows {
                let (id, value) =
                    row.map_err(|error| denied(format!("SQLite drop column failed: {error}.")))?;
                let mut value = serde_json::from_str::<serde_json::Value>(&value)?;

                if let Some(object) = value.as_object_mut() {
                    object.remove(column_name);
                }

                records.push((id, serde_json::to_string(&value)?));
            }

            drop(statement);
            let records_updated = records.len();

            for (id, value) in records {
                transaction
                    .execute(
                        "UPDATE data_record SET value = ?1, updated = ?2 WHERE id = ?3 AND scope = ?4 AND database_name = ?5 AND table_name = ?6",
                        params![value, now(), id, scope, database, table],
                    )
                    .map_err(|error| denied(format!("SQLite drop column failed: {error}.")))?;
            }

            transaction
                .execute(
                    "UPDATE data_table SET columns = ?1 WHERE scope = ?2 AND database_name = ?3 AND name = ?4",
                    params![serde_json::to_string(&columns)?, scope, database, table],
                )
                .map_err(|error| denied(format!("SQLite drop column failed: {error}.")))?;
            serde_json::json!({"database": database, "table": table, "column": column_name, "records_updated": records_updated})
        }

        "drop_table" => {
            let count = transaction
                .execute(
                    "DELETE FROM data_table WHERE scope = ?1 AND database_name = ?2 AND name = ?3",
                    params![scope, database, table],
                )
                .map_err(|error| denied(format!("SQLite drop table failed: {error}.")))?;
            serde_json::json!({"deleted": count == 1})
        }

        "insert" => {
            let columns = table_columns(&transaction, scope, database, table)?;
            let record = validate_record(&params["record"], &columns)?;
            let value = serde_json::to_string(&record)?;

            if value.len() > RECORD_LIMIT {
                return Err(denied("data.record exceeds the size limit."));
            }

            let count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3",
                    params![scope, database, table],
                    |row| row.get(0),
                )
                .map_err(|error| denied(format!("SQLite record count failed: {error}.")))?;

            if count >= 10_000 {
                return Err(denied("SQLite table is full; no record was discarded."));
            }

            transaction
                .execute(
                    "INSERT INTO data_record(scope, database_name, table_name, value, created, updated) VALUES(?1, ?2, ?3, ?4, ?5, ?5)",
                    params![scope, database, table, value, now()],
                )
                .map_err(|error| denied(format!("SQLite insert failed: {error}.")))?;
            serde_json::json!({"id": transaction.last_insert_rowid(), "record": record})
        }

        "query" => {
            let columns = table_columns(&transaction, scope, database, table)?;
            let filter = validate_filter(
                &params.get("filter").cloned().unwrap_or_else(|| serde_json::json!({})),
                &columns,
            )?;
            let limit = params["limit"].as_u64().unwrap_or(20).clamp(1, 20) as i64;
            let after_id = match params.get("after_id") {
                Some(value) => Some(
                    value
                        .as_i64()
                        .filter(|id| *id > 0)
                        .ok_or_else(|| denied("data.after_id must be a positive integer."))?,
                ),
                None => None,
            };

            let mut statement = transaction
                .prepare("SELECT id, value FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3 AND (?4 IS NULL OR id > ?4) ORDER BY id LIMIT 10000")
                .map_err(|error| denied(format!("SQLite query failed: {error}.")))?;
            let rows = statement
                .query_map(params![scope, database, table, after_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| denied(format!("SQLite query failed: {error}.")))?;
            let mut items = Vec::new();
            let mut result_size = 256;
            let mut truncated = false;
            let mut next_after = None;

            for row in rows {
                let (id, value) =
                    row.map_err(|error| denied(format!("SQLite query failed: {error}.")))?;
                let value = serde_json::from_str::<serde_json::Value>(&value)?;

                if filter.as_object().is_some_and(|filter| {
                    filter.iter().all(|(key, expected)| value[key] == *expected)
                }) {
                    let item = serde_json::json!({"id": id, "record": value});
                    let item_size = serde_json::to_vec(&item)?.len() + 1;

                    if items.len() == limit as usize || result_size + item_size > QUERY_LIMIT {
                        if items.is_empty() {
                            return Err(denied(
                                "Stored record exceeds the query size limit; reduce it with data update.",
                            ));
                        }

                        truncated = true;
                        break;
                    }

                    result_size += item_size;
                    next_after = Some(id);
                    items.push(item);
                }
            }

            serde_json::json!({
                "items": items,
                "limit": limit,
                "truncated": truncated,
                "next_after": if truncated { next_after } else { None },
            })
        }

        "update" => {
            let id = record_id(params)?;
            let columns = table_columns(&transaction, scope, database, table)?;
            let existing = transaction
                .query_row(
                    "SELECT value FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3 AND id = ?4",
                    params![scope, database, table, id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| denied(format!("SQLite update lookup failed: {error}.")))?
                .ok_or_else(|| denied("data record was not found."))?;
            let mut record = serde_json::from_str::<serde_json::Value>(&existing)?;
            let changes = params["record"]
                .as_object()
                .ok_or_else(|| denied("data.record must be an object."))?;
            let target =
                record.as_object_mut().ok_or_else(|| denied("Stored record is invalid."))?;

            for (key, value) in changes {
                target.insert(key.clone(), value.clone());
            }

            let record = validate_record(&record, &columns)?;
            let value = serde_json::to_string(&record)?;

            if value.len() > RECORD_LIMIT {
                return Err(denied("data.record exceeds the size limit."));
            }

            transaction
                .execute(
                    "UPDATE data_record SET value = ?1, updated = ?2 WHERE scope = ?3 AND database_name = ?4 AND table_name = ?5 AND id = ?6",
                    params![value, now(), scope, database, table, id],
                )
                .map_err(|error| denied(format!("SQLite update failed: {error}.")))?;
            serde_json::json!({"id": id, "record": record})
        }

        "delete" => {
            let id = record_id(params)?;
            let count = transaction
                .execute(
                    "DELETE FROM data_record WHERE scope = ?1 AND database_name = ?2 AND table_name = ?3 AND id = ?4",
                    params![scope, database, table, id],
                )
                .map_err(|error| denied(format!("SQLite delete failed: {error}.")))?;
            serde_json::json!({"deleted": count == 1})
        }

        _ => return Err(denied("Unknown data operation.")),
    };

    transaction.commit().map_err(|error| denied(format!("SQLite data commit failed: {error}.")))?;
    Ok(result)
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DataColumn {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    required: bool,
}

fn validate_columns(value: &serde_json::Value) -> krabb_core::Result<Vec<DataColumn>> {
    let columns = value.as_array().ok_or_else(|| denied("data.columns must be an array."))?;

    if columns.len() > 64 {
        return Err(denied("data.columns must contain at most 64 columns."));
    }

    let columns = columns
        .iter()
        .map(|value| {
            serde_json::from_value::<DataColumn>(value.clone()).map_err(|_| {
                denied("Each data column needs name, type, and optional required fields.")
            })
        })
        .collect::<krabb_core::Result<Vec<_>>>()?;

    let mut names = std::collections::BTreeSet::new();

    for column in &columns {
        if !valid_identifier(&column.name) || !names.insert(column.name.clone()) {
            return Err(denied(
                "Column names must be unique identifiers using letters, numbers, and underscores.",
            ));
        }

        if !matches!(column.kind.as_str(), "text" | "integer" | "real" | "boolean" | "json") {
            return Err(denied("Column types must be text, integer, real, boolean, or json."));
        }
    }

    Ok(columns)
}

fn validate_record(
    value: &serde_json::Value,
    columns: &[DataColumn],
) -> krabb_core::Result<serde_json::Value> {
    if columns.is_empty() {
        return Err(denied("Add at least one column before inserting records."));
    }

    let record = value.as_object().ok_or_else(|| denied("data.record must be an object."))?;

    for key in record.keys() {
        if !columns.iter().any(|column| column.name == *key) {
            return Err(denied("data.record contains a field not declared by the table."));
        }
    }

    for column in columns {
        let Some(value) = record.get(&column.name) else {
            if column.required {
                return Err(denied(format!("data.record.{} is required.", column.name)));
            }

            continue;
        };

        let valid = match column.kind.as_str() {
            "text" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "real" => value.as_f64().is_some(),
            "boolean" => value.is_boolean(),
            "json" => true,
            _ => false,
        };

        if !valid {
            return Err(denied(format!(
                "data.record.{} must have type {}.",
                column.name, column.kind
            )));
        }
    }

    Ok(value.clone())
}

fn validate_filter(
    value: &serde_json::Value,
    columns: &[DataColumn],
) -> krabb_core::Result<serde_json::Value> {
    let filter = value.as_object().ok_or_else(|| denied("data.filter must be an object."))?;

    for (name, value) in filter {
        let column = columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| denied("data.filter contains a field not declared by the table."))?;

        let valid = match column.kind.as_str() {
            "text" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "real" => value.as_f64().is_some(),
            "boolean" => value.is_boolean(),
            "json" => true,
            _ => false,
        };

        if !valid {
            return Err(denied(format!("data.filter.{name} must have type {}.", column.kind)));
        }
    }

    Ok(value.clone())
}

fn table_columns(
    db: &Connection,
    scope: &str,
    database: &str,
    table: &str,
) -> krabb_core::Result<Vec<DataColumn>> {
    db.query_row(
        "SELECT columns FROM data_table WHERE scope = ?1 AND database_name = ?2 AND name = ?3",
        params![scope, database, table],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|error| denied(format!("SQLite table lookup failed: {error}.")))?
    .ok_or_else(|| denied("data table was not found."))
    .and_then(|value| serde_json::from_str(&value).map_err(Into::into))
}

fn record_id(params: &serde_json::Value) -> krabb_core::Result<i64> {
    params["id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| denied("data.id must be a positive integer."))
}

fn optional_label<'a>(
    params: &'a serde_json::Value,
    key: &str,
    default: &'a str,
    label: &str,
    limit: usize,
) -> krabb_core::Result<&'a str> {
    match params.get(key).and_then(serde_json::Value::as_str) {
        Some(value)
            if !value.trim().is_empty() && value.len() <= limit && valid_identifier(value) =>
        {
            Ok(value)
        }

        Some(_) => Err(denied(format!("{label} must be a valid identifier."))),
        None if default.is_empty() => Err(denied(format!("{label} is required."))),
        None => Ok(default),
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn denied(message: impl Into<String>) -> krabb_core::Error {
    krabb_core::Error::Denied(message.into())
}

fn data_command(args: &[&str]) -> krabb_core::Result<serde_json::Value> {
    let operation = match args {
        ["database", "list"] => "list_databases",
        ["database", "create", _] => "create_database",
        ["database", "drop", _] => "drop_database",
        ["table", "list", _] => "list_tables",
        ["table", "create", _, _] | ["table", "create", _, _, _] => "create_table",
        ["table", "describe", _, _] => "describe_table",
        ["column", "add", _, _, ..] => "add_column",
        ["column", "remove", _, _, _] => "drop_column",
        ["table", "drop", _, _] => "drop_table",
        ["insert", _, _, _] => "insert",
        ["query", _, _] | ["query", _, _, _] | ["query", _, _, _, _] => "query",
        ["update", _, _, _, _] => "update",
        ["delete", _, _, _] => "delete",
        _ => return Err(denied("Run `krabb data help` for structured data commands.")),
    };

    let mut request = serde_json::json!({"operation": operation});

    match args {
        ["database", "list"] => {}
        ["database", _, database] => request["database"] = serde_json::json!(database),
        ["table", "list", database] => request["database"] = serde_json::json!(database),

        ["table", "create", database, table] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["columns"] = serde_json::json!([]);
        }

        ["table", "create", database, table, columns] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["columns"] = parse_json_arg(columns, "columns")?;
        }

        ["column", "add", database, table, column] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["column"] = parse_json_arg(column, "column")?;
        }

        ["column", "add", database, table, name, kind] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["column"] = serde_json::json!({"name": name, "type": column_type(kind)});
        }

        ["column", "add", database, table, name, kind, "--required"] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["column"] = serde_json::json!({
                "name": name, "type": column_type(kind), "required": true
            });
        }

        ["column", "remove", database, table, column] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["column_name"] = serde_json::json!(column);
        }

        ["table", _, database, table] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
        }

        ["insert", database, table, record] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["record"] = parse_json_arg(record, "record")?;
        }

        ["query", database, table] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
        }

        ["query", database, table, filter] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["filter"] = parse_json_arg(filter, "filter")?;
        }

        ["query", database, table, filter, after_id] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["filter"] = parse_json_arg(filter, "filter")?;
            request["after_id"] = serde_json::json!(
                after_id
                    .parse::<i64>()
                    .map_err(|_| denied("after ID must be a positive integer."))?
            );
        }

        ["update", database, table, id, record] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["id"] = serde_json::json!(
                id.parse::<i64>().map_err(|_| denied("record ID must be a positive integer."))?
            );

            request["record"] = parse_json_arg(record, "record")?;
        }

        ["delete", database, table, id] => {
            request["database"] = serde_json::json!(database);
            request["table"] = serde_json::json!(table);
            request["id"] = serde_json::json!(
                id.parse::<i64>().map_err(|_| denied("record ID must be a positive integer."))?
            );
        }

        _ => return Err(denied("Run `krabb data help` for structured data commands.")),
    }

    Ok(request)
}

fn parse_json_arg(value: &str, label: &str) -> krabb_core::Result<serde_json::Value> {
    serde_json::from_str(value).map_err(|_| denied(format!("{label} must be valid JSON.")))
}

fn column_type(kind: &str) -> &str {
    if kind == "string" { "text" } else { kind }
}

fn data_help() -> &'static str {
    r#"Manage structured data through Krabb's SQLite store.

Usage: krabb data [--scope NAME] <COMMAND>

Commands:
  database list                                  List databases
  database create <DB>                           Create a database
  database drop <DB>                             Delete a database and its tables

  table list <DB>                                List tables in a database
  table create <DB> <TABLE>                      Create an empty table
  table describe <DB> <TABLE>                    Show table columns
  table drop <DB> <TABLE>                        Delete a table and its records

  column add <DB> <TABLE> <NAME> <TYPE>          Add a column
  column remove <DB> <TABLE> <COLUMN>            Remove a column

  insert <DB> <TABLE> <RECORD_JSON>              Insert a record
  query <DB> <TABLE> [FILTER_JSON] [AFTER_ID]    Query records
  update <DB> <TABLE> <ID> <CHANGES_JSON>        Update a record
  delete <DB> <TABLE> <ID>                       Delete a record

  help                                           Show this help

Options:
  --scope <NAME>  Use a data scope (default: cli)
  -h, --help      Show this help

Notes:
  Use table create without a schema, then add columns one at a time. You can
  also pass a JSON column definition to column add or a JSON array to table
  create. Column types are text (also accepted as string), integer, real,
  boolean, and json. Quote JSON arguments for your shell. Use --required with
  column add to require a value on insert. Required columns can only be added
  before records exist.
  Databases are logical collections inside the configured SQLite file. Each
  scope supports 100 databases, each database supports 100 tables, and each
  table supports 64 columns and 10,000 records.
  Queries match exact field values and return at most 20 records/128 KiB. To
  continue a truncated query, pass its next_after ID as AFTER_ID after the
  filter. Use '{}' as FILTER_JSON when paging without a filter.
  CLI data uses the local cli scope by default. Agent and TUI data use the
  current conversation scope; pass --scope NAME to inspect another scope."#
}

fn key<'a>(params: &'a serde_json::Value, label: &str) -> krabb_core::Result<&'a str> {
    let key = params["key"]
        .as_str()
        .ok_or_else(|| krabb_core::Error::Denied(format!("{label} is required.")))?;

    if key.trim().is_empty() {
        return Err(krabb_core::Error::Denied(format!("{label} cannot be empty.")));
    }

    if key.len() > KEY_LIMIT {
        return Err(krabb_core::Error::Denied(format!("{label} exceeds the size limit.")));
    }

    Ok(key)
}

fn value(value: &serde_json::Value, label: &str) -> krabb_core::Result<String> {
    let value = serde_json::to_string(value)?;

    if value.len() > VALUE_LIMIT {
        return Err(krabb_core::Error::Denied(format!("{label} exceeds the size limit.")));
    }

    Ok(value)
}

fn lease(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let name = params["name"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| krabb_core::Error::Denied("lease.name is required.".into()))?;

    let owner = params["owner"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| krabb_core::Error::Denied("lease.owner is required.".into()))?;

    // TTL is in seconds; 86,400 seconds is 24 hours.
    let ttl = params["ttl"].as_u64().unwrap_or(30).clamp(1, 86_400) as i64;
    let expires = now() + ttl;
    let count = db
        .execute(
            "INSERT INTO lease(name, owner, expires) VALUES(?1, ?2, ?3) ON CONFLICT(name) DO UPDATE SET owner = excluded.owner, expires = excluded.expires WHERE lease.expires <= ?4 OR lease.owner = excluded.owner",
            params![name, owner, expires, now()],
        )
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite lease failed: {error}.")))?;

    Ok(serde_json::json!({"acquired": count == 1, "expires": expires}))
}

fn release(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let name = params["name"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| krabb_core::Error::Denied("release.name is required.".into()))?;

    let owner = params["owner"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| krabb_core::Error::Denied("release.owner is required.".into()))?;

    let count = db
        .execute("DELETE FROM lease WHERE name = ?1 AND owner = ?2", params![name, owner])
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite release failed: {error}.")))?;

    Ok(serde_json::json!({"released": count == 1}))
}

fn enqueue(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let dedupe = key(params, "enqueue.key")?;
    let value = value(&params["value"], "enqueue.value")?;
    let existing = db
        .query_row("SELECT id FROM outbox WHERE dedupe = ?1", params![dedupe], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite enqueue lookup failed: {error}."))
        })?;

    if let Some(id) = existing {
        return Ok(serde_json::json!({"id": id}));
    }

    let count: i64 =
        db.query_row("SELECT COUNT(*) FROM outbox", [], |row| row.get(0)).map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite outbox count failed: {error}."))
        })?;

    if count >= LIMIT {
        return Err(krabb_core::Error::Denied(
            "SQLite outbox is full; no message was discarded.".into(),
        ));
    }

    db.execute(
        "INSERT INTO outbox(dedupe, value, created) VALUES(?1, ?2, ?3) ON CONFLICT(dedupe) DO NOTHING",
        params![dedupe, value, now()],
    )
    .map_err(|error| krabb_core::Error::Denied(format!("SQLite enqueue failed: {error}.")))?;

    let id: i64 = db
        .query_row("SELECT id FROM outbox WHERE dedupe = ?1", params![dedupe], |row| row.get(0))
        .map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite enqueue lookup failed: {error}."))
        })?;

    Ok(serde_json::json!({"id": id}))
}

fn outbox(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let limit = params["limit"].as_u64().unwrap_or(50).clamp(1, 100) as i64;
    let mut statement = db
        .prepare("SELECT id, dedupe, value, attempts FROM outbox ORDER BY id LIMIT ?1")
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite outbox failed: {error}.")))?;

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
            krabb_core::Error::Denied(format!("SQLite outbox read failed: {error}."))
        })?;

    let mut items = Vec::new();

    for row in rows {
        let (id, key, value, attempts) = row.map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite outbox row failed: {error}."))
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

fn ack(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let id = params["id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| krabb_core::Error::Denied("ack.id is required.".into()))?;

    let count = db
        .execute("DELETE FROM outbox WHERE id = ?1", params![id])
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite ack failed: {error}.")))?;

    Ok(serde_json::json!({"acked": count == 1}))
}

fn retry(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let id = params["id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| krabb_core::Error::Denied("retry.id is required.".into()))?;

    let count = db
        .execute("UPDATE outbox SET attempts = attempts + 1 WHERE id = ?1", params![id])
        .map_err(|error| krabb_core::Error::Denied(format!("SQLite retry failed: {error}.")))?;

    Ok(serde_json::json!({"retried": count == 1}))
}

fn seen(db: &Connection, params: &serde_json::Value) -> krabb_core::Result<serde_json::Value> {
    let value = key(params, "seen.key")?;
    // TTL is in seconds; defaults to 24 hours and is capped at 30 days.
    let ttl = params["ttl"].as_u64().unwrap_or(86_400).clamp(1, 2_592_000) as i64;
    let now = now();
    db.execute("DELETE FROM seen WHERE expires <= ?1", params![now]).map_err(|error| {
        krabb_core::Error::Denied(format!("SQLite deduplication failed: {error}."))
    })?;

    let count = db
        .execute(
            "INSERT INTO seen(key, expires) VALUES(?1, ?2) ON CONFLICT(key) DO NOTHING",
            params![value, now + ttl],
        )
        .map_err(|error| {
            krabb_core::Error::Denied(format!("SQLite deduplication failed: {error}."))
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
    use super::{DB_LIMIT, data_command, migrate, private, store};

    use krabb_core::types::Request;
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
        // 65,536 bytes (64 KiB), SQLite's largest supported page size.
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
    fn structured_data_supports_scoped_database_table_and_record_lifecycle() {
        let db = db();
        let invoke = |id, operation: &str, extra: serde_json::Value| {
            let mut params = json!({"scope": "provider:telegram:dm:42", "operation": operation});
            params.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            store(&db, Request::call(id, "data", params)).unwrap().unwrap().result.unwrap()
        };

        assert_eq!(invoke(1, "create_database", json!({"database": "contacts"}))["created"], true);
        invoke(
            2,
            "create_table",
            json!({
                "database": "contacts",
                "table": "person",
                "columns": [
                    {"name": "name", "type": "text", "required": true},
                    {"name": "age", "type": "integer"},
                    {"name": "tags", "type": "json"}
                ]
            }),
        );

        let inserted = invoke(
            3,
            "insert",
            json!({
                "database": "contacts",
                "table": "person",
                "record": {"name": "Ada", "age": 36, "tags": ["math", "computing"]}
            }),
        );

        let id = inserted["id"].as_i64().unwrap();

        assert_eq!(inserted["record"]["name"], "Ada");
        assert_eq!(
            invoke(
                4,
                "query",
                json!({
                    "database": "contacts", "table": "person", "filter": {"age": 36}
                })
            )["items"][0]["id"],
            id
        );

        assert_eq!(
            invoke(
                5,
                "update",
                json!({
                    "database": "contacts", "table": "person", "id": id, "record": {"age": 37}
                })
            )["record"]["age"],
            37
        );

        invoke(
            6,
            "add_column",
            json!({
                "database": "contacts",
                "table": "person",
                "column": {"name": "active", "type": "boolean"}
            }),
        );

        assert_eq!(
            invoke(
                7,
                "update",
                json!({
                    "database": "contacts", "table": "person", "id": id, "record": {"active": true}
                })
            )["record"]["active"],
            true
        );

        assert_eq!(
            invoke(
                8,
                "drop_column",
                json!({"database": "contacts", "table": "person", "column_name": "age"})
            )["records_updated"],
            1
        );

        assert!(
            invoke(9, "describe_table", json!({"database": "contacts", "table": "person"}))["columns"]
                .as_array()
                .unwrap()
                .iter()
                .all(|column| column["name"] != "age")
        );

        assert_eq!(
            invoke(
                10,
                "delete",
                json!({
                    "database": "contacts", "table": "person", "id": id
                })
            )["deleted"],
            true
        );

        assert!(
            invoke(11, "query", json!({"database": "contacts", "table": "person"}))["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        invoke(
            12,
            "insert",
            json!({
                "database": "contacts",
                "table": "person",
                "record": {"name": "Grace", "tags": ["compiler"]}
            }),
        );

        assert_eq!(invoke(13, "drop_database", json!({"database": "contacts"}))["deleted"], true);
        assert!(
            invoke(14, "list_databases", json!({}))["databases"].as_array().unwrap().is_empty()
        );
    }

    #[test]
    fn structured_data_allows_empty_tables_and_human_column_workflow() {
        let db = db();
        let invoke = |id, operation: &str, extra: serde_json::Value| {
            let mut params = json!({"scope": "cli", "operation": operation});
            params.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            store(&db, Request::call(id, "data", params)).unwrap().unwrap().result.unwrap()
        };

        invoke(1, "create_database", json!({"database": "notes"}));
        let created =
            invoke(2, "create_table", json!({"database": "notes", "table": "note", "columns": []}));

        assert_eq!(created["columns"], json!([]));
        assert_eq!(
            invoke(3, "describe_table", json!({"database": "notes", "table": "note"}))["columns"],
            json!([])
        );

        invoke(
            4,
            "add_column",
            json!({
                "database": "notes",
                "table": "note",
                "column": {"name": "title", "type": "text", "required": true}
            }),
        );

        assert_eq!(
            invoke(
                5,
                "insert",
                json!({"database": "notes", "table": "note", "record": {"title": "Draft"}})
            )["record"]["title"],
            "Draft"
        );
    }

    #[test]
    fn structured_data_isolated_by_scope_and_rejects_invalid_records() {
        let db = db();
        let call = |id, scope: &str, operation: &str, extra: serde_json::Value| {
            let mut params = json!({"scope": scope, "operation": operation});
            params.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            store(&db, Request::call(id, "data", params))
        };

        call(
            1,
            "tui:one",
            "create_table",
            json!({
                "table": "notes", "columns": [{"name": "text", "type": "text", "required": true}]
            }),
        )
        .unwrap();

        assert!(call(2, "tui:two", "query", json!({"table": "notes"})).is_err());
        assert!(
            call(3, "tui:one", "insert", json!({"table": "notes", "record": {"text": 42}}))
                .is_err()
        );

        assert!(
            call(
                4,
                "tui:one",
                "create_table",
                json!({
                    "table": "notes; DROP TABLE item", "columns": [{"name": "text", "type": "text"}]
                })
            )
            .is_err()
        );
    }

    #[test]
    fn structured_data_queries_are_bounded_and_page_by_id() {
        let db = db();
        store(
            &db,
            Request::call(
                1,
                "data",
                json!({
                    "scope": "tui:default",
                    "operation": "create_table",
                    "table": "numbers",
                    "columns": [{"name": "value", "type": "integer", "required": true}]
                }),
            ),
        )
        .unwrap();

        let mut last_id = 0;

        for value in 0..25 {
            let response = store(
                &db,
                Request::call(
                    value + 2,
                    "data",
                    json!({
                        "scope": "tui:default",
                        "operation": "insert",
                        "table": "numbers",
                        "record": {"value": value}
                    }),
                ),
            )
            .unwrap()
            .unwrap();

            last_id = response.result.unwrap()["id"].as_i64().unwrap();
        }

        let first = store(
            &db,
            Request::call(
                30,
                "data",
                json!({"scope": "tui:default", "operation": "query", "table": "numbers"}),
            ),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(first["items"].as_array().unwrap().len(), 20);
        assert_eq!(first["truncated"], true);

        let second = store(
            &db,
            Request::call(
                31,
                "data",
                json!({
                    "scope": "tui:default",
                    "operation": "query",
                    "table": "numbers",
                    "after_id": first["next_after"]
                }),
            ),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(second["items"].as_array().unwrap().len(), 5);
        assert_eq!(second["truncated"], false);
        assert!(
            second["items"].as_array().unwrap().last().unwrap()["id"].as_i64().unwrap() <= last_id
        );
    }

    #[test]
    fn largest_records_remain_retrievable_across_query_pages() {
        let db = db();
        let invoke = |operation: &str, extra: serde_json::Value| {
            let mut params = json!({"scope": "cli", "operation": operation, "table": "notes"});
            params.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            store(&db, Request::call(1, "data", params))
        };

        invoke(
            "create_table",
            json!({
                "columns": [
                    {"name": "a", "type": "text"},
                    {"name": "b", "type": "text"},
                    {"name": "c", "type": "text"}
                ]
            }),
        )
        .unwrap();

        let first = invoke("insert", json!({"record": {"a": "x".repeat(50 * 1024)}}))
            .unwrap()
            .unwrap()
            .result
            .unwrap()["id"]
            .clone();

        invoke("update", json!({"id": first, "record": {"b": "x".repeat(50 * 1024)}})).unwrap();
        invoke("update", json!({"id": first, "record": {"c": "x".repeat(50 * 1024)}})).unwrap();

        // Fill a second record to the existing accepted limit, including escaped text.
        let overhead = serde_json::to_vec(&json!({"a": ""})).unwrap().len();
        let text = "\"".repeat((super::RECORD_LIMIT - overhead) / 2);
        let second = invoke("insert", json!({"record": {"a": text}}))
            .unwrap()
            .unwrap()
            .result
            .unwrap()["id"]
            .clone();

        let last = invoke("insert", json!({"record": {"a": "last"}}))
            .unwrap()
            .unwrap()
            .result
            .unwrap()["id"]
            .clone();

        assert!(
            invoke("insert", json!({"record": {"a": "x".repeat(super::VALUE_LIMIT)}})).is_err()
        );

        assert!(invoke("update", json!({"id": second, "record": {"b": "overflow"}})).is_err());

        let page = invoke("query", json!({})).unwrap().unwrap();

        assert!(serde_json::to_vec(&page).unwrap().len() < super::QUERY_LIMIT);
        let page = page.result.unwrap();

        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(page["items"][0]["record"]["c"], "x".repeat(50 * 1024));
        assert_eq!(page["next_after"], first);
        assert_eq!(page["truncated"], true);

        let page = invoke("query", json!({"after_id": page["next_after"]})).unwrap().unwrap();

        assert!(serde_json::to_vec(&page).unwrap().len() < super::QUERY_LIMIT);
        let page = page.result.unwrap();

        assert_eq!(page["items"][0]["id"], second);
        assert_eq!(page["items"][0]["record"]["a"], text);
        assert_eq!(page["items"][1]["id"], last);
        assert_eq!(page["truncated"], false);

        // A record written by an older version must not produce a page without a cursor.
        db.lock()
            .unwrap()
            .execute(
                "UPDATE data_record SET value = ?1 WHERE id = ?2",
                rusqlite::params![
                    json!({"a": "x".repeat(super::VALUE_LIMIT)}).to_string(),
                    second.as_i64().unwrap()
                ],
            )
            .unwrap();

        let error = invoke("query", json!({"after_id": first})).unwrap_err();

        assert!(error.to_string().contains("reduce it with data update"));
        invoke("update", json!({"id": second, "record": {"a": "repaired"}})).unwrap();

        let page = invoke("query", json!({"after_id": first})).unwrap().unwrap().result.unwrap();

        assert_eq!(page["items"][0]["record"]["a"], "repaired");
    }

    #[test]
    fn structured_data_survives_a_database_reopen() {
        let path =
            std::env::temp_dir().join(format!("krabb-sqlite-data-{}.db", std::process::id()));

        let _ = std::fs::remove_file(&path);
        let connection = Connection::open(&path).unwrap();
        migrate(&connection).unwrap();
        let db = Arc::new(Mutex::new(connection));
        store(
            &db,
            Request::call(
                1,
                "data",
                json!({
                    "scope": "tui:default",
                    "operation": "create_table",
                    "table": "notes",
                    "columns": [{"name": "text", "type": "text", "required": true}]
                }),
            ),
        )
        .unwrap();

        store(
            &db,
            Request::call(
                2,
                "data",
                json!({
                    "scope": "tui:default",
                    "operation": "insert",
                    "table": "notes",
                    "record": {"text": "survives restart"}
                }),
            ),
        )
        .unwrap();

        drop(db);

        let connection = Connection::open(&path).unwrap();
        migrate(&connection).unwrap();
        let db = Arc::new(Mutex::new(connection));
        let result = store(
            &db,
            Request::call(
                3,
                "data",
                json!({"scope": "tui:default", "operation": "query", "table": "notes"}),
            ),
        )
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(result["items"][0]["record"]["text"], "survives restart");

        drop(db);

        for suffix in ["-wal", "-shm", ""] {
            let sidecar = std::path::PathBuf::from(format!("{}{suffix}", path.display()));
            let _ = std::fs::remove_file(sidecar);
        }
    }

    #[test]
    fn cli_data_commands_build_the_bounded_store_requests() {
        let empty = data_command(&["table", "create", "contacts", "people"]).unwrap();

        assert_eq!(empty["operation"], "create_table");
        assert_eq!(empty["columns"], json!([]));

        let create = data_command(&[
            "table",
            "create",
            "contacts",
            "people",
            "[{\"name\":\"name\",\"type\":\"text\"}]",
        ])
        .unwrap();

        assert_eq!(create["operation"], "create_table");
        assert_eq!(create["database"], "contacts");
        assert_eq!(create["table"], "people");
        assert_eq!(create["columns"][0]["name"], "name");

        let query = data_command(&["query", "contacts", "people", "{}", "14"]).unwrap();

        assert_eq!(query["operation"], "query");
        assert_eq!(query["filter"], json!({}));
        assert_eq!(query["after_id"], 14);

        let add_column = data_command(&[
            "column",
            "add",
            "contacts",
            "people",
            "{\"name\":\"active\",\"type\":\"boolean\"}",
        ])
        .unwrap();

        assert_eq!(add_column["operation"], "add_column");
        assert_eq!(add_column["database"], "contacts");
        assert_eq!(add_column["table"], "people");
        assert_eq!(add_column["column"]["name"], "active");

        let add_column =
            data_command(&["column", "add", "contacts", "people", "name", "string"]).unwrap();

        assert_eq!(add_column["column"]["name"], "name");
        assert_eq!(add_column["column"]["type"], "text");

        let add_required =
            data_command(&["column", "add", "contacts", "people", "name", "text", "--required"])
                .unwrap();

        assert_eq!(add_required["column"]["required"], true);

        let remove_column =
            data_command(&["column", "remove", "contacts", "people", "age"]).unwrap();

        assert_eq!(remove_column["operation"], "drop_column");
        assert_eq!(remove_column["column_name"], "age");

        assert!(data_command(&["table", "create", "contacts", "people", "not-json"]).is_err());
        assert!(data_command(&["table", "add-column", "contacts", "people", "{}"]).is_err());
    }

    #[test]
    fn data_command_shows_scheduled_style_help_without_a_subcommand() {
        let db = db();
        let cases: &[&[&str]] = &[
            &[],
            &["help"],
            &["--help"],
            &["--scope", "tui:default"],
            &["--scope", "tui:default", "--help"],
        ];

        for (index, args) in cases.iter().enumerate() {
            let args = args.to_vec();
            let response = store(
                &db,
                Request::call(index as u64 + 1, "command", json!({"name": "data", "args": args})),
            )
            .unwrap()
            .unwrap();

            let help = response.result.unwrap();
            let help = help.as_str().unwrap();

            assert!(help.contains("Usage: krabb data [--scope NAME] <COMMAND>"));
            assert!(help.contains("Commands:"));
            assert!(help.contains("Options:"));
            assert!(help.contains("Notes:"));
            assert!(help.contains("table create <DB> <TABLE>"));
            assert!(help.contains("column add <DB> <TABLE> <NAME> <TYPE>"));
            assert!(help.contains("column remove <DB> <TABLE> <COLUMN>"));
            assert!(help.contains("\n\n  table list <DB>"));
        }
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

        let path = std::env::temp_dir().join(format!("krabb-sqlite-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, b"").unwrap();
        private(&path).unwrap();

        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_file(path);
    }
}
