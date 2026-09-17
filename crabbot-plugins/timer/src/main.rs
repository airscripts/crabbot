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
use jiff::{
    Timestamp,
    tz::{Offset, TimeZone},
};
use serde_json::json;
use tokio::time::{Duration, sleep};

const LIMIT: usize = 1_000;
const TEXT_LIMIT: usize = 256 * 1024;
const BYTE_LIMIT: usize = 32 * 1024 * 1024;
const WAIT_LIMIT: u64 = 3_600_000;
const FRAME_HEADROOM: usize = 64 * 1024;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct Task {
    text: String,
    delay: u64,
    #[serde(default)]
    due: u64,
    #[serde(default)]
    repeat: u64,
    #[serde(default)]
    cron: Option<String>,
    #[serde(default = "zone")]
    zone: String,
}

#[tokio::main]
async fn main() -> crabbot_core::Result<()> {
    let tasks = load();

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "timer".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Timer],
            commands: vec![],
        },
        move |request| {
            let tasks = Arc::clone(&tasks);
            async move { call(&tasks, request).await }
        },
    )
    .await
}

async fn call(
    tasks: &Arc<Mutex<BTreeMap<u64, Task>>>,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    call_at(tasks, request, path().as_deref()).await
}

async fn call_at(
    tasks: &Arc<Mutex<BTreeMap<u64, Task>>>,
    request: Request,
    location: Option<&Path>,
) -> crabbot_core::Result<Option<Response>> {
    let (id, method, params) = match request {
        Request::Call { id, method, params, .. } => (id, method, params),
        Request::Note { .. } => return Ok(None),
    };

    let result = match method.as_str() {
        "add" => {
            let id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("add.id is required.".into()))?;
            let text = params["text"]
                .as_str()
                .ok_or_else(|| crabbot_core::Error::Denied("add.text is required.".into()))?;
            if text.len() > TEXT_LIMIT {
                return Err(crabbot_core::Error::Denied(
                    "Timer text must be smaller than 256 KiB.".into(),
                ));
            }
            let delay = params["delay"].as_u64().unwrap_or(0);
            let repeat = params["repeat"].as_u64().unwrap_or(0);
            let cron = params["cron"].as_str().map(str::to_owned);
            if let Some(value) = &cron {
                validate_cron(value)?;
            }
            let zone = params["zone"].as_str().unwrap_or("UTC");
            validate_zone(zone)?;
            let due = cron.as_deref().map_or_else(
                || Ok(now().saturating_add(delay)),
                |value| next(value, zone, now()),
            )?;
            let previous = {
                let mut tasks = tasks
                    .lock()
                    .map_err(|_| crabbot_core::Error::Denied("Timer lock is poisoned.".into()))?;
                let previous = tasks.clone();
                tasks.insert(
                    id,
                    Task { text: text.into(), delay, due, repeat, cron, zone: zone.into() },
                );
                if tasks.len() > LIMIT {
                    let excess = tasks.len() - LIMIT;
                    let mut keys =
                        tasks.iter().map(|(id, task)| (*id, task.due)).collect::<Vec<_>>();
                    keys.sort_by_key(|(_, due)| *due);
                    let keys = keys.into_iter().take(excess).map(|(id, _)| id).collect::<Vec<_>>();
                    for key in keys {
                        tasks.remove(&key);
                    }
                }
                previous
            };
            if let Err(error) = persist_at(location, tasks) {
                if let Ok(mut tasks) = tasks.lock() {
                    *tasks = previous;
                }
                return Err(error);
            }
            json!({"id": id})
        }
        "list" => {
            let tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Timer lock is poisoned.".into()))?;
            json!({
                "items": tasks
                    .iter()
                    .map(|(id, task)| json!({"id": id, "text": task.text, "delay": task.delay, "due": task.due, "repeat": task.repeat, "cron": task.cron, "zone": task.zone}))
                    .collect::<Vec<_>>()
            })
        }
        "due" => {
            let at = params["at"].as_u64().unwrap_or_else(now);
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Timer lock is poisoned.".into()))?;
            let due = tasks
                .iter()
                .filter(|(_, task)| task.due <= at)
                .map(|(id, task)| (*id, task.clone()))
                .collect::<Vec<_>>();
            let mut next_tasks = tasks.clone();
            for (id, task) in &due {
                if task.cron.is_some() || task.repeat > 0 {
                    if let Some(current) = next_tasks.get_mut(id) {
                        current.due = task.cron.as_deref().map_or_else(
                            || Ok(at.saturating_add(current.repeat)),
                            |cron| next(cron, &current.zone, at),
                        )?;
                    }
                } else {
                    next_tasks.remove(id);
                }
            }
            let response = checked_response(
                id,
                json!({
                "items": due
                    .into_iter()
                    .map(|(id, task)| json!({"id": id, "text": task.text, "zone": task.zone}))
                    .collect::<Vec<_>>()
                }),
            )?;
            persist_locked_at(location, &next_tasks)?;
            *tasks = next_tasks;
            return Ok(Some(response));
        }
        "wait" => {
            let delay = params["delay"].as_u64().unwrap_or(0).min(WAIT_LIMIT);
            sleep(Duration::from_millis(delay)).await;
            json!({"ready": true})
        }
        "remove" => {
            let id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("remove.id is required.".into()))?;
            let previous = {
                let mut tasks = tasks
                    .lock()
                    .map_err(|_| crabbot_core::Error::Denied("Timer lock is poisoned.".into()))?;
                let previous = tasks.clone();
                let deleted = tasks.remove(&id).is_some();
                (previous, deleted)
            };
            if let Err(error) = persist_at(location, tasks) {
                if let Ok(mut tasks) = tasks.lock() {
                    *tasks = previous.0;
                }
                return Err(error);
            }
            let deleted = previous.1;
            json!({"deleted": deleted})
        }
        _ => return Ok(None),
    };

    let response = checked_response(id, result)?;
    Ok(Some(response))
}

fn checked_response(id: u64, result: serde_json::Value) -> crabbot_core::Result<Response> {
    let response = Response::ok(id, result);
    if serde_json::to_vec(&response)?.len().saturating_add(1 + FRAME_HEADROOM)
        > crabbot_core::jsonl::MAX
    {
        return Err(crabbot_core::Error::Denied(
            "Timer response exceeds the protocol frame limit.".into(),
        ));
    }
    Ok(response)
}

fn load() -> Arc<Mutex<BTreeMap<u64, Task>>> {
    load_at(path().as_deref())
}

fn load_at(path: Option<&std::path::Path>) -> Arc<Mutex<BTreeMap<u64, Task>>> {
    let Some(path) = path else {
        return Arc::new(Mutex::new(BTreeMap::new()));
    };
    let bytes = match load_file(path, BYTE_LIMIT as u64) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Arc::new(Mutex::new(BTreeMap::new())),
        Err(_) => {
            quarantine(path);
            return Arc::new(Mutex::new(BTreeMap::new()));
        }
    };
    let tasks: Option<BTreeMap<u64, Task>> =
        String::from_utf8(bytes).ok().and_then(|text| serde_json::from_str(&text).ok());
    if tasks.is_none() {
        quarantine(path);
    }
    let mut tasks = tasks.unwrap_or_default();
    if tasks.len() > LIMIT {
        let excess = tasks.len() - LIMIT;
        let mut keys = tasks.iter().map(|(id, task)| (*id, task.due)).collect::<Vec<_>>();
        keys.sort_by_key(|(_, due)| *due);
        let keys = keys.into_iter().take(excess).map(|(id, _)| id).collect::<Vec<_>>();
        for key in keys {
            tasks.remove(&key);
        }
    }
    Arc::new(Mutex::new(tasks))
}

fn path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_TIMER").map(PathBuf::from)
}

fn quarantine(path: &Path) {
    if !path.is_file() {
        return;
    }
    let target = path.with_file_name(format!(
        ".{}.corrupt-{}",
        path.file_name().and_then(|value| value.to_str()).unwrap_or("timer"),
        nonce()
    ));
    let _ = std::fs::rename(path, target);
}

fn persist_at(
    path: Option<&std::path::Path>,
    tasks: &Arc<Mutex<BTreeMap<u64, Task>>>,
) -> crabbot_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if path.parent().is_some_and(|parent| !parent.is_dir()) {
        return Err(crabbot_core::Error::Denied("Timer state directory is unavailable.".into()));
    }
    let tasks =
        tasks.lock().map_err(|_| crabbot_core::Error::Denied("Timer lock is poisoned.".into()))?;
    persist_locked_at(Some(path), &tasks)
}

fn persist_locked_at(
    path: Option<&std::path::Path>,
    tasks: &BTreeMap<u64, Task>,
) -> crabbot_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let text = serde_json::to_vec_pretty(tasks)?;
    if text.len() > BYTE_LIMIT {
        return Err(crabbot_core::Error::Denied("Timer state exceeds the size limit.".into()));
    }
    save_file(path, text).map_err(Into::into)
}

fn validate_cron(value: &str) -> crabbot_core::Result<()> {
    let fields = value.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(crabbot_core::Error::Denied("Cron must contain five fields.".into()));
    }
    for (field, (minimum, maximum)) in
        fields.iter().zip([(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)])
    {
        parse_field(field, minimum, maximum)?;
    }
    Ok(())
}

fn parse_field(value: &str, minimum: u32, maximum: u32) -> crabbot_core::Result<Vec<u32>> {
    let mut values = Vec::new();
    for item in value.split(',') {
        let (base, step) = item
            .split_once('/')
            .map_or((item, 1), |(base, step)| (base, step.parse::<u32>().unwrap_or(0)));
        if step == 0 {
            return Err(crabbot_core::Error::Denied("Cron step must be positive.".into()));
        }
        let (start, end) = if base == "*" || base == "?" {
            (minimum, maximum)
        } else if let Some((start, end)) = base.split_once('-') {
            let start = start.parse::<u32>().map_err(|_| {
                crabbot_core::Error::Denied("Cron range endpoint is invalid.".into())
            })?;
            let end = end.parse::<u32>().map_err(|_| {
                crabbot_core::Error::Denied("Cron range endpoint is invalid.".into())
            })?;
            (start, end)
        } else if item.contains('/') {
            let point = base.parse::<u32>().unwrap_or(u32::MAX);
            (point, maximum)
        } else {
            let point = base.parse::<u32>().unwrap_or(u32::MAX);
            (point, point)
        };
        if start < minimum || end > maximum || start > end {
            return Err(crabbot_core::Error::Denied(
                "Cron field is outside its valid range.".into(),
            ));
        }
        values.extend((start..=end).step_by(step as usize));
    }
    if values.is_empty() {
        return Err(crabbot_core::Error::Denied("Cron field cannot be empty.".into()));
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn cron_match(value: &str, fields: &[u32; 5]) -> crabbot_core::Result<bool> {
    let parsed = [
        parse_field(value.split_whitespace().next().unwrap_or_default(), 0, 59)?,
        parse_field(value.split_whitespace().nth(1).unwrap_or_default(), 0, 23)?,
        parse_field(value.split_whitespace().nth(2).unwrap_or_default(), 1, 31)?,
        parse_field(value.split_whitespace().nth(3).unwrap_or_default(), 1, 12)?,
        parse_field(value.split_whitespace().nth(4).unwrap_or_default(), 0, 7)?,
    ];
    let minute = parsed[0].contains(&fields[0]);
    let hour = parsed[1].contains(&fields[1]);
    let month = parsed[3].contains(&fields[3]);
    let day = parsed[2].contains(&fields[2]);
    let weekday = parsed[4].contains(&fields[4]) || (fields[4] == 0 && parsed[4].contains(&7));
    let day_field = value.split_whitespace().nth(2).unwrap_or_default();
    let weekday_field = value.split_whitespace().nth(4).unwrap_or_default();
    let day_any = day_field == "*" || day_field == "?";
    let weekday_any = weekday_field == "*" || weekday_field == "?";
    let calendar = match (day_any, weekday_any) {
        (true, true) => true,
        (true, false) => weekday,
        (false, true) => day,
        (false, false) => day || weekday,
    };
    Ok(minute && hour && month && calendar)
}

fn timezone(value: &str) -> crabbot_core::Result<TimeZone> {
    if value == "UTC" {
        return Ok(TimeZone::UTC);
    }
    if let Some(offset) = value.strip_prefix('+').or_else(|| value.strip_prefix('-')) {
        if offset.len() != 5 || offset.as_bytes().get(2) != Some(&b':') {
            return Err(crabbot_core::Error::Denied("Timer zone offset is invalid.".into()));
        }
        let hours = offset[..2]
            .parse::<i32>()
            .map_err(|_| crabbot_core::Error::Denied("Timer zone offset is invalid.".into()))?;
        let minutes = offset[3..]
            .parse::<i32>()
            .map_err(|_| crabbot_core::Error::Denied("Timer zone offset is invalid.".into()))?;
        if hours > 23 || minutes > 59 {
            return Err(crabbot_core::Error::Denied("Timer zone offset is invalid.".into()));
        }
        let sign = if value.starts_with('-') { -1 } else { 1 };
        let seconds = sign * (hours * 3_600 + minutes * 60);
        return Offset::from_seconds(seconds).map(TimeZone::fixed).map_err(|error| {
            crabbot_core::Error::Denied(format!("Timer zone is invalid: {error}."))
        });
    }
    TimeZone::get(value)
        .map_err(|error| crabbot_core::Error::Denied(format!("Timer zone is invalid: {error}.")))
}

fn validate_zone(value: &str) -> crabbot_core::Result<()> {
    timezone(value).map(|_| ())
}

fn next(cron: &str, zone: &str, after: u64) -> crabbot_core::Result<u64> {
    validate_cron(cron)?;
    let zone = timezone(zone)?;
    let start = after.saturating_add(60 - after % 60);
    if start > i64::MAX as u64 {
        return Ok(u64::MAX);
    }
    for minute in 0..(8_u64 * 366 * 24 * 60) {
        let seconds = start.saturating_add(minute.saturating_mul(60));
        let timestamp = Timestamp::from_second(i64::try_from(seconds).map_err(|_| {
            crabbot_core::Error::Denied("Timer timestamp exceeded its supported range.".into())
        })?)
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Timer timestamp is invalid: {error}."))
        })?;
        let local = timestamp.to_zoned(zone.clone());
        let weekday = local.weekday().to_sunday_zero_offset();
        let values = [
            u32::try_from(local.minute()).unwrap_or_default(),
            u32::try_from(local.hour()).unwrap_or_default(),
            u32::try_from(local.day()).unwrap_or_default(),
            u32::try_from(local.month()).unwrap_or_default(),
            u32::try_from(weekday).unwrap_or_default(),
        ];
        if cron_match(cron, &values)? {
            return Ok(seconds);
        }
    }
    Err(crabbot_core::Error::Denied("Cron has no occurrence in the search window.".into()))
}

fn zone() -> String {
    "UTC".into()
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
        BYTE_LIMIT, TEXT_LIMIT, Task, call, call_at, cron_match, load_at, next, parse_field, path,
        persist_at, timezone, validate_cron, validate_zone,
    };
    use crabbot_core::types::Request;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    fn tasks() -> Arc<Mutex<BTreeMap<u64, Task>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    #[tokio::test]
    async fn timer_can_add_list_wait_and_remove() {
        let tasks = tasks();
        let added =
            call(&tasks, Request::call(1, "add", json!({"id": 4, "text": "check", "delay": 0, "repeat": 2, "cron": "*/5 * * * *", "zone": "Europe/Rome"})))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(added.result.unwrap()["id"], 4);

        let listed = call(&tasks, Request::call(2, "list", json!({}))).await.unwrap().unwrap();
        assert_eq!(listed.result.unwrap()["items"][0]["text"], "check");

        let due =
            call(&tasks, Request::call(3, "due", json!({"at": u64::MAX}))).await.unwrap().unwrap();
        assert_eq!(due.result.unwrap()["items"][0]["text"], "check");

        let ready =
            call(&tasks, Request::call(4, "wait", json!({"delay": 0}))).await.unwrap().unwrap();
        assert_eq!(ready.result.unwrap()["ready"], true);

        let removed =
            call(&tasks, Request::call(5, "remove", json!({"id": 4}))).await.unwrap().unwrap();
        assert_eq!(removed.result.unwrap()["deleted"], true);
    }

    #[tokio::test]
    async fn timer_validates_requests_and_missing_tasks() {
        let tasks = tasks();
        assert!(call(&tasks, Request::call(1, "add", json!({"text": "check"}))).await.is_err());
        assert!(
            call(
                &tasks,
                Request::call(9, "add", json!({"id": 9, "text": "x".repeat(TEXT_LIMIT + 1)})),
            )
            .await
            .is_err()
        );
        assert!(call(&tasks, Request::call(2, "add", json!({"id": 4}))).await.is_err());
        assert!(
            call(&tasks, Request::call(3, "add", json!({"id": 4, "text": "check", "cron": "bad"})))
                .await
                .is_err()
        );
        assert!(
            call(
                &tasks,
                Request::call(4, "add", json!({"id": 4, "text": "check", "zone": "local"}))
            )
            .await
            .is_err()
        );
        let cron = call(
            &tasks,
            Request::call(5, "add", json!({"id": 5, "text": "check", "cron": "* * * * *"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(cron.result.unwrap()["id"], 5);
        assert!(
            call(
                &tasks,
                Request::call(6, "add", json!({"id": 6, "text": "check", "zone": "+24:00"}))
            )
            .await
            .is_err()
        );
        assert!(
            call(
                &tasks,
                Request::call(
                    7,
                    "add",
                    json!({"id": 7, "text": "check", "zone": "Europe/../Rome"})
                )
            )
            .await
            .is_err()
        );
        assert!(call(&tasks, Request::call(5, "remove", json!({}))).await.is_err());
        let removed =
            call(&tasks, Request::call(6, "remove", json!({"id": 4}))).await.unwrap().unwrap();
        assert_eq!(removed.result.unwrap()["deleted"], false);
        assert!(call(&tasks, Request::call(7, "unknown", json!({}))).await.unwrap().is_none());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "list".into(), params: json!({}) };
        assert!(call(&tasks, note).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restores_timer_when_persistence_fails() {
        let root =
            std::env::temp_dir().join(format!("crabbot-timer-blocker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "file").unwrap();
        let tasks = tasks();
        assert!(
            call_at(
                &tasks,
                Request::call(1, "add", json!({"id": 1, "text": "later"})),
                Some(&blocker.join("state")),
            )
            .await
            .is_err()
        );
        assert!(tasks.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn timer_persists_and_recovers_tasks() {
        let path = std::env::temp_dir().join(format!("crabbot-timer-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let tasks = tasks();
        tasks.lock().unwrap().insert(
            1,
            Task {
                text: "check".into(),
                delay: 2,
                due: 2,
                repeat: 0,
                cron: None,
                zone: "UTC".into(),
            },
        );
        persist_at(Some(&path), &tasks).unwrap();
        assert_eq!(load_at(Some(&path)).lock().unwrap()[&1].text, "check");
        std::fs::write(&path, "broken").unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().is_empty());
        std::fs::File::create(&path).unwrap().set_len(BYTE_LIMIT as u64 + 1).unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().is_empty());
        assert!(persist_at(Some(&path.with_file_name("missing-dir/item")), &tasks).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn handles_disabled_persistence() {
        let _ = path();
        assert!(load_at(None).lock().unwrap().is_empty());
        persist_at(None, &tasks()).unwrap();
    }

    #[test]
    fn schedules_cron_in_named_zone() {
        let after = 1_700_000_000;
        let due = next("*/15 * * * *", "Europe/Rome", after).unwrap();
        assert!(due > after);
        assert_eq!(due % 60, 0);
    }

    #[test]
    fn validates_cron_fields_and_zones() {
        assert_eq!(
            parse_field("1,3-5,*/10", 0, 59).unwrap(),
            vec![0, 1, 3, 4, 5, 10, 20, 30, 40, 50]
        );
        for value in ["", "0/0", "60", "5-2", "1-99", "nope", "x-1", "1-x"] {
            assert!(parse_field(value, 0, 59).is_err(), "{value} should be rejected");
        }
        assert!(validate_cron("0 12 ? * 1-5").is_ok());
        for value in ["* * * *", "* * * * * *", "0 0 0 0 0", "*/0 * * * *"] {
            assert!(validate_cron(value).is_err(), "{value} should be rejected");
        }
        assert!(validate_zone("UTC").is_ok());
        assert!(validate_zone("+05:30").is_ok());
        assert!(validate_zone("-03:00").is_ok());
        assert!(validate_zone("+24:00").is_err());
        assert!(validate_zone("Not/AZone").is_err());
        assert!(timezone("UTC").is_ok());
        assert!(cron_match("0 12 * * 1-5", &[0, 12, 1, 1, 1]).unwrap());
        assert!(!cron_match("0 12 * * 1-5", &[1, 12, 1, 1, 1]).unwrap());
    }

    #[tokio::test]
    async fn repeats_interval_tasks_and_removes_one_shots() {
        let tasks = tasks();
        call(
            &tasks,
            Request::call(1, "add", json!({"id": 1, "text": "repeat", "delay": 0, "repeat": 5})),
        )
        .await
        .unwrap();
        call(&tasks, Request::call(2, "add", json!({"id": 2, "text": "once", "delay": 0})))
            .await
            .unwrap();
        let due =
            call(&tasks, Request::call(3, "due", json!({"at": u64::MAX}))).await.unwrap().unwrap();
        assert_eq!(due.result.unwrap()["items"].as_array().unwrap().len(), 2);
        assert!(tasks.lock().unwrap().contains_key(&1));
        assert!(!tasks.lock().unwrap().contains_key(&2));
    }

    #[tokio::test]
    async fn keeps_tasks_when_due_response_is_too_large() {
        let tasks = tasks();
        let text = "x".repeat(TEXT_LIMIT);
        {
            let mut scheduled = tasks.lock().unwrap();
            for id in 0..33 {
                scheduled.insert(
                    id,
                    Task {
                        text: text.clone(),
                        delay: 0,
                        due: 0,
                        repeat: 0,
                        cron: None,
                        zone: "UTC".into(),
                    },
                );
            }
        }

        assert!(call(&tasks, Request::call(1, "due", json!({"at": u64::MAX}))).await.is_err());
        assert_eq!(tasks.lock().unwrap().len(), 33);
    }

    #[cfg(unix)]
    #[test]
    fn ignores_shared_state_files() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("crabbot-timer-shared-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, "{}").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();
        assert!(load_at(Some(&path)).lock().unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }
}
