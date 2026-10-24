#![forbid(unsafe_code)]

mod cron;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crabbot_core::{
    plugin::serve_with,
    types::{Capability, Hello, Protocol, Request, Response},
};

use crabbot_file::{load as load_file, save as save_file};
use cron::{next, validate_cron, validate_zone};

#[cfg(test)]
use cron::{CronSchedule, parse_field, timezone};

use serde_json::json;

const LIMIT: usize = 1_000;
const LIST_PAGE_SIZE: usize = 20;
const REPORT_PAGE_SIZE: usize = 20;
const CONTEXT_LIMIT: usize = 16 * 1024;
const TEXT_LIMIT: usize = 256 * 1024;
const BYTE_LIMIT: usize = 32 * 1024 * 1024;
const FRAME_HEADROOM: usize = 64 * 1024;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct Task {
    #[serde(default)]
    name: String,
    text: Arc<str>,
    delay: u64,
    #[serde(default)]
    due: u64,
    #[serde(default)]
    repeat: u64,
    #[serde(default)]
    cron: Option<String>,
    #[serde(default = "zone")]
    zone: String,
    #[serde(default = "pending")]
    status: String,
    #[serde(default)]
    context: Arc<serde_json::Value>,
    #[serde(default)]
    last_result: Option<String>,
    #[serde(default)]
    last_outcome: Option<String>,
    #[serde(default = "skip_missed")]
    missed: String,
    #[serde(default = "pause_failure")]
    on_failure: String,
    #[serde(default)]
    replay_count: u8,
    #[serde(default)]
    retries: u8,
    #[serde(default)]
    attempts: u8,
    #[serde(default)]
    retry_at: Option<u64>,
    #[serde(default)]
    manual_run: bool,
    #[serde(default)]
    manual_retry: bool,
    #[serde(default)]
    manual_origin: bool,
    #[serde(default)]
    cancel_requested: bool,
    #[serde(default)]
    run_id: Option<u64>,
    #[serde(default)]
    finished_run_id: Option<u64>,
    #[serde(default)]
    finish_result: Option<serde_json::Value>,
    #[serde(default)]
    report_pending: bool,
    #[serde(default)]
    imported_timer_id: Option<u64>,
}

type Tasks = BTreeMap<u64, Arc<Task>>;

impl Task {
    fn ready_for_claim_at(&self, at: u64) -> bool {
        self.status == "active"
            && !self.report_pending
            && (self.manual_run || self.retry_at.map_or(self.due <= at, |retry_at| retry_at <= at))
    }
}

fn pending() -> String {
    "pending".into()
}

fn skip_missed() -> String {
    "skip".into()
}

fn pause_failure() -> String {
    "pause".into()
}

fn schedule_name() -> String {
    const ADJECTIVES: &[&str] = &[
        "bouncy", "cheeky", "cosmic", "cranky", "dizzy", "fluffy", "funky", "goofy", "grumpy",
        "jazzy", "jolly", "loopy", "mighty", "mushy", "noisy", "odd", "peppy", "quirky", "sleepy",
        "snazzy", "spicy", "squishy", "wobbly", "zippy",
    ];

    const CREATURES: &[&str] = &[
        "alpaca", "badger", "capybara", "crab", "duck", "ferret", "gecko", "goose", "hamster",
        "kiwi", "lemur", "lobster", "meerkat", "narwhal", "otter", "panda", "penguin", "puffin",
        "raccoon", "seal", "shrew", "sloth", "squid", "walrus",
    ];

    let mut random = [0_u8; 2];

    if getrandom::fill(&mut random).is_err() {
        return "wobbly-walrus".into();
    }

    format!(
        "{}-{}",
        ADJECTIVES[usize::from(random[0]) % ADJECTIVES.len()],
        CREATURES[usize::from(random[1]) % CREATURES.len()]
    )
}

fn valid_schedule_name(name: &str) -> bool {
    let mut characters = name.bytes();

    name.len() <= 80
        && characters.next().is_some_and(|character| character.is_ascii_alphanumeric())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, b'-' | b'_' | b'.')
        })
}

fn unique_schedule_name(names: &BTreeSet<String>) -> String {
    for _ in 0..32 {
        let name = schedule_name();

        if !names.contains(&name) {
            return name;
        }
    }

    let base = schedule_name();
    let mut suffix = 2;

    loop {
        let name = format!("{base}-{suffix}");

        if !names.contains(&name) {
            return name;
        }

        suffix += 1;
    }
}

fn normalize_schedule_names(tasks: &mut Tasks) -> bool {
    let mut names = BTreeSet::new();
    let mut changed = false;

    for shared_task in tasks.values_mut() {
        let task = Arc::make_mut(shared_task);

        if !valid_schedule_name(&task.name) || names.contains(&task.name) {
            task.name = unique_schedule_name(&names);
            changed = true;
        }

        names.insert(task.name.clone());
    }

    changed
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    crabbot_log::run_plugin("scheduler", plugin_main()).await
}

async fn plugin_main() -> crabbot_core::Result<()> {
    prepare_state_directory(path().as_deref())?;

    let tasks = load()?;

    serve_with(
        Hello {
            protocol: Protocol::CURRENT,
            id: "scheduler".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: vec![Capability::Scheduler],
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
    tasks: &Arc<Mutex<Tasks>>,
    request: Request,
) -> crabbot_core::Result<Option<Response>> {
    call_at(tasks, request, path().as_deref()).await
}

async fn call_at(
    tasks: &Arc<Mutex<Tasks>>,
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
                    "Scheduler text must be smaller than 256 KiB.".into(),
                ));
            }

            let tasks_guard = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let name = params
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    let names =
                        tasks_guard.values().map(|task| task.name.clone()).collect::<BTreeSet<_>>();
                    unique_schedule_name(&names)
                });

            if !valid_schedule_name(&name) {
                return Err(crabbot_core::Error::Denied(
                    "Schedule names must start with a letter or number and contain only letters, numbers, hyphens, underscores, or dots (up to 80 characters).".into(),
                ));
            }

            if tasks_guard.values().any(|task| task.name == name) {
                return Err(crabbot_core::Error::Denied(format!(
                    "A schedule named {name} already exists."
                )));
            }

            drop(tasks_guard);

            let context = params.get("context").cloned().unwrap_or_default();

            if serde_json::to_vec(&context)?.len() > CONTEXT_LIMIT {
                return Err(crabbot_core::Error::Denied(
                    "Scheduler context exceeds 16 KiB.".into(),
                ));
            }

            let delay = params["delay"].as_u64().unwrap_or(0);
            let repeat = params["repeat"].as_u64().unwrap_or(0);
            let cron = params["cron"].as_str().map(str::to_owned);
            let at = params["at"].as_u64();
            let retries = params["retries"].as_u64().unwrap_or_default();
            let status = if params["activate"] == true { "active" } else { "pending" };

            if delay > 31_536_000 || repeat > 31_536_000 {
                return Err(crabbot_core::Error::Denied(
                    "Scheduler delays and intervals cannot exceed one year.".into(),
                ));
            }

            if at.is_some() && cron.is_some() {
                return Err(crabbot_core::Error::Denied(
                    "Choose either an exact run time or a recurring cron schedule.".into(),
                ));
            }

            if cron.is_some() && repeat > 0 {
                return Err(crabbot_core::Error::Denied(
                    "Choose either a cron schedule or a fixed repeat interval.".into(),
                ));
            }

            if at.is_some_and(|value| value < now().saturating_add(5)) {
                return Err(crabbot_core::Error::Denied(
                    "Exact scheduled times must be at least five seconds in the future.".into(),
                ));
            }

            if retries > 5 {
                return Err(crabbot_core::Error::Denied(
                    "Scheduled tasks can be configured for at most five retries.".into(),
                ));
            }

            if let Some(value) = &cron {
                validate_cron(value)?;
            }

            let configured_zone = std::env::var("CRABBOT_TIMEZONE").ok();
            let zone =
                params["zone"].as_str().or(configured_zone.as_deref()).unwrap_or("UTC").to_owned();

            if cron.is_some() && params["zone"].as_str().is_none() && configured_zone.is_none() {
                return Err(crabbot_core::Error::Denied("A timezone is required for cron schedules; ask the user or configure scheduler.timezone.".into()));
            }

            validate_zone(&zone)?;
            let configured_missed = std::env::var("CRABBOT_MISSED_RUNS").ok();
            let missed = params["missed"]
                .as_str()
                .or(configured_missed.as_deref())
                .unwrap_or("skip")
                .to_owned();

            if !matches!(missed.as_str(), "skip" | "coalesce" | "replay") {
                return Err(crabbot_core::Error::Denied(
                    "Missed-run policy must be skip, coalesce, or replay.".into(),
                ));
            }

            let on_failure = params["on_failure"].as_str().unwrap_or("pause").to_owned();

            if !matches!(on_failure.as_str(), "pause" | "continue") {
                return Err(crabbot_core::Error::Denied(
                    "Failure policy must be pause or continue.".into(),
                ));
            }

            if on_failure == "continue" && cron.is_none() && repeat == 0 {
                return Err(crabbot_core::Error::Denied(
                    "Failure policy continue requires a recurring schedule.".into(),
                ));
            }

            let due = if let Some(at) = at {
                at
            } else {
                cron.as_deref().map_or_else(
                    || Ok(now().saturating_add(delay)),
                    |value| next(value, &zone, now()),
                )?
            };

            let previous = {
                let mut tasks = tasks.lock().map_err(|_| {
                    crabbot_core::Error::Denied("Scheduler lock is poisoned.".into())
                })?;

                if tasks.contains_key(&id) {
                    return Err(crabbot_core::Error::Denied(format!(
                        "Schedule ID {id} already exists."
                    )));
                }

                if tasks.values().any(|task| task.name == name) {
                    return Err(crabbot_core::Error::Denied(format!(
                        "A schedule named {name} already exists."
                    )));
                }

                if tasks.len() >= LIMIT {
                    return Err(crabbot_core::Error::Denied(format!(
                        "The scheduler already has {LIMIT} tasks. Remove a task before adding another."
                    )));
                }

                let previous = tasks.clone();
                tasks.insert(
                    id,
                    Arc::new(Task {
                        name: name.clone(),
                        text: Arc::from(text),
                        delay,
                        due,
                        repeat,
                        cron: cron.clone(),
                        zone: zone.clone(),
                        status: status.into(),
                        context: Arc::new(context),
                        last_result: None,
                        last_outcome: None,
                        missed,
                        on_failure: on_failure.clone(),
                        replay_count: 0,
                        retries: retries as u8,
                        attempts: 0,
                        retry_at: None,
                        manual_run: false,
                        manual_retry: false,
                        manual_origin: false,
                        cancel_requested: false,
                        run_id: None,
                        finished_run_id: None,
                        finish_result: None,
                        report_pending: false,
                        imported_timer_id: None,
                    }),
                );

                previous
            };

            if let Err(error) = persist_at(location, tasks) {
                if let Ok(mut tasks) = tasks.lock() {
                    *tasks = previous;
                }

                return Err(error);
            }

            json!({"id": id, "name": name, "due": due, "cron": cron, "repeat": repeat, "zone": zone, "retries": retries, "on_failure": on_failure, "status": status})
        }

        "reports" => {
            let tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let after = params["after"].as_u64();
            let items = tasks
                .iter()
                .filter(|(task_id, task)| {
                    task.report_pending && after.is_none_or(|after| **task_id > after)
                })
                .take(REPORT_PAGE_SIZE)
                .map(|(task_id, task)| {
                    json!({
                        "id": task_id,
                        "name": task.name,
                        "run_id": task.finished_run_id,
                        "context": task.context,
                        "result": task.last_result,
                        "finish": task.finish_result
                    })
                })
                .collect::<Vec<_>>();

            json!({"items": items})
        }

        "report" => {
            let task_id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("report.id is required.".into()))?;
            let run_id = params["run_id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("report.run_id is required.".into()))?;
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let Some(task) = tasks.get(&task_id) else {
                return Err(crabbot_core::Error::Denied("Scheduled task was not found.".into()));
            };

            if task.finished_run_id != Some(run_id) {
                return Err(crabbot_core::Error::Denied(
                    "report.run_id does not match the finished execution.".into(),
                ));
            }

            if !task.report_pending {
                return Ok(Some(checked_response(id, json!({"id": task_id, "reported": true}))?));
            }

            let mut next_tasks = tasks.clone();
            let task = next_tasks.get_mut(&task_id).ok_or_else(|| {
                crabbot_core::Error::Denied("Scheduled task was not found.".into())
            })?;
            let task = Arc::make_mut(task);
            task.report_pending = false;
            persist_locked_at(location, &next_tasks)?;
            *tasks = next_tasks;
            json!({"id": task_id, "reported": true})
        }

        "list" => {
            let tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let page = params["page"].as_u64().unwrap_or(1);

            if page == 0 {
                return Err(crabbot_core::Error::Denied("List page must be at least 1.".into()));
            }

            let total = tasks.len();
            let pages = total.max(1).div_ceil(LIST_PAGE_SIZE);

            if page > pages as u64 {
                return Err(crabbot_core::Error::Denied(format!(
                    "List page {page} is outside the available range 1–{pages}."
                )));
            }

            let start = (page as usize - 1).saturating_mul(LIST_PAGE_SIZE);
            let items = tasks
                .iter()
                .skip(start)
                .take(LIST_PAGE_SIZE)
                .map(|(id, task)| {
                    json!({
                        "id": id,
                        "name": if task.name.is_empty() {
                            format!("schedule-{id}")
                        } else {
                            task.name.clone()
                        },
                        "text": task.text.chars().take(1_000).collect::<String>(),
                        "delay": task.delay,
                        "due": task.due,
                        "repeat": task.repeat,
                        "cron": task.cron,
                        "zone": task.zone,
                        "status": task.status,
                        "last_outcome": task.last_outcome,
                        "context": task.context,
                        "missed": task.missed,
                        "on_failure": task.on_failure,
                        "retries": task.retries,
                        "attempts": task.attempts,
                        "retry_at": task.retry_at,
                        "run_requested": task.manual_run,
                    })
                })
                .collect::<Vec<_>>();
            let from = if total == 0 { 0 } else { start + 1 };

            let to = start + items.len();

            json!({"items": items, "total": total, "page": page, "pages": pages, "from": from, "to": to})
        }

        "claim" => {
            let at = params["at"].as_u64().unwrap_or_else(now);
            let since = params["since"].as_u64().unwrap_or_default();
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let due = tasks
                .iter()
                .filter(|(_, task)| task.ready_for_claim_at(at))
                .min_by_key(|(_, task)| task.due)
                .map(|(id, task)| (*id, task.clone()));

            let Some((task_id, task)) = due else {
                return checked_response(id, json!({"item": null})).map(Some);
            };

            let mut next_tasks = tasks.clone();
            let run_id = params["lease"].as_u64().unwrap_or(id);
            let scheduled_run = !task.manual_run && task.retry_at.is_none() && !task.manual_retry;
            let was_missed = scheduled_run && since > 0 && task.due < since;
            let was_overdue = scheduled_run && task.due < at;
            let was_retry = task.retry_at.is_some() || task.manual_retry;
            let manual_origin = task.manual_origin || (task.manual_run && !task.manual_retry);
            let replay_exhausted =
                scheduled_run && task.missed == "replay" && task.replay_count >= 5;

            if (was_missed && task.missed == "skip") || replay_exhausted {
                let report_run_id = params["lease"].as_u64().unwrap_or(id);
                let result = "Missed occurrence skipped because the daemon was offline.";

                if task.cron.is_some() || task.repeat > 0 {
                    let next_due = task.cron.as_deref().map_or_else(
                        || Ok(at.saturating_add(task.repeat)),
                        |cron| next(cron, &task.zone, at),
                    )?;
                    let current = next_tasks.get_mut(&task_id).ok_or_else(|| {
                        crabbot_core::Error::Denied("Scheduled task was not found.".into())
                    })?;
                    let current = Arc::make_mut(current);
                    current.due = next_due;
                    current.replay_count = 0;
                    current.last_result = Some(result.into());
                    current.last_outcome = Some("skipped".into());
                } else {
                    let current = next_tasks.get_mut(&task_id).ok_or_else(|| {
                        crabbot_core::Error::Denied("Scheduled task was not found.".into())
                    })?;
                    let current = Arc::make_mut(current);
                    current.status = "skipped".into();
                    current.last_result = Some(result.into());
                    current.last_outcome = Some("skipped".into());
                }

                let current = next_tasks.get_mut(&task_id).ok_or_else(|| {
                    crabbot_core::Error::Denied("Scheduled task was not found.".into())
                })?;
                let current = Arc::make_mut(current);
                current.finished_run_id = Some(report_run_id);
                current.finish_result = Some(json!({
                    "status": "skipped",
                    "success": false,
                    "finished_at": at,
                    "scheduled_for": task.due,
                    "result": result
                }));
                current.report_pending = true;

                let response = checked_response(
                    id,
                    json!({"item": {"id": task_id, "name": task.name, "text": task.text, "context": task.context, "scheduled_for": task.due, "skipped_at": at, "run_id": report_run_id, "skipped": true, "missed": task.missed}}),
                )?;
                persist_locked_at(location, &next_tasks)?;
                *tasks = next_tasks;
                return Ok(Some(response));
            }

            if let Some(current) = next_tasks.get_mut(&task_id) {
                let current = Arc::make_mut(current);
                current.status = "running".into();
                current.run_id = Some(run_id);
                current.manual_origin |= current.manual_run && !current.manual_retry;
                current.manual_run = false;
                current.manual_retry = false;
                current.retry_at = None;
                current.attempts = current.attempts.saturating_add(1);

                if was_overdue && current.missed == "replay" {
                    current.replay_count = current.replay_count.saturating_add(1);
                }
            }

            let response = checked_response(
                id,
                json!({"item": {"id": task_id, "name": task.name, "text": task.text, "zone": task.zone, "cron": task.cron, "repeat": task.repeat, "context": task.context, "scheduled_for": task.due, "missed": task.missed, "manual": manual_origin, "retry": was_retry, "attempt": task.attempts.saturating_add(1), "run_id": run_id}}),
            )?;
            persist_locked_at(location, &next_tasks)?;
            *tasks = next_tasks;
            return Ok(Some(response));
        }

        "finish" => {
            let task_id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("finish.id is required.".into()))?;
            let success = params["success"].as_bool().unwrap_or(false);
            let result = params["result"].as_str().unwrap_or_default();
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let Some(current) = tasks.get(&task_id) else {
                return Err(crabbot_core::Error::Denied("Scheduled task was not found.".into()));
            };

            let run_id = params["run_id"]
                .as_u64()
                .or(current.run_id)
                .ok_or_else(|| crabbot_core::Error::Denied("finish.run_id is required.".into()))?;

            if current.finished_run_id == Some(run_id)
                && let Some(result) = current.finish_result.clone()
            {
                return checked_response(id, result).map(Some);
            }

            if current.status != "running" {
                return Err(crabbot_core::Error::Denied("Scheduled task is not running.".into()));
            }

            if current.run_id != Some(run_id) {
                return Err(crabbot_core::Error::Denied(
                    "finish.run_id does not match the active execution.".into(),
                ));
            }

            let mut next_tasks = tasks.clone();
            let task = next_tasks.get_mut(&task_id).ok_or_else(|| {
                crabbot_core::Error::Denied("Scheduled task was not found.".into())
            })?;
            let task = Arc::make_mut(task);

            task.last_result = Some(result.chars().take(2_000).collect());

            let manual_run = params["manual"].as_bool().unwrap_or(false);
            let cancelled = task.cancel_requested || params["cancelled"] == true;
            let scheduled_for = task.due;

            if cancelled {
                task.status = "cancelled".into();
            } else if params["pause"] == true {
                task.status = "paused".into();
                task.retry_at = None;
                task.manual_run = false;
                task.manual_retry = false;
            } else if success && (task.cron.is_some() || task.repeat > 0) {
                let current_time = now();

                if !manual_run {
                    let next_due = task.cron.as_deref().map_or_else(
                        || Ok(task.due.saturating_add(task.repeat)),
                        |cron| next(cron, &task.zone, task.due),
                    )?;
                    task.due = if task.missed == "replay" && next_due <= current_time {
                        if task.replay_count < 5 {
                            next_due
                        } else {
                            task.replay_count = 0;
                            task.cron.as_deref().map_or_else(
                                || Ok(current_time.saturating_add(task.repeat)),
                                |cron| next(cron, &task.zone, current_time),
                            )?
                        }
                    } else {
                        task.replay_count = 0;
                        task.cron.as_deref().map_or_else(
                            || Ok(current_time.saturating_add(task.repeat)),
                            |cron| next(cron, &task.zone, current_time),
                        )?
                    };
                } else if task.due <= current_time {
                    task.replay_count = 0;
                    task.due = task.cron.as_deref().map_or_else(
                        || Ok(current_time.saturating_add(task.repeat)),
                        |cron| next(cron, &task.zone, current_time),
                    )?;
                }

                task.status = "active".into();
                task.attempts = 0;
                task.retry_at = None;
                task.manual_run = false;
                task.manual_origin = false;
            } else if success {
                task.status = "completed".into();
            } else if task.attempts <= task.retries {
                let backoff = 5_u64.saturating_mul(1_u64 << task.attempts.saturating_sub(1).min(6));
                task.retry_at = Some(now().saturating_add(backoff));
                task.status = "active".into();
                task.manual_run = false;
            } else if task.on_failure == "continue" && (task.cron.is_some() || task.repeat > 0) {
                let current_time = now();
                task.due = task.cron.as_deref().map_or_else(
                    || Ok(current_time.saturating_add(task.repeat)),
                    |cron| next(cron, &task.zone, current_time),
                )?;
                task.replay_count = 0;
                task.status = "active".into();
                task.attempts = 0;
                task.retry_at = None;
                task.manual_run = false;
                task.manual_retry = false;
                task.manual_origin = false;
            } else {
                task.status = "paused".into();
                task.retry_at = None;
                task.manual_run = false;
            }

            let status = task.status.clone();
            task.last_outcome = Some(
                if cancelled {
                    "cancelled"
                } else if success {
                    "completed"
                } else if status == "active" && task.retry_at.is_some() {
                    "retrying"
                } else if status == "active" {
                    "continuing"
                } else {
                    "failed"
                }
                .into(),
            );
            let result = json!({
                "finished": true,
                "status": status,
                "due": task.due,
                "scheduled_for": scheduled_for,
                "retry_at": task.retry_at,
                "attempts": task.attempts,
                "retries": task.retries,
                "on_failure": task.on_failure,
                "success": success,
                "cancelled": cancelled,
                "finished_at": now(),
                "manual": manual_run,
                "retry": params["retry"] == true,
                "attempt": task.attempts,
                "missed": task.missed,
                "scheduled_for": scheduled_for,
                "result": result
            });

            task.finished_run_id = Some(run_id);
            task.finish_result = Some(result.clone());
            task.report_pending = true;

            persist_locked_at(location, &next_tasks)?;
            *tasks = next_tasks;
            result
        }

        "cancelled" => {
            let task_id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("id is required.".into()))?;
            let tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            json!({"cancelled": tasks.get(&task_id).is_some_and(|task| task.cancel_requested)})
        }

        "approve" | "pause" | "resume" | "run" | "retry" => {
            let task_id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("id is required.".into()))?;
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let Some(current) = tasks.get(&task_id) else {
                return Err(crabbot_core::Error::Denied("Scheduled task was not found.".into()));
            };

            let status = match method.as_str() {
                "approve" if current.status == "pending" => "active",
                "pause" if current.status == "active" => "paused",
                "resume" if current.status == "paused" => "active",

                "run" if matches!(current.status.as_str(), "active" | "completed" | "skipped") => {
                    "active"
                }

                "retry" if matches!(current.status.as_str(), "paused" | "skipped") => "active",

                "approve" => {
                    return Err(crabbot_core::Error::Denied(
                        "Only pending tasks can be approved.".into(),
                    ));
                }

                "pause" => {
                    return Err(crabbot_core::Error::Denied(
                        "Only active tasks can be paused.".into(),
                    ));
                }

                "run" => {
                    return Err(crabbot_core::Error::Denied(
                        "Only active schedules or completed and skipped one-time tasks can be run now."
                            .into(),
                    ));
                }

                "retry" => {
                    return Err(crabbot_core::Error::Denied(
                        "Only paused or skipped schedules can be retried.".into(),
                    ));
                }

                _ => {
                    return Err(crabbot_core::Error::Denied(
                        "Only paused tasks can be resumed.".into(),
                    ));
                }
            };

            let mut next_tasks = tasks.clone();
            let task = next_tasks.get_mut(&task_id).ok_or_else(|| {
                crabbot_core::Error::Denied("Scheduled task was not found.".into())
            })?;
            let task = Arc::make_mut(task);

            match method.as_str() {
                "run" => {
                    task.manual_run = true;
                    task.manual_retry = false;
                    task.manual_origin = true;
                    task.cancel_requested = false;
                    task.retry_at = None;
                    task.attempts = 0;
                }

                "retry" => {
                    task.manual_run = true;
                    task.manual_retry = true;
                    task.cancel_requested = false;
                    task.retry_at = None;
                    task.attempts = 0;
                }

                "resume" => {
                    task.attempts = 0;
                    task.retry_at = None;
                    task.manual_run = false;
                    task.manual_retry = false;
                    task.manual_origin = false;
                    task.cancel_requested = false;
                }

                "pause" => {
                    task.attempts = 0;
                    task.retry_at = None;
                    task.manual_run = false;
                    task.manual_retry = false;
                    task.manual_origin = false;
                }

                _ => {}
            }

            task.status = status.into();
            persist_locked_at(location, &next_tasks)?;
            *tasks = next_tasks;
            json!({"id": task_id, "status": status})
        }

        "recover" => {
            let active = params["active"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_u64)
                .collect::<BTreeSet<_>>();
            let mut tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            let mut next_tasks = tasks.clone();
            let mut recovered = 0;
            let mut cancelled = 0;

            next_tasks.retain(|id, task| match task.status.as_str() {
                "running" if !active.contains(id) => {
                    recovered += 1;

                    if task.cancel_requested {
                        cancelled += 1;
                        return false;
                    }

                    let task = Arc::make_mut(task);
                    task.status = "paused".into();
                    task.retry_at = None;
                    task.manual_run = false;
                    task.manual_retry = false;
                    task.cancel_requested = false;
                    task.last_result =
                        Some("Execution was interrupted; review before resuming.".into());
                    task.last_outcome = Some("failed".into());
                    let run_id = task.run_id.take().unwrap_or(*id);
                    task.finished_run_id = Some(run_id);
                    task.finish_result = Some(json!({
                        "finished": true,
                        "status": "paused",
                        "success": false,
                        "cancelled": false,
                        "finished_at": now(),
                        "scheduled_for": task.due,
                        "result": task.last_result
                    }));
                    task.report_pending = true;
                    true
                }

                _ => true,
            });

            if recovered > 0 || cancelled > 0 {
                persist_locked_at(location, &next_tasks)?;
                *tasks = next_tasks;
            }

            json!({"recovered": recovered, "cancelled": cancelled})
        }

        "due" => {
            let at = params["at"].as_u64().unwrap_or_else(now);
            let tasks = tasks
                .lock()
                .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;
            json!({"items": tasks.iter().filter(|(_, task)| task.status == "active" && task.due <= at).map(|(id, task)| json!({"id": id, "text": task.text, "due": task.due})).collect::<Vec<_>>()})
        }

        "remove" => {
            let id = params["id"]
                .as_u64()
                .ok_or_else(|| crabbot_core::Error::Denied("remove.id is required.".into()))?;
            let previous = {
                let mut tasks = tasks.lock().map_err(|_| {
                    crabbot_core::Error::Denied("Scheduler lock is poisoned.".into())
                })?;

                let previous = tasks.clone();
                let cancel_requested = if let Some(task) = tasks.get_mut(&id)
                    && task.status == "running"
                {
                    let task = Arc::make_mut(task);
                    task.cancel_requested = true;
                    true
                } else {
                    false
                };

                if !cancel_requested {
                    tasks.remove(&id);
                }

                (previous, cancel_requested)
            };

            if let Err(error) = persist_at(location, tasks) {
                if let Ok(mut tasks) = tasks.lock() {
                    *tasks = previous.0;
                }

                return Err(error);
            }

            if previous.1 {
                json!({"deleted": false, "cancel_requested": true})
            } else {
                json!({"deleted": previous.0.contains_key(&id)})
            }
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
            "Scheduler response exceeds the protocol frame limit.".into(),
        ));
    }

    Ok(response)
}

fn load() -> crabbot_core::Result<Arc<Mutex<Tasks>>> {
    load_with_legacy_at(path().as_deref(), legacy_path().as_deref())
}

fn load_with_legacy_at(
    schedule_path: Option<&std::path::Path>,
    legacy_path: Option<&std::path::Path>,
) -> crabbot_core::Result<Arc<Mutex<Tasks>>> {
    let Some(schedule_path) = schedule_path else {
        return load_at(None);
    };

    let schedule = load_file(schedule_path, BYTE_LIMIT as u64).map_err(|error| {
        crabbot_core::Error::Denied(format!(
            "Could not load scheduler state {}: {error}",
            schedule_path.display()
        ))
    })?;

    let Some(legacy_path) = legacy_path.filter(|legacy_path| *legacy_path != schedule_path) else {
        return schedule.map_or_else(
            || load_at(Some(schedule_path)),
            |bytes| load_bytes(schedule_path, bytes),
        );
    };

    let legacy = load_file(legacy_path, BYTE_LIMIT as u64).map_err(|error| {
        crabbot_core::Error::Denied(format!(
            "Could not load legacy timer state {}: {error}",
            legacy_path.display()
        ))
    })?;

    let Some(bytes) = legacy else {
        return schedule.map_or_else(
            || load_at(Some(schedule_path)),
            |bytes| load_bytes(schedule_path, bytes),
        );
    };

    let migration_marker = schedule_path.with_file_name("timer-migration-complete");
    let marker = load_file(&migration_marker, 1).map_err(|error| {
        crabbot_core::Error::Denied(format!(
            "Could not load timer migration marker {}: {error}",
            migration_marker.display()
        ))
    })?;

    if let Some(marker) = marker {
        if marker != b"1" {
            return Err(crabbot_core::Error::Denied(format!(
                "Timer migration marker {} is corrupt; repair it before restarting the scheduler.",
                migration_marker.display()
            )));
        }

        return schedule.map_or_else(
            || load_at(Some(schedule_path)),
            |bytes| load_bytes(schedule_path, bytes),
        );
    }

    let mut tasks: Tasks = match schedule {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(|_| {
            crabbot_core::Error::Denied(format!(
                "Scheduler state {} is corrupt; repair it before restarting the scheduler.",
                schedule_path.display()
            ))
        })?,
        None => BTreeMap::new(),
    };

    let text = String::from_utf8(bytes).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Legacy timer state {} is not valid UTF-8; repair it before restarting the scheduler.",
            legacy_path.display()
        ))
    })?;

    let legacy_tasks: Tasks = serde_json::from_str(&text).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Legacy timer state {} is corrupt; repair it before restarting the scheduler.",
            legacy_path.display()
        ))
    })?;

    if legacy_tasks.len() > LIMIT {
        return Err(crabbot_core::Error::Denied(format!(
            "Legacy timer state {} exceeds the scheduler task limit; remove excess timers before restarting.",
            legacy_path.display()
        )));
    }

    let mut imported = 0;

    for (legacy_id, shared_task) in legacy_tasks {
        let mut task = (*shared_task).clone();

        if tasks.values().any(|current| current.imported_timer_id == Some(legacy_id)) {
            continue;
        }

        if tasks.len() >= LIMIT {
            return Err(crabbot_core::Error::Denied(format!(
                "Legacy timer state {} cannot be fully imported because the scheduler is at its task limit.",
                legacy_path.display()
            )));
        }

        let id = if tasks.contains_key(&legacy_id) {
            (1..=u64::MAX).find(|id| !tasks.contains_key(id)).ok_or_else(|| {
                crabbot_core::Error::Denied(
                    "No scheduler task IDs remain for timer migration.".into(),
                )
            })?
        } else {
            legacy_id
        };

        task.status = "active".into();
        task.missed = "coalesce".into();
        task.imported_timer_id = Some(legacy_id);
        tasks.insert(id, Arc::new(task));
        imported += 1;
    }

    normalize_schedule_names(&mut tasks);
    persist_locked_at(Some(schedule_path), &tasks)?;
    save_file(&migration_marker, b"1").map_err(|error| {
        crabbot_core::Error::Denied(format!(
            "Could not save timer migration marker {}: {error}",
            migration_marker.display()
        ))
    })?;

    if imported > 0 {
        eprintln!(
            "Migrated {imported} existing timer task(s) from {} into scheduler state.",
            legacy_path.display()
        );
    }

    Ok(Arc::new(Mutex::new(tasks)))
}

fn load_bytes(path: &std::path::Path, bytes: Vec<u8>) -> crabbot_core::Result<Arc<Mutex<Tasks>>> {
    let text = String::from_utf8(bytes).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Scheduler state {} is not valid UTF-8; repair it before restarting the scheduler.",
            path.display()
        ))
    })?;

    let mut tasks = serde_json::from_str(&text).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Scheduler state {} is corrupt; repair it before restarting the scheduler.",
            path.display()
        ))
    })?;

    if normalize_schedule_names(&mut tasks) {
        persist_locked_at(Some(path), &tasks)?;
    }

    Ok(Arc::new(Mutex::new(tasks)))
}

fn load_at(path: Option<&std::path::Path>) -> crabbot_core::Result<Arc<Mutex<Tasks>>> {
    let Some(path) = path else {
        return Ok(Arc::new(Mutex::new(BTreeMap::new())));
    };

    let bytes = match load_file(path, BYTE_LIMIT as u64) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(Arc::new(Mutex::new(BTreeMap::new()))),

        Err(error) => {
            return Err(crabbot_core::Error::Denied(format!(
                "Could not load scheduler state {}: {error}",
                path.display()
            )));
        }
    };

    let text = String::from_utf8(bytes).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Scheduler state {} is not valid UTF-8; repair it before restarting the scheduler.",
            path.display()
        ))
    })?;

    let mut tasks = serde_json::from_str(&text).map_err(|_| {
        crabbot_core::Error::Denied(format!(
            "Scheduler state {} is corrupt; repair it before restarting the scheduler.",
            path.display()
        ))
    })?;

    if normalize_schedule_names(&mut tasks) {
        persist_locked_at(Some(path), &tasks)?;
    }

    Ok(Arc::new(Mutex::new(tasks)))
}

fn path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_HOME")
        .map(PathBuf::from)
        .map(|home| home.join("data/schedules/schedules.json"))
}

fn prepare_state_directory(schedule_path: Option<&Path>) -> crabbot_core::Result<()> {
    let Some(schedule_path) = schedule_path else {
        return Ok(());
    };

    let data = schedule_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| crabbot_core::Error::Denied("Scheduler state path is invalid.".into()))?;

    let schedules = schedule_path
        .parent()
        .ok_or_else(|| crabbot_core::Error::Denied("Scheduler state path is invalid.".into()))?;

    for directory in [data, schedules] {
        match std::fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(crabbot_core::Error::Denied(format!(
                    "Scheduler state path must be a real directory: {}.",
                    directory.display()
                )));
            }

            Ok(_) => {}

            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(directory).map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Could not create scheduler state directory {}: {error}",
                        directory.display()
                    ))
                })?;
            }

            Err(error) => {
                return Err(crabbot_core::Error::Denied(format!(
                    "Could not inspect scheduler state directory {}: {error}",
                    directory.display()
                )));
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = std::fs::metadata(directory)
                .map_err(|error| {
                    crabbot_core::Error::Denied(format!(
                        "Could not inspect scheduler state directory {}: {error}",
                        directory.display()
                    ))
                })?
                .permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(directory, permissions).map_err(|error| {
                crabbot_core::Error::Denied(format!(
                    "Could not secure scheduler state directory {}: {error}",
                    directory.display()
                ))
            })?;
        }
    }

    Ok(())
}

fn legacy_path() -> Option<PathBuf> {
    std::env::var_os("CRABBOT_TIMER").map(PathBuf::from)
}

fn persist_at(
    path: Option<&std::path::Path>,
    tasks: &Arc<Mutex<Tasks>>,
) -> crabbot_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    if path.parent().is_some_and(|parent| !parent.is_dir()) {
        return Err(crabbot_core::Error::Denied(
            "Scheduler state directory is unavailable.".into(),
        ));
    }

    let tasks = tasks
        .lock()
        .map_err(|_| crabbot_core::Error::Denied("Scheduler lock is poisoned.".into()))?;

    persist_locked_at(Some(path), &tasks)
}

fn persist_locked_at(path: Option<&std::path::Path>, tasks: &Tasks) -> crabbot_core::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    let text = serde_json::to_vec_pretty(tasks)?;

    if text.len() > BYTE_LIMIT {
        return Err(crabbot_core::Error::Denied("Scheduler state exceeds the size limit.".into()));
    }

    save_file(path, text).map_err(Into::into)
}

fn zone() -> String {
    "UTC".into()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs())
}

#[cfg(test)]
mod tests {
    use super::{
        BYTE_LIMIT, LIMIT, TEXT_LIMIT, Task, Tasks, call, call_at, load_at, load_with_legacy_at,
        next, now, parse_field, path, persist_at, timezone, validate_cron, validate_zone,
    };

    use crabbot_core::types::Request;
    use serde_json::json;
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    fn tasks() -> Arc<Mutex<Tasks>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    #[tokio::test]
    async fn task_clones_share_large_immutable_fields() {
        let tasks = tasks();
        let context = json!({"value": "x".repeat(super::CONTEXT_LIMIT - 20)});
        let context_for_request = context.clone();

        call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 1, "text": "x".repeat(super::TEXT_LIMIT), "context": context_for_request}),
            ),
        )
        .await
        .unwrap();

        let tasks = tasks.lock().unwrap();
        let task = tasks.get(&1).unwrap();
        let map_snapshot = tasks.clone();
        let clone = Task::clone(task.as_ref());

        assert!(Arc::ptr_eq(task, map_snapshot.get(&1).unwrap()));
        assert!(Arc::ptr_eq(&task.text, &clone.text));
        assert!(Arc::ptr_eq(&task.context, &clone.context));
        assert_eq!(serde_json::to_value(task).unwrap()["text"], "x".repeat(super::TEXT_LIMIT));
        assert_eq!(serde_json::to_value(task).unwrap()["context"], context);
    }

    #[tokio::test]
    async fn task_snapshots_copy_on_write_only_changed_tasks() {
        let tasks = tasks();

        for id in 1..=2 {
            call(
                &tasks,
                Request::call(10 + id, "add", json!({"id": id, "text": "task", "activate": true})),
            )
            .await
            .unwrap();
        }

        let snapshot = tasks.lock().unwrap().clone();

        {
            let mut live = tasks.lock().unwrap();
            Arc::make_mut(live.get_mut(&1).unwrap()).status = "paused".into();
        }

        let live = tasks.lock().unwrap();

        assert_eq!(snapshot[&1].status, "active");
        assert_eq!(live[&1].status, "paused");
        assert!(!Arc::ptr_eq(&snapshot[&1], &live[&1]));
        assert!(Arc::ptr_eq(&snapshot[&2], &live[&2]));
    }

    #[tokio::test]
    async fn schedule_proposals_require_approval_and_execute_once() {
        let tasks = tasks();
        let added =
            call(&tasks, Request::call(1, "add", json!({"id": 4, "text": "check", "delay": 0, "cron": "*/5 * * * *", "zone": "Europe/Rome"})))
                .await
                .unwrap()
                .unwrap();

        assert_eq!(added.result.unwrap()["id"], 4);

        let listed = call(&tasks, Request::call(2, "list", json!({}))).await.unwrap().unwrap();

        let listed = listed.result.unwrap();

        assert_eq!(listed["items"][0]["text"], "check");
        assert_eq!(listed["items"][0]["status"], "pending");

        call(&tasks, Request::call(3, "approve", json!({"id": 4}))).await.unwrap();
        let claimed = call(&tasks, Request::call(4, "claim", json!({"at": u64::MAX})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(claimed["item"]["text"], "check");
        assert_eq!(claimed["item"]["name"], listed["items"][0]["name"]);

        call(
            &tasks,
            Request::call(5, "finish", json!({"id": 4, "success": true, "result": "done"})),
        )
        .await
        .unwrap();

        assert!(tasks.lock().unwrap().contains_key(&4));

        let removed =
            call(&tasks, Request::call(6, "remove", json!({"id": 4}))).await.unwrap().unwrap();

        assert_eq!(removed.result.unwrap()["deleted"], true);
    }

    #[tokio::test]
    async fn repeated_finish_calls_return_the_saved_result_without_reapplying_it() {
        let tasks = tasks();
        call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 30, "text": "send a report", "delay": 0, "activate": true}),
            ),
        )
        .await
        .unwrap();

        let claim = call(&tasks, Request::call(2, "claim", json!({"at": u64::MAX, "lease": 9001})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(claim["item"]["run_id"], 9001);
        assert!(claim["item"]["scheduled_for"].as_u64().is_some());
        assert_eq!(claim["item"]["attempt"], 1);

        let first = call(
            &tasks,
            Request::call(
                3,
                "finish",
                json!({"id": 30, "run_id": 9001, "success": true, "result": "sent"}),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        let duplicate = call(
            &tasks,
            Request::call(
                4,
                "finish",
                json!({"id": 30, "run_id": 9001, "success": false, "result": "uncertain"}),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(duplicate, first);
        assert_eq!(first["attempt"], 1);
        assert_eq!(first["manual"], false);
        assert!(first["scheduled_for"].as_u64().is_some());
        assert!(first["finished_at"].as_u64().is_some());
        assert_eq!(tasks.lock().unwrap()[&30].last_result.as_deref(), Some("sent"));
        assert_eq!(tasks.lock().unwrap()[&30].status, "completed");

        let reports = call(&tasks, Request::call(5, "reports", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(reports["items"][0]["id"], 30);
        assert!(reports["items"][0]["name"].as_str().is_some());
        assert_eq!(reports["items"][0]["run_id"], 9001);

        call(&tasks, Request::call(6, "report", json!({"id": 30, "run_id": 9001}))).await.unwrap();

        let missing_parent = std::env::temp_dir()
            .join(format!("crabbot-scheduler-report-{}", std::process::id()))
            .join("missing/state.json");

        let repeated_report = call_at(
            &tasks,
            Request::call(8, "report", json!({"id": 30, "run_id": 9001})),
            Some(&missing_parent),
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(repeated_report["reported"], true);

        let reports = call(&tasks, Request::call(7, "reports", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert!(reports["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pending_reports_are_returned_in_bounded_id_pages() {
        let tasks = tasks();
        let mut task = Task {
            name: "check".into(),
            text: "check".into(),
            delay: 0,
            due: 1,
            repeat: 0,
            cron: None,
            zone: "UTC".into(),
            status: "completed".into(),
            context: Arc::new(json!({"value": "x".repeat(super::CONTEXT_LIMIT - 20)})),
            last_result: Some("done".into()),
            last_outcome: Some("completed".into()),
            missed: "skip".into(),
            on_failure: "pause".into(),
            replay_count: 0,
            retries: 0,
            attempts: 0,
            retry_at: None,
            manual_run: false,
            manual_retry: false,
            manual_origin: false,
            cancel_requested: false,
            run_id: None,
            finished_run_id: Some(1),
            finish_result: Some(json!({"success": true, "result": "done"})),
            report_pending: true,
            imported_timer_id: None,
        };

        for id in 1..=45 {
            task.finished_run_id = Some(id);
            tasks.lock().unwrap().insert(id, Arc::new(task.clone()));
        }

        let first = call(&tasks, Request::call(1, "reports", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        let first_items = first["items"].as_array().unwrap();

        assert_eq!(first_items.len(), 20);
        assert_eq!(first_items.first().unwrap()["id"], 1);
        assert_eq!(first_items.last().unwrap()["id"], 20);

        let second = call(&tasks, Request::call(2, "reports", json!({"after": 20})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        let second_items = second["items"].as_array().unwrap();

        assert_eq!(second_items.len(), 20);
        assert_eq!(second_items.first().unwrap()["id"], 21);
        assert_eq!(second_items.last().unwrap()["id"], 40);
        assert!(serde_json::to_vec(&second).unwrap().len() < crabbot_core::jsonl::MAX);
    }

    #[tokio::test]
    async fn recurring_task_waits_for_report_acknowledgement_before_next_run() {
        let tasks = tasks();
        call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 46, "text": "check", "delay": 0, "repeat": 10, "activate": true}),
            ),
        )
        .await
        .unwrap();

        let first = call(&tasks, Request::call(2, "claim", json!({"at": u64::MAX})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()["item"]
            .clone();

        let run_id = first["run_id"].as_u64().unwrap();

        call(
            &tasks,
            Request::call(
                3,
                "finish",
                json!({"id": 46, "run_id": run_id, "success": true, "result": "done"}),
            ),
        )
        .await
        .unwrap();

        let blocked = call(&tasks, Request::call(4, "claim", json!({"at": u64::MAX})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert!(blocked["item"].is_null());

        call(&tasks, Request::call(5, "report", json!({"id": 46, "run_id": run_id})))
            .await
            .unwrap();

        let next = call(&tasks, Request::call(6, "claim", json!({"at": u64::MAX})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(next["item"]["id"], 46);
        assert_ne!(next["item"]["run_id"], run_id);
    }

    #[tokio::test]
    async fn explicit_schedule_creation_activates_the_task_immediately() {
        let tasks = tasks();
        let added = call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 5, "name": "daily-ping", "text": "notify", "delay": 60, "activate": true}),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        let added = added.result.unwrap();

        assert_eq!(added["status"], "active");
        assert_eq!(added["name"], "daily-ping");

        let claimed = call(&tasks, Request::call(2, "claim", json!({"at": u64::MAX})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(claimed.result.unwrap()["item"]["id"], 5);
    }

    #[tokio::test]
    async fn unnamed_schedules_get_a_random_adjective_creature_name() {
        let tasks = tasks();
        let added = call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 6, "text": "write a backup", "delay": 60, "activate": true}),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        let name = added["name"].as_str().unwrap();

        assert!(name.contains('-'));
        assert!(name.chars().all(|character| character.is_ascii_lowercase() || character == '-'));

        let listed = call(&tasks, Request::call(2, "list", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(listed["items"][0]["name"], name);
    }

    #[test]
    fn generated_schedule_names_use_the_funny_word_dictionary() {
        let name = super::schedule_name();

        assert!(name.contains('-'));
        assert!(name.chars().all(|character| character.is_ascii_lowercase() || character == '-'));
        assert!(name.len() <= 32);
    }

    #[test]
    fn schedule_names_are_unique_selectable_and_reject_spaces() {
        assert!(super::valid_schedule_name("mighty-raccoon"));
        assert!(super::valid_schedule_name("backup_2.daily"));
        assert!(super::valid_schedule_name(&"a".repeat(80)));
        assert!(!super::valid_schedule_name("my backup"));
        assert!(!super::valid_schedule_name("-backup"));
        assert!(!super::valid_schedule_name(&"a".repeat(81)));
        assert!(!super::valid_schedule_name("mïghty-raccoon"));

        let names = BTreeSet::from(["mighty-raccoon".to_owned()]);

        assert_ne!(super::unique_schedule_name(&names), "mighty-raccoon");
    }

    #[tokio::test]
    async fn listing_is_paginated_bounded_and_rejects_invalid_pages() {
        let tasks = tasks();

        for id in 1..=21 {
            let text = if id == 1 { "x".repeat(5_000) } else { format!("task {id}") };

            call_at(
                &tasks,
                Request::call(
                    id,
                    "add",
                    json!({"id": id, "text": text, "delay": 60, "activate": true}),
                ),
                None,
            )
            .await
            .unwrap();
        }

        let first = call_at(&tasks, Request::call(30, "list", json!({"page": 1})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(first["total"], 21);
        assert_eq!(first["page"], 1);
        assert_eq!(first["pages"], 2);
        assert_eq!(first["from"], 1);
        assert_eq!(first["to"], 20);
        assert_eq!(first["items"][0]["text"].as_str().unwrap().len(), 1_000);

        let second = call_at(&tasks, Request::call(31, "list", json!({"page": 2})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(second["from"], 21);
        assert_eq!(second["to"], 21);
        assert_eq!(second["items"].as_array().unwrap().len(), 1);
        assert!(
            call_at(&tasks, Request::call(32, "list", json!({"page": 0})), None).await.is_err()
        );

        assert!(
            call_at(&tasks, Request::call(33, "list", json!({"page": 3})), None).await.is_err()
        );
    }

    #[tokio::test]
    async fn adding_tasks_never_overwrites_or_evicts_existing_schedules() {
        let tasks = tasks();

        for id in 1..=LIMIT as u64 {
            tasks.lock().unwrap().insert(
                id,
                Arc::new(Task {
                    name: format!("task {id}"),
                    text: format!("task {id}").into(),
                    delay: 60,
                    due: id,
                    repeat: 0,
                    cron: None,
                    zone: "UTC".into(),
                    status: "active".into(),
                    context: Arc::new(serde_json::Value::Null),
                    last_result: None,
                    last_outcome: None,
                    missed: "skip".into(),
                    on_failure: "pause".into(),
                    replay_count: 0,
                    retries: 0,
                    attempts: 0,
                    retry_at: None,
                    manual_run: false,
                    manual_retry: false,
                    manual_origin: false,
                    cancel_requested: false,
                    run_id: None,
                    finished_run_id: None,
                    finish_result: None,
                    report_pending: false,
                    imported_timer_id: None,
                }),
            );
        }

        let error = call(
            &tasks,
            Request::call(1, "add", json!({"id": LIMIT as u64 + 1, "text": "new task"})),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Remove a task before adding another"));
        assert_eq!(tasks.lock().unwrap().len(), LIMIT);
        assert_eq!(tasks.lock().unwrap()[&1].text.as_ref(), "task 1");

        let error = call(&tasks, Request::call(2, "add", json!({"id": 1, "text": "replace task"})))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("already exists"));
        assert_eq!(tasks.lock().unwrap()[&1].text.as_ref(), "task 1");
    }

    #[tokio::test]
    async fn manual_runs_and_configured_retries_are_durable_and_bounded() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 10, "text": "notify", "delay": 3600, "retries": 1}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "approve", json!({"id": 10})), None).await.unwrap();
        call_at(&tasks, Request::call(3, "run", json!({"id": 10})), None).await.unwrap();

        let run = call_at(&tasks, Request::call(4, "claim", json!({"at": 1, "since": 1})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()["item"]
            .clone();

        assert_eq!(run["manual"], true);
        assert_eq!(run["attempt"], 1);

        call_at(
            &tasks,
            Request::call(5, "finish", json!({"id": 10, "success": false, "result": "failed"})),
            None,
        )
        .await
        .unwrap();

        call_at(
            &tasks,
            Request::call(6, "report", json!({"id": 10, "run_id": run["run_id"]})),
            None,
        )
        .await
        .unwrap();

        let retry_at = tasks.lock().unwrap()[&10].retry_at.unwrap();

        assert_eq!(tasks.lock().unwrap()[&10].status, "active");

        let listing = call_at(&tasks, Request::call(6, "list", json!({"page": 1})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(listing["items"][0]["last_outcome"], "retrying");
        assert!(listing["items"][0].get("last_result").is_none());

        let not_due = call_at(
            &tasks,
            Request::call(7, "claim", json!({"at": retry_at - 1, "since": retry_at - 1})),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert!(not_due["item"].is_null());

        let retry = call_at(
            &tasks,
            Request::call(8, "claim", json!({"at": retry_at, "since": retry_at})),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap()["item"]
            .clone();

        assert_eq!(retry["retry"], true);
        assert_eq!(retry["attempt"], 2);

        call_at(
            &tasks,
            Request::call(
                8,
                "finish",
                json!({"id": 10, "success": false, "result": "failed again"}),
            ),
            None,
        )
        .await
        .unwrap();

        assert_eq!(tasks.lock().unwrap()[&10].status, "paused");

        call_at(
            &tasks,
            Request::call(9, "report", json!({"id": 10, "run_id": retry["run_id"]})),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(10, "retry", json!({"id": 10})), None).await.unwrap();

        let manually_retried =
            call_at(&tasks, Request::call(11, "claim", json!({"at": 1, "since": 1})), None)
                .await
                .unwrap()
                .unwrap()
                .result
                .unwrap()["item"]
                .clone();

        assert_eq!(manually_retried["manual"], true);
        assert_eq!(manually_retried["attempt"], 1);
    }

    #[tokio::test]
    async fn uncertain_failures_pause_without_using_configured_retries() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 11, "text": "send a message", "delay": 3600, "retries": 3}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "approve", json!({"id": 11})), None).await.unwrap();
        call_at(&tasks, Request::call(3, "run", json!({"id": 11})), None).await.unwrap();
        call_at(&tasks, Request::call(4, "claim", json!({"at": now(), "since": now()})), None)
            .await
            .unwrap();

        let finished = call_at(
            &tasks,
            Request::call(
                5,
                "finish",
                json!({
                    "id": 11,
                    "success": false,
                    "pause": true,
                    "result": "The message may already have been sent."
                }),
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(finished["status"], "paused");
        assert!(finished["retry_at"].is_null());
        assert_eq!(tasks.lock().unwrap()[&11].attempts, 1);
        assert_eq!(tasks.lock().unwrap()[&11].retries, 3);
    }

    #[tokio::test]
    async fn configured_continue_skips_a_failed_occurrence_and_keeps_the_next_run() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({
                    "id": 31,
                    "text": "check the backup",
                    "delay": 0,
                    "repeat": 5,
                    "on_failure": "continue",
                    "activate": true
                }),
            ),
            None,
        )
        .await
        .unwrap();

        let original_due = tasks.lock().unwrap()[&31].due;
        call_at(&tasks, Request::call(2, "claim", json!({"at": now()})), None).await.unwrap();

        let finished = call_at(
            &tasks,
            Request::call(
                3,
                "finish",
                json!({"id": 31, "success": false, "result": "Provider unavailable."}),
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(finished["status"], "active");
        assert!(finished["retry_at"].is_null());
        assert!(finished["due"].as_u64().unwrap() > original_due);
        assert_eq!(finished["on_failure"], "continue");
        assert_eq!(tasks.lock().unwrap()[&31].status, "active");
    }

    #[tokio::test]
    async fn removing_a_running_schedule_requests_cancellation_and_stops_future_runs() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 20, "text": "create a backup", "delay": 60, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "claim", json!({"at": u64::MAX})), None).await.unwrap();

        let remove = call_at(&tasks, Request::call(3, "remove", json!({"id": 20})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(remove["cancel_requested"], true);
        assert_eq!(tasks.lock().unwrap()[&20].status, "running");
        assert!(tasks.lock().unwrap()[&20].cancel_requested);

        let cancelled = call_at(&tasks, Request::call(4, "cancelled", json!({"id": 20})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(cancelled["cancelled"], true);

        let finish = call_at(
            &tasks,
            Request::call(
                5,
                "finish",
                json!({"id": 20, "success": false, "cancelled": true, "result": "cancelled"}),
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(finish["status"], "cancelled");
        assert_eq!(tasks.lock().unwrap()[&20].status, "cancelled");
    }

    #[tokio::test]
    async fn recovery_does_not_replay_a_schedule_cancelled_during_interruption() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 21, "text": "create a backup", "delay": 60, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "claim", json!({"at": u64::MAX})), None).await.unwrap();
        call_at(&tasks, Request::call(3, "remove", json!({"id": 21})), None).await.unwrap();

        let recovered = call_at(&tasks, Request::call(4, "recover", json!({})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(recovered["recovered"], 1);
        assert_eq!(recovered["cancelled"], 1);
        assert!(!tasks.lock().unwrap().contains_key(&21));
    }

    #[tokio::test]
    async fn recovery_preserves_runs_owned_by_live_daemon_workers() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 22, "text": "create a backup", "delay": 60, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "claim", json!({"at": u64::MAX, "lease": 7})), None)
            .await
            .unwrap();

        let recovered = call_at(&tasks, Request::call(3, "recover", json!({"active": [22]})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(recovered["recovered"], 0);
        assert_eq!(tasks.lock().unwrap()[&22].status, "running");

        let finish = call_at(
            &tasks,
            Request::call(4, "finish", json!({"id": 22, "run_id": 7, "success": true})),
            None,
        )
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap();

        assert_eq!(finish["status"], "completed");
    }

    #[tokio::test]
    async fn recovery_reports_interrupted_run_once() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 23, "text": "create a backup", "delay": 60, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "claim", json!({"at": u64::MAX, "lease": 700})), None)
            .await
            .unwrap();

        call_at(&tasks, Request::call(3, "recover", json!({})), None).await.unwrap();

        let reports = call_at(&tasks, Request::call(4, "reports", json!({})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(reports["items"][0]["run_id"], 700);
        assert_eq!(reports["items"][0]["finish"]["status"], "paused");
        assert_eq!(reports["items"][0]["finish"]["success"], false);

        call_at(&tasks, Request::call(5, "report", json!({"id": 23, "run_id": 700})), None)
            .await
            .unwrap();

        let reports = call_at(&tasks, Request::call(6, "reports", json!({})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert!(reports["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_retry_limits_above_five() {
        let tasks = tasks();

        assert!(
            call(&tasks, Request::call(1, "add", json!({"id": 1, "text": "check", "retries": 6})),)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn running_a_recurring_task_now_keeps_its_regular_due_time() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 12, "text": "check", "delay": 3600, "repeat": 3600}),
            ),
            None,
        )
        .await
        .unwrap();

        let original_due = tasks.lock().unwrap()[&12].due;
        call_at(&tasks, Request::call(2, "approve", json!({"id": 12})), None).await.unwrap();
        call_at(&tasks, Request::call(3, "run", json!({"id": 12})), None).await.unwrap();

        let run = call_at(&tasks, Request::call(4, "claim", json!({"at": 1, "since": 1})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()["item"]
            .clone();

        assert_eq!(run["manual"], true);

        call_at(
            &tasks,
            Request::call(
                5,
                "finish",
                json!({"id": 12, "success": true, "manual": true, "result": "done"}),
            ),
            None,
        )
        .await
        .unwrap();

        let scheduled = tasks.lock().unwrap();

        assert_eq!(scheduled[&12].due, original_due);
        assert_eq!(scheduled[&12].status, "active");
    }

    #[tokio::test]
    async fn manually_running_a_due_recurring_task_consumes_that_occurrence() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 14, "text": "check", "delay": 0, "repeat": 60, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "run", json!({"id": 14})), None).await.unwrap();
        let first = call_at(&tasks, Request::call(3, "claim", json!({"at": u64::MAX})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()["item"]
            .clone();

        assert_eq!(first["manual"], true);

        call_at(
            &tasks,
            Request::call(
                4,
                "finish",
                json!({"id": 14, "success": true, "manual": true, "result": "done"}),
            ),
            None,
        )
        .await
        .unwrap();

        let scheduled = tasks.lock().unwrap();

        assert!(scheduled[&14].due > now());
        assert_eq!(scheduled[&14].status, "active");
    }

    #[tokio::test]
    async fn running_a_one_time_task_now_retains_its_completion() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(1, "add", json!({"id": 13, "text": "send a message", "delay": 3600})),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "approve", json!({"id": 13})), None).await.unwrap();
        call_at(&tasks, Request::call(3, "run", json!({"id": 13})), None).await.unwrap();
        call_at(&tasks, Request::call(4, "claim", json!({"at": 1, "since": 1})), None)
            .await
            .unwrap();

        let finished = call_at(
            &tasks,
            Request::call(
                5,
                "finish",
                json!({"id": 13, "success": true, "manual": true, "result": "sent"}),
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(finished.result.unwrap()["status"], "completed");
        assert_eq!(tasks.lock().unwrap()[&13].status, "completed");
    }

    #[tokio::test]
    async fn missed_one_time_task_is_retained_as_skipped_for_review() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 44, "text": "send the report", "delay": 0, "missed": "skip"}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "approve", json!({"id": 44})), None).await.unwrap();
        Arc::make_mut(tasks.lock().unwrap().get_mut(&44).unwrap()).due = 100;

        let skipped =
            call_at(&tasks, Request::call(3, "claim", json!({"at": 200, "since": 150})), None)
                .await
                .unwrap()
                .unwrap()
                .result
                .unwrap();

        assert_eq!(skipped["item"]["skipped"], true);
        assert_eq!(skipped["item"]["skipped_at"], 200);
        assert_eq!(tasks.lock().unwrap()[&44].status, "skipped");
        assert!(
            tasks.lock().unwrap()[&44]
                .last_result
                .as_deref()
                .is_some_and(|result| result.contains("daemon was offline"))
        );
    }

    #[tokio::test]
    async fn completed_and_skipped_one_time_tasks_can_be_run_again() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({"id": 60, "text": "create a report", "delay": 0, "activate": true}),
            ),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(2, "claim", json!({"at": u64::MAX})), None).await.unwrap();
        call_at(
            &tasks,
            Request::call(3, "finish", json!({"id": 60, "success": true, "result": "done"})),
            None,
        )
        .await
        .unwrap();

        call_at(&tasks, Request::call(4, "report", json!({"id": 60, "run_id": 2})), None)
            .await
            .unwrap();

        call_at(&tasks, Request::call(4, "run", json!({"id": 60})), None).await.unwrap();
        let completed = call_at(&tasks, Request::call(5, "claim", json!({"at": u64::MAX})), None)
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(completed["item"]["manual"], true);
        assert!(completed["item"]["skipped"].is_null());

        call_at(
            &tasks,
            Request::call(
                6,
                "add",
                json!({
                    "id": 61,
                    "text": "send the report",
                    "delay": 0,
                    "missed": "skip",
                    "activate": true
                }),
            ),
            None,
        )
        .await
        .unwrap();

        Arc::make_mut(tasks.lock().unwrap().get_mut(&61).unwrap()).due = 100;
        call_at(&tasks, Request::call(7, "claim", json!({"at": 200, "since": 150})), None)
            .await
            .unwrap();

        call_at(&tasks, Request::call(8, "report", json!({"id": 61, "run_id": 7})), None)
            .await
            .unwrap();

        call_at(&tasks, Request::call(9, "run", json!({"id": 61})), None).await.unwrap();
        let skipped =
            call_at(&tasks, Request::call(10, "claim", json!({"at": 200, "since": 150})), None)
                .await
                .unwrap()
                .unwrap()
                .result
                .unwrap();

        assert_eq!(skipped["item"]["id"], 61);
        assert_eq!(skipped["item"]["manual"], true);
        assert!(skipped["item"]["skipped"].is_null());
    }

    #[tokio::test]
    async fn skipped_one_time_task_can_be_retried_after_daemon_downtime() {
        let tasks = tasks();
        call_at(
            &tasks,
            Request::call(
                1,
                "add",
                json!({
                    "id": 62,
                    "text": "send the reminder",
                    "delay": 0,
                    "missed": "skip",
                    "activate": true
                }),
            ),
            None,
        )
        .await
        .unwrap();

        Arc::make_mut(tasks.lock().unwrap().get_mut(&62).unwrap()).due = 100;
        call_at(&tasks, Request::call(2, "claim", json!({"at": 200, "since": 150})), None)
            .await
            .unwrap();

        assert_eq!(tasks.lock().unwrap()[&62].status, "skipped");
        call_at(&tasks, Request::call(3, "report", json!({"id": 62, "run_id": 2})), None)
            .await
            .unwrap();

        call_at(&tasks, Request::call(4, "retry", json!({"id": 62})), None).await.unwrap();

        let claimed =
            call_at(&tasks, Request::call(5, "claim", json!({"at": 200, "since": 150})), None)
                .await
                .unwrap()
                .unwrap()
                .result
                .unwrap();

        assert_eq!(claimed["item"]["id"], 62);
        assert_eq!(claimed["item"]["retry"], true);
        assert_eq!(claimed["item"]["manual"], false);
        assert!(claimed["item"]["skipped"].is_null());

        call_at(
            &tasks,
            Request::call(6, "finish", json!({"id": 62, "success": true, "result": "sent"})),
            None,
        )
        .await
        .unwrap();

        assert_eq!(tasks.lock().unwrap()[&62].status, "completed");
    }

    #[tokio::test]
    async fn schedule_validates_requests_and_missing_tasks() {
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

        assert!(
            call(
                &tasks,
                Request::call(10, "add", json!({"id": 10, "text": "check", "repeat": 31_536_001}),),
            )
            .await
            .is_err()
        );

        assert!(
            call(
                &tasks,
                Request::call(
                    11,
                    "add",
                    json!({"id": 11, "text": "check", "repeat": 300, "cron": "0 9 * * *", "zone": "UTC"}),
                ),
            )
            .await
            .is_err()
        );

        let cron = call(
            &tasks,
            Request::call(
                5,
                "add",
                json!({"id": 5, "text": "check", "cron": "* * * * *", "zone": "UTC"}),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(cron.result.unwrap()["id"], 5);

        let exact_time = now().saturating_add(600);
        let exact = call(
            &tasks,
            Request::call(
                6,
                "add",
                json!({"id": 6, "text": "check", "at": exact_time, "zone": "UTC"}),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(exact.result.unwrap()["id"], 6);
        assert_eq!(tasks.lock().unwrap()[&6].due, exact_time);

        assert!(
            call(
                &tasks,
                Request::call(7, "add", json!({"id": 7, "text": "check", "zone": "+24:00"}))
            )
            .await
            .is_err()
        );

        assert!(
            call(
                &tasks,
                Request::call(
                    8,
                    "add",
                    json!({"id": 7, "text": "check", "zone": "Europe/../Rome"})
                )
            )
            .await
            .is_err()
        );

        assert!(call(&tasks, Request::call(9, "remove", json!({}))).await.is_err());
        let removed =
            call(&tasks, Request::call(10, "remove", json!({"id": 4}))).await.unwrap().unwrap();

        assert_eq!(removed.result.unwrap()["deleted"], false);
        assert!(call(&tasks, Request::call(7, "unknown", json!({}))).await.unwrap().is_none());
        let note =
            Request::Note { jsonrpc: "2.0".into(), method: "list".into(), params: json!({}) };

        assert!(call(&tasks, note).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restores_schedule_when_persistence_fails() {
        let root =
            std::env::temp_dir().join(format!("crabbot-scheduler-blocker-{}", std::process::id()));

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
    fn schedule_persists_and_recovers_tasks() {
        let path =
            std::env::temp_dir().join(format!("crabbot-scheduler-{}.json", std::process::id()));

        let _ = std::fs::remove_file(&path);
        let tasks = tasks();
        tasks.lock().unwrap().insert(
            1,
            Arc::new(Task {
                name: "check".into(),
                text: "check".into(),
                delay: 2,
                due: 2,
                repeat: 0,
                cron: None,
                zone: "UTC".into(),
                status: "active".into(),
                context: Arc::new(serde_json::Value::Null),
                last_result: None,
                last_outcome: None,
                missed: "skip".into(),
                on_failure: "pause".into(),
                replay_count: 0,
                retries: 0,
                attempts: 0,
                retry_at: None,
                manual_run: false,
                manual_retry: false,
                manual_origin: false,
                cancel_requested: false,
                run_id: None,
                finished_run_id: None,
                finish_result: None,
                report_pending: false,
                imported_timer_id: None,
            }),
        );

        let template = tasks.lock().unwrap()[&1].clone();

        for id in 2..=LIMIT as u64 + 1 {
            let mut task = template.clone();
            Arc::make_mut(&mut task).due = id;
            tasks.lock().unwrap().insert(id, task);
        }

        persist_at(Some(&path), &tasks).unwrap();

        let loaded = load_at(Some(&path)).unwrap();

        assert_eq!(loaded.lock().unwrap()[&1].text.as_ref(), "check");
        assert_eq!(loaded.lock().unwrap().len(), LIMIT + 1);
        std::fs::write(&path, "broken").unwrap();

        assert!(load_at(Some(&path)).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"broken");
        std::fs::File::create(&path).unwrap().set_len(BYTE_LIMIT as u64 + 1).unwrap();

        assert!(load_at(Some(&path)).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), BYTE_LIMIT as u64 + 1);
        assert!(persist_at(Some(&path.with_file_name("missing-dir/item")), &tasks).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn startup_prepares_private_state_directory() {
        let root =
            std::env::temp_dir().join(format!("crabbot-scheduler-startup-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let schedule_path = root.join("data/schedules/schedules.json");

        super::prepare_state_directory(Some(&schedule_path)).unwrap();

        let data = root.join("data");
        let schedules = data.join("schedules");

        assert!(data.is_dir());
        assert!(schedules.is_dir());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(std::fs::metadata(data).unwrap().permissions().mode() & 0o777, 0o700);
            assert_eq!(std::fs::metadata(schedules).unwrap().permissions().mode() & 0o777, 0o700);
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn missing_legacy_names_are_generated_uniquely_and_persisted() {
        let root =
            std::env::temp_dir().join(format!("crabbot-scheduler-names-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("schedules.json");
        super::save_file(
            &path,
            br#"{"1":{"text":"first","delay":60},"2":{"text":"second","delay":60}}"#,
        )
        .unwrap();

        let loaded = load_at(Some(&path)).unwrap();
        let names =
            loaded.lock().unwrap().values().map(|task| task.name.clone()).collect::<BTreeSet<_>>();

        assert_eq!(names.len(), 2);
        assert!(names.iter().all(|name| super::valid_schedule_name(name)));

        let persisted: Tasks = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();

        assert!(persisted.values().all(|task| !task.name.is_empty()));

        let reloaded = load_at(Some(&path)).unwrap();
        let reloaded_names = reloaded
            .lock()
            .unwrap()
            .values()
            .map(|task| task.name.clone())
            .collect::<BTreeSet<_>>();

        assert_eq!(reloaded_names, names);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn imports_legacy_timer_tasks_as_active_scheduler_tasks() {
        let root = std::env::temp_dir()
            .join(format!("crabbot-scheduler-migration-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let scheduler_path = root.join("data/schedules/schedules.json");
        let timer_path = root.join("timer.json");
        std::fs::create_dir_all(scheduler_path.parent().unwrap()).unwrap();
        super::save_file(
            &scheduler_path,
            br#"{"42":{"text":"existing","delay":0,"due":2000,"repeat":0,"cron":null,"zone":"UTC","status":"pending"}}"#,
        )
        .unwrap();

        super::save_file(
            &timer_path,
            br#"{"42":{"text":"remind me","delay":60,"due":1234,"repeat":300,"cron":null,"zone":"UTC"}}"#,
        )
        .unwrap();

        let loaded = load_with_legacy_at(Some(&scheduler_path), Some(&timer_path)).unwrap();
        {
            let guard = loaded.lock().unwrap();
            let task = guard.values().find(|task| task.text.as_ref() == "remind me").unwrap();

            assert_eq!(task.text.as_ref(), "remind me");
            assert_eq!(task.delay, 60);
            assert_eq!(task.due, 1234);
            assert_eq!(task.repeat, 300);
            assert_eq!(task.status, "active");
            assert_eq!(task.missed, "coalesce");
            assert_eq!(guard.len(), 2);
            assert_eq!(guard[&42].text.as_ref(), "existing");
        }

        let loaded_again = load_with_legacy_at(Some(&scheduler_path), Some(&timer_path)).unwrap();

        assert_eq!(loaded_again.lock().unwrap().len(), 2);
        assert!(
            loaded_again
                .lock()
                .unwrap()
                .values()
                .any(|task| { task.text.as_ref() == "remind me" && task.status == "active" })
        );

        let imported_id = loaded_again
            .lock()
            .unwrap()
            .iter()
            .find_map(|(id, task)| (task.text.as_ref() == "remind me").then_some(*id))
            .unwrap();

        call_at(
            &loaded_again,
            Request::call(1, "remove", json!({"id": imported_id})),
            Some(&scheduler_path),
        )
        .await
        .unwrap();

        let after_removal = load_with_legacy_at(Some(&scheduler_path), Some(&timer_path)).unwrap();

        assert_eq!(after_removal.lock().unwrap().len(), 1);
        assert!(
            !after_removal.lock().unwrap().values().any(|task| task.text.as_ref() == "remind me")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn handles_disabled_persistence() {
        let _ = path();

        assert!(load_at(None).unwrap().lock().unwrap().is_empty());
        persist_at(None, &tasks()).unwrap();
    }

    #[test]
    fn schedules_cron_in_named_zone() {
        // 2023-11-14T22:13:20Z, in Unix seconds.
        let after = 1_700_000_000;
        let due = next("*/15 * * * *", "Europe/Rome", after).unwrap();
        let sunday = next("0 9 * * 7", "UTC", after).unwrap();
        let after_spring_transition = 1_678_597_200;
        let after_first_fall_occurrence = 1_699_163_100;
        let spring_due = next("0 2 * * *", "America/New_York", after_spring_transition).unwrap();
        let fall_due = next("30 1 * * *", "America/New_York", after_first_fall_occurrence).unwrap();

        assert_eq!(due, 1_700_000_100);
        assert!(sunday > after);
        assert_eq!(spring_due, 1_678_687_200);
        assert_eq!(fall_due, 1_699_165_800);
    }

    #[test]
    fn cron_search_jumps_between_allowed_minutes() {
        let schedule = super::CronSchedule::parse("5,20,50 * * * *").unwrap();

        assert_eq!(schedule.minutes_until_next(4), 1);
        assert_eq!(schedule.minutes_until_next(5), 15);
        assert_eq!(schedule.minutes_until_next(50), 15);
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
        assert!(super::CronSchedule::parse("0 9 * * 7").unwrap().matches(&[0, 9, 19, 11, 0]));

        for value in ["* * * *", "* * * * * *", "0 0 0 0 0", "*/0 * * * *"] {
            assert!(validate_cron(value).is_err(), "{value} should be rejected");
        }

        assert!(validate_zone("UTC").is_ok());
        assert!(validate_zone("+05:30").is_ok());
        assert!(validate_zone("-03:00").is_ok());
        assert!(validate_zone("+24:00").is_err());
        assert!(validate_zone("Not/AZone").is_err());
        assert!(timezone("UTC").is_ok());
        let weekday_noon = super::CronSchedule::parse("0 12 * * 1-5").unwrap();

        assert!(weekday_noon.matches(&[0, 12, 1, 1, 1]));
        assert!(!weekday_noon.matches(&[1, 12, 1, 1, 1]));

        let weekday_with_full_day = super::CronSchedule::parse("0 9 */1 * 1-5").unwrap();
        let full_weekday_with_day_range = super::CronSchedule::parse("0 9 1-5 * */1").unwrap();

        assert!(weekday_with_full_day.matches(&[0, 9, 1, 1, 1]));
        assert!(!weekday_with_full_day.matches(&[0, 9, 4, 1, 0]));
        assert!(full_weekday_with_day_range.matches(&[0, 9, 4, 1, 1]));
        assert!(!full_weekday_with_day_range.matches(&[0, 9, 7, 1, 0]));
    }

    #[tokio::test]
    async fn repeats_interval_tasks_and_retains_completed_one_shots() {
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

        call(&tasks, Request::call(3, "approve", json!({"id": 1}))).await.unwrap();
        call(&tasks, Request::call(4, "approve", json!({"id": 2}))).await.unwrap();

        for task in tasks.lock().unwrap().values_mut() {
            let task = Arc::make_mut(task);
            task.due = 0;
        }

        let claimed =
            call(&tasks, Request::call(5, "claim", json!({"at": 1}))).await.unwrap().unwrap();

        let claimed_id = claimed.result.unwrap()["item"]["id"].as_u64().unwrap();

        assert_eq!(claimed_id, 1);
        call(
            &tasks,
            Request::call(
                6,
                "finish",
                json!({"id": claimed_id, "success": true, "result": "done"}),
            ),
        )
        .await
        .unwrap();

        let claimed =
            call(&tasks, Request::call(7, "claim", json!({"at": 1}))).await.unwrap().unwrap();

        assert_eq!(claimed.result.unwrap()["item"]["id"], 2);
        call(
            &tasks,
            Request::call(8, "finish", json!({"id": 2, "success": true, "result": "done"})),
        )
        .await
        .unwrap();

        assert_eq!(tasks.lock().unwrap()[&2].status, "completed");
        assert_eq!(tasks.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn missed_occurrences_follow_the_configured_policy() {
        let tasks = tasks();

        for (id, missed) in [(1, "skip"), (2, "coalesce"), (3, "replay")] {
            call(
                &tasks,
                Request::call(
                    id,
                    "add",
                    json!({"id": id, "text": "run", "delay": 0, "repeat": 10, "missed": missed}),
                ),
            )
            .await
            .unwrap();

            call(&tasks, Request::call(10 + id, "approve", json!({"id": id}))).await.unwrap();
            let mut tasks = tasks.lock().unwrap();
            let task = tasks.get_mut(&id).unwrap();
            Arc::make_mut(task).due = 100;
        }

        let skipped = call(&tasks, Request::call(20, "claim", json!({"at": 200, "since": 150})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(skipped.result.unwrap()["item"]["skipped"], true);
        let reports = call(&tasks, Request::call(23, "reports", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert_eq!(reports["items"][0]["id"], 1);
        assert_eq!(reports["items"][0]["run_id"], 20);
        assert_eq!(reports["items"][0]["finish"]["status"], "skipped");
        assert_eq!(tasks.lock().unwrap()[&1].due, 210);

        call(&tasks, Request::call(24, "report", json!({"id": 1, "run_id": 20}))).await.unwrap();

        let reports = call(&tasks, Request::call(25, "reports", json!({})))
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap();

        assert!(reports["items"].as_array().unwrap().is_empty());

        let coalesced = call(&tasks, Request::call(21, "claim", json!({"at": 200, "since": 150})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(coalesced.result.unwrap()["item"]["id"], 2);

        let replayed = call(&tasks, Request::call(22, "claim", json!({"at": 200, "since": 150})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(replayed.result.unwrap()["item"]["id"], 3);
        assert_eq!(tasks.lock().unwrap()[&3].replay_count, 1);
    }

    #[tokio::test]
    async fn replay_limit_counts_occurrences_that_become_overdue_after_startup() {
        let tasks = tasks();
        call(
            &tasks,
            Request::call(
                1,
                "add",
                json!({
                    "id": 1,
                    "text": "run",
                    "repeat": 10,
                    "missed": "replay",
                    "activate": true
                }),
            ),
        )
        .await
        .unwrap();

        Arc::make_mut(tasks.lock().unwrap().get_mut(&1).unwrap()).due = 160;
        call(&tasks, Request::call(2, "claim", json!({"at": 200, "since": 150}))).await.unwrap();

        assert_eq!(tasks.lock().unwrap()[&1].replay_count, 1);
        call(&tasks, Request::call(4, "finish", json!({"id": 1, "run_id": 2, "success": true})))
            .await
            .unwrap();

        call(&tasks, Request::call(5, "report", json!({"id": 1, "run_id": 2}))).await.unwrap();

        {
            let mut scheduled = tasks.lock().unwrap();
            let task = scheduled.get_mut(&1).unwrap();
            let task = Arc::make_mut(task);
            task.due = 190;
            task.replay_count = 5;
        }

        let exhausted = call(&tasks, Request::call(3, "claim", json!({"at": 200, "since": 150})))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(exhausted.result.unwrap()["item"]["skipped"], true);
        assert_eq!(tasks.lock().unwrap()[&1].due, 210);
        assert_eq!(tasks.lock().unwrap()[&1].replay_count, 0);
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
                    Arc::new(Task {
                        name: "scheduled task".into(),
                        text: text.clone().into(),
                        delay: 0,
                        due: 0,
                        repeat: 0,
                        cron: None,
                        zone: "UTC".into(),
                        status: "active".into(),
                        context: Arc::new(serde_json::Value::Null),
                        last_result: None,
                        last_outcome: None,
                        missed: "skip".into(),
                        on_failure: "pause".into(),
                        replay_count: 0,
                        retries: 0,
                        attempts: 0,
                        retry_at: None,
                        manual_run: false,
                        manual_retry: false,
                        manual_origin: false,
                        cancel_requested: false,
                        run_id: None,
                        finished_run_id: None,
                        finish_result: None,
                        report_pending: false,
                        imported_timer_id: None,
                    }),
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

        let path = std::env::temp_dir()
            .join(format!("crabbot-scheduler-shared-{}.json", std::process::id()));

        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, "{}").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();

        assert!(load_at(Some(&path)).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"{}");
        let _ = std::fs::remove_file(path);
    }
}
