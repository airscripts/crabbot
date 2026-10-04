use crabbot_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

pub(super) const HISTORY_LIMIT: usize = 100;
const HISTORY_FILE_LIMIT: u64 = 1024 * 1024;

#[derive(Debug, Default, Deserialize, Serialize)]
struct HistoryFile {
    entries: Vec<String>,
}

pub(super) fn load_history(path: &Path) -> Vec<String> {
    let Ok(_lock) = lock(path) else {
        return Vec::new();
    };

    let Ok(Some(bytes)) = crabbot_file::load(path, HISTORY_FILE_LIMIT) else {
        return Vec::new();
    };

    let Ok(history) = serde_json::from_slice::<HistoryFile>(&bytes) else {
        return Vec::new();
    };

    history
        .entries
        .into_iter()
        .rev()
        .take(HISTORY_LIMIT)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(super) fn save_history(path: &Path, entries: &[String]) -> Result<()> {
    let _lock = lock(path)?;
    let mut history = HistoryFile {
        entries: entries.iter().rev().take(HISTORY_LIMIT).cloned().collect::<Vec<_>>(),
    };

    history.entries.reverse();

    while serde_json::to_vec(&history)?.len() as u64 > HISTORY_FILE_LIMIT {
        if history.entries.is_empty() {
            return Err(Error::Denied("Input history exceeds its storage limit.".into()));
        }

        history.entries.remove(0);
    }

    crabbot_file::save(path, serde_json::to_vec(&history)?)?;
    Ok(())
}

pub(super) fn load_last_session(path: &Path) -> Option<String> {
    let _lock = lock(path).ok()?;
    let bytes = crabbot_file::load(path, 4096).ok()??;
    serde_json::from_slice(&bytes).ok()
}

pub(super) fn save_last_session(path: &Path, session: &str) -> Result<()> {
    let _lock = lock(path)?;
    crabbot_file::save(path, serde_json::to_vec(session)?)?;
    Ok(())
}

pub(super) fn plugin_file(home: impl AsRef<Path>, plugin: &str, file: &str) -> PathBuf {
    home.as_ref().join("data").join("plugins").join(plugin).join(file)
}

pub(super) fn delete_fallback_session(home: &str, id: &str) -> Result<bool> {
    let path = plugin_file(home, "tui", "sessions.json");

    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::Denied("Local TUI session store is not a regular file.".into()));
        }

        Ok(_) => {}

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }

    let _lock = lock(&path)?;
    let Some(bytes) = crabbot_file::load(&path, 8 * 1024 * 1024)? else {
        return Ok(false);
    };

    let mut store: Value = serde_json::from_slice(&bytes)?;
    let Some(sessions) = store.get_mut("sessions").and_then(Value::as_object_mut) else {
        return Err(Error::Denied("Local TUI session store is invalid.".into()));
    };

    if sessions.remove(id).is_none() {
        return Ok(false);
    }

    if sessions.is_empty() {
        std::fs::remove_file(&path)?;
    } else {
        let bytes = serde_json::to_vec(&store)?;
        crabbot_file::save(&path, bytes)?;
    }

    Ok(true)
}

fn lock(path: &Path) -> Result<File> {
    let parent =
        path.parent().ok_or_else(|| Error::Denied("Local TUI session path is invalid.".into()))?;

    let lock_path = path.with_extension("lock");

    match std::fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(Error::Denied("Local TUI session lock cannot be a symbolic link.".into()));
        }

        Ok(_) => {}

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    std::fs::create_dir_all(parent)?;

    let file =
        OpenOptions::new().read(true).write(true).create(true).truncate(false).open(lock_path)?;
    file.lock()?;

    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::{
        HISTORY_LIMIT, load_history, load_last_session, plugin_file, save_history,
        save_last_session,
    };

    use std::{fs, path::PathBuf, time::SystemTime};

    fn root(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir()
            .join(format!("crabbot-tui-data-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn saves_bounded_input_history() {
        let root = root("history");
        let path = plugin_file(&root, "tui", "history.json");
        let history =
            (0..HISTORY_LIMIT + 1).map(|index| format!("entry-{index}")).collect::<Vec<_>>();

        save_history(&path, &history).unwrap();

        let saved = load_history(&path);

        assert_eq!(saved.len(), HISTORY_LIMIT);
        assert_eq!(saved.first().map(String::as_str), Some("entry-1"));
        assert_eq!(saved.last().map(String::as_str), Some("entry-100"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persists_the_last_session() {
        let root = root("session");
        let path = plugin_file(&root, "tui", "last-session.json");

        assert_eq!(load_last_session(&path), None);
        save_last_session(&path, "tui-work").unwrap();

        assert_eq!(load_last_session(&path).as_deref(), Some("tui-work"));
        let _ = fs::remove_dir_all(root);
    }
}
