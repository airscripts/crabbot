use crabbot_core::{Error, Result};
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

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
