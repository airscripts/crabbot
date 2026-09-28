use std::path::{Path, PathBuf};

pub(super) fn plugin_file(home: impl AsRef<Path>, plugin: &str, file: &str) -> PathBuf {
    home.as_ref().join("data").join("plugins").join(plugin).join(file)
}
