use std::path::PathBuf;

pub(crate) use krabb_log::{Mode, Verbosity};

pub(crate) fn initialize(mode: Mode) {
    krabb_log::initialize(mode);
}

pub(crate) fn initialize_with_command_log(mode: Mode) -> Option<PathBuf> {
    krabb_log::initialize_with_command_log(mode)
}

pub(crate) fn application_log_path() -> PathBuf {
    krabb_log::application_log_path()
}
