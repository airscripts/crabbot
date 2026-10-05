use std::path::PathBuf;

pub(crate) use crabbot_log::{Mode, Verbosity};

pub(crate) fn initialize(mode: Mode) {
    crabbot_log::initialize(mode);
}

pub(crate) fn initialize_with_command_log(mode: Mode) -> Option<PathBuf> {
    crabbot_log::initialize_with_command_log(mode)
}
