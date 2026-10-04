pub(crate) use crabbot_log::{Mode, Verbosity};

pub(crate) fn initialize(mode: Mode) {
    crabbot_log::initialize(mode);
}
