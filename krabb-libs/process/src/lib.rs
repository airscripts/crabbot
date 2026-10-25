#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::restrict_process_group;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{CREATE_SUSPENDED, TerminalJob};
