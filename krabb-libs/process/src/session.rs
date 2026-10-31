use std::{io, os::unix::process::CommandExt, process::Command};

/// Gives the command a session that contains its descendants' process groups.
pub fn isolate_session(command: &mut Command) -> &mut Command {
    // SAFETY: setsid is async-signal-safe and the callback accesses no shared state.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::isolate_session;

    use std::process::Command;

    #[test]
    fn child_owns_a_separate_session() {
        let mut command = Command::new("sleep");
        command.arg("30");
        let mut child = isolate_session(&mut command).spawn().unwrap();

        // SAFETY: getsid only queries the live child's session ID.
        let session = unsafe { libc::getsid(child.id() as libc::pid_t) };

        let id = child.id();
        child.kill().unwrap();
        child.wait().unwrap();

        assert_eq!(session as u32, id);
    }
}
