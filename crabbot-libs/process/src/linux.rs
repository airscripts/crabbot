#![deny(unsafe_op_in_unsafe_fn)]

use std::io;

#[cfg(target_os = "linux")]
use std::{os::unix::process::CommandExt, process::Command};

#[cfg(target_arch = "x86_64")]
#[path = "linux/x86_64.rs"]
mod arch;
#[cfg(target_arch = "aarch64")]
#[path = "linux/aarch64.rs"]
mod arch;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[path = "linux/other.rs"]
mod arch;

/// Prevents the command and its descendants from leaving their process group.
#[cfg(target_os = "linux")]
pub fn restrict_process_group(command: &mut Command) -> &mut Command {
    // SAFETY: The callback only uses async-signal-safe syscalls and does not
    // access memory shared with other threads after fork.
    unsafe { command.pre_exec(restrict_process_group_in_child) }
}

#[cfg(target_os = "linux")]
fn restrict_process_group_in_child() -> io::Result<()> {
    let (mut filter, length) = process_group_filter();
    let mut program = libc::sock_fprog { len: length as u16, filter: filter.as_mut_ptr() };

    // SAFETY: The filter pointer remains valid through prctl and the syscall
    // copies the filter before returning.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: program points to a valid sock_fprog with a live filter array.
    if unsafe {
        libc::prctl(
            libc::PR_SET_SECCOMP,
            libc::SECCOMP_MODE_FILTER,
            &mut program as *mut libc::sock_fprog,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn process_group_filter() -> ([libc::sock_filter; 13], usize) {
    let allow = libc::sock_filter {
        code: (libc::BPF_RET | libc::BPF_K) as u16,
        jt: 0,
        jf: 0,
        k: libc::SECCOMP_RET_ALLOW,
    };

    let mut filter = [allow; 13];

    filter[0] = libc::sock_filter {
        code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
        jt: 0,
        jf: 0,
        k: 4,
    };

    filter[1] = libc::sock_filter {
        code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
        jt: 1,
        jf: 0,
        k: arch::AUDIT_ARCH,
    };

    filter[2] = libc::sock_filter {
        code: (libc::BPF_RET | libc::BPF_K) as u16,
        jt: 0,
        jf: 0,
        k: libc::SECCOMP_RET_KILL_PROCESS,
    };

    filter[3] = libc::sock_filter {
        code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
        jt: 0,
        jf: 0,
        k: 0,
    };

    let errno = libc::SECCOMP_RET_ERRNO | libc::EPERM as u32;
    let mut length = 4;

    for syscall in
        [libc::SYS_setsid as u32, libc::SYS_setpgid as u32].into_iter().chain(arch::COMPAT_SYSCALLS)
    {
        filter[length] = libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: 0,
            jf: 1,
            k: syscall,
        };

        filter[length + 1] = libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: errno,
        };

        length += 2;
    }

    filter[length] = allow;

    length += 1;

    (filter, length)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{process_group_filter, restrict_process_group, restrict_process_group_in_child};
    use std::{io, os::unix::process::CommandExt, process::Command};

    #[test]
    fn blocks_process_group_syscalls() {
        let mut command = Command::new("/bin/true");
        restrict_process_group(&mut command);

        // SAFETY: This callback only makes syscalls and reads errno after fork.
        unsafe {
            command.pre_exec(|| {
                #[cfg(target_arch = "x86_64")]
                let numbers = [
                    libc::SYS_setsid as libc::c_long,
                    libc::SYS_setpgid as libc::c_long,
                    0x4000_0000 + libc::SYS_setsid as libc::c_long,
                    0x4000_0000 + libc::SYS_setpgid as libc::c_long,
                ];

                #[cfg(not(target_arch = "x86_64"))]
                let numbers = [libc::SYS_setsid as libc::c_long, libc::SYS_setpgid as libc::c_long];

                for number in numbers {
                    let result = libc::syscall(number, 0, 0);

                    let error = *libc::__errno_location();

                    if result != -1 || error != libc::EPERM {
                        return Err(io::Error::from_raw_os_error(libc::EACCES));
                    }
                }

                Ok(())
            });
        }

        assert!(command.status().expect("spawn command").success());
    }

    #[test]
    fn builds_a_filter_that_rejects_process_group_changes() {
        let (filter, length) = process_group_filter();
        let instructions = &filter[..length];
        let syscalls = instructions.iter().map(|instruction| instruction.k).collect::<Vec<_>>();

        assert_eq!(instructions[2].k, libc::SECCOMP_RET_KILL_PROCESS);
        assert!(syscalls.contains(&(libc::SYS_setsid as u32)));
        assert!(syscalls.contains(&(libc::SYS_setpgid as u32)));

        for syscall in super::arch::COMPAT_SYSCALLS {
            assert!(syscalls.contains(&syscall));
        }

        assert_eq!(instructions.last().unwrap().k, libc::SECCOMP_RET_ALLOW);
    }

    #[test]
    fn installs_filter_and_denies_group_changes_on_current_thread() {
        restrict_process_group_in_child().expect("install process group filter");

        #[cfg(target_arch = "x86_64")]
        let numbers = [
            libc::SYS_setsid as libc::c_long,
            libc::SYS_setpgid as libc::c_long,
            0x4000_0000 + libc::SYS_setsid as libc::c_long,
            0x4000_0000 + libc::SYS_setpgid as libc::c_long,
        ];

        #[cfg(not(target_arch = "x86_64"))]
        let numbers = [libc::SYS_setsid as libc::c_long, libc::SYS_setpgid as libc::c_long];

        for number in numbers {
            // SAFETY: These syscalls are intentionally denied by the installed filter.
            let result = unsafe { libc::syscall(number, 0, 0) };

            // SAFETY: errno is thread-local and valid immediately after syscall returns.
            let error = unsafe { *libc::__errno_location() };

            assert_eq!(result, -1);
            assert_eq!(error, libc::EPERM);
        }
    }
}
