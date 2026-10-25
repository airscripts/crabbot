pub const AUDIT_ARCH: u32 = 0xc000_003e;
pub const COMPAT_SYSCALLS: [u32; 2] =
    [0x4000_0000 + libc::SYS_setsid as u32, 0x4000_0000 + libc::SYS_setpgid as u32];
