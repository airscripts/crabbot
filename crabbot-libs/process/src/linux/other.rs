#[cfg(target_arch = "x86")]
pub const AUDIT_ARCH: u32 = 0x4000_0003;
#[cfg(target_arch = "arm")]
pub const AUDIT_ARCH: u32 = 0x4000_0028;
#[cfg(target_arch = "riscv32")]
pub const AUDIT_ARCH: u32 = 0x4000_00f3;
#[cfg(target_arch = "riscv64")]
pub const AUDIT_ARCH: u32 = 0xc000_00f3;
#[cfg(target_arch = "powerpc")]
pub const AUDIT_ARCH: u32 = 0x0000_0014;
#[cfg(all(target_arch = "powerpc64", target_endian = "big"))]
pub const AUDIT_ARCH: u32 = 0x8000_0015;
#[cfg(all(target_arch = "powerpc64", target_endian = "little"))]
pub const AUDIT_ARCH: u32 = 0xc000_0015;
#[cfg(target_arch = "s390x")]
pub const AUDIT_ARCH: u32 = 0x8000_0016;
#[cfg(all(target_arch = "mips", target_endian = "big"))]
pub const AUDIT_ARCH: u32 = 0x0000_0008;
#[cfg(all(target_arch = "mips", target_endian = "little"))]
pub const AUDIT_ARCH: u32 = 0x4000_0008;
#[cfg(all(target_arch = "mips64", target_endian = "big"))]
pub const AUDIT_ARCH: u32 = 0x8000_0008;
#[cfg(all(target_arch = "mips64", target_endian = "little"))]
pub const AUDIT_ARCH: u32 = 0xc000_0008;
#[cfg(target_arch = "loongarch64")]
pub const AUDIT_ARCH: u32 = 0xc000_0102;

pub const COMPAT_SYSCALLS: [u32; 0] = [];
