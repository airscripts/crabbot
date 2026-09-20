#![allow(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const CREDENTIAL_LIMIT: u64 = 1024 * 1024;

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

#[derive(Debug, Deserialize, Serialize)]
struct Transaction {
    path: String,
    temporary: String,
    backup: String,
    phase: String,
}

pub fn load(path: impl AsRef<Path>, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let path = path.as_ref();

    match recover(path) {
        Ok(()) => {}
        Err(error)
            if matches!(error.kind(), io::ErrorKind::NotADirectory | io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }

        Err(error) => return Err(error),
    }

    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "State file cannot be a symbolic link.",
        ));
    }

    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "State path must be a regular file.",
        ));
    }

    if metadata.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "State file exceeds the size limit.",
        ));
    }

    check(path)?;
    fs::read(path).map(Some)
}

pub fn save(path: impl AsRef<Path>, content: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    reject_link(path)?;
    let nonce = nonce();
    let temporary = path.with_file_name(format!(".{}.tmp-{}", name(path), nonce));
    let backup = path.with_file_name(format!(".{}.backup-{}", name(path), nonce));
    let journal = path.with_file_name(format!(".{}.txn-{}", name(path), nonce));

    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary)?;
        file.write_all(content.as_ref())?;
        file.sync_all()?;
        private(&temporary)?;

        let mut transaction = Transaction {
            path: name(path),
            temporary: name(&temporary),
            backup: name(&backup),
            phase: "prepared".into(),
        };

        write_journal(&journal, &transaction)?;

        if matches!(fs::symlink_metadata(path), Ok(metadata) if metadata.is_file()) {
            fs::rename(path, &backup)?;
            transaction.phase = "backed_up".into();
            write_journal(&journal, &transaction)?;
        }

        fs::rename(&temporary, path)?;
        private(path)?;
        transaction.phase = "installed".into();
        write_journal(&journal, &transaction)?;
        let _ = fs::remove_file(&backup);
        fs::remove_file(&journal)?;

        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }

        Ok::<(), io::Error>(())
    })();

    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }

    Ok(())
}

pub fn recover(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    let prefix = format!(".{}.txn-", name(path));
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let journal = entry.path();

        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }

        let transaction: Transaction = match serde_json::from_slice(&fs::read(&journal)?) {
            Ok(transaction) => transaction,

            Err(_) => {
                let _ = fs::remove_file(journal);
                continue;
            }
        };

        if transaction.path != name(path) {
            continue;
        }

        let temporary = parent.join(&transaction.temporary);
        let backup = parent.join(&transaction.backup);

        match transaction.phase.as_str() {
            "backed_up" if !regular(path) => {
                if regular(&temporary) {
                    fs::rename(&temporary, path)?;
                } else if regular(&backup) {
                    fs::rename(&backup, path)?;
                }
            }

            "prepared" if regular(path) => {
                let _ = fs::remove_file(&temporary);
            }

            "prepared" if !regular(path) && regular(&backup) => {
                fs::rename(&backup, path)?;
            }

            "installed" if !regular(path) && regular(&backup) => {
                fs::rename(&backup, path)?;
            }

            _ => {}
        }

        if regular(path) {
            private(path)?;
        }

        let _ = fs::remove_file(temporary);
        let _ = fs::remove_file(backup);
        let _ = fs::remove_file(journal);
    }

    Ok(())
}

pub fn private(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let metadata = metadata(path)?;
    #[cfg(windows)]
    let _ = metadata;
    #[cfg(unix)]
    {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)?;
    }

    #[cfg(windows)]
    protect(path)?;

    Ok(())
}

pub fn credential(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    check(path)?;

    if metadata(path)?.len() > CREDENTIAL_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "Credential file exceeds the size limit.",
        ));
    }

    Ok(())
}

pub fn check(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let metadata = metadata(path)?;
    #[cfg(windows)]
    let _ = metadata;
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "State file must be private."));
    }

    #[cfg(windows)]
    protect(path)?;
    Ok(())
}

fn metadata(path: &Path) -> io::Result<std::fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;

    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "State file cannot be a symbolic link.",
        ));
    }

    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "State path must be a regular file.",
        ));
    }

    Ok(metadata)
}

fn regular(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

fn write_journal(path: &Path, transaction: &Transaction) -> io::Result<()> {
    let bytes = serde_json::to_vec(transaction).map_err(io::Error::other)?;
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    private(path)
}

fn reject_link(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "State file cannot be a symbolic link.",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn name(path: &Path) -> String {
    path.file_name().and_then(|value| value.to_str()).unwrap_or("state").into()
}

fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |value| value.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::{Transaction, check, credential, load, private, recover, save};

    use std::fs;

    fn root(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("crabbot-file-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn saves_and_loads_private_content() {
        let root = root("roundtrip");
        let path = root.join("state.json");
        save(&path, b"hello").unwrap();
        assert_eq!(load(&path, 16).unwrap().as_deref(), Some(&b"hello"[..]));
        check(&path).unwrap();
        private(&path).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_links() {
        let root = root("links");
        let target = root.join("target");
        let link = root.join("state.json");
        fs::write(&target, b"secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&target, &link).unwrap();
        assert!(save(&link, b"changed").is_err());
        assert!(load(&link, 16).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_oversized_credentials() {
        let root = root("credentials");
        let path = root.join("credentials.json");
        fs::write(&path, vec![b'x'; 1024 * 1024 + 1]).unwrap();
        assert!(credential(&path).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovers_interrupted_transactions_and_rejects_invalid_state() {
        let root = root("recovery");
        let path = root.join("state.json");
        let temporary = root.join(".state.json.tmp");
        let journal = root.join(".state.json.txn-test");
        fs::write(&temporary, b"temporary").unwrap();
        fs::write(
            &journal,
            serde_json::to_vec(&Transaction {
                path: "state.json".into(),
                temporary: ".state.json.tmp".into(),
                backup: ".state.json.backup".into(),
                phase: "backed_up".into(),
            })
            .unwrap(),
        )
        .unwrap();

        recover(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"temporary");

        fs::write(&path, b"current").unwrap();
        fs::write(&temporary, b"stale").unwrap();
        fs::write(&journal, b"not-json").unwrap();
        recover(&path).unwrap();
        assert!(!journal.exists());
        assert_eq!(fs::read(&path).unwrap(), b"current");

        assert!(load(root.join("missing"), 16).unwrap().is_none());
        fs::write(root.join("large"), b"0123456789").unwrap();
        assert!(load(root.join("large"), 2).is_err());
        fs::create_dir(root.join("directory")).unwrap();
        assert!(load(root.join("directory"), 16).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovers_each_transaction_phase_and_validates_credentials() {
        let root = root("recovery-phases");
        let path = root.join("state.json");

        recover(std::path::Path::new("")).unwrap();
        assert!(recover(root.join("missing").join("state.json")).is_ok());

        fs::write(&path, b"current").unwrap();
        fs::write(root.join(".state.json.tmp-prepared"), b"stale").unwrap();
        write_transaction(&root, "prepared", "tmp-prepared", "backup-prepared");
        recover(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"current");

        let _ = fs::remove_file(&path);
        fs::write(root.join(".backup-prepared"), b"prepared backup").unwrap();
        write_transaction(&root, "prepared", "tmp-unused", "backup-prepared");
        recover(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"prepared backup");

        let _ = fs::remove_file(&path);
        fs::write(root.join(".backup-installed"), b"installed backup").unwrap();
        write_transaction(&root, "installed", "tmp-unused", "backup-installed");
        recover(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"installed backup");

        let _ = fs::remove_file(&path);
        fs::write(root.join(".backup-backed-up"), b"backed up backup").unwrap();
        write_transaction(&root, "backed_up", "tmp-missing", "backup-backed-up");
        recover(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"backed up backup");

        fs::write(&path, b"current").unwrap();
        write_transaction_with_path(&root, "ignored", "prepared", "tmp", "backup");
        fs::write(root.join(".state.json.txn-unknown"), b"not-json").unwrap();
        recover(&path).unwrap();

        credential(&path).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    fn write_transaction(root: &std::path::Path, phase: &str, temporary: &str, backup: &str) {
        write_transaction_with_path(root, "state.json", phase, temporary, backup);
    }

    fn write_transaction_with_path(
        root: &std::path::Path,
        path: &str,
        phase: &str,
        temporary: &str,
        backup: &str,
    ) {
        let journal = root.join(format!(".state.json.txn-{phase}"));
        fs::write(
            journal,
            serde_json::to_vec(&Transaction {
                path: path.into(),
                temporary: format!(".{temporary}"),
                backup: format!(".{backup}"),
                phase: phase.into(),
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_shared_credentials() {
        use std::os::unix::fs::PermissionsExt;

        let root = root("shared-credentials");
        let path = root.join("credentials.json");
        fs::write(&path, "secret").unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&path, permissions).unwrap();
        assert!(credential(&path).is_err());
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(windows)]
fn protect(path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::{
        Foundation::{ERROR_SUCCESS, LocalFree},
        Security::{
            Authorization::{
                ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
                SE_FILE_OBJECT, SetNamedSecurityInfoW,
            },
            DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl,
            PROTECTED_DACL_SECURITY_INFORMATION,
        },
    };

    let mut descriptor = std::ptr::null_mut();
    let mut length = 0;
    let security = "D:P(A;;FA;;;OW)\0".encode_utf16().collect::<Vec<_>>();
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            security.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            &mut length,
        )
    };

    if converted == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut present = 0;
    let mut defaulted = 0;
    let mut dacl = std::ptr::null_mut();
    let result =
        unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) };

    if result == 0 {
        unsafe { LocalFree(descriptor.cast()) };

        return Err(io::Error::last_os_error());
    }

    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    wide.push(0);
    let result = unsafe {
        SetNamedSecurityInfoW(
            wide.as_mut_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            dacl,
            std::ptr::null_mut(),
        )
    };

    unsafe { LocalFree(descriptor.cast()) };

    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }

    Ok(())
}
