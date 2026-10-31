#![allow(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

// Stable SID for NT SERVICE\Krabb, as reported by `sc.exe showsid Krabb`.
#[cfg(any(windows, test))]
const SERVICE_SID: &str = "S-1-5-80-3700705418-1466718386-3227256717-750624257-2359456539";

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

pub fn append(path: impl AsRef<Path>, content: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    reject_link(path)?;

    let mut options = OpenOptions::new();
    options.append(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;

    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Append path must be a regular file.",
        ));
    }

    private(path)?;
    file.write_all(content.as_ref())?;
    file.sync_all()
}

pub fn append_jsonl(path: impl AsRef<Path>, record: impl AsRef<[u8]>) -> io::Result<()> {
    let record = record.as_ref();

    if record.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "JSONL record must end with a newline.",
        ));
    }

    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    reject_link(path)?;

    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;

    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Append path must be a regular file.",
        ));
    }

    private(path)?;

    let length = file.metadata()?.len();

    if length > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0_u8; 1];
        file.read_exact(&mut last)?;

        if last[0] != b'\n' {
            let mut start = length;
            let mut chunk = [0_u8; 8192];

            while start > 0 {
                let chunk_start = start.saturating_sub(chunk.len() as u64);
                let count = (start - chunk_start) as usize;
                file.seek(SeekFrom::Start(chunk_start))?;
                file.read_exact(&mut chunk[..count])?;

                if let Some(index) = chunk[..count].iter().rposition(|byte| *byte == b'\n') {
                    start = chunk_start + index as u64 + 1;
                    break;
                }

                start = chunk_start;
            }

            file.seek(SeekFrom::Start(start))?;

            if serde_json::from_reader::<_, serde_json::Value>(
                (&mut file).take(length.saturating_sub(start)),
            )
            .is_ok()
            {
                file.seek(SeekFrom::End(0))?;

                file.write_all(b"\n")?;
            } else {
                file.set_len(start)?;
            }
        }
    }

    file.seek(SeekFrom::End(0))?;
    file.write_all(record)?;
    file.sync_all()
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
    let temporary = path.with_file_name(format!(".journal-{}-{}", name(path), nonce()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary)?;

    let result = (|| {
        private(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;

        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }

        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }

    result
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

/// Prepare existing private state for the Windows Krabb virtual service account.
#[cfg(windows)]
pub fn service(path: impl AsRef<Path>) -> io::Result<()> {
    let mut pending = vec![path.as_ref().to_path_buf()];

    while let Some(path) = pending.pop() {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };

        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Service state cannot contain symbolic links.",
            ));
        }

        protect(&path)?;

        if metadata.is_dir() {
            for entry in fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
        }
    }

    Ok(())
}

#[cfg(windows)]
fn owner(path: &Path) -> io::Result<String> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::{
        Foundation::{ERROR_SUCCESS, LocalFree},
        Security::{
            Authorization::{ConvertSidToStringSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT},
            OWNER_SECURITY_INFORMATION,
        },
    };

    let wide = path.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
    let mut descriptor = std::ptr::null_mut();
    let mut sid = std::ptr::null_mut();
    let result = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut sid,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };

    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }

    let mut text = std::ptr::null_mut();
    let converted = unsafe { ConvertSidToStringSidW(sid, &mut text) };

    let error = io::Error::last_os_error();
    unsafe { LocalFree(descriptor) };

    if converted == 0 {
        return Err(error);
    }

    let mut length = 0;

    while unsafe { *text.add(length) } != 0 {
        length += 1;
    }

    let value = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) });

    unsafe { LocalFree(text.cast()) };

    Ok(value)
}

#[cfg(any(windows, test))]
fn principal(
    path: &Path,
    mut owner: impl FnMut(&Path) -> io::Result<String>,
) -> io::Result<String> {
    // Service-created files belong to the service account; retain the user's access
    // by finding the owning account of their enclosing data directory.
    for ancestor in path.ancestors() {
        let sid = owner(ancestor)?;

        if sid != "S-1-5-18" && sid != SERVICE_SID {
            return Ok(sid);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "Service state needs a user-owned directory.",
    ))
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
    let principal = principal(path, owner)?;
    let inheritance = if fs::symlink_metadata(path)?.is_dir() { "OICI" } else { "" };

    let security = format!(
        "D:P(A;{inheritance};FA;;;{principal})(A;{inheritance};FA;;;{SERVICE_SID})(A;{inheritance};FA;;;SY)\0"
    )
        .encode_utf16()
        .collect::<Vec<_>>();

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

#[cfg(test)]
mod tests {
    use super::{
        Transaction, append, append_jsonl, check, credential, load, private, recover, save,
    };

    use std::fs;

    fn root(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("krabb-file-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn parallel_saves_keep_independent_transaction_journals() {
        let root = root("parallel-journals");
        let barrier = std::sync::Barrier::new(8);

        std::thread::scope(|scope| {
            for index in 0..8 {
                let path = root.join(format!("state-{index}.json"));
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();

                    for revision in 0..20 {
                        save(&path, revision.to_string()).unwrap();
                    }
                });
            }
        });

        for index in 0..8 {
            assert_eq!(load(root.join(format!("state-{index}.json")), 16).unwrap().unwrap(), b"19");
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incomplete_journal_replacement_keeps_the_previous_recovery_record() {
        let root = root("journal-replacement");
        let path = root.join("state.json");
        let backup = root.join(".state.json.backup-test");
        let journal = root.join(".state.json.txn-test");
        fs::write(&path, b"saved").unwrap();
        let mut transaction = Transaction {
            path: "state.json".into(),
            temporary: ".state.json.tmp-test".into(),
            backup: ".state.json.backup-test".into(),
            phase: "prepared".into(),
        };

        super::write_journal(&journal, &transaction).unwrap();
        fs::rename(&path, &backup).unwrap();
        fs::write(root.join(".journal-interrupted"), b"{\"path\":").unwrap();

        assert_eq!(load(&path, 32).unwrap().unwrap(), b"saved");

        super::write_journal(&journal, &transaction).unwrap();
        transaction.phase = "backed_up".into();
        super::write_journal(&journal, &transaction).unwrap();
        let published: Transaction = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();

        assert_eq!(published.phase, "backed_up");
        check(&journal).unwrap();
        let _ = fs::remove_dir_all(root);
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
    fn appends_to_private_files_without_replacing_existing_content() {
        let root = root("append");
        let path = root.join("logs/schedule.jsonl");

        append(&path, b"{\"run\":1}\n").unwrap();
        append(&path, b"{\"run\":2}\n").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"{\"run\":1}\n{\"run\":2}\n");
        check(&path).unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jsonl_append_discards_a_torn_trailing_record() {
        let root = root("append-jsonl-torn");
        let path = root.join("logs/schedule.jsonl");

        append_jsonl(&path, b"{\"run\":1}\n").unwrap();
        append(&path, b"{\"run\":").unwrap();
        append_jsonl(&path, b"{\"run\":2}\n").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"{\"run\":1}\n{\"run\":2}\n");

        fs::write(&path, b"{\"run\":3}").unwrap();
        append_jsonl(&path, b"{\"run\":4}\n").unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"{\"run\":3}\n{\"run\":4}\n");

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn append_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let root = root("append-link");
        fs::create_dir_all(&root).unwrap();
        let target = root.join("target");
        let link = root.join("link");
        fs::write(&target, b"original").unwrap();
        symlink(&target, &link).unwrap();

        assert!(append(&link, b"unsafe").is_err());
        assert_eq!(fs::read(&target).unwrap(), b"original");

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

    #[cfg(windows)]
    #[test]
    fn service_sid_matches_windows_account_identity() {
        let output =
            std::process::Command::new("sc.exe").args(["showsid", "Krabb"]).output().unwrap();

        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains(super::SERVICE_SID));
    }

    #[cfg(windows)]
    #[test]
    fn service_state_allows_only_the_owner_service_and_system_after_replacement() {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::{
            Foundation::{ERROR_SUCCESS, LocalFree},
            Security::{
                ACCESS_ALLOWED_ACE,
                Authorization::{
                    ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW,
                    SDDL_REVISION_1, SE_FILE_OBJECT,
                },
                DACL_SECURITY_INFORMATION, EqualSid, GetAce, OWNER_SECURITY_INFORMATION,
            },
        };

        let root = root("service-access");
        let path = root.join("state.json");
        fs::write(&path, "old").unwrap();
        super::service(&root).unwrap();

        for content in ["first", "second"] {
            save(&path, content).unwrap();
            check(&path).unwrap();
            let wide = path.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
            let mut descriptor = std::ptr::null_mut();
            let mut owner = std::ptr::null_mut();
            let mut acl = std::ptr::null_mut();

            let result = unsafe {
                GetNamedSecurityInfoW(
                    wide.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION,
                    &mut owner,
                    std::ptr::null_mut(),
                    &mut acl,
                    std::ptr::null_mut(),
                    &mut descriptor,
                )
            };

            assert_eq!(result, ERROR_SUCCESS);

            let mut ace = std::ptr::null_mut();

            assert_ne!(unsafe { GetAce(acl, 0, &mut ace) }, 0);

            let ace = ace.cast::<ACCESS_ALLOWED_ACE>();
            let sid = unsafe { &raw mut (*ace).SidStart }.cast();

            assert_ne!(unsafe { EqualSid(owner, sid) }, 0);

            let mut text = std::ptr::null_mut();
            let mut length = 0;
            let result = unsafe {
                ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    descriptor,
                    SDDL_REVISION_1,
                    DACL_SECURITY_INFORMATION,
                    &mut text,
                    &mut length,
                )
            };

            unsafe { LocalFree(descriptor) };

            assert_ne!(result, 0);

            let dacl = String::from_utf16_lossy(unsafe {
                std::slice::from_raw_parts(text, length as usize - 1)
            });

            unsafe { LocalFree(text.cast()) };

            assert!(dacl.starts_with("D:P(A;;FA;;;"));
            assert!(dacl.ends_with(")(A;;FA;;;SY)"));
            assert!(dacl.contains(&format!("(A;;FA;;;{})", super::SERVICE_SID)));
            assert_eq!(dacl.matches('(').count(), 3);
            assert_eq!(load(&path, 1024).unwrap().unwrap(), content.as_bytes());
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn service_owned_state_retains_the_enclosing_users_access() {
        let path = std::path::Path::new("user/data/state.json");
        let user = "S-1-5-21-1-2-3-1001";

        for service in ["S-1-5-18", super::SERVICE_SID] {
            let sid = super::principal(path, |path| {
                Ok(if path == std::path::Path::new("user") { user } else { service }.into())
            })
            .unwrap();

            assert_eq!(sid, user);
            assert!(super::principal(path, |_| Ok(service.into())).is_err());
        }

        assert_eq!(super::principal(path, |_| Ok(user.into())).unwrap(), user);
        assert!(super::principal(path, |_| Ok("S-1-5-18".into())).is_err());
        assert!(
            super::principal(path, |_| Err(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied
            )))
            .is_err()
        );
    }
}
