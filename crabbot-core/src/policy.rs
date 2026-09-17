use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Shell {
    #[default]
    Off,
    Ask,
    On,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Policy {
    pub shell: Shell,
    pub root: Option<std::path::PathBuf>,
}

impl Policy {
    pub fn shell(&self, approved: bool) -> Result<()> {
        match self.shell {
            Shell::Off => Err(Error::Denied("Shell is disabled.".into())),
            Shell::Ask if !approved => Err(Error::Denied("Shell approval is required.".into())),
            Shell::Ask | Shell::On => Ok(()),
        }
    }

    pub fn path(&self, path: impl AsRef<std::path::Path>) -> Result<std::path::PathBuf> {
        let root =
            self.root.as_ref().ok_or_else(|| Error::Denied("Workspace root is unset.".into()))?;
        let root =
            std::fs::canonicalize(root).map_err(|e| Error::Denied(format!("Workspace: {e}.")))?;
        let path = path.as_ref();
        let joined = if path.is_absolute() { path.to_path_buf() } else { root.join(path) };
        let clean =
            std::fs::canonicalize(&joined).map_err(|e| Error::Denied(format!("Path: {e}.")))?;
        if clean.starts_with(&root) {
            Ok(clean)
        } else {
            Err(Error::Denied("Path leaves the workspace.".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{Policy, Shell};

    #[test]
    fn shell_is_off_by_default() {
        assert!(Policy::default().shell(true).is_err());
    }

    #[test]
    fn shell_ask_needs_approval() {
        let policy = Policy { shell: Shell::Ask, ..Policy::default() };
        assert!(policy.shell(false).is_err());
        assert!(policy.shell(true).is_ok());
    }

    #[test]
    fn path_is_confined_to_root() {
        let root = std::env::temp_dir().join(format!("crabbot-policy-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("note.txt"), "hello").unwrap();
        fs::write(root.parent().unwrap().join("crabbot-policy-outside"), "outside").unwrap();
        let policy = Policy { root: Some(root.clone()), ..Policy::default() };

        assert_eq!(
            policy.path("note.txt").unwrap(),
            fs::canonicalize(root.join("note.txt")).unwrap()
        );
        assert!(policy.path("../crabbot-policy-outside").is_err());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_file(std::env::temp_dir().join("crabbot-policy-outside"));
    }

    #[test]
    fn path_requires_a_root() {
        assert!(Policy::default().path("note.txt").is_err());
    }
}
