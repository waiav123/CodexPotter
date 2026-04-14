//! Creates a Codex-compatible home directory for upstream processes.
//!
//! CodexPotter needs to spawn the upstream `codex` backend while keeping its own state under
//! `~/.codexpotter/`. The upstream backend expects a `CODEX_HOME` directory containing config,
//! auth, agent configs, skills, and rules. To avoid mutating the user's real Codex home, we create a
//! `~/.codexpotter/codex-compat/` directory that symlinks to the corresponding files/dirs in the
//! real `CODEX_HOME` (or `~/.codex` when unset).
//!
//! The resulting path is passed to upstream via `CODEX_HOME` so existing Codex configuration is
//! honored while CodexPotter continues to own its own on-disk artifacts.

use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CodexCompatEntryKind {
    File,
    Directory,
}

impl CodexCompatEntryKind {
    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        match self {
            Self::File => metadata.is_file(),
            Self::Directory => metadata.is_dir(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CodexCompatEntry {
    name: &'static str,
    kind: CodexCompatEntryKind,
}

const CODEX_COMPAT_ENTRIES: &[CodexCompatEntry] = &[
    CodexCompatEntry {
        name: "AGENTS.md",
        kind: CodexCompatEntryKind::File,
    },
    CodexCompatEntry {
        name: "config.toml",
        kind: CodexCompatEntryKind::File,
    },
    CodexCompatEntry {
        name: "auth.json",
        kind: CodexCompatEntryKind::File,
    },
    CodexCompatEntry {
        name: "agents",
        kind: CodexCompatEntryKind::Directory,
    },
    CodexCompatEntry {
        name: "skills",
        kind: CodexCompatEntryKind::Directory,
    },
    CodexCompatEntry {
        name: "rules",
        kind: CodexCompatEntryKind::Directory,
    },
];

pub fn ensure_default_codex_compat_home() -> anyhow::Result<Option<PathBuf>> {
    let Some(home) = dirs::home_dir() else {
        return Ok(None);
    };
    let real_codex_home = resolve_real_codex_home(&home)?;
    ensure_codex_compat_home(&home, &real_codex_home).map(Some)
}

fn resolve_real_codex_home(home: &Path) -> anyhow::Result<PathBuf> {
    let codex_home_env = std::env::var("CODEX_HOME").ok();
    resolve_real_codex_home_from_env(home, codex_home_env.as_deref())
}

fn resolve_real_codex_home_from_env(
    home: &Path,
    codex_home_env: Option<&str>,
) -> anyhow::Result<PathBuf> {
    let Some(val) = codex_home_env.filter(|val| !val.is_empty()) else {
        return Ok(home.join(".codex"));
    };

    let path = PathBuf::from(val);
    let metadata = std::fs::metadata(&path).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => {
            anyhow::anyhow!("CODEX_HOME points to {val:?}, but that path does not exist")
        }
        _ => anyhow::anyhow!("failed to read CODEX_HOME {val:?}: {err}"),
    })?;
    if !metadata.is_dir() {
        anyhow::bail!("CODEX_HOME points to {val:?}, but that path is not a directory");
    }
    path.canonicalize()
        .map_err(|err| anyhow::anyhow!("failed to canonicalize CODEX_HOME {val:?}: {err}"))
}

fn ensure_codex_compat_home(home: &Path, real_codex_home: &Path) -> anyhow::Result<PathBuf> {
    let codex_home = home.join(".codexpotter").join("codex-compat");
    std::fs::create_dir_all(&codex_home)
        .with_context(|| format!("create directory {}", codex_home.display()))?;

    for entry in CODEX_COMPAT_ENTRIES {
        ensure_symlink(
            &codex_home.join(entry.name),
            &real_codex_home.join(entry.name),
            entry.kind,
        )?;
    }

    Ok(codex_home)
}

fn ensure_symlink(
    link_path: &Path,
    target_path: &Path,
    expected_kind: CodexCompatEntryKind,
) -> anyhow::Result<()> {
    if link_path == target_path {
        return Ok(());
    }

    validate_target_kind(target_path, expected_kind)?;

    match std::fs::symlink_metadata(link_path) {
        Ok(metadata) => {
            let target_exists = target_path.exists();
            if metadata.file_type().is_symlink() {
                let current_target = std::fs::read_link(link_path)
                    .with_context(|| format!("read symlink {}", link_path.display()))?;
                if current_target == target_path
                    && target_exists
                    && std::fs::metadata(link_path).is_ok_and(|meta| expected_kind.matches(&meta))
                {
                    return Ok(());
                }
            }

            remove_existing_entry(link_path, &metadata, expected_kind)?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(anyhow::Error::new(err)
                .context(format!("inspect existing entry {}", link_path.display())));
        }
    }

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target_path, link_path)
            .with_context(|| format!("create symlink {}", link_path.display()))?;
        Ok(())
    }

    #[cfg(windows)]
    {
        match expected_kind {
            CodexCompatEntryKind::File => {
                std::os::windows::fs::symlink_file(target_path, link_path)
                    .with_context(|| format!("create file symlink {}", link_path.display()))?
            }
            CodexCompatEntryKind::Directory => {
                std::os::windows::fs::symlink_dir(target_path, link_path)
                    .with_context(|| format!("create directory symlink {}", link_path.display()))?
            }
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    anyhow::bail!("symlinks are not supported on this platform");
}

fn validate_target_kind(
    target_path: &Path,
    expected_kind: CodexCompatEntryKind,
) -> anyhow::Result<()> {
    let metadata = match std::fs::metadata(target_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(anyhow::Error::new(err)
                .context(format!("read target metadata {}", target_path.display())));
        }
    };

    anyhow::ensure!(
        expected_kind.matches(&metadata),
        "expected {} to be a {}, but found a {}",
        target_path.display(),
        expected_kind_label(expected_kind),
        actual_kind_label(&metadata)
    );
    Ok(())
}

fn remove_existing_entry(
    link_path: &Path,
    metadata: &std::fs::Metadata,
    expected_kind: CodexCompatEntryKind,
) -> anyhow::Result<()> {
    let is_directory_entry = if metadata.file_type().is_symlink() {
        matches!(expected_kind, CodexCompatEntryKind::Directory)
    } else {
        metadata.file_type().is_dir()
    };

    if is_directory_entry {
        std::fs::remove_dir_all(link_path)
            .with_context(|| format!("remove directory {}", link_path.display()))?;
    } else {
        std::fs::remove_file(link_path)
            .with_context(|| format!("remove entry {}", link_path.display()))?;
    }
    Ok(())
}

fn expected_kind_label(kind: CodexCompatEntryKind) -> &'static str {
    match kind {
        CodexCompatEntryKind::File => "file",
        CodexCompatEntryKind::Directory => "directory",
    }
}

fn actual_kind_label(metadata: &std::fs::Metadata) -> &'static str {
    if metadata.is_dir() {
        "directory"
    } else if metadata.is_file() {
        "file"
    } else {
        "special entry"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::path::Path;

    #[cfg(windows)]
    fn is_windows_symlink_privilege_error(err: &std::io::Error) -> bool {
        err.raw_os_error() == Some(1314)
    }

    #[cfg(windows)]
    fn is_windows_symlink_privilege_anyhow(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(is_windows_symlink_privilege_error)
        })
    }

    #[test]
    fn ensures_codex_compat_home_and_links() {
        let home_dir = tempfile::tempdir().expect("home dir");
        let real_codex_home = tempfile::tempdir().expect("real codex home");
        for entry in CODEX_COMPAT_ENTRIES {
            let path = real_codex_home.path().join(entry.name);
            match entry.kind {
                CodexCompatEntryKind::File => {
                    std::fs::write(&path, entry.name).expect("write file entry");
                }
                CodexCompatEntryKind::Directory => {
                    std::fs::create_dir_all(&path).expect("create dir entry");
                }
            }
        }
        let codex_home = match ensure_codex_compat_home(home_dir.path(), real_codex_home.path()) {
            Ok(path) => path,
            #[cfg(windows)]
            Err(err) if is_windows_symlink_privilege_anyhow(&err) => return,
            Err(err) => panic!("ensure home: {err:#}"),
        };

        assert!(codex_home.is_dir());

        for entry in CODEX_COMPAT_ENTRIES {
            let link_path = codex_home.join(entry.name);
            let link_meta = std::fs::symlink_metadata(&link_path)
                .unwrap_or_else(|err| panic!("missing symlink {}: {err}", entry.name));
            assert!(
                link_meta.file_type().is_symlink(),
                "{} should be a symlink",
                entry.name
            );
            assert_eq!(
                std::fs::read_link(&link_path)
                    .unwrap_or_else(|err| panic!("failed to read {} symlink: {err}", entry.name)),
                real_codex_home.path().join(entry.name),
            );
            let resolved_meta = std::fs::metadata(&link_path)
                .unwrap_or_else(|err| panic!("failed to stat resolved {}: {err}", entry.name));
            assert!(
                entry.kind.matches(&resolved_meta),
                "{} should resolve to a {}",
                entry.name,
                expected_kind_label(entry.kind)
            );
        }

        // Running it again should be a no-op (even if the targets are missing).
        let codex_home_again = ensure_codex_compat_home(home_dir.path(), real_codex_home.path())
            .expect("ensure home again");
        assert_eq!(codex_home_again, codex_home);
    }

    #[test]
    fn ensures_codex_compat_home_repairs_stale_entries() {
        let home_dir = tempfile::tempdir().expect("home dir");
        let real_codex_home = tempfile::tempdir().expect("real codex home");
        let stale_target = tempfile::tempdir().expect("stale target");

        for entry in CODEX_COMPAT_ENTRIES {
            let path = real_codex_home.path().join(entry.name);
            match entry.kind {
                CodexCompatEntryKind::File => {
                    std::fs::write(&path, entry.name).expect("write file entry");
                    std::fs::write(stale_target.path().join(entry.name), "stale")
                        .expect("write stale file entry");
                }
                CodexCompatEntryKind::Directory => {
                    std::fs::create_dir_all(&path).expect("create dir entry");
                    std::fs::create_dir_all(stale_target.path().join(entry.name))
                        .expect("create stale dir entry");
                }
            }
        }

        let codex_home = home_dir.path().join(".codexpotter").join("codex-compat");
        std::fs::create_dir_all(&codex_home).expect("create compat dir");

        for entry in CODEX_COMPAT_ENTRIES {
            let link_path = codex_home.join(entry.name);
            let result = create_test_symlink(
                &stale_target.path().join(entry.name),
                &link_path,
                entry.kind,
            );
            match result {
                Ok(()) => {}
                #[cfg(windows)]
                Err(err) if is_windows_symlink_privilege_error(&err) => return,
                Err(err) => panic!("create stale {} symlink: {err}", entry.name),
            }
        }

        match ensure_codex_compat_home(home_dir.path(), real_codex_home.path()) {
            Ok(_) => {}
            #[cfg(windows)]
            Err(err) if is_windows_symlink_privilege_anyhow(&err) => return,
            Err(err) => panic!("repair home: {err:#}"),
        }

        for entry in CODEX_COMPAT_ENTRIES {
            let link_path = codex_home.join(entry.name);
            assert_eq!(
                std::fs::read_link(&link_path).unwrap_or_else(|err| panic!(
                    "failed to read repaired {} symlink: {err}",
                    entry.name
                )),
                real_codex_home.path().join(entry.name),
            );
        }
    }

    #[cfg(unix)]
    fn create_test_symlink(
        target_path: &Path,
        link_path: &Path,
        _kind: CodexCompatEntryKind,
    ) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target_path, link_path)
    }

    #[cfg(windows)]
    fn create_test_symlink(
        target_path: &Path,
        link_path: &Path,
        kind: CodexCompatEntryKind,
    ) -> std::io::Result<()> {
        match kind {
            CodexCompatEntryKind::File => {
                std::os::windows::fs::symlink_file(target_path, link_path)
            }
            CodexCompatEntryKind::Directory => {
                std::os::windows::fs::symlink_dir(target_path, link_path)
            }
        }
    }

    #[test]
    fn resolve_real_codex_home_falls_back_to_dot_codex() {
        let home_dir = tempfile::tempdir().expect("home dir");
        let resolved = resolve_real_codex_home_from_env(home_dir.path(), None).expect("resolve");
        assert_eq!(resolved, home_dir.path().join(".codex"));
    }

    #[test]
    fn resolve_real_codex_home_uses_canonicalized_env_path() {
        let home_dir = tempfile::tempdir().expect("home dir");
        let codex_home = tempfile::tempdir().expect("codex home");
        let codex_home_env = codex_home.path().to_string_lossy().to_string();
        let resolved = resolve_real_codex_home_from_env(home_dir.path(), Some(&codex_home_env))
            .expect("resolve");
        let expected = codex_home.path().canonicalize().expect("canonicalize");
        assert_eq!(resolved, expected);
    }
}
