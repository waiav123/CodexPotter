//! Potter runtime diagnostics log.
//!
//! CodexPotter persists a second append-only JSONL file alongside `potter-rollout.jsonl`:
//! `potter-runtime.jsonl`.
//!
//! Unlike rollout, this file is diagnostic-only. It records:
//! - session lifecycle restarts
//! - per-round execution starts
//! - abnormal stop reasons such as manual interrupt or event-stream closure
//!
//! This log is intentionally best-effort but still strict for writes: callers receive errors so
//! they can decide whether to surface diagnostics immediately.

use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;

pub const POTTER_RUNTIME_LOG_FILENAME: &str = "potter-runtime.jsonl";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PotterRuntimeDiagnosticReason {
    ManualInterrupt,
    EventStreamClosed,
    BackendSpawnFailed,
    RuntimeError,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PotterRuntimeLogLine {
    SessionStarted {
        unix_secs: u64,
        mode: String,
    },
    RoundStarted {
        unix_secs: u64,
        current: u32,
        total: u32,
    },
    Diagnostic {
        unix_secs: u64,
        reason: PotterRuntimeDiagnosticReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        round_current: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        round_total: Option<u32>,
        message: String,
    },
}

pub fn potter_runtime_log_path(project_dir: &Path) -> PathBuf {
    project_dir.join(POTTER_RUNTIME_LOG_FILENAME)
}

pub fn append_line(path: &Path, line: &PotterRuntimeLogLine) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        anyhow::bail!(
            "invalid potter-runtime path (no parent): {}",
            path.display()
        );
    };
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;

    let mut json = serde_json::to_string(line)
        .with_context(|| format!("serialize potter-runtime line for {}", path.display()))?;
    json.push('\n');

    file.write_all(json.as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn append_session_started(path: &Path, mode: impl Into<String>) -> anyhow::Result<()> {
    append_line(
        path,
        &PotterRuntimeLogLine::SessionStarted {
            unix_secs: now_unix_secs(),
            mode: mode.into(),
        },
    )
}

pub fn append_round_started(path: &Path, current: u32, total: u32) -> anyhow::Result<()> {
    append_line(
        path,
        &PotterRuntimeLogLine::RoundStarted {
            unix_secs: now_unix_secs(),
            current,
            total,
        },
    )
}

pub fn append_diagnostic(
    path: &Path,
    reason: PotterRuntimeDiagnosticReason,
    round_current: Option<u32>,
    round_total: Option<u32>,
    message: impl Into<String>,
) -> anyhow::Result<()> {
    append_line(
        path,
        &PotterRuntimeLogLine::Diagnostic {
            unix_secs: now_unix_secs(),
            reason,
            round_current,
            round_total,
            message: message.into(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn append_and_read_runtime_log_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(POTTER_RUNTIME_LOG_FILENAME);

        append_session_started(&path, "fresh_start").expect("append session");
        append_round_started(&path, 1, 100).expect("append round");
        append_diagnostic(
            &path,
            PotterRuntimeDiagnosticReason::EventStreamClosed,
            Some(1),
            Some(100),
            "event stream closed unexpectedly",
        )
        .expect("append diagnostic");

        let file = std::fs::File::open(&path).expect("open");
        let reader = std::io::BufReader::new(file);
        let lines = std::io::BufRead::lines(reader)
            .map(|line| line.expect("line"))
            .map(|line| {
                serde_json::from_str::<PotterRuntimeLogLine>(&line).expect("deserialize runtime")
            })
            .collect::<Vec<_>>();

        assert_eq!(lines.len(), 3);
        assert!(matches!(
            lines[0],
            PotterRuntimeLogLine::SessionStarted { ref mode, .. } if mode == "fresh_start"
        ));
        assert!(matches!(
            lines[1],
            PotterRuntimeLogLine::RoundStarted {
                current: 1,
                total: 100,
                ..
            }
        ));
        assert!(matches!(
            lines[2],
            PotterRuntimeLogLine::Diagnostic {
                reason: PotterRuntimeDiagnosticReason::EventStreamClosed,
                round_current: Some(1),
                round_total: Some(100),
                ..
            }
        ));
    }
}
