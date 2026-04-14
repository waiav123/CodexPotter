//! Backend event bridge for persistence and synthetic markers.
//!
//! While a round is running, CodexPotter forwards backend `EventMsg` items to the UI. This bridge
//! observes the same events to:
//! - Record `RoundConfigured` / terminal `RoundFinished` (and optional `ProjectSucceeded`)
//!   entries into `potter-rollout.jsonl`.
//! - Inject a `PotterProjectSucceeded` event into the UI stream when `finite_incantatem: true` is
//!   set in the progress file and the current round finishes successfully.
//! - Inject a `PotterProjectBudgetExhausted` event into the UI stream when the last budgeted
//!   round finishes successfully without `finite_incantatem: true`.
//!
//! The bridge is designed to be strict: persistence failures are treated as fatal so resume never
//! reads a partially diverged log.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::PotterRoundOutcome;
use codex_protocol::protocol::SessionConfiguredEvent;

#[derive(Debug, Clone)]
pub struct PotterRoundEventBridgeConfig {
    pub record_round_configured: bool,
    pub strict_rounds: bool,

    pub workdir: PathBuf,
    pub progress_file_rel: PathBuf,
    pub potter_xmodel_runtime: bool,
    pub user_prompt_file: PathBuf,
    pub git_commit_start: String,
    pub potter_rollout_path: PathBuf,
    pub project_started_at: Instant,
    pub round_current: u32,
    pub round_total: u32,
    /// Number of rounds executed in the current iteration window, including the active round.
    ///
    /// This value is used for summary markers (`PotterProjectSucceeded` /
    /// `PotterProjectBudgetExhausted`) and for the persisted `ProjectSucceeded` rollout entry. It
    /// intentionally resets when a project is resumed so the round count matches the elapsed
    /// duration for the current iteration window.
    pub project_rounds_run: u32,
}

#[derive(Debug, Clone)]
pub struct PotterRoundEventBridge {
    workdir: PathBuf,
    progress_file_rel: PathBuf,
    potter_xmodel_runtime: bool,
    strict_rounds: bool,
    user_prompt_file: PathBuf,
    git_commit_start: String,
    potter_rollout_path: PathBuf,
    project_started_at: Instant,
    round_current: u32,
    round_total: u32,
    project_rounds_run: u32,
    has_recorded_round_configured: bool,
    session_model: Option<String>,
}

impl PotterRoundEventBridge {
    pub fn new(config: PotterRoundEventBridgeConfig) -> Self {
        Self {
            has_recorded_round_configured: !config.record_round_configured,
            workdir: config.workdir,
            progress_file_rel: config.progress_file_rel,
            potter_xmodel_runtime: config.potter_xmodel_runtime,
            strict_rounds: config.strict_rounds,
            user_prompt_file: config.user_prompt_file,
            git_commit_start: config.git_commit_start,
            potter_rollout_path: config.potter_rollout_path,
            project_started_at: config.project_started_at,
            round_current: config.round_current,
            round_total: config.round_total,
            project_rounds_run: config.project_rounds_run,
            session_model: None,
        }
    }

    pub fn observe_backend_event(&mut self, event: &Event) -> anyhow::Result<Option<Event>> {
        if let EventMsg::SessionConfigured(cfg) = &event.msg {
            self.session_model = Some(cfg.model.clone());
        }

        if !self.has_recorded_round_configured
            && let EventMsg::SessionConfigured(cfg) = &event.msg
        {
            self.has_recorded_round_configured = true;
            self.record_round_configured(cfg)
                .context("record potter-rollout round_configured")?;
        }

        let mut injected: Option<Event> = None;
        if let EventMsg::PotterRoundFinished { outcome } = &event.msg {
            if matches!(outcome, PotterRoundOutcome::Interrupted) {
                // Live ESC interruptions do not immediately finalize the round in
                // `potter-rollout.jsonl`. The round stays open until the user resolves the paused
                // project: `continue iterate` reuses the same thread and later appends a terminal
                // `RoundFinished`, while `stop iterate` records `RoundFinished(Interrupted)` when
                // the stop action is confirmed.
                return Ok(None);
            }

            if matches!(outcome, PotterRoundOutcome::Completed) {
                let stop_due_to_finite_incantatem =
                    crate::workflow::project::progress_file_has_finite_incantatem_true(
                        &self.workdir,
                        &self.progress_file_rel,
                    )
                    .context("check progress file finite_incantatem")?;

                if stop_due_to_finite_incantatem {
                    let potter_xmodel_enabled =
                        crate::workflow::project::effective_potter_xmodel_enabled(
                            &self.workdir,
                            &self.progress_file_rel,
                            self.potter_xmodel_runtime,
                        )
                        .context("read potter xmodel mode")?;

                    let should_emit_project_succeeded =
                        crate::workflow::potter_xmodel::should_emit_project_succeeded(
                            potter_xmodel_enabled,
                            self.session_model.as_deref(),
                        );
                    if should_emit_project_succeeded
                        && (!self.strict_rounds || self.round_current >= self.round_total)
                    {
                        let git_commit_end =
                            crate::workflow::project::resolve_git_commit(&self.workdir);
                        crate::workflow::rollout::append_line(
                            &self.potter_rollout_path,
                            &crate::workflow::rollout::PotterRolloutLine::ProjectSucceeded {
                                rounds: self.project_rounds_run,
                                duration_secs: self.project_started_at.elapsed().as_secs(),
                                user_prompt_file: self.user_prompt_file.clone(),
                                git_commit_start: self.git_commit_start.clone(),
                                git_commit_end: git_commit_end.clone(),
                            },
                        )
                        .context("append potter-rollout project_succeeded")?;

                        injected = Some(Event {
                            id: "".to_string(),
                            msg: EventMsg::PotterProjectSucceeded {
                                rounds: self.project_rounds_run,
                                duration: self.project_started_at.elapsed(),
                                user_prompt_file: self.user_prompt_file.clone(),
                                git_commit_start: self.git_commit_start.clone(),
                                git_commit_end,
                            },
                        });
                    }
                } else if self.round_current == self.round_total {
                    let git_commit_end =
                        crate::workflow::project::resolve_git_commit(&self.workdir);
                    injected = Some(Event {
                        id: "".to_string(),
                        msg: EventMsg::PotterProjectBudgetExhausted {
                            rounds: self.project_rounds_run,
                            duration: self.project_started_at.elapsed(),
                            user_prompt_file: self.user_prompt_file.clone(),
                            git_commit_start: self.git_commit_start.clone(),
                            git_commit_end,
                        },
                    });
                }
            }

            crate::workflow::rollout::append_line(
                &self.potter_rollout_path,
                &crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                    outcome: outcome.clone(),
                },
            )
            .context("append potter-rollout round_finished")?;
        }

        Ok(injected)
    }

    fn record_round_configured(&self, cfg: &SessionConfiguredEvent) -> anyhow::Result<()> {
        let (rollout_path, rollout_path_raw, rollout_base_dir) =
            crate::workflow::rollout::resolve_rollout_path_for_recording(
                cfg.rollout_path.clone(),
                &self.workdir,
            );
        crate::workflow::rollout::append_line(
            &self.potter_rollout_path,
            &crate::workflow::rollout::PotterRolloutLine::RoundConfigured {
                thread_id: cfg.session_id,
                rollout_path,
                service_tier: cfg.service_tier,
                rollout_path_raw,
                rollout_base_dir,
            },
        )
        .context("append potter-rollout round_configured")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use std::path::Path;

    fn write_progress_file(workdir: &Path, progress_file_rel: &Path, finite: bool) {
        write_progress_file_with_potter_xmodel(workdir, progress_file_rel, finite, false);
    }

    fn write_progress_file_with_potter_xmodel(
        workdir: &Path,
        progress_file_rel: &Path,
        finite: bool,
        potter_xmodel: bool,
    ) {
        let progress_file = workdir.join(progress_file_rel);
        std::fs::create_dir_all(progress_file.parent().expect("parent")).expect("mkdir");
        std::fs::write(
            &progress_file,
            format!(
                r#"---
finite_incantatem: {finite}
potter.xmodel: {potter_xmodel}
---

# Overall Goal
"#
            ),
        )
        .expect("write progress file");
    }

    fn session_configured_event(
        workdir: &Path,
        rollout_path: PathBuf,
        service_tier: Option<codex_protocol::protocol::ServiceTier>,
        model: &str,
    ) -> Event {
        Event {
            id: "event_1".to_string(),
            msg: EventMsg::SessionConfigured(SessionConfiguredEvent {
                session_id: codex_protocol::ThreadId::from_string(
                    "019ca423-63d9-7641-ae83-db060ad3c000",
                )
                .expect("thread id"),
                forked_from_id: None,
                model: model.to_string(),
                model_provider_id: "test".to_string(),
                service_tier,
                cwd: workdir.to_path_buf(),
                reasoning_effort: None,
                history_log_id: 0,
                history_entry_count: 0,
                initial_messages: None,
                rollout_path,
            }),
        }
    }

    #[test]
    fn observe_backend_event_records_round_configured_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let potter_rollout_path = workdir.join("potter-rollout.jsonl");

        let rollout_path = workdir.join("upstream.jsonl");
        std::fs::write(&rollout_path, "").expect("write upstream rollout");

        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: true,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md"),
            potter_xmodel_runtime: false,
            user_prompt_file: PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md"),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 1,
            round_total: 10,
            project_rounds_run: 1,
        });

        let ev = session_configured_event(workdir, PathBuf::from("upstream.jsonl"), None, "test");
        bridge.observe_backend_event(&ev).expect("observe #1");
        bridge.observe_backend_event(&ev).expect("observe #2");

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(
            lines,
            vec![
                crate::workflow::rollout::PotterRolloutLine::RoundConfigured {
                    thread_id: codex_protocol::ThreadId::from_string(
                        "019ca423-63d9-7641-ae83-db060ad3c000",
                    )
                    .expect("thread id"),
                    rollout_path: rollout_path.canonicalize().expect("canonical rollout"),
                    service_tier: None,
                    rollout_path_raw: None,
                    rollout_base_dir: None,
                }
            ]
        );
    }

    #[test]
    fn observe_backend_event_records_round_configured_service_tier() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let potter_rollout_path = workdir.join("potter-rollout.jsonl");

        let rollout_path = workdir.join("upstream.jsonl");
        std::fs::write(&rollout_path, "").expect("write upstream rollout");

        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: true,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md"),
            potter_xmodel_runtime: false,
            user_prompt_file: PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md"),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 1,
            round_total: 10,
            project_rounds_run: 1,
        });

        let ev = session_configured_event(
            workdir,
            PathBuf::from("upstream.jsonl"),
            Some(codex_protocol::protocol::ServiceTier::Fast),
            "test",
        );
        bridge.observe_backend_event(&ev).expect("observe");

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(
            lines,
            vec![
                crate::workflow::rollout::PotterRolloutLine::RoundConfigured {
                    thread_id: codex_protocol::ThreadId::from_string(
                        "019ca423-63d9-7641-ae83-db060ad3c000",
                    )
                    .expect("thread id"),
                    rollout_path: rollout_path.canonicalize().expect("canonical rollout"),
                    service_tier: Some(codex_protocol::protocol::ServiceTier::Fast),
                    rollout_path_raw: None,
                    rollout_base_dir: None,
                }
            ]
        );
    }

    #[test]
    fn observe_backend_event_injects_project_succeeded_before_round_finished_when_finite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file(workdir, &progress_file_rel, true);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel.clone(),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 3,
            round_total: 10,
            project_rounds_run: 3,
        });

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(matches!(
            injected.as_ref().map(|event| &event.msg),
            Some(EventMsg::PotterProjectSucceeded { rounds: 3, .. })
        ));

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            &lines[0],
            crate::workflow::rollout::PotterRolloutLine::ProjectSucceeded { rounds: 3, .. }
        ));
        assert!(matches!(
            &lines[1],
            crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                outcome: PotterRoundOutcome::Completed
            }
        ));
    }

    #[test]
    fn observe_backend_event_does_not_inject_project_succeeded_when_not_finite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file(workdir, &progress_file_rel, false);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel.clone(),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 3,
            round_total: 10,
            project_rounds_run: 3,
        });

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(injected.is_none());

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            &lines[0],
            crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                outcome: PotterRoundOutcome::Completed
            }
        ));
    }

    #[test]
    fn observe_backend_event_does_not_inject_project_succeeded_for_xmodel_before_gpt_5_4() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file_with_potter_xmodel(workdir, &progress_file_rel, true, true);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel.clone(),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 1,
            round_total: 10,
            project_rounds_run: 1,
        });

        bridge
            .observe_backend_event(&session_configured_event(
                workdir,
                PathBuf::from("upstream.jsonl"),
                None,
                "gpt-5.2",
            ))
            .expect("observe session configured");

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(injected.is_none());

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            &lines[0],
            crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                outcome: PotterRoundOutcome::Completed
            }
        ));
    }

    #[test]
    fn observe_backend_event_does_not_inject_project_succeeded_for_runtime_xmodel_before_gpt_5_4() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file(workdir, &progress_file_rel, true);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: true,
            user_prompt_file: progress_file_rel.clone(),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 1,
            round_total: 10,
            project_rounds_run: 1,
        });

        bridge
            .observe_backend_event(&session_configured_event(
                workdir,
                PathBuf::from("upstream.jsonl"),
                None,
                "gpt-5.2",
            ))
            .expect("observe session configured");

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(injected.is_none());

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            &lines[0],
            crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                outcome: PotterRoundOutcome::Completed
            }
        ));
    }

    #[test]
    fn observe_backend_event_injects_budget_exhausted_before_round_finished_when_last_round() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file(workdir, &progress_file_rel, false);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel.clone(),
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 10,
            round_total: 10,
            project_rounds_run: 10,
        });

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(matches!(
            injected.as_ref().map(|event| &event.msg),
            Some(EventMsg::PotterProjectBudgetExhausted { rounds: 10, .. })
        ));

        let lines = crate::workflow::rollout::read_lines(&potter_rollout_path).expect("read");
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            &lines[0],
            crate::workflow::rollout::PotterRolloutLine::RoundFinished {
                outcome: PotterRoundOutcome::Completed
            }
        ));
    }

    #[test]
    fn observe_backend_event_does_not_record_interrupted_round_finish() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");
        write_progress_file(workdir, &progress_file_rel, false);

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel,
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 3,
            round_total: 10,
            project_rounds_run: 3,
        });

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Interrupted,
            },
        };

        let injected = bridge
            .observe_backend_event(&finished)
            .expect("observe finished");
        assert!(injected.is_none());
        assert!(
            !potter_rollout_path.exists(),
            "interrupted rounds should remain unfinished in potter-rollout"
        );
    }

    #[test]
    fn observe_backend_event_errors_when_progress_file_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path();

        let potter_rollout_path = workdir.join("potter-rollout.jsonl");
        let progress_file_rel = PathBuf::from(".codexpotter/projects/2026/03/04/1/MAIN.md");

        let mut bridge = PotterRoundEventBridge::new(PotterRoundEventBridgeConfig {
            record_round_configured: false,
            strict_rounds: false,
            workdir: workdir.to_path_buf(),
            progress_file_rel: progress_file_rel.clone(),
            potter_xmodel_runtime: false,
            user_prompt_file: progress_file_rel,
            git_commit_start: "start".to_string(),
            potter_rollout_path: potter_rollout_path.clone(),
            project_started_at: Instant::now(),
            round_current: 1,
            round_total: 1,
            project_rounds_run: 1,
        });

        let finished = Event {
            id: "event_2".to_string(),
            msg: EventMsg::PotterRoundFinished {
                outcome: PotterRoundOutcome::Completed,
            },
        };

        let err = bridge
            .observe_backend_event(&finished)
            .expect_err("expected error");
        assert!(
            err.to_string().contains("finite_incantatem"),
            "error should mention finite_incantatem: {err:#}"
        );
        assert!(
            !potter_rollout_path.exists(),
            "should not write rollout on error"
        );
    }
}
