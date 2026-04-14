//! Single-round orchestration.
//!
//! A "round" is one upstream `codex app-server` session driven by the UI. This module wires:
//! - A backend task that runs the upstream app-server and emits `EventMsg` notifications.
//! - A forwarder task that persists boundary markers to `potter-rollout.jsonl` via
//!   [`super::round_event_bridge::PotterRoundEventBridge`] and forwards events to the UI.
//! - A UI driver ([`PotterRoundUi`]) that renders the round and sends `Op` requests.
//!
//! On non-completed UI exits (user/fatal/task failure) we abort the backend to avoid orphaned
//! processes.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Instant;

use anyhow::Context;
use codex_protocol::ThreadId;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_tui::ExitReason;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::watch;

/// Boxed future returned by [`PotterRoundUi`] implementations.
pub type UiFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + 'a>>;

/// Abstraction over the round renderer/driver.
///
/// This exists so CodexPotter can reuse the same round orchestration logic for both interactive
/// TUI rendering and non-interactive headless runners (for example `exec --json`).
pub trait PotterRoundUi {
    fn set_project_started_at(&mut self, started_at: Instant);

    fn render_round<'a>(
        &'a mut self,
        params: codex_tui::RenderRoundParams,
    ) -> UiFuture<'a, codex_tui::AppExitInfo>;
}

impl PotterRoundUi for codex_tui::CodexPotterTui {
    fn set_project_started_at(&mut self, started_at: Instant) {
        codex_tui::CodexPotterTui::set_project_started_at(self, started_at);
    }

    fn render_round<'a>(
        &'a mut self,
        params: codex_tui::RenderRoundParams,
    ) -> UiFuture<'a, codex_tui::AppExitInfo> {
        Box::pin(codex_tui::CodexPotterTui::render_round(self, params))
    }
}

#[derive(Debug, Clone)]
pub struct PotterRoundContext {
    pub codex_bin: String,
    pub developer_prompt: String,
    pub backend_launch: crate::app_server::AppServerLaunchConfig,
    pub backend_event_mode: crate::app_server::AppServerEventMode,
    pub upstream_cli_args: crate::app_server::UpstreamCodexCliArgs,
    /// Enable Potter xmodel for this process (`--xmodel`).
    ///
    /// This flag is process-local: it must not be persisted into the progress file. The effective
    /// xmodel mode for a project is `potter_xmodel_runtime || potter.xmodel` from the progress
    /// file.
    pub potter_xmodel_runtime: bool,
    pub codex_compat_home: Option<PathBuf>,
    pub thread_cwd: Option<PathBuf>,
    pub turn_prompt: String,
    pub workdir: PathBuf,
    pub progress_file_rel: PathBuf,
    pub user_prompt_file: PathBuf,
    pub git_commit_start: String,
    pub potter_rollout_path: PathBuf,
    pub strict_rounds: bool,
    pub project_started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct PotterProjectStartedInfo {
    pub user_message: Option<String>,
    pub working_dir: PathBuf,
    pub project_dir: PathBuf,
    pub user_prompt_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PotterRoundOptions {
    pub pad_before_first_cell: bool,
    pub project_started: Option<PotterProjectStartedInfo>,
    pub round_current: u32,
    pub round_total: u32,
    /// When Potter xmodel is enabled, force gpt-5.4 for this round and all subsequent rounds.
    ///
    /// This flag is intentionally transient-only: it is not persisted in the progress file and
    /// should reset on resume (unless re-triggered).
    pub potter_xmodel_force_gpt_5_4: bool,
    /// Number of rounds executed in the current iteration window, including this round.
    ///
    /// This intentionally differs from `round_current` when resuming unfinished rounds: resume may
    /// preserve the original round position (for example `7/10`) while summary metrics should
    /// reflect only the newly executed rounds since the resume continuation began.
    pub project_rounds_run: u32,
}

#[derive(Debug, Clone)]
pub struct PotterContinueRoundOptions {
    /// Whether to pad the transcript with a blank line before the first rendered cell.
    pub pad_before_first_cell: bool,
    pub round_current: u32,
    pub round_total: u32,
    /// Number of rounds executed in the current iteration window, including this round.
    ///
    /// This intentionally differs from `round_current` when resuming unfinished rounds: resume may
    /// preserve the original round position (for example `7/10`) while summary metrics should
    /// reflect only the newly executed rounds since the resume continuation began.
    pub project_rounds_run: u32,
    /// Existing Codex thread to resume for this unfinished round.
    pub resume_thread_id: codex_protocol::ThreadId,
    /// Persisted EventMsg items from the upstream rollout to replay before continuing.
    pub replay_event_msgs: Vec<EventMsg>,
}

#[derive(Debug)]
pub struct PotterRoundResult {
    pub exit_reason: ExitReason,
    pub stop_due_to_finite_incantatem: bool,
    pub thread_id: Option<ThreadId>,
    pub session_model: Option<String>,
}

pub async fn run_potter_round(
    ui: &mut impl PotterRoundUi,
    context: &PotterRoundContext,
    options: PotterRoundOptions,
) -> anyhow::Result<PotterRoundResult> {
    let PotterRoundOptions {
        pad_before_first_cell,
        project_started,
        round_current,
        round_total,
        potter_xmodel_force_gpt_5_4,
        project_rounds_run,
    } = options;

    run_potter_round_inner(
        ui,
        context,
        PotterRoundInnerOptions {
            pad_before_first_cell,
            project_started,
            round_current,
            round_total,
            potter_xmodel_force_gpt_5_4,
            project_rounds_run,
            prompt: context.turn_prompt.clone(),
            resume_thread_id: None,
            emit_round_started_event: true,
            record_round_started: true,
            record_round_configured: true,
            replay_event_msgs: Vec::new(),
        },
    )
    .await
}

/// Continue an unfinished round by resuming its thread and sending a `Continue` prompt.
///
/// This is primarily used by `codex-potter resume` when the last recorded round has no
/// `PotterRoundFinished` marker yet.
pub async fn continue_potter_round(
    ui: &mut impl PotterRoundUi,
    context: &PotterRoundContext,
    options: PotterContinueRoundOptions,
) -> anyhow::Result<PotterRoundResult> {
    let PotterContinueRoundOptions {
        pad_before_first_cell,
        round_current,
        round_total,
        project_rounds_run,
        resume_thread_id,
        replay_event_msgs,
    } = options;

    run_potter_round_inner(
        ui,
        context,
        PotterRoundInnerOptions {
            pad_before_first_cell,
            project_started: None,
            round_current,
            round_total,
            potter_xmodel_force_gpt_5_4: false,
            project_rounds_run,
            prompt: context.turn_prompt.clone(),
            resume_thread_id: Some(resume_thread_id),
            emit_round_started_event: false,
            record_round_started: false,
            record_round_configured: false,
            replay_event_msgs,
        },
    )
    .await
}

struct PotterRoundInnerOptions {
    pad_before_first_cell: bool,
    project_started: Option<PotterProjectStartedInfo>,
    round_current: u32,
    round_total: u32,
    potter_xmodel_force_gpt_5_4: bool,
    project_rounds_run: u32,
    prompt: String,
    resume_thread_id: Option<codex_protocol::ThreadId>,
    emit_round_started_event: bool,
    record_round_started: bool,
    record_round_configured: bool,
    replay_event_msgs: Vec<EventMsg>,
}

async fn run_potter_round_inner(
    ui: &mut impl PotterRoundUi,
    context: &PotterRoundContext,
    options: PotterRoundInnerOptions,
) -> anyhow::Result<PotterRoundResult> {
    let PotterRoundInnerOptions {
        pad_before_first_cell,
        project_started,
        round_current,
        round_total,
        potter_xmodel_force_gpt_5_4,
        project_rounds_run,
        prompt,
        resume_thread_id,
        emit_round_started_event,
        record_round_started,
        record_round_configured,
        replay_event_msgs,
    } = options;

    let (op_tx, op_rx) = unbounded_channel::<Op>();
    let (backend_event_tx, mut backend_event_rx) = unbounded_channel::<Event>();
    let (ui_event_tx, ui_event_rx) = unbounded_channel::<Event>();
    let (fatal_exit_tx, fatal_exit_rx) = unbounded_channel::<String>();
    let (session_model_tx, session_model_rx) = watch::channel::<Option<String>>(None);

    if let Some(project_started) = project_started {
        let _ = ui_event_tx.send(Event {
            id: "".to_string(),
            msg: EventMsg::PotterProjectStarted {
                user_message: project_started.user_message.clone(),
                working_dir: project_started.working_dir,
                project_dir: project_started.project_dir,
                user_prompt_file: project_started.user_prompt_file.clone(),
            },
        });
        crate::workflow::rollout::append_line(
            &context.potter_rollout_path,
            &crate::workflow::rollout::PotterRolloutLine::ProjectStarted {
                user_message: project_started.user_message,
                user_prompt_file: project_started.user_prompt_file,
            },
        )
        .context("append potter-rollout project_started")?;
    }

    if emit_round_started_event {
        let _ = ui_event_tx.send(Event {
            id: "".to_string(),
            msg: EventMsg::PotterRoundStarted {
                current: round_current,
                total: round_total,
            },
        });
        if record_round_started {
            crate::workflow::rollout::append_line(
                &context.potter_rollout_path,
                &crate::workflow::rollout::PotterRolloutLine::RoundStarted {
                    current: round_current,
                    total: round_total,
                },
            )
            .context("append potter-rollout round_started")?;
        }
    } else if record_round_started {
        anyhow::bail!("internal error: record_round_started without emitting PotterRoundStarted");
    }

    for msg in replay_event_msgs {
        let _ = ui_event_tx.send(Event {
            id: "".to_string(),
            msg,
        });
    }

    let forwarder = {
        let ui_event_tx = ui_event_tx.clone();
        let potter_rollout_path = context.potter_rollout_path.clone();
        let fatal_exit_tx = fatal_exit_tx.clone();
        let session_model_tx = session_model_tx.clone();
        let mut bridge = super::round_event_bridge::PotterRoundEventBridge::new(
            super::round_event_bridge::PotterRoundEventBridgeConfig {
                record_round_configured,
                strict_rounds: context.strict_rounds,
                workdir: context.workdir.clone(),
                progress_file_rel: context.progress_file_rel.clone(),
                potter_xmodel_runtime: context.potter_xmodel_runtime,
                user_prompt_file: context.user_prompt_file.clone(),
                git_commit_start: context.git_commit_start.clone(),
                potter_rollout_path: potter_rollout_path.clone(),
                project_started_at: context.project_started_at,
                round_current,
                round_total,
                project_rounds_run,
            },
        );

        tokio::spawn(async move {
            while let Some(event) = backend_event_rx.recv().await {
                if let EventMsg::SessionConfigured(cfg) = &event.msg {
                    session_model_tx.send_replace(Some(cfg.model.clone()));
                }

                let injected = match bridge.observe_backend_event(&event) {
                    Ok(injected) => injected,
                    Err(err) => {
                        let _ = fatal_exit_tx.send(format!(
                            "failed to write {}: {err:#}",
                            potter_rollout_path.display()
                        ));
                        break;
                    }
                };

                if let Some(injected) = injected
                    && ui_event_tx.send(injected).is_err()
                {
                    break;
                }

                if ui_event_tx.send(event).is_err() {
                    break;
                }
            }
        })
    };

    let potter_xmodel_enabled = if resume_thread_id.is_none() {
        crate::workflow::project::effective_potter_xmodel_enabled(
            &context.workdir,
            &context.progress_file_rel,
            context.potter_xmodel_runtime,
        )
        .context("read potter xmodel mode")?
    } else {
        false
    };

    let upstream_cli_args = prepare_upstream_cli_args_for_round(
        context.upstream_cli_args.clone(),
        round_current,
        resume_thread_id.is_some(),
        potter_xmodel_enabled,
        potter_xmodel_force_gpt_5_4,
    );

    let backend = tokio::spawn(crate::app_server::run_app_server_backend(
        crate::app_server::AppServerBackendConfig {
            codex_bin: context.codex_bin.clone(),
            developer_instructions: Some(context.developer_prompt.clone()),
            launch: context.backend_launch,
            upstream_cli_args,
            codex_home: context.codex_compat_home.clone(),
            thread_cwd: context.thread_cwd.clone(),
            resume_thread_id,
            event_mode: context.backend_event_mode,
        },
        op_rx,
        backend_event_tx,
        fatal_exit_tx,
    ));

    ui.set_project_started_at(context.project_started_at);
    let status_header_prefix = Some(format!("Round {round_current}/{round_total}"));
    let prompt_footer = codex_tui::PromptFooterContext::new(
        context.workdir.clone(),
        crate::workflow::project::resolve_git_branch(&context.workdir),
    );
    let exit_info = ui
        .render_round(codex_tui::RenderRoundParams {
            prompt,
            pad_before_first_cell,
            status_header_prefix,
            prompt_footer,
            codex_op_tx: op_tx,
            codex_event_rx: ui_event_rx,
            fatal_exit_rx,
        })
        .await?;

    let thread_id = exit_info.thread_id;
    let exit_reason = exit_info.exit_reason;
    match &exit_reason {
        ExitReason::Completed => {}
        ExitReason::Interrupted
        | ExitReason::UserRequested
        | ExitReason::TaskFailed(_)
        | ExitReason::Fatal(_) => {
            backend.abort();
            forwarder.abort();
            let _ = backend.await;
            let _ = forwarder.await;
            return Ok(PotterRoundResult {
                exit_reason,
                stop_due_to_finite_incantatem: false,
                thread_id,
                session_model: session_model_rx.borrow().clone(),
            });
        }
    }

    backend
        .await
        .context("app-server render backend panicked")??;
    let _ = forwarder.await;

    let stop_due_to_finite_incantatem =
        crate::workflow::project::progress_file_has_finite_incantatem_true(
            &context.workdir,
            &context.progress_file_rel,
        )
        .context("check progress file finite_incantatem")?;

    Ok(PotterRoundResult {
        exit_reason,
        stop_due_to_finite_incantatem,
        thread_id,
        session_model: session_model_rx.borrow().clone(),
    })
}

fn prepare_upstream_cli_args_for_round(
    mut upstream_cli_args: crate::app_server::UpstreamCodexCliArgs,
    round_current: u32,
    is_resume_unfinished_round: bool,
    potter_xmodel_enabled: bool,
    potter_xmodel_force_gpt_5_4: bool,
) -> crate::app_server::UpstreamCodexCliArgs {
    if is_resume_unfinished_round {
        // Unfinished round continuation must resume the original session configuration, so
        // explicitly ignore runtime `--model` overrides.
        upstream_cli_args.model = None;
        return upstream_cli_args;
    }

    if potter_xmodel_enabled {
        crate::workflow::potter_xmodel::apply_potter_xmodel_overrides(
            &mut upstream_cli_args,
            round_current,
            potter_xmodel_force_gpt_5_4,
        );
    }

    upstream_cli_args
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn unfinished_round_resume_ignores_model_override() {
        let args = crate::app_server::UpstreamCodexCliArgs {
            model: Some("user-model".to_string()),
            ..Default::default()
        };

        let prepared = prepare_upstream_cli_args_for_round(args, 1, true, true, false);
        assert_eq!(prepared.model, None);
        assert_eq!(prepared.config_overrides, Vec::<String>::new());
    }

    #[test]
    fn potter_xmodel_overrides_rounds_1_to_3_to_gpt_5_2_xhigh() {
        let mut args = crate::app_server::UpstreamCodexCliArgs {
            model: Some("user-model".to_string()),
            config_overrides: vec!["model_reasoning_effort=\"low\"".to_string()],
            ..Default::default()
        };

        crate::workflow::potter_xmodel::apply_potter_xmodel_overrides(&mut args, 3, false);
        assert_eq!(
            args.model.as_deref(),
            Some(crate::workflow::potter_xmodel::POTTER_XMODEL_GPT_5_2_MODEL)
        );
        assert_eq!(
            args.config_overrides.last().map(String::as_str),
            Some("model_reasoning_effort=\"xhigh\"")
        );
    }

    #[test]
    fn potter_xmodel_overrides_round_4_plus_to_gpt_5_4_xhigh() {
        let mut args = crate::app_server::UpstreamCodexCliArgs {
            model: Some("user-model".to_string()),
            config_overrides: Vec::new(),
            ..Default::default()
        };

        crate::workflow::potter_xmodel::apply_potter_xmodel_overrides(&mut args, 4, false);
        assert_eq!(
            args.model.as_deref(),
            Some(crate::workflow::potter_xmodel::POTTER_XMODEL_GPT_5_4_MODEL)
        );
        assert_eq!(
            args.config_overrides,
            vec!["model_reasoning_effort=\"xhigh\"".to_string()]
        );
    }

    #[test]
    fn potter_xmodel_force_gpt_5_4_overrides_round_2_to_gpt_5_4_xhigh() {
        let mut args = crate::app_server::UpstreamCodexCliArgs {
            model: Some("user-model".to_string()),
            config_overrides: Vec::new(),
            ..Default::default()
        };

        crate::workflow::potter_xmodel::apply_potter_xmodel_overrides(&mut args, 2, true);
        assert_eq!(
            args.model.as_deref(),
            Some(crate::workflow::potter_xmodel::POTTER_XMODEL_GPT_5_4_MODEL)
        );
        assert_eq!(
            args.config_overrides,
            vec!["model_reasoning_effort=\"xhigh\"".to_string()]
        );
    }
}
