//! CodexPotter workflow modules.
//!
//! This module tree owns the "project lifecycle" orchestration:
//!
//! - **Project init**: create a new `.codexpotter/projects/.../MAIN.md` progress file from the
//!   prompt templates, record git metadata, and derive the developer prompt.
//! - **Round orchestration**: run one or more rounds by driving the backend app-server and a UI
//!   renderer, and persist `potter-rollout.jsonl` for replay.
//! - **Resume**: read the persisted rollout/progress file, reconstruct the latest known state, and
//!   replay events into a UI (optionally continuing unfinished work).
//!
//! Key artifacts:
//! - Progress file (`MAIN.md`) with YAML front matter (e.g. `status`, `finite_incantatem`).
//! - `potter-rollout.jsonl`, a CodexPotter-specific event log used for resume and auditing.
//!
//! Boundaries:
//! - Rendering is delegated to the `codex-tui` crate; workflow code should not contain TUI layout
//!   logic.
//! - Backend interactions are handled by `crate::app_server`; workflow consumes the resulting
//!   `EventMsg` stream and persists/replays it.

pub mod potter_xmodel;
pub mod project;
pub mod project_render_loop;
pub mod project_runner;
pub mod prompt_queue;
pub mod replay_session_config;
pub mod resume;
pub mod resume_picker_index;
pub mod rollout;
pub mod runtime_log;
pub mod rollout_resume_index;
mod round_event_bridge;
pub mod round_runner;
