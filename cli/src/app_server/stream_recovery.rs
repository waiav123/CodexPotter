//! CodexPotter-specific turn recovery policy.
//!
//! CodexPotter runs multi-round workflows. When Codex emits certain transient network/streaming
//! errors mid-turn (e.g. response stream disconnected), or certain known auth-refresh noise, we
//! want to keep the current round alive and let the agent recover by issuing a follow-up
//! `continue` prompt.

use std::time::Duration;

use codex_protocol::potter_stream_recovery as protocol_recovery;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::TurnCompleteEvent;

const UNLIMITED_RETRY_SENTINEL: u32 = 0;
const MAX_BACKOFF_SECS: u64 = 300;

/// A plan to retry a failed turn by sending a follow-up `continue` prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinueRetryPlan {
    /// 1-based attempt number within the current continuous-error streak.
    pub attempt: u32,
    /// Maximum number of attempts allowed before giving up.
    ///
    /// `0` means unlimited retries.
    pub max_attempts: u32,
    /// Backoff duration to wait before sending `continue`.
    pub backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContinueRetryDecision {
    Retry(ContinueRetryPlan),
}

/// Tracks retry/backoff state for "continue after stream disconnect" behavior.
#[derive(Debug, Default)]
pub struct PotterStreamRecovery {
    continue_sends_since_activity: u32,
}

impl PotterStreamRecovery {
    pub fn new() -> Self {
        Self {
            continue_sends_since_activity: 0,
        }
    }

    /// Returns `true` when CodexPotter is still in a continuous-error retry streak.
    pub fn is_in_retry_streak(&self) -> bool {
        self.continue_sends_since_activity > 0
    }

    /// Returns `true` when `turn_complete` should be suppressed from the UI.
    ///
    /// While in a retry streak, Codex can emit an empty `TurnComplete` that corresponds to a
    /// transient stream/network failure. CodexPotter keeps the round alive by retrying with
    /// follow-up `continue` turns, so the UI must not interpret the empty `TurnComplete` as the
    /// end of the round.
    pub fn should_suppress_turn_complete(&self, turn_complete: &TurnCompleteEvent) -> bool {
        self.is_in_retry_streak()
            && turn_complete
                .last_agent_message
                .as_deref()
                .is_none_or(|message| message.is_empty())
    }

    /// Observe a non-error event and reset backoff state when we see activity.
    pub fn observe_event(&mut self, msg: &EventMsg) {
        if protocol_recovery::is_activity_event(msg) {
            self.continue_sends_since_activity = 0;
        }
    }

    /// If `error` is retryable, returns a decision describing how to retry.
    pub fn plan_retry(&mut self, error: &ErrorEvent) -> Option<ContinueRetryDecision> {
        if !protocol_recovery::is_retryable_stream_error(error) {
            return None;
        }

        let attempt = self.continue_sends_since_activity + 1;
        let backoff_secs = if self.continue_sends_since_activity == 0 {
            0
        } else {
            let shift = self.continue_sends_since_activity.saturating_sub(1);
            (1u64 << shift.min(62)).min(MAX_BACKOFF_SECS)
        };
        let backoff = Duration::from_secs(backoff_secs);
        self.continue_sends_since_activity += 1;

        Some(ContinueRetryDecision::Retry(ContinueRetryPlan {
            attempt,
            max_attempts: UNLIMITED_RETRY_SENTINEL,
            backoff,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::AgentMessageDeltaEvent;
    use codex_protocol::protocol::CodexErrorInfo;
    use pretty_assertions::assert_eq;

    fn retryable_error_event() -> ErrorEvent {
        ErrorEvent {
            message: "stream disconnected before completion: error sending request for url (...)"
                .to_string(),
            codex_error_info: Some(CodexErrorInfo::ResponseStreamDisconnected {
                http_status_code: None,
            }),
        }
    }

    fn retryable_sign_in_again_error_event() -> ErrorEvent {
        ErrorEvent {
            message: "unexpected status 401: Your access token could not be refreshed because you have since logged out or signed in to another account. Please sign in again.".to_string(),
            codex_error_info: Some(CodexErrorInfo::Unauthorized),
        }
    }

    #[test]
    fn plan_retry_sends_immediately_then_backs_off_exponentially() {
        let mut state = PotterStreamRecovery::new();
        let err = retryable_error_event();

        let mut plans = Vec::new();
        for _ in 0..3 {
            let Some(ContinueRetryDecision::Retry(plan)) = state.plan_retry(&err) else {
                panic!("expected retry plan");
            };
            plans.push(plan);
        }

        assert_eq!(
            plans,
            vec![
                ContinueRetryPlan {
                    attempt: 1,
                    max_attempts: 0,
                    backoff: Duration::from_secs(0),
                },
                ContinueRetryPlan {
                    attempt: 2,
                    max_attempts: 0,
                    backoff: Duration::from_secs(1),
                },
                ContinueRetryPlan {
                    attempt: 3,
                    max_attempts: 0,
                    backoff: Duration::from_secs(2),
                },
            ]
        );
    }

    #[test]
    fn observe_event_resets_retry_budget_on_activity() {
        let mut state = PotterStreamRecovery::new();
        let err = retryable_error_event();

        let Some(ContinueRetryDecision::Retry(first)) = state.plan_retry(&err) else {
            panic!("expected retry plan");
        };
        assert_eq!(first.attempt, 1);

        let Some(ContinueRetryDecision::Retry(second)) = state.plan_retry(&err) else {
            panic!("expected retry plan");
        };
        assert_eq!(second.attempt, 2);

        state.observe_event(&EventMsg::AgentMessageDelta(AgentMessageDeltaEvent {
            delta: "hello".to_string(),
        }));

        let Some(ContinueRetryDecision::Retry(reset)) = state.plan_retry(&err) else {
            panic!("expected retry plan");
        };
        assert_eq!(reset.attempt, 1);
    }

    #[test]
    fn plan_retry_accepts_sign_in_again_message() {
        let mut state = PotterStreamRecovery::new();

        let Some(ContinueRetryDecision::Retry(plan)) =
            state.plan_retry(&retryable_sign_in_again_error_event())
        else {
            panic!("expected retry plan");
        };

        assert_eq!(
            plan,
            ContinueRetryPlan {
                attempt: 1,
                max_attempts: 0,
                backoff: Duration::from_secs(0),
            }
        );
    }

    #[test]
    fn plan_retry_never_gives_up_and_caps_backoff() {
        let mut state = PotterStreamRecovery::new();
        let err = retryable_error_event();

        let mut last_plan = None;
        for _ in 0..20 {
            let Some(ContinueRetryDecision::Retry(plan)) = state.plan_retry(&err) else {
                panic!("expected retry plan");
            };
            last_plan = Some(plan);
        }

        let last_plan = last_plan.expect("last plan");
        assert_eq!(last_plan.attempt, 20);
        assert_eq!(last_plan.max_attempts, 0);
        assert_eq!(last_plan.backoff, Duration::from_secs(MAX_BACKOFF_SECS));
    }

    #[test]
    fn should_suppress_turn_complete_during_retry_streak() {
        let mut state = PotterStreamRecovery::new();
        let err = retryable_error_event();

        let Some(ContinueRetryDecision::Retry(_)) = state.plan_retry(&err) else {
            panic!("expected retry plan");
        };

        assert!(state.should_suppress_turn_complete(&TurnCompleteEvent {
            turn_id: "turn-1".to_string(),
            last_agent_message: None,
        }));

        assert!(!state.should_suppress_turn_complete(&TurnCompleteEvent {
            turn_id: "turn-1".to_string(),
            last_agent_message: Some("done".to_string()),
        }));
    }
}
