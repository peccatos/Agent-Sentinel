//! Reliability Analyzer (Phase 3): classifies anomalies in recorded
//! session event streams.
//!
//! Input: events of one session, ordered by ingestion time.
//! Output: [`SessionReport`] — a list of anomalies, each with a kind,
//! severity and confidence.
//!
//! Design goals:
//! - **Zero false positives by default**: only flag what is provably
//!   wrong (broken lifecycle FSM, duplicate event ids, out-of-order
//!   timestamps); heuristic findings carry lower confidence and are
//!   labeled as such.
//! - **Explain every finding**: each anomaly carries a human-readable
//!   message pointing at the concrete events involved.

pub mod anomaly;
pub mod detectors;

#[cfg(test)]
mod tests;

pub use anomaly::{Anomaly, AnomalyKind, Confidence, Severity, SessionReport};

use agent_sentinel_core::{AgentEvent, SessionState, SessionTracker};

/// Analyzes one session's event stream.
pub fn analyze_session(events: &[AgentEvent]) -> SessionReport {
    let mut report = SessionReport {
        event_count: events.len(),
        state: final_state(events),
        anomalies: Vec::new(),
    };

    detectors::lifecycle(events, &mut report);
    detectors::duplicates(events, &mut report);
    detectors::timestamps(events, &mut report);
    detectors::tool_pairing(events, &mut report);
    detectors::permission_pairing(events, &mut report);

    // INV-4: an open session at end-of-stream is only an anomaly if
    // we ever saw it start (a stream that never opened is already
    // flagged by the lifecycle detector as missing-start).
    let mut tracker = SessionTracker::new();
    for e in events {
        tracker.observe(e.kind);
    }
    if tracker.is_open() {
        report.anomalies.push(Anomaly {
            kind: AnomalyKind::UnclosedSession,
            severity: Severity::Warning,
            confidence: Confidence::Certain,
            message: "session started but never completed or failed".into(),
            event_index: events
                .iter()
                .rposition(|_| true)
                .map(|i| i as u64)
                .unwrap_or(0),
            details: serde_json::json!({
                "events": events.len(),
            }),
        });
    }

    report
}

fn final_state(events: &[AgentEvent]) -> SessionState {
    let mut tracker = SessionTracker::new();
    for e in events {
        tracker.observe(e.kind);
    }
    tracker.state()
}
