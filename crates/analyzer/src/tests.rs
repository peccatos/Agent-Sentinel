//! Analyzer tests: each anomaly kind is detected on a crafted broken
//! stream, and — just as important — healthy streams produce zero
//! findings.

use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};
use uuid::Uuid;

use crate::{analyze_session, AnomalyKind};

fn ev(kind: EventKind, name: Option<&str>) -> AgentEvent {
    let mut e = AgentEvent::new(
        Uuid::new_v4(),
        AgentProvider::ClaudeCode,
        kind,
        serde_json::json!({"n": std::time::SystemTime::now().elapsed().unwrap().subsec_nanos() as u64}),
    );
    e.name = name.map(str::to_string);
    e
}

fn named(kind: EventKind, name: &str) -> AgentEvent {
    ev(kind, Some(name))
}

/// A realistic healthy session: start → prompt → tools (paired) →
/// permission (paired) → subagent → stop.
fn healthy_session() -> Vec<AgentEvent> {
    vec![
        ev(EventKind::SessionStarted, None),
        ev(EventKind::UserPrompt, None),
        named(EventKind::ToolCall, "Read"),
        named(EventKind::ToolResult, "Read"),
        ev(EventKind::PermissionRequested, None),
        ev(EventKind::PermissionResolved, None),
        named(EventKind::ToolCall, "Bash"),
        named(EventKind::ToolResult, "Bash"),
        ev(EventKind::SubagentStarted, None),
        ev(EventKind::SubagentCompleted, None),
        ev(EventKind::SessionCompleted, None),
    ]
}

#[test]
fn healthy_session_is_clean() {
    // The zero-false-positive contract: a fully healthy stream must
    // produce an empty anomaly list.
    let report = analyze_session(&healthy_session());
    assert!(
        report.is_healthy(),
        "expected no anomalies, got: {:?}",
        report.anomalies
    );
    assert_eq!(report.event_count, 11);
}

#[test]
fn healthy_sessions_always_clean_parametrized() {
    // Variations of healthy sessions (different tools, counts) — none
    // may produce a false positive.
    for tools in 0..5 {
        let mut events = vec![
            ev(EventKind::SessionStarted, None),
            ev(EventKind::UserPrompt, None),
        ];
        for t in 0..tools {
            events.push(named(EventKind::ToolCall, &format!("Tool{t}")));
            events.push(named(EventKind::ToolResult, &format!("Tool{t}")));
        }
        events.push(ev(EventKind::SessionCompleted, None));
        let report = analyze_session(&events);
        assert!(
            report.is_healthy(),
            "false positive with {tools} tools: {:?}",
            report.anomalies
        );
    }
}

#[test]
fn detects_missing_session_start() {
    let events = vec![ev(EventKind::UserPrompt, None)];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::MissingSessionStart));
}

#[test]
fn detects_unclosed_session() {
    let events = vec![
        ev(EventKind::SessionStarted, None),
        ev(EventKind::UserPrompt, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::UnclosedSession));
}

#[test]
fn detects_events_after_close() {
    let events = vec![
        ev(EventKind::SessionStarted, None),
        ev(EventKind::SessionCompleted, None),
        ev(EventKind::UserPrompt, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::EventsAfterClose));
}

#[test]
fn detects_duplicate_event_id() {
    let dup = ev(EventKind::ToolCall, Some("Bash"));
    let events = vec![
        ev(EventKind::SessionStarted, None),
        dup.clone(),
        dup,
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::DuplicateEventId));
}

#[test]
fn detects_duplicate_content() {
    // Same kind + name + payload at different ids. Non-tool kinds
    // (UserPrompt) flag on the second occurrence.
    let mut a = ev(EventKind::UserPrompt, None);
    a.payload = serde_json::json!({"prompt": "run the tests again"});
    let mut b = ev(EventKind::UserPrompt, None);
    b.payload = serde_json::json!({"prompt": "run the tests again"});
    assert_ne!(a.event_id, b.event_id);
    let events = vec![
        ev(EventKind::SessionStarted, None),
        a,
        b,
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|x| x.kind == AnomalyKind::DuplicateContent));
}

#[test]
fn three_identical_tool_results_are_flagged() {
    // Tool kinds tolerate 2 identical events (parallel calls), but
    // the 3rd identical one is flagged.
    let mk = || {
        let mut e = named(EventKind::ToolResult, "Read");
        e.payload = serde_json::json!({"file": "lib.rs", "ok": true});
        e
    };
    let events = vec![
        ev(EventKind::SessionStarted, None),
        named(EventKind::ToolCall, "Read"),
        named(EventKind::ToolCall, "Read"),
        named(EventKind::ToolCall, "Read"),
        mk(),
        mk(),
        mk(),
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|x| x.kind == AnomalyKind::DuplicateContent));
}

#[test]
fn identical_empty_payloads_are_not_flagged() {
    // Two SessionStarted events with empty payloads: content is
    // trivial, no flag (avoids a false positive class).
    let mut a = ev(EventKind::UserPrompt, None);
    a.payload = serde_json::json!({});
    let mut b = ev(EventKind::UserPrompt, None);
    b.payload = serde_json::json!({});
    let events = vec![
        ev(EventKind::SessionStarted, None),
        a,
        b,
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(!report
        .anomalies
        .iter()
        .any(|x| x.kind == AnomalyKind::DuplicateContent));
}

#[test]
fn detects_unpaired_tool_call() {
    let events = vec![
        ev(EventKind::SessionStarted, None),
        named(EventKind::ToolCall, "Bash"),
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::UnpairedToolCall));
}

#[test]
fn detects_orphan_tool_result() {
    let events = vec![
        ev(EventKind::SessionStarted, None),
        named(EventKind::ToolResult, "Bash"),
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::OrphanToolResult));
}

#[test]
fn interleaved_tool_calls_pair_correctly() {
    // Read+Read is legal: two concurrent reads, results in any order.
    let events = vec![
        ev(EventKind::SessionStarted, None),
        named(EventKind::ToolCall, "Read"),
        named(EventKind::ToolCall, "Read"),
        named(EventKind::ToolResult, "Read"),
        named(EventKind::ToolResult, "Read"),
        ev(EventKind::SessionCompleted, None),
    ];
    let report = analyze_session(&events);
    assert!(
        report.is_healthy(),
        "interleaved legal calls must not be flagged: {:?}",
        report.anomalies
    );
}

#[test]
fn detects_out_of_order_timestamps() {
    let mut events = vec![
        ev(EventKind::SessionStarted, None),
        ev(EventKind::UserPrompt, None),
    ];
    // Force the second event's timestamp into the past.
    events[1].timestamp = events[0].timestamp - chrono::Duration::seconds(10);
    let events = {
        let mut v = events;
        v.push(ev(EventKind::SessionCompleted, None));
        v
    };
    let report = analyze_session(&events);
    assert!(report
        .anomalies
        .iter()
        .any(|a| a.kind == AnomalyKind::OutOfOrderTimestamps));
}

#[test]
fn report_counts_by_severity() {
    let events = vec![ev(EventKind::UserPrompt, None)];
    let report = analyze_session(&events);
    assert!(report.count_at_least(crate::Severity::Critical) >= 1);
}
