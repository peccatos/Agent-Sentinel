//! Session timeline + full analyzer diagnostics (Phase 3).

use std::path::Path;

use agent_sentinel_analyzer::{analyze_session, Anomaly, Severity, SessionReport};
use agent_sentinel_core::{AgentEvent, EventKind};
use agent_sentinel_store::EventStore;
use uuid::Uuid;

pub async fn print_report(db: &Path, session: Uuid) -> anyhow::Result<()> {
    let mut store = EventStore::open(db).await?;
    let events = store.session_events(session).await?;
    if events.is_empty() {
        println!("session {session}: no events found");
        return Ok(());
    }
    store.close().await?;

    let report = analyze_session(&events);
    print_analysis(&events, &report);
    Ok(())
}

/// Audit every recorded session; returns sessions that had findings.
pub async fn print_audit(db: &Path, min_severity: Severity) -> anyhow::Result<usize> {
    let mut store = EventStore::open(db).await?;
    let sessions = store.sessions().await?;
    if sessions.is_empty() {
        println!("no sessions recorded yet — run `sentinel demo` or `import-spool`");
        return Ok(0);
    }
    let mut flagged = 0;
    for s in &sessions {
        let events = store.session_events(*s).await?;
        let report = analyze_session(&events);
        let hits = report.count_at_least(min_severity);
        if hits > 0 {
            flagged += 1;
            println!("{s}  {} events  {:?}", events.len(), report.state);
            for a in &report.anomalies {
                if a.severity >= min_severity {
                    println!("  [{:?}/{:?}] {}", a.severity, a.confidence, a.message);
                }
            }
        }
    }
    println!(
        "\n{} of {} sessions flagged (severity >= {:?})",
        flagged,
        sessions.len(),
        min_severity
    );
    store.close().await?;
    Ok(flagged)
}

pub fn print_analysis(events: &[AgentEvent], report: &SessionReport) {
    println!("SESSION: {}", events[0].session_id);
    println!("PROVIDER: {}", provider_label(events[0].provider));
    println!("STATUS: {:?}", report.state);
    println!("EVENTS: {}", report.event_count);
    println!();
    println!("Timeline:");

    for e in events {
        println!("  [{}] {}", fmt_time(e), kind_label(e));
    }

    println!();
    println!("Diagnostics:");
    let tool_calls = events
        .iter()
        .filter(|e| e.kind == EventKind::ToolCall)
        .count();
    let subagents = events
        .iter()
        .filter(|e| e.kind == EventKind::SubagentStarted)
        .count();
    let permissions = events
        .iter()
        .filter(|e| e.kind == EventKind::PermissionResolved)
        .count();
    println!("  - {tool_calls} tool calls");
    println!("  - {permissions} permission decisions");
    println!("  - {subagents} subagents");

    if report.is_healthy() {
        println!("  - lifecycle: OK (no anomalies)");
    } else {
        println!(
            "  - anomalies ({}):",
            report.anomalies.len()
        );
        for a in &report.anomalies {
            println!("    ! {}", format_anomaly(a));
        }
    }
}

fn format_anomaly(a: &Anomaly) -> String {
    format!(
        "[{:?}/{:?}] {}",
        a.severity, a.confidence, a.message
    )
}

pub fn lifecycle_status(events: &[AgentEvent]) -> &'static str {
    match analyze_session(events).state {
        agent_sentinel_core::SessionState::NotStarted => "NotStarted",
        agent_sentinel_core::SessionState::Running => "Running (unclosed)",
        agent_sentinel_core::SessionState::Completed => "Completed",
        agent_sentinel_core::SessionState::Failed => "Failed",
    }
}

fn kind_label(e: &AgentEvent) -> String {
    match (&e.name, e.kind) {
        (Some(n), EventKind::ToolCall) => format!("ToolCall: {n}"),
        (Some(n), EventKind::ToolResult) => format!("ToolResult: {n}"),
        (Some(n), EventKind::HookFired) => format!("HookFired: {n}"),
        _ => format!("{:?}", e.kind),
    }
}

fn fmt_time(e: &AgentEvent) -> String {
    e.timestamp.format("%H:%M:%S%.3f").to_string()
}

fn provider_label(p: agent_sentinel_core::AgentProvider) -> &'static str {
    match p {
        agent_sentinel_core::AgentProvider::ClaudeCode => "Claude Code",
        agent_sentinel_core::AgentProvider::Codex => "Codex",
        agent_sentinel_core::AgentProvider::TestHarness => "TestHarness",
        agent_sentinel_core::AgentProvider::Unknown => "Unknown",
    }
}
