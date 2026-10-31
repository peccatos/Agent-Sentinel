//! Individual anomaly detectors. Each is a pure function over the
//! session's event stream; all findings are explainable — the message
//! names the events involved.

use std::collections::{HashMap, HashSet};

use agent_sentinel_core::{AgentEvent, EventKind, SessionTracker, Transition};

use crate::anomaly::{Anomaly, AnomalyKind, Confidence, Severity, SessionReport};

/// Lifecycle FSM check: missing start, restarts, events after close.
pub fn lifecycle(events: &[AgentEvent], report: &mut SessionReport) {
    let mut tracker = SessionTracker::new();
    for (i, ev) in events.iter().enumerate() {
        match tracker.observe(ev.kind) {
            Transition::Legal(_) => {}
            Transition::MissingSessionStart => report.anomalies.push(Anomaly {
                kind: AnomalyKind::MissingSessionStart,
                severity: Severity::Critical,
                confidence: Confidence::Certain,
                message: format!(
                    "event #{} ({}) before SessionStarted — opening event lost or misordered",
                    i,
                    kind_label(ev)
                ),
                event_index: i as u64,
                details: serde_json::json!({"kind": ev.kind, "name": ev.name}),
            }),
            Transition::UnexpectedRestart => report.anomalies.push(Anomaly {
                kind: AnomalyKind::UnexpectedRestart,
                severity: Severity::Warning,
                confidence: Confidence::Certain,
                message: format!(
                    "event #{}: SessionStarted after the session already closed",
                    i
                ),
                event_index: i as u64,
                details: serde_json::json!({"kind": ev.kind}),
            }),
            Transition::EventsAfterClose => report.anomalies.push(Anomaly {
                kind: AnomalyKind::EventsAfterClose,
                severity: Severity::Warning,
                confidence: Confidence::Certain,
                message: format!(
                    "event #{} ({}) arrived after the session closed",
                    i,
                    kind_label(ev)
                ),
                event_index: i as u64,
                details: serde_json::json!({"kind": ev.kind, "name": ev.name}),
            }),
        }
    }
}

/// Duplicate detection: repeated event ids (hard invariant INV-3
/// violated at the source) and repeated content (same kind+name+
/// payload twice — usually a retried hook or a duplicated spool line).
pub fn duplicates(events: &[AgentEvent], report: &mut SessionReport) {
    // Exact event-id duplication.
    let mut seen_ids: HashSet<uuid::Uuid> = HashSet::new();
    for (i, ev) in events.iter().enumerate() {
        if !seen_ids.insert(ev.event_id) {
            report.anomalies.push(Anomaly {
                kind: AnomalyKind::DuplicateEventId,
                severity: Severity::Critical,
                confidence: Confidence::Certain,
                message: format!(
                    "event #{}: event id {} already seen (duplicate delivery)",
                    i, ev.event_id
                ),
                event_index: i as u64,
                details: serde_json::json!({"event_id": ev.event_id}),
            });
        }
    }

    // Content duplication: identical kind+name+payload at different ids.
    // False-positive guards:
    // - ToolCall/ToolResult are legitimately repeatable (parallel
    //   calls to the same tool): only flag from the *third* identical
    //   occurrence on.
    // - Empty payloads are trivial and never counted.
    let mut by_content: HashMap<(EventKind, Option<String>, String), Vec<usize>> = HashMap::new();
    for (i, ev) in events.iter().enumerate() {
        let payload_str = serde_json::to_string(&ev.payload).unwrap_or_default();
        let trivial = ev.payload.as_object().is_some_and(|m| m.is_empty());
        if trivial {
            continue;
        }
        let key = (ev.kind, ev.name.clone(), payload_str);
        let occurrences = by_content.entry(key).or_default();
        occurrences.push(i);

        let toolish = matches!(ev.kind, EventKind::ToolCall | EventKind::ToolResult);
        // Non-tool kinds flag on the 2nd occurrence; tool kinds need 3.
        let threshold = if toolish { 3 } else { 2 };
        if occurrences.len() == threshold {
            report.anomalies.push(Anomaly {
                kind: AnomalyKind::DuplicateContent,
                severity: Severity::Warning,
                confidence: Confidence::Probable,
                message: format!(
                    "event #{} ({}) repeats identical content {} times since #{} — likely a retried hook or duplicated spool line",
                    i,
                    kind_label(ev),
                    occurrences.len(),
                    occurrences[0]
                ),
                event_index: i as u64,
                details: serde_json::json!({
                    "first_index": occurrences[0],
                    "count": occurrences.len(),
                    "kind": ev.kind,
                    "name": ev.name,
                }),
            });
        }
    }
}

/// Timestamp ordering: adapter-reported timestamps must be monotonic
/// within a session. Violations indicate clock skew, replayed traces
/// or misordered ingestion.
pub fn timestamps(events: &[AgentEvent], report: &mut SessionReport) {
    for i in 1..events.len() {
        if events[i].timestamp < events[i - 1].timestamp {
            report.anomalies.push(Anomaly {
                kind: AnomalyKind::OutOfOrderTimestamps,
                severity: Severity::Info,
                confidence: Confidence::Probable,
                message: format!(
                    "event #{} ({}) timestamp {} is earlier than #{} ({}) — clock skew or replay",
                    i,
                    kind_label(&events[i]),
                    events[i].timestamp.to_rfc3339(),
                    i - 1,
                    events[i - 1].timestamp.to_rfc3339()
                ),
                event_index: i as u64,
                details: serde_json::json!({
                    "ts": events[i].timestamp.to_rfc3339(),
                    "prev_ts": events[i - 1].timestamp.to_rfc3339(),
                }),
            });
        }
    }
}

/// Tool call/result pairing: every `ToolCall` should be followed by a
/// `ToolResult` for the same call, and vice versa.
///
/// Pairing key precedence:
/// 1. `payload.tool_call_id` — the provider's stable call id, present
///    in transcript-adapter events (and in hook events that carry
///    `tool_use_id`). Exact pairing, parallel-safe.
/// 2. tool `name` — fallback for adapters that only know the name
///    (hook adapter). FIFO matching, with the tool-parallelism guard
///    from ANA-2.
pub fn tool_pairing(events: &[AgentEvent], report: &mut SessionReport) {
    // Calls with a stable id: matched exactly by id.
    let mut open_by_id: HashMap<String, (usize, String)> = HashMap::new();
    // Calls without an id: matched by name, FIFO.
    let mut open_by_name: Vec<(usize, String)> = Vec::new();

    for (i, ev) in events.iter().enumerate() {
        let call_id = ev.payload.get("tool_call_id").and_then(|v| v.as_str());
        match ev.kind {
            EventKind::ToolCall => match call_id.filter(|s| !s.is_empty()) {
                Some(id) => {
                    open_by_id.insert(id.to_string(), (i, ev.name.clone().unwrap_or_default()));
                }
                None => open_by_name.push((i, ev.name.clone().unwrap_or_default())),
            },
            EventKind::ToolResult => {
                let name = ev.name.clone().unwrap_or_default();
                match call_id.filter(|s| !s.is_empty()) {
                    Some(id) => {
                        if open_by_id.remove(id).is_none() {
                            report.anomalies.push(Anomaly {
                                kind: AnomalyKind::OrphanToolResult,
                                severity: Severity::Warning,
                                confidence: Confidence::Probable,
                                message: format!(
                                    "event #{}: ToolResult for '{}' (call {}) has no preceding ToolCall — the call event was lost",
                                    i, if name.is_empty() { "?" } else { &name }, id
                                ),
                                event_index: i as u64,
                                details: serde_json::json!({"name": name, "tool_call_id": id}),
                            });
                        }
                    }
                    None => {
                        if let Some(pos) = open_by_name.iter().position(|(_, n)| *n == name) {
                            open_by_name.remove(pos);
                        } else {
                            report.anomalies.push(Anomaly {
                                kind: AnomalyKind::OrphanToolResult,
                                severity: Severity::Warning,
                                confidence: Confidence::Probable,
                                message: format!(
                                    "event #{}: ToolResult for '{}' has no preceding ToolCall — the call event was lost",
                                    i, name
                                ),
                                event_index: i as u64,
                                details: serde_json::json!({"name": name}),
                            });
                        }
                    }
                }
            }
            EventKind::SessionCompleted | EventKind::SessionFailed => {
                // Session end resolves nothing; anything still open at
                // end-of-stream is flagged below in one summary.
                break;
            }
            _ => {}
        }
    }

    for (_, (i, name)) in open_by_id {
        report.anomalies.push(Anomaly {
            kind: AnomalyKind::UnpairedToolCall,
            severity: Severity::Warning,
            confidence: Confidence::Probable,
            message: format!(
                "ToolCall '{}' at #{} has no ToolResult — the result was lost or the tool hung",
                name, i
            ),
            event_index: i as u64,
            details: serde_json::json!({"name": name}),
        });
    }
    for (i, name) in open_by_name {
        report.anomalies.push(Anomaly {
            kind: AnomalyKind::UnpairedToolCall,
            severity: Severity::Warning,
            confidence: Confidence::Probable,
            message: format!(
                "ToolCall '{}' at #{} has no ToolResult — the result was lost or the tool hung",
                name, i
            ),
            event_index: i as u64,
            details: serde_json::json!({"name": name}),
        });
    }
}

/// Permission pairing: `PermissionResolved` must follow a
/// `PermissionRequested`. A missing resolution is only flagged at
/// session end (the agent may simply not have answered yet).
pub fn permission_pairing(events: &[AgentEvent], report: &mut SessionReport) {
    let open_requests = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.kind == EventKind::PermissionRequested)
        .count();
    let resolved = events
        .iter()
        .filter(|e| e.kind == EventKind::PermissionResolved)
        .count();

    // Orphan resolutions (more resolutions than requests).
    if resolved > open_requests {
        report.anomalies.push(Anomaly {
            kind: AnomalyKind::OrphanPermissionResolution,
            severity: Severity::Info,
            confidence: Confidence::Possible,
            message: format!(
                "{resolved} PermissionResolved events but only {open_requests} PermissionRequested — request events may be lost"
            ),
            event_index: events.len().saturating_sub(1) as u64,
            details: serde_json::json!({"resolved": resolved, "requested": open_requests}),
        });
    }
}

fn kind_label(ev: &AgentEvent) -> String {
    match &ev.name {
        Some(n) => format!("{:?}({n})", ev.kind),
        None => format!("{:?}", ev.kind),
    }
}
