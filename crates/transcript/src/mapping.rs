//! Mapping of parsed transcript lines onto the event contract
//! (session correlation reuses the Claude adapter's UUIDv5 namespace,
//! so transcript events and hook events for the same Claude session
//! land in the same Sentinel session).

use agent_sentinel_claude::claude_session_uuid;
use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::entry::ParsedLine;

/// Outcome of mapping one line.
#[derive(Debug, PartialEq)]
pub enum TranscriptEvent {
    /// One or more events (parallel tool calls yield several).
    Events(Vec<AgentEvent>),
    /// The line carries no signal we map.
    ///
    /// Two flavors of skip, both deliberate:
    /// - *structural bookkeeping* (see [`BOOKKEEPING_TYPES`]) —
    ///   Claude Code's own UI/runtime state, not agent behavior;
    /// - empty/carry lines.
    ///
    /// This is not an INV-1 violation: INV-1 governs *ingestion*
    /// (nothing that is submitted is ever dropped). The adapter's
    /// contract is to emit *agent behavior*; runtime bookkeeping is
    /// not that, and emitting it would only produce analyzer noise.
    /// Unknown agent-level entries are still preserved (INV-7).
    Skip,
}

/// Transcript entry types that are Claude Code internal bookkeeping —
/// never agent behavior. Skipped by the mapper (see [`TranscriptEvent::Skip`]).
///
/// Everything not on this list that we cannot map is still emitted as
/// `EventKind::Unknown` (INV-7), so a *new* meaningful entry type in a
/// future Claude Code version shows up in the data instead of being
/// silently swallowed.
pub const BOOKKEEPING_TYPES: &[&str] = &[
    // UI/protocol state latch entries
    "mode",
    "permission-mode",
    "atis-latch",
    "cost-state",
    "last-prompt",
    "ai-title",
    "file-history-snapshot",
    "prompt_snapshot",
    "remote_session_change",
    // Deltas delivered as top-level types in some versions
    "agent_listing_delta",
    "deferred_tools_delta",
    "skill_listing",
    "date",
    "model",
    // API-transport bookkeeping (request envelopes, queue state) —
    // the *behavior* is already captured via tool_use/tool_result;
    // these are the wire format around it.
    "api-request",
    "api-request-shape",
    "api-request-blob",
    "api-response",
    "queue-operation",
    "ephemeral",
];

/// Maps one parsed line onto event(s).
pub fn map_line(session_uuid: Uuid, line: &ParsedLine) -> TranscriptEvent {
    match line {
        ParsedLine::SessionStart { timestamp, meta, .. } => {
            let mut ev = base(session_uuid, EventKind::SessionStarted, json_meta(meta));
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::UserPrompt { prompt_id, timestamp, prompt_text, .. } => {
            let payload = serde_json::json!({
                "prompt_id": prompt_id,
                "prompt": prompt_text,
                "source": "transcript",
            });
            let mut ev = base(session_uuid, EventKind::UserPrompt, payload);
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::ToolUse { tool_call_id, tool_name, timestamp, input, is_sidechain, .. } => {
            let payload = serde_json::json!({
                "tool_call_id": tool_call_id,
                "input": input,
                "is_sidechain": is_sidechain,
            });
            let mut ev = base(session_uuid, EventKind::ToolCall, payload);
            ev.name = Some(tool_name.clone());
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::ToolResult { tool_call_id, timestamp, is_error, content, is_sidechain, .. } => {
            let payload = serde_json::json!({
                "tool_call_id": tool_call_id,
                "is_error": is_error,
                "content": content,
                "is_sidechain": is_sidechain,
            });
            let mut ev = base(session_uuid, EventKind::ToolResult, payload);
            // Tool name is not present on result blocks; the analyzer
            // pairs by call id, not by name.
            ev.name = None;
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::ApiError { timestamp, error, is_terminal, .. } => {
            // An api_error NEVER closes a transcript session: real
            // data shows sessions continuing normally after even a
            // fully-exhausted retry chain (10/10). Closure is always
            // synthetic ([`synthetic_completion`]); the error is kept
            // as a diagnostic Unknown event (INV-7).
            let payload = serde_json::json!({
                "error": error,
                "source": "api",
                "terminal": is_terminal,
            });
            let mut ev = base(session_uuid, EventKind::Unknown, payload);
            ev.name = Some("api_error".to_string());
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::SidechainMark { timestamp, is_assistant, .. } => {
            // Sidechain assistant activity marks subagent turns. We map
            // the *first* sidechain entry of a run to SubagentStarted;
            // run detection is the analyzer's job (it owns the FSM).
            // Here we emit a low-signal event the analyzer can use.
            let kind = if *is_assistant {
                EventKind::SubagentStarted
            } else {
                EventKind::SubagentCompleted
            };
            let payload = serde_json::json!({ "source": "transcript_sidechain" });
            let mut ev = base(session_uuid, kind, payload);
            apply_ts(&mut ev, timestamp);
            TranscriptEvent::Events(vec![ev])
        }
        ParsedLine::Unknown { session_id, entry_type, raw } => {
            // Structural bookkeeping: skipped, not mapped (see
            // `TranscriptEvent::Skip` docs for why this is not INV-1).
            //
            // Non-human user entries: `isMeta` internal context
            // (command caveats, system reminders, skill listings) and
            // slash-command/`local-command` stdout. These are Claude
            // Code plumbing, not agent behavior.
            if entry_type.as_deref() == Some("user") {
                let is_meta = raw.get("isMeta").and_then(|m| m.as_bool()).unwrap_or(false);
                let text = raw
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_str())
                    .unwrap_or_default();
                let looks_local_command = text.starts_with("<local-command")
                    || text.starts_with("<command-name>")
                    || text.starts_with("<command-message>")
                    || text.starts_with("<command-args");
                let is_command_output = text.starts_with("<local-command-stdout>");
                if is_meta || looks_local_command || is_command_output {
                    return TranscriptEvent::Skip;
                }
            }
            // Top-level types…
            if entry_type
                .as_deref()
                .is_some_and(|t| BOOKKEEPING_TYPES.contains(&t))
            {
                return TranscriptEvent::Skip;
            }
            // …and attachment subtypes (environment/date/model/… are
            // attachments whose subtype nests one level down).
            let is_bookkeeping_attachment = entry_type.as_deref() == Some("attachment")
                && raw
                    .get("attachment")
                    .and_then(|a| a.get("type"))
                    .and_then(|t| t.as_str())
                    .is_some_and(|t| BOOKKEEPING_TYPES.contains(&t) || t == "environment");
            if is_bookkeeping_attachment {
                return TranscriptEvent::Skip;
            }
            // INV-7: unknown kinds preserved verbatim.
            let payload = serde_json::json!({
                "entry_type": entry_type,
                "raw_session_id": session_id,
                "raw": raw,
            });
            let mut ev = base(session_uuid, EventKind::Unknown, payload);
            if let Some(ts) = raw.get("timestamp").and_then(|t| t.as_str()) {
                apply_ts(&mut ev, &Some(ts.to_string()));
            }
            TranscriptEvent::Events(vec![ev])
        }
    }
}

/// Builds a synthetic `SessionStarted` for transcripts whose file
/// does not begin with a `session_context` attachment (activity
/// before the opening entry is real: e.g. prompts that failed with
/// API errors before the session context was attached).
pub fn synthetic_start(session_uuid: Uuid, first_event_ts: Option<DateTime<Utc>>) -> AgentEvent {
    let payload = serde_json::json!({
        "synthetic": true,
        "reason": "transcript_activity_before_context",
        "first_event_ts": first_event_ts.map(|t| t.to_rfc3339()),
    });
    let mut ev = base(session_uuid, EventKind::SessionStarted, payload);
    if let Some(t) = first_event_ts {
        ev.timestamp = t;
    }
    ev
}

/// Builds a synthetic `SessionCompleted` for a transcript whose file
/// has gone quiet (no explicit close entry exists in the format).
/// The timestamp is "now" — the format gives us nothing better, and
/// INV-5 keeps ordering decisions off adapter timestamps anyway.
pub fn synthetic_completion(session_uuid: Uuid, last_event_ts: Option<DateTime<Utc>>) -> AgentEvent {
    let payload = serde_json::json!({
        "synthetic": true,
        "reason": "transcript_quiescent",
        "last_event_ts": last_event_ts.map(|t| t.to_rfc3339()),
    });
    let mut ev = base(session_uuid, EventKind::SessionCompleted, payload);
    if let Some(t) = last_event_ts {
        ev.timestamp = t;
    }
    ev
}

fn base(session_uuid: Uuid, kind: EventKind, payload: serde_json::Value) -> AgentEvent {
    AgentEvent::new(session_uuid, AgentProvider::ClaudeCode, kind, payload)
}

fn json_meta(meta: &serde_json::Value) -> serde_json::Value {
    // Keep only the identifying subset of the session head: cwd,
    // version, model, gitBranch, entrypoint. The full attachment blob
    // (system reminders, git status) is noise for our purposes.
    let pick = serde_json::json!({
        "cwd": meta.get("cwd"),
        "version": meta.get("version"),
        "gitBranch": meta.get("gitBranch"),
        "entrypoint": meta.get("entrypoint"),
        "model": meta.get("model"),
    });
    pick
}

fn apply_ts(ev: &mut AgentEvent, ts: &Option<String>) {
    if let Some(s) = ts {
        if let Ok(t) = DateTime::parse_from_rfc3339(s) {
            ev.timestamp = t.with_timezone(&Utc);
        }
    }
}

/// Correlates a Claude session string to the Sentinel UUID
/// (re-exported for command code; same namespace as the hook adapter,
/// so both sources converge on one session).
pub fn session_uuid_for(session_id: &str) -> Uuid {
    claude_session_uuid(session_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::parse_line_multi;

    #[test]
    fn session_start_maps_with_metadata() {
        let line = r#"{"type":"attachment","attachment":{"type":"session_context"},"cwd":"C:\\repo","version":"2.1.302","timestamp":"2026-10-30T15:32:30.622Z","session_id":"s"}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        let uuid = session_uuid_for("s");
        match map_line(uuid, &parsed) {
            TranscriptEvent::Events(evts) => {
                assert_eq!(evts.len(), 1);
                assert_eq!(evts[0].kind, EventKind::SessionStarted);
                assert_eq!(evts[0].session_id, uuid);
                assert_eq!(evts[0].payload["cwd"], "C:\\repo");
                // Transcript timestamp honored.
                assert_eq!(
                    evts[0].timestamp.to_rfc3339(),
                    "2026-10-30T15:32:30.622+00:00"
                );
            }
            _ => panic!("expected events"),
        }
    }

    #[test]
    fn tool_call_carries_name_and_id() {
        let line = r#"{"type":"assistant","timestamp":"2026-10-30T15:33:00.000Z","session_id":"s","message":{"content":[{"type":"tool_use","id":"call_7","name":"Bash","input":{"command":"cargo test"}}]}}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        match map_line(session_uuid_for("s"), &parsed) {
            TranscriptEvent::Events(evts) => {
                assert_eq!(evts[0].kind, EventKind::ToolCall);
                assert_eq!(evts[0].name.as_deref(), Some("Bash"));
                assert_eq!(evts[0].payload["tool_call_id"], "call_7");
                assert_eq!(evts[0].payload["input"]["command"], "cargo test");
            }
            _ => panic!("expected events"),
        }
    }

    #[test]
    fn unknown_line_kept_verbatim() {
        // A *non*-bookkeeping unknown type: kept verbatim (INV-7).
        let line = r#"{"type":"brand-new-future-type","sessionId":"s","timestamp":"2026-10-30T15:32:31.000Z"}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        match map_line(session_uuid_for("s"), &parsed) {
            TranscriptEvent::Events(evts) => {
                assert_eq!(evts[0].kind, EventKind::Unknown);
                assert_eq!(evts[0].payload["entry_type"], "brand-new-future-type");
                // INV-7: raw preserved.
                assert!(evts[0].payload["raw"].is_object());
            }
            _ => panic!("expected events"),
        }
    }

    #[test]
    fn bookkeeping_types_are_skipped() {
        for t in ["mode", "atis-latch", "cost-state", "ai-title", "last-prompt"] {
            let line = format!(r#"{{"type":"{t}","sessionId":"s"}}"#);
            let parsed = parse_line_multi(&line, "s").pop().expect("one entry");
            match map_line(session_uuid_for("s"), &parsed) {
                TranscriptEvent::Skip => {}
                other => panic!("{t} should be skipped, got {other:?}"),
            }
        }
    }

    #[test]
    fn environment_attachment_is_skipped() {
        // Double-start guard: only session_context opens.
        let line = r#"{"type":"attachment","attachment":{"type":"environment"},"sessionId":"s"}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        match map_line(session_uuid_for("s"), &parsed) {
            TranscriptEvent::Skip => {}
            other => panic!("environment should be skipped, got {other:?}"),
        }
    }

    #[test]
    fn same_session_string_converges_with_hook_adapter() {
        // Transcript and hook adapters must agree on the session UUID.
        let a = session_uuid_for("abc");
        let b = claude_session_uuid("abc");
        assert_eq!(a, b);
    }

    #[test]
    fn synthetic_completion_is_session_completed() {
        let uuid = session_uuid_for("s");
        let ev = synthetic_completion(uuid, None);
        assert_eq!(ev.kind, EventKind::SessionCompleted);
        assert_eq!(ev.payload["synthetic"], true);
        assert_eq!(ev.provider, AgentProvider::ClaudeCode);
    }

    #[test]
    fn api_error_never_closes_the_session() {
        // Even a terminal api_error must not close a transcript
        // session: real data shows sessions continuing after an
        // exhausted retry chain. Closure is synthetic-only.
        let line = r#"{"type":"system","subtype":"api_error","session_id":"s","timestamp":"2026-10-30T15:42:11.397Z","error":{"status":401,"message":"401"}}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        match map_line(session_uuid_for("s"), &parsed) {
            TranscriptEvent::Events(evts) => {
                assert_eq!(evts[0].kind, EventKind::Unknown);
                assert_eq!(evts[0].payload["error"]["status"], 401);
                assert_eq!(evts[0].payload["terminal"], true);
            }
            _ => panic!("expected events"),
        }
    }

    #[test]
    fn api_error_retry_is_flagged_non_terminal() {
        let line = r#"{"type":"system","subtype":"api_error","session_id":"s","error":{"message":"timeout"},"source":"request_retry","retryAttempt":1,"maxRetries":10}"#;
        let parsed = parse_line_multi(line, "s").pop().expect("one entry");
        match map_line(session_uuid_for("s"), &parsed) {
            TranscriptEvent::Events(evts) => {
                assert_eq!(evts[0].kind, EventKind::Unknown);
                assert_eq!(evts[0].payload["terminal"], false);
            }
            _ => panic!("expected events"),
        }
    }
}
