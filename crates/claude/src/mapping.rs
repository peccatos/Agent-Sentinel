//! Mapping of parsed hook inputs onto the provider-agnostic event
//! contract, including session correlation.

use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};
use uuid::Uuid;

use crate::hook_input::{ClaudeHookName, HookInput};

/// Claude Code session IDs are opaque strings, not UUIDs. We keep a
/// deterministic mapping: UUIDv5 in the Sentinel namespace, so the
/// same Claude session always maps to the same Sentinel session ID —
/// critical for correlating events from multiple hooks (INV: session
/// correlation).
const SENTINEL_NAMESPACE: Uuid = Uuid::from_u128(0x9d4d_a7d0_1b2c_4e5f_8a9b_0c1d_2e3f_4a5b);

/// Maps a Claude session string onto a stable Sentinel UUID.
pub fn claude_session_uuid(session_id: &str) -> Uuid {
    Uuid::new_v5(&SENTINEL_NAMESPACE, session_id.as_bytes())
}

/// Outcome of mapping one hook input.
#[derive(Debug, PartialEq)]
pub enum ClaudeHookEvent {
    /// Mapped to a normalized event.
    Mapped(AgentEvent),
    /// The input carried no session id — cannot be correlated.
    /// Still emitted as an event under the null session so that
    /// nothing is silently dropped (INV-1), but flagged.
    Uncorrelated(AgentEvent),
}

/// Maps a raw hook input into a normalized event.
///
/// Session start/end lifecycle: Claude Code hooks are per-turn; the
/// session lifecycle is reconstructed by the analyzer from the event
/// stream, not from the hook names alone. We map:
///
/// | Claude hook     | EventKind                   |
/// |-----------------|-----------------------------|
/// | SessionStart    | SessionStarted              |
/// | SessionEnd      | SessionCompleted            |
/// | Stop            | SessionCompleted            |
/// | SubagentStop    | SubagentCompleted           |
/// | UserPromptSubmit| UserPrompt                  |
/// | PreToolUse      | ToolCall                    |
/// | PostToolUse     | ToolResult                  |
/// | Notification    | HookFired (name=Notification) |
/// | PreCompact      | HookFired (name=PreCompact)   |
/// | unknown         | Unknown                     |
pub fn map_hook_input(input: &HookInput) -> ClaudeHookEvent {
    let name = input.hook_name();
    let kind = match name {
        ClaudeHookName::SessionStart => EventKind::SessionStarted,
        ClaudeHookName::SessionEnd => EventKind::SessionCompleted,
        ClaudeHookName::Stop => EventKind::SessionCompleted,
        ClaudeHookName::SubagentStop => EventKind::SubagentCompleted,
        ClaudeHookName::UserPromptSubmit => EventKind::UserPrompt,
        ClaudeHookName::PreToolUse => EventKind::ToolCall,
        ClaudeHookName::PostToolUse => EventKind::ToolResult,
        ClaudeHookName::Notification => EventKind::HookFired,
        ClaudeHookName::PreCompact => EventKind::HookFired,
        ClaudeHookName::Unknown => EventKind::Unknown,
    };

    // The `name` field on AgentEvent: tool name for tool events,
    // hook name for hook-diagnostics events.
    let event_name = match kind {
        EventKind::ToolCall | EventKind::ToolResult => input.tool_name.clone(),
        EventKind::HookFired => Some(name.as_str().to_string()),
        _ => None,
    };

    // Payload: raw hook input preserved (redaction happens later, in
    // the pipeline — single choke point, INV-6).
    let mut payload = input.raw();
    if let serde_json::Value::Object(map) = &mut payload {
        map.insert("claude_hook".into(), serde_json::json!(name.as_str()));
    }

    let session_id = input
        .session_id
        .as_deref()
        .map(claude_session_uuid)
        .unwrap_or_else(Uuid::nil);

    let mut ev = AgentEvent::new(session_id, AgentProvider::ClaudeCode, kind, payload);
    ev.name = event_name;
    if input.session_id.is_none() {
        ClaudeHookEvent::Uncorrelated(ev)
    } else {
        ClaudeHookEvent::Mapped(ev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hook_input::HookInput;

    fn map(json: &str) -> ClaudeHookEvent {
        let input = HookInput::parse(json).unwrap();
        map_hook_input(&input)
    }

    #[test]
    fn pre_tool_use_maps_to_tool_call() {
        let ClaudeHookEvent::Mapped(ev) = map(
            r#"{"hook_event_name": "PreToolUse", "session_id": "s1", "tool_name": "Bash",
                "tool_input": {"command": "cargo test"}}"#,
        ) else {
            panic!("expected Mapped");
        };
        assert_eq!(ev.kind, EventKind::ToolCall);
        assert_eq!(ev.name.as_deref(), Some("Bash"));
        assert_eq!(ev.provider, AgentProvider::ClaudeCode);
        assert_eq!(ev.payload["claude_hook"], "PreToolUse");
    }

    #[test]
    fn session_correlation_is_deterministic() {
        // Same Claude session string → same Sentinel UUID, always.
        let a = claude_session_uuid("session-abc");
        let b = claude_session_uuid("session-abc");
        let c = claude_session_uuid("session-xyz");
        assert_eq!(a, b);
        assert_ne!(a, c);

        let ClaudeHookEvent::Mapped(ev) =
            map(r#"{"hook_event_name": "UserPromptSubmit", "session_id": "session-abc"}"#)
        else {
            panic!("expected Mapped");
        };
        assert_eq!(ev.session_id, a);
    }

    #[test]
    fn missing_session_id_is_flagged_not_dropped() {
        // INV-1: no session id → still an event, flagged Uncorrelated.
        let ClaudeHookEvent::Uncorrelated(ev) =
            map(r#"{"hook_event_name": "PreToolUse", "tool_name": "Bash"}"#)
        else {
            panic!("expected Uncorrelated");
        };
        assert_eq!(ev.kind, EventKind::ToolCall);
        assert_eq!(ev.session_id, Uuid::nil());
    }

    #[test]
    fn unknown_hook_maps_to_unknown_event() {
        let input = HookInput::parse(
            r#"{"hook_event_name": "BrandNewHook", "session_id": "s"}"#,
        )
        .unwrap();
        let ClaudeHookEvent::Mapped(ev) = map_hook_input(&input) else {
            panic!("expected Mapped");
        };
        assert_eq!(ev.kind, EventKind::Unknown);
        // Raw preserved.
        assert_eq!(ev.payload["claude_hook"], "Unknown");
    }

    #[test]
    fn stop_and_session_end_both_close() {
        // Both terminal hooks map to SessionCompleted; the analyzer's
        // duplicate-close detection (Phase 3) will handle the overlap.
        let ClaudeHookEvent::Mapped(ev) =
            map(r#"{"hook_event_name": "Stop", "session_id": "s"}"#)
        else {
            panic!("expected Mapped");
        };
        assert_eq!(ev.kind, EventKind::SessionCompleted);

        let ClaudeHookEvent::Mapped(ev) =
            map(r#"{"hook_event_name": "SessionEnd", "session_id": "s"}"#)
        else {
            panic!("expected Mapped");
        };
        assert_eq!(ev.kind, EventKind::SessionCompleted);
    }
}
