//! # Event contract
//!
//! The single normalized model every provider adapter must produce.
//! Invariant (INV-1): ingestion never fails because of event content вЂ”
//! unknown kinds, malformed payloads and missing fields degrade to
//! [`EventKind::Unknown`] / raw payloads, they are never dropped.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Which agent runtime produced the event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProvider {
    ClaudeCode,
    Codex,
    /// A synthetic agent used by the conformance test-suite.
    TestHarness,
    Unknown,
}

/// Lifecycle classification of the event.
///
/// Order within a session is expected to follow the session FSM
/// (see `lifecycle` module): only [`EventKind::SessionStarted`] may
/// open a session, [`EventKind::SessionCompleted`] /
/// [`EventKind::SessionFailed`] close it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    SessionStarted,
    SessionCompleted,
    SessionFailed,
    UserPrompt,
    ToolCall,
    ToolResult,
    PermissionRequested,
    PermissionResolved,
    SubagentStarted,
    SubagentCompleted,
    /// A hook fired (adapter-level diagnostic event).
    HookFired,
    /// Adapter could not map the raw event to any known kind.
    /// INV-1: this is the fallback, never an error.
    Unknown,
}

/// Well-known payload keys that carry secrets and must be redacted
/// before the event is persisted (INV-6).
pub const REDACTED: &str = "[REDACTED]";

/// A normalized agent event.
///
/// `schema_version` allows the storage layer to migrate old rows
/// without re-parsing; adapters must always emit the current
/// [`SCHEMA_VERSION`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEvent {
    pub event_id: Uuid,
    pub session_id: Uuid,
    /// Monotonic wall-clock time as reported by the adapter.
    /// May be in the past relative to ingestion time (clock skew,
    /// replayed traces) вЂ” never trusted for ordering decisions.
    pub timestamp: DateTime<Utc>,
    /// Time the event entered the pipeline. Assigned by the ingest
    /// layer, never by adapters.
    pub ingested_at: DateTime<Utc>,
    pub provider: AgentProvider,
    pub kind: EventKind,
    /// Tool / hook name for `ToolCall`, `ToolResult`, `HookFired` kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Free-form provider payload, preserved verbatim (after redaction).
    pub payload: serde_json::Value,
    pub schema_version: u16,
}

/// Current event schema revision. Bump on breaking changes to
/// `AgentEvent` semantics and add a migration in the store crate.
pub const SCHEMA_VERSION: u16 = 1;

impl AgentEvent {
    /// Builds a fresh event (used by adapters for live events).
    pub fn new(
        session_id: Uuid,
        provider: AgentProvider,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            session_id,
            timestamp: Utc::now(),
            ingested_at: Utc::now(),
            provider,
            kind,
            name: None,
            payload,
            schema_version: SCHEMA_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_serializes_roundtrip() {
        let ev = AgentEvent::new(
            Uuid::new_v4(),
            AgentProvider::ClaudeCode,
            EventKind::ToolCall,
            serde_json::json!({"tool": "Bash"}),
        );
        let json = serde_json::to_string(&ev).unwrap();
        let back: AgentEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn unknown_kind_falls_back_instead_of_failing() {
        // INV-1: an unmappable raw kind must parse as Unknown, not error.
        let json = serde_json::json!("definitely_not_a_kind");
        let kind: EventKind = serde_json::from_value(json).unwrap_or(EventKind::Unknown);
        assert_eq!(kind, EventKind::Unknown);
    }

    #[test]
    fn unknown_provider_falls_back_instead_of_failing() {
        let json = serde_json::json!("some-new-agent");
        let provider: AgentProvider =
            serde_json::from_value(json).unwrap_or(AgentProvider::Unknown);
        assert_eq!(provider, AgentProvider::Unknown);
    }
}
