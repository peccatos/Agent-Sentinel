//! Agent Sentinel core: the provider-agnostic event contract, the
//! session lifecycle FSM and the redaction policy shared by every
//! other crate.

pub mod event;
pub mod lifecycle;
pub mod redact;

pub use event::{AgentEvent, AgentProvider, EventKind, SCHEMA_VERSION};
pub use lifecycle::{SessionState, SessionTracker, Transition};

/// Pipeline invariants. Every one of these is enforced by a test
/// somewhere in the workspace; see INVARIANTS.md.
pub mod invariants {
    /// Ingestion never rejects an event because of its content.
    pub const NEVER_DROP_UNKNOWN: &str = "INV-1";
    /// Every event entering the pipeline gets a durable record.
    pub const DURABLE_EVERY_EVENT: &str = "INV-2";
    /// Event IDs are unique (UUIDv7).
    pub const UNIQUE_EVENT_IDS: &str = "INV-3";
    /// Sessions are closed by a terminal event.
    pub const SESSION_CLOSED: &str = "INV-4";
    /// Adapters never assign ingestion timestamps.
    pub const INGEST_TIME_AUTHORITY: &str = "INV-5";
    /// Secrets are redacted before persistence.
    pub const REDACT_BEFORE_STORE: &str = "INV-6";
    /// Unknown event kinds are preserved, not mapped away.
    pub const PRESERVE_RAW: &str = "INV-7";
}
