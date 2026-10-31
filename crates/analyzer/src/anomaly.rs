//! Anomaly model: what a finding is, how severe it is, and how much
//! we trust it.

use agent_sentinel_core::SessionState;
use serde::{Deserialize, Serialize};

/// One detected anomaly in a session's event stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub severity: Severity,
    pub confidence: Confidence,
    /// Human-readable explanation, pointing at concrete evidence.
    pub message: String,
    /// Index of the most relevant event in the stream (0-based).
    pub event_index: u64,
    /// Machine-readable evidence for tooling.
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    /// An event arrived before `SessionStarted`.
    MissingSessionStart,
    /// `SessionStarted` for an already-closed session.
    UnexpectedRestart,
    /// Any event after the session closed.
    EventsAfterClose,
    /// Session opened but never closed (INV-4).
    UnclosedSession,
    /// The same event id appears more than once.
    DuplicateEventId,
    /// Two distinct events carry identical (kind, name, payload).
    DuplicateContent,
    /// Event timestamps violate monotonic order.
    OutOfOrderTimestamps,
    /// A `ToolCall` with no matching `ToolResult`.
    UnpairedToolCall,
    /// A `ToolResult` with no preceding `ToolCall`.
    OrphanToolResult,
    /// `PermissionResolved` without `PermissionRequested`.
    OrphanPermissionResolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Cosmetic / informational; the stream is still trustworthy.
    Info,
    /// Something is off; data may be incomplete.
    Warning,
    /// The stream violates a hard contract; downstream analysis of
    /// this session should treat data as unreliable.
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Provably wrong (deterministic check failed).
    Certain,
    /// Likely a problem, but a benign explanation exists.
    Probable,
    /// Weak signal, listed for completeness.
    Possible,
}

/// Full analysis result for one session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionReport {
    pub event_count: usize,
    pub state: SessionState,
    pub anomalies: Vec<Anomaly>,
}

impl SessionReport {
    /// Number of anomalies at or above the given severity.
    pub fn count_at_least(&self, severity: Severity) -> usize {
        self.anomalies
            .iter()
            .filter(|a| a.severity >= severity)
            .count()
    }

    pub fn is_healthy(&self) -> bool {
        self.anomalies.is_empty()
    }
}
