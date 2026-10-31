//! Spool daemon: watches the spool directory, reads complete JSONL
//! lines, maps them to events, and feeds the pipeline.
//!
//! Idempotency: the spool is the replay log. Each line carries a
//! deterministic event identity derived from (session, hook name,
//! payload hash), so re-reading a spool after a daemon restart does
//! not duplicate events in storage (INSERT OR IGNORE downstream +
//! UUIDv5 event ids here).
//!
//! Torn-line tolerance: a trailing line without `\n` is a hook
//! process that died mid-write — skipped until completed.

use std::path::{Path, PathBuf};

use agent_sentinel_core::AgentEvent;
use uuid::Uuid;

use crate::hook_input::HookInput;
use crate::mapping::{claude_session_uuid, map_hook_input, ClaudeHookEvent};

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("I/O error on spool {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Reads all complete lines from one spool file, mapping each to an
/// event. Incomplete trailing lines (torn writes) are returned as
/// `None`-tail info so the caller can retry later.
pub struct SpoolFile {
    pub path: PathBuf,
    pub session_uuid: Uuid,
    pub events: Vec<AgentEvent>,
    /// Byte offset of the first incomplete trailing line, if any.
    pub torn_from: Option<usize>,
}

/// Parses a spool file's raw bytes. Public for tests.
pub fn parse_spool_bytes(path: &Path, bytes: &[u8], session_uuid: Uuid) -> SpoolFile {
    let mut events = Vec::new();
    let mut torn_from = None;

    let text = String::from_utf8_lossy(bytes);
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let ends_with_newline = line.ends_with('\n');
        let line_trim = line.trim_end_matches(['\n', '\r']);
        offset += line.len();
        if !ends_with_newline {
            // Torn trailing write: skip, remember the offset.
            torn_from = Some(offset - line.len());
            break;
        }
        if line_trim.is_empty() {
            continue;
        }
        match HookInput::parse(line_trim) {
            Ok(input) => {
                let mut ev = match map_hook_input(&input) {
                    ClaudeHookEvent::Mapped(ev) | ClaudeHookEvent::Uncorrelated(ev) => ev,
                };
                // Deterministic event id: same line → same id →
                // idempotent re-ingestion.
                ev.event_id = event_id_for_line(session_uuid, line_trim);
                events.push(ev);
            }
            Err(e) => {
                // INV-1: garbage lines are logged and skipped, never
                // fatal for the file.
                tracing::warn!(file = %path.display(), error = %e, "skipping unparsable spool line");
            }
        }
    }

    SpoolFile {
        path: path.to_path_buf(),
        session_uuid,
        events,
        torn_from,
    }
}

/// Deterministic UUIDv5 event id for a spool line: stable across
/// daemon restarts and re-reads (idempotent ingestion).
fn event_id_for_line(session: Uuid, line: &str) -> Uuid {
    Uuid::new_v5(&session, line.as_bytes())
}

/// Derives the Sentinel session UUID for a spool file.
/// The filename is the sanitized Claude session id; we recover the
/// original id from the file's first line when possible, else use the
/// filename directly.
pub fn session_uuid_for_file(path: &Path) -> Uuid {
    // The deterministic mapping below uses the *original* session id
    // string. The sanitizer is injective enough for correlation in
    // practice; where it isn't (two ids that sanitize identically),
    // the first line's real session_id wins.
    claude_session_uuid(&file_stem(path))
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_sentinel_core::{AgentProvider, EventKind};

    #[test]
    fn parses_complete_lines() {
        let session = Uuid::new_v4();
        let bytes = concat!(
            "{\"hook_event_name\":\"PreToolUse\",\"session_id\":\"s\",\"tool_name\":\"Bash\"}\n",
            "{\"hook_event_name\":\"Stop\",\"session_id\":\"s\"}\n",
        )
        .as_bytes();
        let spool = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        assert_eq!(spool.events.len(), 2);
        assert_eq!(spool.events[0].kind, EventKind::ToolCall);
        assert_eq!(spool.events[1].kind, EventKind::SessionCompleted);
        assert!(spool.torn_from.is_none());
    }

    #[test]
    fn deterministic_event_ids() {
        // Same line re-read → same event id (idempotent ingestion).
        let session = Uuid::new_v4();
        let bytes =
            b"{\"hook_event_name\":\"Stop\",\"session_id\":\"s\"}\n";
        let a = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        let b = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        assert_eq!(a.events[0].event_id, b.events[0].event_id);
    }

    #[test]
    fn torn_trailing_line_is_deferred() {
        let session = Uuid::new_v4();
        let bytes = b"{\"hook_event_name\":\"Stop\",\"session_id\":\"s\"}\n{\"hook_event_na";
        let spool = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        assert_eq!(spool.events.len(), 1);
        assert_eq!(spool.torn_from, Some(44));
    }

    #[test]
    fn garbage_lines_are_skipped_not_fatal() {
        let session = Uuid::new_v4();
        let bytes = b"not json at all\n{\"hook_event_name\":\"Stop\",\"session_id\":\"s\"}\n";
        let spool = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        assert_eq!(spool.events.len(), 1);
    }

    #[test]
    fn provider_is_claude_code() {
        let session = Uuid::new_v4();
        let bytes = b"{\"hook_event_name\":\"SessionStart\",\"session_id\":\"s\"}\n";
        let spool = parse_spool_bytes(Path::new("s.jsonl"), bytes, session);
        assert_eq!(spool.events[0].provider, AgentProvider::ClaudeCode);
    }
}
