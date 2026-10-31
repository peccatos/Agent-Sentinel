//! Raw transcript line parsing. Tolerant by design (INV-1): a line
//! that cannot be understood is surfaced as [`ParsedLine::Unknown`],
//! never an error. Torn trailing lines are deferred like in the spool.
//!
//! One line can yield **several** parsed entries: an assistant message
//! with N `tool_use` blocks is N parallel tool calls, and a user
//! message with N `tool_result` blocks is N parallel results. All are
//! preserved — parallel tool calls are legal and common.

use serde::Deserialize;
use uuid::Uuid;

/// Parsed classification of one transcript line, before mapping onto
/// the event contract.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedLine {
    /// The opening entry of a session (`session_context` attachment).
    SessionStart {
        session_id: String,
        timestamp: Option<String>,
        meta: serde_json::Value,
    },
    /// A human-submitted prompt.
    UserPrompt {
        session_id: String,
        prompt_id: Option<String>,
        timestamp: Option<String>,
        prompt_text: String,
    },
    /// One tool call (an assistant `tool_use` block). Parallel calls
    /// in one message yield several `ToolUse` entries.
    ToolUse {
        session_id: String,
        tool_call_id: String,
        tool_name: String,
        timestamp: Option<String>,
        input: serde_json::Value,
        is_sidechain: bool,
    },
    /// One tool result (a `tool_result` content block in a user entry).
    /// Parallel results in one message yield several `ToolResult` entries.
    ToolResult {
        session_id: String,
        tool_call_id: String,
        timestamp: Option<String>,
        is_error: bool,
        content: serde_json::Value,
        is_sidechain: bool,
    },
    /// An API-level error recorded by Claude Code.
    ///
    /// IMPORTANT: in transcripts an `api_error` never marks the end
    /// of a session — Claude Code retries, and the user can continue
    /// in the same file even after retries are exhausted (observed
    /// in real data: 10/10 retries, then normal activity resumed).
    /// Session closure is always synthetic (end-of-file / quiescence).
    ApiError {
        session_id: String,
        timestamp: Option<String>,
        error: serde_json::Value,
        /// True when retries were exhausted or the error is not a
        /// retry. Diagnostic signal only — never a session-close.
        is_terminal: bool,
    },
    /// Sidechain activity markers (subagent turns).
    SidechainMark {
        session_id: String,
        timestamp: Option<String>,
        is_assistant: bool,
    },
    /// Parsed JSON we do not map (kept verbatim, INV-1/INV-7).
    Unknown {
        session_id: Option<String>,
        entry_type: Option<String>,
        raw: serde_json::Value,
    },
}

/// One parsed entry plus its stable identity for idempotent ingestion.
#[derive(Debug, Clone)]
pub struct TranscriptLine {
    /// Deterministic line id: UUIDv5 over (line bytes, block index) —
    /// re-importing the same transcript never duplicates events.
    pub line_id: Uuid,
    pub parsed: ParsedLine,
}

impl ParsedLine {
    /// Timestamp of the entry as reported by the transcript, if any.
    pub fn timestamp_str(&self) -> Option<&str> {
        match self {
            ParsedLine::SessionStart { timestamp, .. }
            | ParsedLine::UserPrompt { timestamp, .. }
            | ParsedLine::ToolUse { timestamp, .. }
            | ParsedLine::ToolResult { timestamp, .. }
            | ParsedLine::ApiError { timestamp, .. }
            | ParsedLine::SidechainMark { timestamp, .. } => timestamp.as_deref(),
            ParsedLine::Unknown { raw, .. } => {
                raw.get("timestamp").and_then(|t| t.as_str())
            }
        }
    }
}

/// Result of parsing a transcript file's bytes.
pub struct TranscriptParse {
    /// Parsed entries, in file order. A single source line with
    /// parallel tool blocks expands to several entries (same order
    /// as the blocks appear in the message).
    pub lines: Vec<TranscriptLine>,
    /// Byte offset of a torn trailing line, if any.
    pub torn_from: Option<usize>,
}

/// Parses raw transcript bytes into classified entries.
pub fn parse_transcript_bytes(bytes: &[u8]) -> TranscriptParse {
    let mut lines = Vec::new();
    let mut torn_from = None;
    let mut session_id = String::new();

    let text = String::from_utf8_lossy(bytes);
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let complete = line.ends_with('\n');
        let trimmed = line.trim_end_matches(['\n', '\r']);
        offset += line.len();
        if !complete {
            torn_from = Some(offset - line.len());
            break;
        }
        if trimmed.is_empty() {
            continue;
        }
        // Track the session id from any line that carries it.
        if session_id.is_empty() {
            if let Some(sid) = extract_session_id(trimmed) {
                session_id = sid;
            }
        }
        for (block_idx, parsed) in parse_line_multi(trimmed, &session_id).into_iter().enumerate() {
            let line_id = line_uuid(trimmed, block_idx);
            lines.push(TranscriptLine { line_id, parsed });
        }
    }

    TranscriptParse { lines, torn_from }
}

/// Session id of a line, if present (both `session_id` and `sessionId`
/// spellings occur across Claude Code versions).
fn extract_session_id(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    v.get("session_id")
        .or_else(|| v.get("sessionId"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

/// Deterministic UUIDv5 id for one parsed entry of a source line:
/// scoped by (line text, block index) so parallel blocks in one line
/// are distinct, and re-reads are stable.
fn line_uuid(line: &str, block_idx: usize) -> Uuid {
    const LINE_NAMESPACE: Uuid = Uuid::from_u128(0x5a11_1e5a_0b2d_4f6c_9a7b_1d2e_3f4a_5b6c);
    let mut bytes = line.as_bytes().to_vec();
    bytes.push(0xFF); // separator: no ambiguity between text and index
    bytes.extend_from_slice(&block_idx.to_le_bytes());
    Uuid::new_v5(&LINE_NAMESPACE, &bytes)
}

/// Parses one complete JSONL line into 1..N classified entries.
/// Public for tests and the mapper.
pub fn parse_line_multi(line: &str, fallback_session: &str) -> Vec<ParsedLine> {
    let raw: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => {
            // Not JSON at all. INV-1: keep, do not fail.
            return vec![ParsedLine::Unknown {
                session_id: None,
                entry_type: None,
                raw: serde_json::Value::String(line.to_string()),
            }];
        }
    };

    let entry: RawEntry = match serde_json::from_str(line) {
        Ok(e) => e,
        Err(_) => {
            return vec![ParsedLine::Unknown {
                session_id: raw.get("session_id").and_then(|s| s.as_str()).map(String::from),
                entry_type: raw.get("type").and_then(|s| s.as_str()).map(String::from),
                raw,
            }];
        }
    };

    let session = entry
        .session_id
        .clone()
        .or(entry.sessionId.clone())
        .unwrap_or_else(|| fallback_session.to_string());
    let timestamp = entry.timestamp.clone();
    let is_sidechain = entry.isSidechain.unwrap_or(false);
    let entry_type = entry.entry_type.as_deref().unwrap_or("");

    // Session-opening entry: the `session_context` attachment. The
    // `environment` attachment (same head, different snapshot) is
    // deliberately *not* a start — otherwise every session would open
    // twice and trip the analyzer's duplicate-start detection.
    let attachment_type = raw
        .get("attachment")
        .and_then(|a| a.get("type"))
        .and_then(|t| t.as_str());
    if attachment_type == Some("session_context") {
        return vec![ParsedLine::SessionStart {
            session_id: session,
            timestamp,
            meta: raw,
        }];
    }

    match entry_type {
        "user" => {
            let origin_human = raw
                .get("origin")
                .and_then(|o| o.get("kind"))
                .and_then(|k| k.as_str())
                == Some("human");
            let blocks = entry
                .message
                .as_ref()
                .map(|m| match &m.content {
                    RawContent::Blocks(b) => b.as_slice().to_vec(),
                    _ => Vec::new(),
                })
                .unwrap_or_default();

            // Every tool_result block becomes its own entry:
            // parallel results are distinct events.
            let mut out = Vec::new();
            for block in &blocks {
                if block.block_type.as_deref() == Some("tool_result") {
                    out.push(ParsedLine::ToolResult {
                        session_id: session.clone(),
                        tool_call_id: block.tool_use_id.clone().unwrap_or_default(),
                        timestamp: timestamp.clone(),
                        is_error: block.is_error.unwrap_or(false),
                        content: block.content.clone(),
                        is_sidechain,
                    });
                }
            }
            if !out.is_empty() {
                return out;
            }

            // No tool results: a text message.
            let prompt_text = match entry.message.as_ref().map(|m| &m.content) {
                Some(RawContent::Text(s)) => s.clone(),
                _ => user_text(&raw),
            };
            if origin_human {
                return vec![ParsedLine::UserPrompt {
                    session_id: session,
                    prompt_id: entry.promptId.clone(),
                    timestamp,
                    prompt_text,
                }];
            }
            vec![ParsedLine::Unknown {
                session_id: Some(session),
                entry_type: Some("user".into()),
                raw,
            }]
        }
        "assistant" => {
            let blocks = entry
                .message
                .as_ref()
                .map(|m| match &m.content {
                    RawContent::Blocks(b) => b.as_slice().to_vec(),
                    _ => Vec::new(),
                })
                .unwrap_or_default();
            // Every tool_use block becomes its own entry: parallel
            // tool calls are distinct events (and common).
            let mut out = Vec::new();
            for block in &blocks {
                if block.block_type.as_deref() == Some("tool_use") {
                    out.push(ParsedLine::ToolUse {
                        session_id: session.clone(),
                        tool_call_id: block.id.clone().unwrap_or_default(),
                        tool_name: block.name.clone().unwrap_or_default(),
                        timestamp: timestamp.clone(),
                        input: block.input.clone(),
                        is_sidechain,
                    });
                }
            }
            if !out.is_empty() {
                return out;
            }
            if is_sidechain {
                return vec![ParsedLine::SidechainMark {
                    session_id: session,
                    timestamp,
                    is_assistant: true,
                }];
            }
            vec![ParsedLine::Unknown {
                session_id: Some(session),
                entry_type: Some("assistant".into()),
                raw,
            }]
        }
        "system" => {
            if entry.subtype.as_deref() == Some("api_error") {
                let source = raw.get("source").and_then(|s| s.as_str());
                let retry_attempt = raw.get("retryAttempt").and_then(|r| r.as_u64());
                let max_retries = raw.get("maxRetries").and_then(|m| m.as_u64());
                let is_retry_in_progress = source == Some("request_retry")
                    && retry_attempt
                        .zip(max_retries)
                        .is_some_and(|(a, m)| a < m);
                return vec![ParsedLine::ApiError {
                    session_id: session,
                    timestamp,
                    error: raw.get("error").cloned().unwrap_or(serde_json::Value::Null),
                    is_terminal: !is_retry_in_progress,
                }];
            }
            vec![ParsedLine::Unknown {
                session_id: Some(session),
                entry_type: Some("system".into()),
                raw,
            }]
        }
        _ => vec![ParsedLine::Unknown {
            session_id: Some(session),
            entry_type: Some(entry_type.to_string()).filter(|s| !s.is_empty()),
            raw,
        }],
    }
}

/// Loose wire shape of a transcript entry. Every field optional:
/// the transcript format is Claude Code's internal format and may
/// change between versions — we must never hard-fail on it.
#[derive(Deserialize, Debug)]
#[allow(non_snake_case, dead_code)] // field names mirror the transcript wire format verbatim
struct RawEntry {
    #[serde(rename = "type")]
    entry_type: Option<String>,
    subtype: Option<String>,
    session_id: Option<String>,
    sessionId: Option<String>,
    timestamp: Option<String>,
    uuid: Option<String>,
    isSidechain: Option<bool>,
    isMeta: Option<bool>,
    promptId: Option<String>,
    message: Option<RawMessage>,
}

#[derive(Deserialize, Debug)]
struct RawMessage {
    #[serde(default)]
    content: RawContent,
}

/// `content` is either a plain string or an array of typed blocks.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum RawContent {
    Blocks(Vec<RawBlock>),
    Text(String),
    Null,
}

impl Default for RawContent {
    fn default() -> Self {
        RawContent::Null
    }
}

#[derive(Deserialize, Debug, Clone)]
struct RawBlock {
    #[serde(rename = "type")]
    block_type: Option<String>,
    id: Option<String>,
    name: Option<String>,
    tool_use_id: Option<String>,
    is_error: Option<bool>,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    content: serde_json::Value,
}

/// Extracts the human prompt text from a user entry's raw JSON.
fn user_text(raw: &serde_json::Value) -> String {
    raw.get("message")
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .map(String::from)
        .or_else(|| {
            let blocks = raw
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_array())?;
            blocks
                .iter()
                .filter_map(|b| {
                    if b.get("type").and_then(|t| t.as_str()) == Some("text") {
                        b.get("text").and_then(|t| t.as_str()).map(String::from)
                    } else {
                        None
                    }
                })
                .next()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: &str = "11111111-2222-3333-4444-555555555555";

    fn one(line: &str) -> ParsedLine {
        let mut v = parse_line_multi(line, "fallback");
        assert_eq!(v.len(), 1, "expected exactly one entry from this line");
        v.pop().unwrap()
    }

    #[test]
    fn parses_session_context_as_session_start() {
        let line = format!(
            r#"{{"type":"attachment","attachment":{{"type":"session_context"}},"timestamp":"2026-10-30T15:32:30.622Z","sessionId":"{SESSION}"}}"#
        );
        match one(&line) {
            ParsedLine::SessionStart { session_id, .. } => {
                assert_eq!(session_id, SESSION);
            }
            other => panic!("expected SessionStart, got {other:?}"),
        }
    }

    #[test]
    fn environment_head_is_bookkeeping_not_session_start() {
        // Only `session_context` opens a session; `environment` would
        // double-open and trip duplicate-start detection.
        let line = r#"{"type":"attachment","attachment":{"type":"environment","snapshot":{"workingDirectory":"C:\\repo"}},"timestamp":"2026-10-30T15:32:30.622Z","session_id":"s-env"}"#;
        let p = one(line);
        assert!(matches!(p, ParsedLine::Unknown { .. }), "got {p:?}");
    }

    #[test]
    fn parses_human_user_prompt() {
        let line = r#"{"type":"user","origin":{"kind":"human"},"promptId":"p1","timestamp":"2026-10-30T15:32:34.380Z","sessionId":"s","message":{"role":"user","content":[{"type":"text","text":"привет"}]}}"#;
        match one(line) {
            ParsedLine::UserPrompt { prompt_text, prompt_id, .. } => {
                assert_eq!(prompt_text, "привет");
                assert_eq!(prompt_id.as_deref(), Some("p1"));
            }
            other => panic!("expected UserPrompt, got {other:?}"),
        }
    }

    #[test]
    fn meta_user_entries_are_not_prompts() {
        // isMeta:true — internal bookkeeping, must not become UserPrompt.
        let line = r#"{"type":"user","isMeta":true,"sessionId":"s","message":{"role":"user","content":"system-reminder"}}"#;
        let p = one(line);
        assert!(matches!(p, ParsedLine::Unknown { .. }), "got {p:?}");
    }

    #[test]
    fn parses_tool_use_block() {
        let line = r#"{"type":"assistant","timestamp":"2026-10-30T15:33:00.000Z","sessionId":"s","message":{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"file_path":"C:\\x.rs"}}]}}"#;
        match one(line) {
            ParsedLine::ToolUse { tool_name, tool_call_id, .. } => {
                assert_eq!(tool_name, "Read");
                assert_eq!(tool_call_id, "call_1");
            }
            other => panic!("expected ToolUse, got {other:?}"),
        }
    }

    #[test]
    fn parallel_tool_use_blocks_all_parsed() {
        // Parallel calls: both blocks must become entries (ANA-2 guard).
        let line = r#"{"type":"assistant","sessionId":"s","message":{"content":[{"type":"tool_use","id":"call_1","name":"Read","input":{}},{"type":"tool_use","id":"call_2","name":"Read","input":{}}]}}"#;
        let v = parse_line_multi(line, "fallback");
        assert_eq!(v.len(), 2);
        assert!(matches!(&v[0], ParsedLine::ToolUse { tool_call_id, .. } if tool_call_id == "call_1"));
        assert!(matches!(&v[1], ParsedLine::ToolUse { tool_call_id, .. } if tool_call_id == "call_2"));
    }

    #[test]
    fn parallel_tool_results_all_parsed() {
        let line = r#"{"type":"user","sessionId":"s","message":{"content":[{"type":"tool_result","tool_use_id":"call_1","content":"a"},{"type":"tool_result","tool_use_id":"call_2","content":"b"}]}}"#;
        let v = parse_line_multi(line, "fallback");
        assert_eq!(v.len(), 2);
        assert!(matches!(&v[0], ParsedLine::ToolResult { tool_call_id, .. } if tool_call_id == "call_1"));
        assert!(matches!(&v[1], ParsedLine::ToolResult { tool_call_id, .. } if tool_call_id == "call_2"));
    }

    #[test]
    fn parses_tool_result_block() {
        let line = r#"{"type":"user","timestamp":"2026-10-30T15:33:05.000Z","sessionId":"s","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"file text"}]}}"#;
        match one(line) {
            ParsedLine::ToolResult { tool_call_id, is_error, .. } => {
                assert_eq!(tool_call_id, "call_1");
                assert!(!is_error);
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn tool_result_with_error_flag() {
        let line = r#"{"type":"user","sessionId":"s","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_9","is_error":true,"content":"boom"}]}}"#;
        match one(line) {
            ParsedLine::ToolResult { is_error, .. } => assert!(is_error),
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn parses_api_error() {
        let line = r#"{"type":"system","subtype":"api_error","level":"error","timestamp":"2026-10-30T15:42:11.397Z","sessionId":"s","error":{"message":"401","status":401}}"#;
        match one(line) {
            ParsedLine::ApiError { error, is_terminal, .. } => {
                assert_eq!(error["status"], 401);
                // No retry metadata at all → terminal.
                assert!(is_terminal);
            }
            other => panic!("expected ApiError, got {other:?}"),
        }
    }

    #[test]
    fn api_error_retry_in_progress_is_not_terminal() {
        let line = r#"{"type":"system","subtype":"api_error","timestamp":"2026-10-30T15:42:11.397Z","sessionId":"s","error":{"message":"401"},"source":"request_retry","retryAttempt":2,"maxRetries":10}"#;
        match one(line) {
            ParsedLine::ApiError { is_terminal, .. } => assert!(!is_terminal),
            other => panic!("expected ApiError, got {other:?}"),
        }
    }

    #[test]
    fn api_error_exhausted_retries_is_terminal() {
        let line = r#"{"type":"system","subtype":"api_error","sessionId":"s","error":{"message":"x"},"source":"request_retry","retryAttempt":10,"maxRetries":10}"#;
        match one(line) {
            ParsedLine::ApiError { is_terminal, .. } => assert!(is_terminal),
            other => panic!("expected ApiError, got {other:?}"),
        }
    }

    #[test]
    fn unknown_entry_types_survive() {
        // INV-1: `atis-latch`, `mode`, `cost-state`… — kept, not errors.
        let line = r#"{"type":"atis-latch","atis":"","sessionId":"s"}"#;
        match one(line) {
            ParsedLine::Unknown { entry_type, .. } => {
                assert_eq!(entry_type.as_deref(), Some("atis-latch"));
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn garbage_line_is_unknown_not_error() {
        let p = one("this is not json");
        assert!(matches!(p, ParsedLine::Unknown { .. }));
    }

    #[test]
    fn torn_trailing_line_is_deferred() {
        let bytes = b"{\"type\":\"user\",\"sessionId\":\"s\"}\n{\"type\":\"user";
        let r = parse_transcript_bytes(bytes);
        assert_eq!(r.lines.len(), 1);
        assert!(r.torn_from.is_some());
    }

    #[test]
    fn line_ids_are_deterministic() {
        let bytes = b"{\"type\":\"user\",\"sessionId\":\"s\"}\n";
        let a = parse_transcript_bytes(bytes);
        let b = parse_transcript_bytes(bytes);
        assert_eq!(a.lines[0].line_id, b.lines[0].line_id);
    }

    #[test]
    fn parallel_blocks_get_distinct_line_ids() {
        let bytes = b"{\"type\":\"assistant\",\"sessionId\":\"s\",\"message\":{\"content\":[{\"type\":\"tool_use\",\"id\":\"c1\",\"name\":\"Read\",\"input\":{}},{\"type\":\"tool_use\",\"id\":\"c2\",\"name\":\"Read\",\"input\":{}}]}}\n";
        let r = parse_transcript_bytes(bytes);
        assert_eq!(r.lines.len(), 2);
        assert_ne!(r.lines[0].line_id, r.lines[1].line_id);
    }

    #[test]
    fn session_id_fallback_from_earlier_lines() {
        // `mode` lines carry sessionId but not `type:user`; a later
        // user line without session falls back to the tracked id.
        let bytes = concat!(
            "{\"type\":\"mode\",\"mode\":\"normal\",\"sessionId\":\"sess-1\"}\n",
            "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
        )
        .as_bytes();
        let r = parse_transcript_bytes(bytes);
        match &r.lines[1].parsed {
            ParsedLine::UserPrompt { session_id, .. } => assert_eq!(session_id, "sess-1"),
            // Not human origin → Unknown, but session must be tracked.
            ParsedLine::Unknown { session_id, .. } => {
                assert_eq!(session_id.as_deref(), Some("sess-1"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
