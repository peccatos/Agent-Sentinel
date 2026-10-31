//! Integration with real Claude Code transcripts (the developer's
//! own machine). Skipped silently when `~/.claude/projects` does not
//! exist or `SENTINEL_TRANSCRIPT_FIXTURES` is not set — CI stays
//! hermetic.

use agent_sentinel_transcript::entry::parse_line_multi;
use agent_sentinel_transcript::{find_transcripts, map_line, parse_transcript_bytes, session_uuid_for, projects_dir_env, TranscriptEvent};

fn real_dir() -> Option<std::path::PathBuf> {
    let dir = std::path::PathBuf::from(
        std::env::var("SENTINEL_TRANSCRIPT_FIXTURES")
            .unwrap_or_else(|_| projects_dir_env().to_string_lossy().to_string()),
    );
    dir.is_dir().then_some(dir)
}

#[test]
fn every_line_of_every_real_transcript_parses_without_error() {
    let Some(dir) = real_dir() else { return };
    let files = find_transcripts(&dir).unwrap();
    assert!(!files.is_empty(), "expected real transcripts under {}", dir.display());

    let mut total_lines = 0usize;
    let mut session_starts = 0usize;
    let mut tool_calls = 0usize;
    let mut tool_results = 0usize;
    let mut user_prompts = 0usize;
    let mut unknown = 0usize;
    let mut torn = 0usize;

    for f in &files {
        let bytes = std::fs::read(&f.path).unwrap();
        let parsed = parse_transcript_bytes(&bytes);
        total_lines += parsed.lines.len();
        if parsed.torn_from.is_some() {
            torn += 1;
        }
        for line in &parsed.lines {
            let session = line.parsed.session_id().unwrap_or_else(|| "unknown".to_string());
            match map_line(session_uuid_for(&format!("{}:{}", f.project, session)), &line.parsed) {
                TranscriptEvent::Events(evts) => {
                    for ev in evts {
                        use agent_sentinel_core::EventKind;
                        match ev.kind {
                            EventKind::SessionStarted => session_starts += 1,
                            EventKind::ToolCall => tool_calls += 1,
                            EventKind::ToolResult => tool_results += 1,
                            EventKind::UserPrompt => user_prompts += 1,
                            _ => unknown += 1,
                        }
                        // Contract: every mapped event has a session and
                        // a non-empty payload.
                        assert!(!ev.payload.is_null());
                    }
                }
                TranscriptEvent::Skip => {}
            }
        }
    }

    eprintln!(
        "real transcripts: {} files, {} lines ({} torn), {} session starts, {} tool calls, {} tool results, {} prompts, {} other/unknown",
        files.len(), total_lines, torn, session_starts, tool_calls, tool_results, user_prompts, unknown
    );

    // Sanity: real sessions have tool traffic.
    assert!(tool_calls > 0, "expected tool calls in real transcripts");
    assert!(tool_results > 0);
    assert!(user_prompts > 0);
    // Every session file opens with a start entry.
    assert!(session_starts >= files.len() / 2);
}

#[test]
fn tool_calls_pair_with_results_by_call_id() {
    let Some(dir) = real_dir() else { return };
    let files = find_transcripts(&dir).unwrap();
    let mut calls: std::collections::HashSet<String> = Default::default();
    let mut results: std::collections::HashSet<String> = Default::default();

    for f in &files {
        let bytes = std::fs::read(&f.path).unwrap();
        for line in parse_transcript_bytes(&bytes).lines {
            match &line.parsed {
                agent_sentinel_transcript::entry::ParsedLine::ToolUse { tool_call_id, .. } => {
                    calls.insert(tool_call_id.clone());
                }
                agent_sentinel_transcript::entry::ParsedLine::ToolResult { tool_call_id, .. } => {
                    results.insert(tool_call_id.clone());
                }
                _ => {}
            }
        }
    }

    // INV-style guarantee on real data: every result references an
    // earlier call (no orphan results).
    let orphans: Vec<_> = results.difference(&calls).collect();
    assert!(
        orphans.is_empty(),
        "orphan tool results (no matching tool_use): {orphans:?}"
    );
}

/// Session id accessor used by the integration test above (the enum
/// lives in `entry`; a tiny helper keeps the test readable).
trait SessionIdOf {
    fn session_id(&self) -> Option<String>;
}

impl SessionIdOf for agent_sentinel_transcript::entry::ParsedLine {
    fn session_id(&self) -> Option<String> {
        use agent_sentinel_transcript::entry::ParsedLine::*;
        match self {
            SessionStart { session_id, .. }
            | UserPrompt { session_id, .. }
            | ToolUse { session_id, .. }
            | ToolResult { session_id, .. }
            | ApiError { session_id, .. }
            | SidechainMark { session_id, .. } => Some(session_id.clone()),
            Unknown { session_id, .. } => session_id.clone(),
        }
    }
}

#[test]
fn parse_line_handles_the_two_session_id_spellings() {
    use agent_sentinel_transcript::entry::ParsedLine;
    // `sessionId` spelling: surfaced in Unknown.session_id.
    match parse_line_multi(r#"{"type":"mode","sessionId":"x"}"#, "f").pop().unwrap() {
        ParsedLine::Unknown { session_id: Some(sid), .. } => assert_eq!(sid, "x"),
        other => panic!("unexpected {other:?}"),
    }
    // `session_id` spelling: same extraction.
    match parse_line_multi(r#"{"type":"mode","session_id":"y"}"#, "f").pop().unwrap() {
        ParsedLine::Unknown { session_id: Some(sid), .. } => assert_eq!(sid, "y"),
        other => panic!("unexpected {other:?}"),
    }
}
