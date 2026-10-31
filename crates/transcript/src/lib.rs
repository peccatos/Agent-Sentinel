//! # Claude Code transcript adapter
//!
//! An **out-of-band** observer of Claude Code: instead of hooking into
//! the agent's lifecycle (and risking its stability), we read the
//! session transcripts that Claude Code already writes to
//! `~/.claude/projects/<project>/<session>.jsonl`.
//!
//! The agent is never touched; if Sentinel dies, Claude Code does not
//! even notice. This is the same philosophy as Codex `rollout-trace`.
//!
//! ## Event mapping
//!
//! | Transcript entry                          | EventKind            |
//! |-------------------------------------------|----------------------|
//! | first entry of a file (or `session_context`/`environment`) | SessionStarted |
//! | `type:"user"` with human `origin`         | UserPrompt           |
//! | assistant message containing `tool_use`   | ToolCall (per block) |
//! | `type:"user"` with `tool_result` content  | ToolResult (per block) |
//! | `isSidechain:true` user/assistant entries | SubagentStarted/Completed |
//! | `type:"system"` `subtype:"api_error"`     | SessionFailed        |
//! | anything else                             | Unknown (kept, INV-1) |
//!
//! Session completion: transcripts have no explicit "session ended"
//! entry — the file simply stops growing. The caller (tail/import
//! commands) synthesizes `SessionCompleted` via
//! [`synthetic_completion`] when a session is known to be finished
//! (e.g. the tail daemon sees no writes for a quiet period, or the
//! import of an historical file is complete).

pub mod entry;
pub mod mapping;

pub use entry::{parse_transcript_bytes, ParsedLine, TranscriptLine};
pub use mapping::{
    map_line, session_uuid_for, synthetic_completion, synthetic_start, TranscriptEvent,
};

use std::path::PathBuf;

/// Default Claude Code projects directory: `~/.claude/projects`.
pub fn projects_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".claude").join("projects")
}

/// Overrides the projects directory (tests, non-default installs).
/// Reads `SENTINEL_PROJECTS_DIR`, falling back to the default.
pub fn projects_dir_env() -> PathBuf {
    std::env::var_os("SENTINEL_PROJECTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(projects_dir)
}

/// One transcript file under the projects tree.
#[derive(Debug, Clone)]
pub struct TranscriptFile {
    /// Absolute path of the `.jsonl` transcript.
    pub path: PathBuf,
    /// Project directory name (e.g. `C--Users-madei-...`).
    pub project: String,
}

/// Scans the projects tree for transcript files.
/// Returns files sorted by modification time (oldest first) so that
/// historical import yields chronological event streams.
pub fn find_transcripts(dir: &std::path::Path) -> std::io::Result<Vec<TranscriptFile>> {
    let mut out: Vec<(std::time::SystemTime, TranscriptFile)> = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(Vec::new()), // no projects dir yet — nothing to observe
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let project = entry.file_name().to_string_lossy().to_string();
        let sub = match std::fs::read_dir(&path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        for f in sub.flatten() {
            let fpath = f.path();
            if fpath.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            if !f.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let modified = f
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            out.push((modified, TranscriptFile { path: fpath, project: project.clone() }));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out.into_iter().map(|(_, f)| f).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projects_dir_points_at_claude_projects() {
        let dir = projects_dir();
        assert!(dir.ends_with("projects"));
    }

    #[test]
    fn find_transcripts_on_empty_dir_is_ok() {
        let tmp = std::env::temp_dir().join(format!("sentinel-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        assert!(find_transcripts(&tmp).unwrap().is_empty());
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn find_transcripts_finds_jsonl_only() {
        let tmp = std::env::temp_dir().join(format!("sentinel-test-{}", uuid::Uuid::new_v4()));
        let proj = tmp.join("Proj--x");
        std::fs::create_dir_all(&proj).unwrap();
        std::fs::write(proj.join("s1.jsonl"), b"{}\n").unwrap();
        std::fs::write(proj.join("notes.txt"), b"hi").unwrap();
        std::fs::write(proj.join("memory"), b"m").unwrap();
        let found = find_transcripts(&tmp).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].project, "Proj--x");
        std::fs::remove_dir_all(&tmp).ok();
    }
}
