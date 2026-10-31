//! `sentinel-hook` — the Claude Code hook binary.
//!
//! Installed in Claude Code settings as a hook command. Claude Code
//! pipes the hook payload as JSON on stdin; this binary appends it to
//! a per-session spool file and exits immediately (0).
//!
//! Design constraints:
//! - **Never fails the hook chain.** Any error is swallowed (logged
//!   to stderr, exit 0) — the hook must not break the agent's work.
//! - **Atomic appends.** One payload → one line, written with a
//!   single `write_all` on a line-buffered append handle; torn
//!   writes are detected and skipped by the daemon.
//! - **No parsing.** Raw payload goes to the spool; mapping happens
//!   in the daemon (single choke point for redaction, INV-6).
//! - **Deterministic spool path** per Claude session id so the
//!   daemon can correlate.

use std::io::{Read, Write};

fn main() {
    if let Err(e) = run() {
        // Never fail the hook chain: log and exit 0.
        eprintln!("sentinel-hook: {e}");
    }
}

fn run() -> Result<(), String> {
    let mut raw = String::new();
    std::io::stdin()
        .read_to_string(&mut raw)
        .map_err(|e| e.to_string())?;

    // An empty payload is a no-op (some hooks fire with no stdin).
    if raw.trim().is_empty() {
        return Ok(());
    }

    // Sanity: it must be a JSON object (we don't parse further).
    let v: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("stdin is not JSON: {e}"))?;
    let session_id = v
        .get("session_id")
        .and_then(|s| s.as_str())
        .map(sanitize_filename)
        .unwrap_or_else(|| "uncorrelated".to_string());

    let dir = agent_sentinel_claude::spool_dir();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let path = dir.join(format!("{session_id}.jsonl"));

    // Single line + newline, one write syscall on an append handle.
    let mut line = raw;
    line.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| e.to_string())?;
    file.write_all(line.as_bytes())
        .map_err(|e| e.to_string())?;
    // Best-effort flush; if this fails the daemon will skip the torn
    // line — the hook chain still succeeds.
    let _ = file.flush();
    Ok(())
}

/// Session ids are opaque strings; keep the spool filename safe on
/// both Unix and Windows.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_safe_chars() {
        assert_eq!(sanitize_filename("abc-123_X"), "abc-123_X");
    }

    #[test]
    fn sanitize_replaces_path_separators() {
        assert_eq!(
            sanitize_filename(r"a/b\c:d*e?f"),
            "a_b_c_d_e_f"
        );
    }
}
