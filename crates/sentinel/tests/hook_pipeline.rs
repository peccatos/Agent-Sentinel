//! End-to-end integration test of the Claude Code adapter path:
//! spool file (as written by `sentinel-hook`) → watcher/import →
//! normalized events in storage.
//!
//! Does not spawn the hook binary itself (that needs a Claude Code
//! install); it writes spool files in exactly the format the hook
//! binary produces and verifies the ingestion contract.

use agent_sentinel_core::{AgentProvider, EventKind};
use agent_sentinel_store::EventStore;

fn write_spool(dir: &std::path::Path, session: &str, lines: &[&str]) -> std::path::PathBuf {
    use std::io::Write;
    let path = dir.join(format!("{session}.jsonl"));
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    for l in lines {
        writeln!(f, "{l}").unwrap();
    }
    path
}

#[tokio::test]
async fn spool_import_maps_and_stores_events() {
    let dir = tempfile::tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let db = dir.path().join("sentinel.db");
    std::fs::create_dir_all(&spool_dir).unwrap();

    // Simulate what sentinel-hook writes for one Claude session.
    write_spool(
        &spool_dir,
        "claude-session-1",
        &[
            r#"{"hook_event_name":"SessionStart","session_id":"claude-session-1","cwd":"/repo"}"#,
            r#"{"hook_event_name":"UserPromptSubmit","session_id":"claude-session-1","prompt":"hi"}"#,
            r#"{"hook_event_name":"PreToolUse","session_id":"claude-session-1","tool_name":"Bash","tool_input":{"command":"ls","env":{"API_KEY":"sk-leak"}}}"#,
            r#"{"hook_event_name":"PostToolUse","session_id":"claude-session-1","tool_name":"Bash","tool_response":{"exit":0}}"#,
            r#"{"hook_event_name":"Stop","session_id":"claude-session-1"}"#,
        ],
    );

    // Import via the real pipeline (same code path as `import-spool`).
    let store = EventStore::open(&db).await.unwrap();
    let pipeline = agent_sentinel_ingest::Pipeline::start(store);

    let mut entries = tokio::fs::read_dir(&spool_dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let bytes = tokio::fs::read(&path).await.unwrap();
        let session_uuid = agent_sentinel_claude::session_uuid_for_file(&path);
        let spool = agent_sentinel_claude::parse_spool_bytes(&path, &bytes, session_uuid);
        assert!(spool.torn_from.is_none());
        assert_eq!(spool.events.len(), 5);
        for ev in spool.events {
            pipeline.submit(ev).await.unwrap();
        }
    }
    pipeline.shutdown().await.unwrap();

    // Verify stored events.
    let mut reader = EventStore::open(&db).await.unwrap();
    let sessions = reader.sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);
    let events = reader.session_events(sessions[0]).await.unwrap();
    assert_eq!(events.len(), 5);

    // Lifecycle order and kinds.
    assert_eq!(events[0].kind, EventKind::SessionStarted);
    assert_eq!(events[1].kind, EventKind::UserPrompt);
    assert_eq!(events[2].kind, EventKind::ToolCall);
    assert_eq!(events[2].name.as_deref(), Some("Bash"));
    assert_eq!(events[3].kind, EventKind::ToolResult);
    assert_eq!(events[4].kind, EventKind::SessionCompleted);

    // Provider.
    assert!(events.iter().all(|e| e.provider == AgentProvider::ClaudeCode));

    // INV-6: the API_KEY inside the tool_input must be redacted.
    assert_eq!(
        events[2].payload["tool_input"]["env"]["API_KEY"],
        serde_json::json!("[REDACTED]")
    );
    assert_eq!(events[2].payload["tool_input"]["command"], "ls");

    // Session correlation: all events share one session uuid.
    assert!(events.iter().all(|e| e.session_id == sessions[0]));
    reader.close().await.unwrap();
}

#[tokio::test]
async fn reimport_is_idempotent() {
    // Restart-safety: importing the same spool twice must not
    // duplicate events (deterministic event ids + INSERT OR IGNORE).
    let dir = tempfile::tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let db = dir.path().join("sentinel.db");
    std::fs::create_dir_all(&spool_dir).unwrap();

    write_spool(
        &spool_dir,
        "claude-session-2",
        &[
            r#"{"hook_event_name":"SessionStart","session_id":"claude-session-2"}"#,
            r#"{"hook_event_name":"Stop","session_id":"claude-session-2"}"#,
        ],
    );

    for _ in 0..2 {
        let store = EventStore::open(&db).await.unwrap();
        let pipeline = agent_sentinel_ingest::Pipeline::start(store);
        let mut entries = tokio::fs::read_dir(&spool_dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            let bytes = tokio::fs::read(&path).await.unwrap();
            let session_uuid = agent_sentinel_claude::session_uuid_for_file(&path);
            let spool = agent_sentinel_claude::parse_spool_bytes(&path, &bytes, session_uuid);
            for ev in spool.events {
                pipeline.submit(ev).await.unwrap();
            }
        }
        pipeline.shutdown().await.unwrap();
    }

    let mut reader = EventStore::open(&db).await.unwrap();
    let sessions = reader.sessions().await.unwrap();
    let events = reader.session_events(sessions[0]).await.unwrap();
    assert_eq!(events.len(), 2, "double import must not duplicate");
    reader.close().await.unwrap();
}

#[tokio::test]
async fn torn_write_is_recovered_when_completed() {
    // A hook process died mid-write → torn line. Later the rest
    // arrives; the watcher must then parse the complete line.
    let dir = tempfile::tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let db = dir.path().join("sentinel.db");
    std::fs::create_dir_all(&spool_dir).unwrap();

    // First write: torn (no newline).
    let path = spool_dir.join("claude-session-3.jsonl");
    std::fs::write(&path, b"{\"hook_event_name\":\"Stop\",\"session_id\":\"claude-se").unwrap();

    let store = EventStore::open(&db).await.unwrap();
    let pipeline = agent_sentinel_ingest::Pipeline::start(store);

    let bytes = tokio::fs::read(&path).await.unwrap();
    let session_uuid = agent_sentinel_claude::session_uuid_for_file(&path);
    let spool = agent_sentinel_claude::parse_spool_bytes(&path, &bytes, session_uuid);
    assert_eq!(spool.events.len(), 0, "torn line must not be parsed yet");
    assert!(spool.torn_from.is_some());

    // Append the completion (as the next hook invocation would).
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "{}", r#"ssion-3"}"#).unwrap();
    drop(f);

    // Simulate watcher behavior: pending_tail + new bytes.
    let mut bytes = tokio::fs::read(&path).await.unwrap();
    let session_uuid = agent_sentinel_claude::session_uuid_for_file(&path);
    let spool = agent_sentinel_claude::parse_spool_bytes(&path, &bytes, session_uuid);
    assert_eq!(spool.events.len(), 1, "completed line must parse");
    let _ = &mut bytes;
    for ev in spool.events {
        pipeline.submit(ev).await.unwrap();
    }
    pipeline.shutdown().await.unwrap();

    let mut reader = EventStore::open(&db).await.unwrap();
    let sessions = reader.sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);
    let events = reader.session_events(sessions[0]).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::SessionCompleted);
    reader.close().await.unwrap();
}
