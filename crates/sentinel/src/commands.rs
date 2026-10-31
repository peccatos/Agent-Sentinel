//! CLI command implementations.

use std::path::PathBuf;

use uuid::Uuid;

use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};
use agent_sentinel_ingest::Pipeline;
use agent_sentinel_store::EventStore;

use crate::report;

/// Generates a synthetic but realistic session and runs it through
/// the actual pipeline (normalizer + redaction + journal + SQLite).
pub async fn demo(db: PathBuf, anomalies: bool) -> anyhow::Result<()> {
    let store = EventStore::open(&db).await?;
    let session = Uuid::new_v4();
    let pipeline = Pipeline::start(store);

    let mut events: Vec<AgentEvent> = vec![
        ev(session, EventKind::SessionStarted, json!({"cwd": "/repo"})),
        ev(session, EventKind::UserPrompt, json!({"prompt": "fix the failing test"})),
        named_ev(session, EventKind::ToolCall, "Read", json!({"file": "src/lib.rs"})),
        named_ev(session, EventKind::ToolResult, "Read", json!({"file": "src/lib.rs", "ok": true})),
        named_ev(session, EventKind::ToolCall, "Bash", json!({
            "command": "cargo test",
            "env": {"API_KEY": "sk-this-should-be-redacted"}
        })),
        named_ev(session, EventKind::ToolResult, "Bash", json!({"command": "cargo test", "exit_code": 1})),
        ev(session, EventKind::PermissionRequested, json!({"tool": "Bash", "command": "cargo test"})),
        ev(session, EventKind::PermissionResolved, json!({"tool": "Bash", "allowed": true})),
        ev(session, EventKind::SubagentStarted, json!({"agent": "fork"})),
        named_ev(session, EventKind::ToolCall, "Edit", json!({"file": "src/lib.rs"})),
        named_ev(session, EventKind::ToolResult, "Edit", json!({"file": "src/lib.rs", "ok": true})),
        ev(session, EventKind::SubagentCompleted, json!({"agent": "fork"})),
        ev(session, EventKind::SessionCompleted, json!({"summary": "fixed"})),
    ];

    if anomalies {
        // Drop the closing event (INV-4 violation в†’ analyzer will flag).
        events.pop();
        // Duplicate an event (duplicate detection target).
        if let Some(dup) = events.get(4).cloned() {
            events.push(dup);
        }
    }

    for e in events {
        pipeline.submit(e).await?;
    }
    pipeline.shutdown().await?;

    println!("session: {session}");
    println!("events persisted through the full pipeline");
    if anomalies {
        println!("anomalies injected: missing close + duplicate event");
    }
    println!();
    report::print_report(&db, session).await?;
    Ok(())
}

pub async fn sessions(db: PathBuf) -> anyhow::Result<()> {
    let mut store = EventStore::open(&db).await?;
    let sessions = store.sessions().await?;
    if sessions.is_empty() {
        println!("no sessions recorded yet вЂ” run `sentinel demo`");
        return Ok(());
    }
    for s in sessions {
        let events = store.session_events(s).await?;
        let status = report::lifecycle_status(&events);
        println!("{s}  {} events  {}", events.len(), status);
    }
    store.close().await?;
    Ok(())
}

pub async fn report(db: PathBuf, session: Uuid) -> anyhow::Result<()> {
    report::print_report(&db, session).await
}

/// Daemon: watch the Claude Code hook spool and ingest in real time.
pub async fn watch(db: PathBuf) -> anyhow::Result<()> {
    let spool = agent_sentinel_claude::spool_dir();
    println!("watching spool: {}", spool.display());
    println!("db: {}", db.display());
    println!("(Ctrl+C to stop)");
    let watcher = crate::watch::Watcher::new(
        spool,
        std::time::Duration::from_millis(500),
    );
    watcher.run(&db).await
}

/// One-shot import: drain all spool files into the store, then exit.
pub async fn import_spool(db: PathBuf) -> anyhow::Result<()> {
    let spool_dir = agent_sentinel_claude::spool_dir();
    let store = EventStore::open(&db).await?;
    let pipeline = Pipeline::start(store);

    let mut entries = tokio::fs::read_dir(&spool_dir).await?;
    let mut total = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let bytes = tokio::fs::read(&path).await?;
        let session_uuid = agent_sentinel_claude::session_uuid_for_file(&path);
        let spool = agent_sentinel_claude::parse_spool_bytes(&path, &bytes, session_uuid);
        if let Some(_torn) = spool.torn_from {
            eprintln!("warning: torn tail in {} (skipped)", path.display());
        }
        for ev in spool.events {
            pipeline.submit(ev).await?;
            total += 1;
        }
    }
    pipeline.shutdown().await?;
    println!("imported {total} events from {}", spool_dir.display());
    Ok(())
}

/// One-shot import: read every transcript under the projects dir,
/// map lines to events and ingest (idempotent — line ids are UUIDv5).
///
/// Session bracketing: transcripts have no explicit start/close
/// entries for every case, so the importer synthesizes them:
/// - a synthetic `SessionStarted` when a file begins with activity
///   but no `session_context` attachment (real case: failed prompts
///   written before the context was attached);
/// - a synthetic `SessionCompleted` at end-of-file (import = the file
///   is final from the observer's perspective).
pub async fn import_transcripts(
    db: PathBuf,
    projects_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let dir = projects_dir
        .clone()
        .or_else(|| std::env::var_os("SENTINEL_PROJECTS_DIR").map(PathBuf::from))
        .unwrap_or_else(agent_sentinel_transcript::projects_dir);
    println!("scanning {}", dir.display());
    let files = agent_sentinel_transcript::find_transcripts(&dir)?;
    if files.is_empty() {
        println!("no transcripts found");
        return Ok(());
    }

    let store = EventStore::open(&db).await?;
    let pipeline = Pipeline::start(store);
    let mut total = 0usize;
    let n_files = files.len();

    for f in files {
        let bytes = tokio::fs::read(&f.path).await?;
        let session_key = f
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let session_uuid = agent_sentinel_transcript::session_uuid_for(&session_key);
        let parsed = agent_sentinel_transcript::parse_transcript_bytes(&bytes);
        if parsed.torn_from.is_some() {
            eprintln!("warning: torn tail in {} (deferred)", f.path.display());
        }

        // Does the file open with a real SessionStarted?
        let has_real_start = parsed
            .lines
            .first()
            .is_some_and(|l| {
                matches!(
                    l.parsed,
                    agent_sentinel_transcript::ParsedLine::SessionStart { .. }
                )
            });

        let first_ts = parsed
            .lines
            .first()
            .and_then(|l| l.parsed.timestamp_str())
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|t| t.with_timezone(&chrono::Utc));

        // First pass: would any *agent-behavior* event be emitted?
        // (Bookkeeping-only files — a few `mode`/`cost-state` lines
        // from an aborted launch — produce zero events and are
        // skipped entirely: no synthetic brackets, no empty session.)
        let has_agent_activity = parsed.lines.iter().any(|l| {
            matches!(
                agent_sentinel_transcript::map_line(session_uuid, &l.parsed),
                agent_sentinel_transcript::TranscriptEvent::Events(_)
            )
        });
        if !has_agent_activity {
            continue;
        }

        // Synthetic opening BEFORE the stream when the file has no
        // real start (activity predates the `session_context` entry).
        let mut emitted_start = has_real_start;
        if !has_real_start {
            let mut ev = agent_sentinel_transcript::synthetic_start(session_uuid, first_ts);
            // Deterministic id so re-import stays idempotent.
            ev.event_id = synthetic_id(session_uuid, b"start");
            pipeline.submit(ev).await?;
            total += 1;
        }

        let mut last_ts: Option<chrono::DateTime<chrono::Utc>> = None;

        for line in parsed.lines {
            if let agent_sentinel_transcript::TranscriptEvent::Events(evts) =
                agent_sentinel_transcript::map_line(session_uuid, &line.parsed)
            {
                for mut ev in evts {
                    ev.event_id = line.line_id;
                    last_ts = Some(ev.timestamp);
                    if matches!(ev.kind, agent_sentinel_core::EventKind::SessionStarted) {
                        emitted_start = true;
                    }
                    pipeline.submit(ev).await?;
                    total += 1;
                }
            }
        }

        // Synthetic closure at end-of-file (import = the file is
        // final from the observer's perspective).
        if emitted_start {
            let mut ev = agent_sentinel_transcript::synthetic_completion(session_uuid, last_ts);
            ev.event_id = synthetic_id(session_uuid, b"completion");
            pipeline.submit(ev).await?;
            total += 1;
        }
    }
    pipeline.shutdown().await?;
    println!("imported {total} events from {n_files} transcripts");
    Ok(())
}

/// Deterministic UUIDv5 id for synthetic events (idempotent re-import).
fn synthetic_id(session: uuid::Uuid, tag: &[u8]) -> uuid::Uuid {
    use uuid::Uuid;
    const SYNTH_NAMESPACE: Uuid = Uuid::from_u128(0x5311_7a6e_0c3e_4d7a_8b9c_2e3f_4a5b_6c7d);
    let mut bytes = session.as_bytes().to_vec();
    bytes.extend_from_slice(tag);
    Uuid::new_v5(&SYNTH_NAMESPACE, &bytes)
}

/// Out-of-band daemon: tail transcripts and ingest live.
pub async fn tail(db: PathBuf, projects_dir: Option<PathBuf>) -> anyhow::Result<()> {
    let dir = projects_dir
        .or_else(|| std::env::var_os("SENTINEL_PROJECTS_DIR").map(PathBuf::from))
        .unwrap_or_else(agent_sentinel_transcript::projects_dir);
    println!("tailing transcripts under {}", dir.display());
    println!("db: {}", db.display());
    println!("(out-of-band: Claude Code is never touched; Ctrl+C to stop)");
    let watcher = crate::tail::TailWatcher::new(dir, std::time::Duration::from_millis(500));
    watcher.run(&db).await
}

/// Audit every session with the reliability analyzer.
pub async fn audit(db: PathBuf, severity: &str) -> anyhow::Result<()> {
    let min = match severity.to_ascii_lowercase().as_str() {
        "info" => agent_sentinel_analyzer::Severity::Info,
        "warning" | "warn" => agent_sentinel_analyzer::Severity::Warning,
        "critical" => agent_sentinel_analyzer::Severity::Critical,
        other => anyhow::bail!("unknown severity {other:?} (use info|warning|critical)"),
    };
    crate::report::print_audit(&db, min).await?;
    Ok(())
}

fn ev(session: Uuid, kind: EventKind, payload: serde_json::Value) -> AgentEvent {
    AgentEvent::new(session, AgentProvider::TestHarness, kind, payload)
}

fn named_ev(
    session: Uuid,
    kind: EventKind,
    name: &str,
    payload: serde_json::Value,
) -> AgentEvent {
    let mut e = ev(session, kind, payload);
    e.name = Some(name.to_string());
    e
}

use serde_json::json;
