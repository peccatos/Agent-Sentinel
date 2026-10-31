//! Durable event storage: SQLite (WAL) as the source of truth plus a
//! JSONL write-ahead journal for crash recovery (INV-2).

use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("journal I/O error: {0}")]
    Journal(#[from] std::io::Error),
    #[error("journal line {line} is not valid JSON: {error}")]
    CorruptJournal { line: usize, error: String },
}

const MIGRATIONS: &[&str] = &[
    // v1: initial schema
    r#"
    CREATE TABLE IF NOT EXISTS events (
        event_id       TEXT PRIMARY KEY,
        session_id     TEXT NOT NULL,
        timestamp      TEXT NOT NULL,
        ingested_at    TEXT NOT NULL,
        provider       TEXT NOT NULL,
        kind           TEXT NOT NULL,
        name           TEXT,
        payload        TEXT NOT NULL,
        schema_version INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
    CREATE INDEX IF NOT EXISTS idx_events_ingested ON events(ingested_at);
    "#,
];

/// SQLite-backed event store.
pub struct EventStore {
    pool: SqlitePool,
    journal: JournalWriter,
}

impl EventStore {
    /// Opens (or creates) the store at `path` (SQLite file) with a
    /// sibling `.journal.jsonl` file, then replays any journal entries
    /// not yet flushed to SQLite (crash recovery).
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await?;

        Self::migrate(&pool).await?;

        let journal_path = journal_path_for(path);
        let journal = JournalWriter::open(&journal_path).await?;
        let recovered = journal.replay_into(&pool).await?;

        if recovered > 0 {
            tracing::info!(recovered, journal = %journal_path.display(), "recovered events from journal after crash");
        }

        Ok(Self { pool, journal })
    }

    async fn migrate(pool: &SqlitePool) -> Result<(), StoreError> {
        for (i, sql) in MIGRATIONS.iter().enumerate() {
            sqlx::query(sql).execute(pool).await.map_err(|e| {
                tracing::error!(version = i + 1, error = %e, "migration failed");
                e
            })?;
        }
        Ok(())
    }

    /// Persists an event: journal first (durable append), then SQLite.
    /// On crash between the two, `open` recovers from the journal.
    pub async fn insert(&mut self, ev: &AgentEvent) -> Result<(), StoreError> {
        // INV-2: journal write first вЂ” an event that was acknowledged
        // is never lost, even if the process dies before the SQLite
        // insert commits.
        self.journal.append(ev).await?;

        let name = ev.name.as_deref();
        let payload = serde_json::to_string(&ev.payload).map_err(|e| {
            StoreError::CorruptJournal {
                line: 0,
                error: e.to_string(),
            }
        })?;

        let res = sqlx::query(
            "INSERT OR IGNORE INTO events
             (event_id, session_id, timestamp, ingested_at, provider, kind, name, payload, schema_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(ev.event_id.to_string())
        .bind(ev.session_id.to_string())
        .bind(ev.timestamp.to_rfc3339())
        .bind(ev.ingested_at.to_rfc3339())
        .bind(provider_str(ev.provider))
        .bind(kind_str(ev.kind))
        .bind(name)
        .bind(&payload)
        .bind(ev.schema_version)
        .execute(&self.pool)
        .await;

        // INSERT OR IGNORE keeps INV-3 (uniqueness) without failing
        // the pipeline on duplicate delivery (idempotent retry).
        res.map(|_| ())?;

        // Entries are fsynced on rotate/truncate boundaries; periodic
        // truncation keeps the journal from growing unboundedly.
        if self.journal.should_rotate(1024) {
            self.journal.rotate().await?;
        }

        Ok(())
    }

    /// All events of a session, ordered by ingestion time.
    pub async fn session_events(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<AgentEvent>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM events WHERE session_id = ?1 ORDER BY ingested_at, rowid",
        )
        .bind(session_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_event).collect()
    }

    /// All events, ordered by ingestion time (bounded by `limit`).
    pub async fn recent_events(&self, limit: i64) -> Result<Vec<AgentEvent>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM events ORDER BY ingested_at DESC, rowid DESC LIMIT ?1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut events: Vec<AgentEvent> = rows.iter().map(row_to_event).collect::<Result<_, _>>()?;
        events.reverse();
        Ok(events)
    }

    /// Known session IDs (derived from stored events), ordered by
    /// the first event ingested for each session.
    pub async fn sessions(&self) -> Result<Vec<Uuid>, StoreError> {
        let rows = sqlx::query(
            "SELECT session_id FROM events GROUP BY session_id ORDER BY MIN(ingested_at)",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                r.try_get::<String, _>("session_id")
                    .map_err(StoreError::from)
                    .and_then(|s| Uuid::parse_str(&s).map_err(|e| StoreError::CorruptJournal {
                        line: 0,
                        error: e.to_string(),
                    }))
            })
            .collect()
    }

    pub async fn close(&mut self) -> Result<(), StoreError> {
        // All journal entries are committed to SQLite by now (insert
        // commits synchronously); safe to truncate the journal so a
        // clean shutdown does not masquerade as a crash on reopen.
        self.journal.rotate().await?;
        self.pool.close().await;
        Ok(())
    }
}

fn journal_path_for(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".journal.jsonl");
    PathBuf::from(s)
}

fn provider_str(p: AgentProvider) -> &'static str {
    match p {
        AgentProvider::ClaudeCode => "claude_code",
        AgentProvider::Codex => "codex",
        AgentProvider::TestHarness => "test_harness",
        AgentProvider::Unknown => "unknown",
    }
}

fn kind_str(k: EventKind) -> &'static str {
    match k {
        EventKind::SessionStarted => "session_started",
        EventKind::SessionCompleted => "session_completed",
        EventKind::SessionFailed => "session_failed",
        EventKind::UserPrompt => "user_prompt",
        EventKind::ToolCall => "tool_call",
        EventKind::ToolResult => "tool_result",
        EventKind::PermissionRequested => "permission_requested",
        EventKind::PermissionResolved => "permission_resolved",
        EventKind::SubagentStarted => "subagent_started",
        EventKind::SubagentCompleted => "subagent_completed",
        EventKind::HookFired => "hook_fired",
        EventKind::Unknown => "unknown",
    }
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Result<AgentEvent, StoreError> {
    let event_id: String = row.try_get("event_id")?;
    let session_id: String = row.try_get("session_id")?;
    let timestamp: String = row.try_get("timestamp")?;
    let ingested_at: String = row.try_get("ingested_at")?;
    let provider: String = row.try_get("provider")?;
    let kind: String = row.try_get("kind")?;
    let name: Option<String> = row.try_get("name")?;
    let payload: String = row.try_get("payload")?;
    let schema_version: i64 = row.try_get("schema_version")?;

    Ok(AgentEvent {
        event_id: Uuid::parse_str(&event_id).map_err(|e| StoreError::CorruptJournal {
            line: 0,
            error: e.to_string(),
        })?,
        session_id: Uuid::parse_str(&session_id).map_err(|e| StoreError::CorruptJournal {
            line: 0,
            error: e.to_string(),
        })?,
        timestamp: chrono::DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|e| StoreError::CorruptJournal {
                line: 0,
                error: e.to_string(),
            })?
            .with_timezone(&chrono::Utc),
        ingested_at: chrono::DateTime::parse_from_rfc3339(&ingested_at)
            .map_err(|e| StoreError::CorruptJournal {
                line: 0,
                error: e.to_string(),
            })?
            .with_timezone(&chrono::Utc),
        provider: serde_json::from_value(serde_json::json!(provider))
            .unwrap_or(AgentProvider::Unknown),
        kind: serde_json::from_value(serde_json::json!(kind)).unwrap_or(EventKind::Unknown),
        name,
        payload: serde_json::from_str(&payload).map_err(|e| StoreError::CorruptJournal {
            line: 0,
            error: e.to_string(),
        })?,
        schema_version: schema_version as u16,
    })
}

/// Append-only JSONL journal with crash-recovery replay.
struct JournalWriter {
    path: PathBuf,
    pending: u64,
    file: Option<tokio::fs::File>,
}

impl JournalWriter {
    async fn open(path: &Path) -> Result<Self, StoreError> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            path: path.to_path_buf(),
            pending: 0,
            file: Some(file),
        })
    }

    async fn append(&mut self, ev: &AgentEvent) -> Result<(), StoreError> {
        let mut line = serde_json::to_string(&JournalledEvent {
            event_id: ev.event_id,
            session_id: ev.session_id,
            timestamp: ev.timestamp,
            ingested_at: ev.ingested_at,
            provider: provider_str(ev.provider).to_string(),
            kind: kind_str(ev.kind).to_string(),
            name: ev.name.clone(),
            payload: ev.payload.clone(),
            schema_version: ev.schema_version,
        })
        .map_err(|e| StoreError::CorruptJournal {
            line: 0,
            error: e.to_string(),
        })?;
        line.push('\n');
        use tokio::io::AsyncWriteExt;
        if let Some(f) = self.file.as_mut() {
            f.write_all(line.as_bytes()).await?;
        }
        self.pending += 1;
        Ok(())
    }

    fn should_rotate(&self, threshold: u64) -> bool {
        self.pending >= threshold
    }

    /// Truncates the journal once all its entries are safely in SQLite.
    async fn rotate(&mut self) -> Result<(), StoreError> {
        if let Some(f) = self.file.as_mut() {
            f.sync_all().await?;
        }
        // Drop the append handle first: on Windows, truncating a file
        // that still has a write handle open fails.
        self.file = None;
        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await?;
        f.sync_all().await?;
        self.pending = 0;
        // Reopen for future appends.
        self.file = Some(
            tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await?,
        );
        Ok(())
    }

    /// Replays journal entries into SQLite and truncates the journal.
    /// Corrupt trailing lines (partial write during a crash) are
    /// logged and skipped вЂ” the entry was never acknowledged, so
    /// dropping it is correct, not a violation of INV-2.
    async fn replay_into(&self, pool: &SqlitePool) -> Result<usize, StoreError> {
        let content = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(_) => return Ok(0),
        };
        let mut recovered = 0;
        for (i, line) in content.lines().map(str::to_string).collect::<Vec<_>>().into_iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: JournalledEvent = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(line = i + 1, error = %e, "skipping corrupt journal line");
                    continue;
                }
            };
            let payload = serde_json::to_string(&entry.payload).unwrap_or_default();
            sqlx::query(
                "INSERT OR IGNORE INTO events
                 (event_id, session_id, timestamp, ingested_at, provider, kind, name, payload, schema_version)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .bind(entry.event_id.to_string())
            .bind(entry.session_id.to_string())
            .bind(entry.timestamp.to_rfc3339())
            .bind(entry.ingested_at.to_rfc3339())
            .bind(entry.provider)
            .bind(entry.kind)
            .bind(entry.name)
            .bind(&payload)
            .bind(entry.schema_version)
            .execute(pool)
            .await?;
            recovered += 1;
        }
        if recovered > 0 {
            let f = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.path)
                .await?;
            f.sync_all().await?;
        }
        Ok(recovered)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct JournalledEvent {
    event_id: Uuid,
    session_id: Uuid,
    timestamp: chrono::DateTime<chrono::Utc>,
    ingested_at: chrono::DateTime<chrono::Utc>,
    provider: String,
    kind: String,
    name: Option<String>,
    payload: serde_json::Value,
    schema_version: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_sentinel_core::{AgentProvider, EventKind};

    async fn temp_store() -> (EventStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let store = EventStore::open(&db).await.unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn insert_and_read_back_roundtrip() {
        let (mut store, _dir) = temp_store().await;
        let session = Uuid::new_v4();
        let mut ev = AgentEvent::new(
            session,
            AgentProvider::ClaudeCode,
            EventKind::ToolCall,
            serde_json::json!({"tool": "Bash", "command": "ls"}),
        );
        ev.name = Some("Bash".into());
        store.insert(&ev).await.unwrap();

        let events = store.session_events(session).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, ev.event_id);
        assert_eq!(events[0].name.as_deref(), Some("Bash"));
        assert_eq!(events[0].payload["command"], "ls");
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_insert_is_idempotent() {
        // INV-3: unique event IDs; duplicate delivery must not error.
        let (mut store, _dir) = temp_store().await;
        let session = Uuid::new_v4();
        let ev = AgentEvent::new(
            session,
            AgentProvider::ClaudeCode,
            EventKind::UserPrompt,
            serde_json::json!({"prompt": "hi"}),
        );
        store.insert(&ev).await.unwrap();
        store.insert(&ev).await.unwrap();

        let events = store.session_events(session).await.unwrap();
        assert_eq!(events.len(), 1);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn journal_replays_unflushed_events_on_reopen() {
        // INV-2: simulate a crash вЂ” journal entries exist, SQLite insert
        // never happened. Reopening must recover them.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let session = Uuid::new_v4();

        {
            let mut store = EventStore::open(&db).await.unwrap();
            let ev = AgentEvent::new(
                session,
                AgentProvider::ClaudeCode,
                EventKind::SessionStarted,
                serde_json::json!({}),
            );
            // Journal write happens inside insert before SQLite; simulate
            // crash by writing to the journal and abandoning the store.
            store.journal.append(&ev).await.unwrap();
            // NOTE: pool not closed gracefully вЂ” dropping is the "crash".
        }

        let mut store = EventStore::open(&db).await.unwrap();
        let events = store.session_events(session).await.unwrap();
        assert_eq!(events.len(), 1, "journal entry must be recovered after crash");
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_journal_tail_is_skipped_not_fatal() {
        // INV-1 adjacent: partial line from a mid-crash write must not
        // prevent the store from opening.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("corrupt.db");
        let journal = journal_path_for(&db);
        tokio::fs::write(
            &journal,
            "{\"event_id\":\"nope\"\n{\"also\":\"bad\"}\n",
        )
        .await
        .unwrap();

        let mut store = EventStore::open(&db).await.unwrap();
        store.close().await.unwrap();
    }
}
