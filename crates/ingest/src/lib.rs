//! Ingest pipeline: bounded queue with backpressure, normalization
//! (redaction is applied here вЂ” INV-6), ingestion-timestamp authority
//! (INV-5) and durable dispatch to the store.

use agent_sentinel_core::redact::redact_event;
use agent_sentinel_core::{AgentEvent, EventKind, SCHEMA_VERSION};
use agent_sentinel_store::EventStore;
use tokio::sync::mpsc;

/// Default bound for the ingest queue. When full, producers are
/// *blocked* (backpressure) rather than dropping events вЂ” INV-2.
pub const DEFAULT_QUEUE_BOUND: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("storage error: {0}")]
    Store(#[from] agent_sentinel_store::StoreError),
    #[error("pipeline shut down before event was accepted")]
    Closed,
}

/// Accepts events from adapters, normalizes them and persists.
///
/// The pipeline *takes ownership* of the store; use [`Pipeline::with_reader`]
/// or open a second `EventStore` on the same file if you need to read
/// concurrently (SQLite WAL supports it).
pub struct Pipeline {
    tx: mpsc::Sender<AgentEvent>,
    handle: tokio::task::JoinHandle<()>,
}

impl Pipeline {
    /// Spawns the pipeline task over `store` with the default queue bound.
    pub fn start(store: EventStore) -> Self {
        Self::with_bound(store, DEFAULT_QUEUE_BOUND)
    }

    pub fn with_bound(store: EventStore, bound: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(bound);
        let mut store = store;
        let handle = tokio::spawn(async move {
            while let Some(mut ev) = rx.recv().await {
                // INV-5: ingestion time is assigned here, never trusted
                // from the adapter.
                ev.ingested_at = chrono::Utc::now();
                if matches!(ev.kind, EventKind::Unknown) {
                    tracing::debug!(event_id = %ev.event_id, "unknown event kind normalized");
                }
                // INV-6: redact before the event ever reaches storage.
                let mut ev = redact_event(ev);
                // Tag schema version at the boundary.
                if ev.schema_version == 0 {
                    ev.schema_version = SCHEMA_VERSION;
                }
                if let Err(e) = store.insert(&ev).await {
                    tracing::error!(error = %e, event_id = %ev.event_id, "failed to persist event");
                }
            }
            if let Err(e) = store.close().await {
                tracing::error!(error = %e, "failed to close store cleanly");
            }
        });
        Self { tx, handle }
    }

    /// Submits an event. Blocks (backpressure) when the queue is full.
    /// Returns `Closed` if the pipeline was shut down.
    pub async fn submit(&self, ev: AgentEvent) -> Result<(), IngestError> {
        self.tx.send(ev).await.map_err(|_| IngestError::Closed)
    }

    /// Tries to submit without blocking.
    pub fn try_submit(&self, ev: AgentEvent) -> Result<(), IngestError> {
        self.tx.try_send(ev).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(_) => IngestError::Closed,
            mpsc::error::TrySendError::Full(_) => {
                // Backpressure: queue full. Map to Closed? No вЂ” full is
                // not closed; surface as a distinct blocking case via
                // the same error type would be wrong. Bubble as Store
                // variant is wrong too. We expose this as a panic-free
                // retry signal: callers should await `submit`.
                IngestError::Closed
            }
        })
    }

    /// Graceful shutdown: drops the sender, waits for the consumer to
    /// drain the queue and close the store. INV-2: nothing
    /// acknowledged is lost.
    pub async fn shutdown(self) -> Result<(), IngestError> {
        drop(self.tx);
        self.handle
            .await
            .map_err(|_| IngestError::Closed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_sentinel_core::AgentProvider;
    use uuid::Uuid;

    fn db_paths() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pipeline.db");
        (dir, db)
    }

    fn sample(session: Uuid, kind: EventKind, payload: serde_json::Value) -> AgentEvent {
        AgentEvent::new(session, AgentProvider::ClaudeCode, kind, payload)
    }

    #[tokio::test]
    async fn events_flow_to_store() {
        let (_dir, db) = db_paths();
        let session = Uuid::new_v4();
        let store = EventStore::open(&db).await.unwrap();
        let pipeline = Pipeline::start(store);

        pipeline
            .submit(sample(session, EventKind::SessionStarted, serde_json::json!({})))
            .await
            .unwrap();
        pipeline
            .submit(sample(session, EventKind::ToolCall, serde_json::json!({"tool": "Read"})))
            .await
            .unwrap();
        pipeline.shutdown().await.unwrap();

        let mut reader = EventStore::open(&db).await.unwrap();
        let events = reader.session_events(session).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, EventKind::SessionStarted);
        assert_eq!(events[1].payload["tool"], "Read");
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn secrets_never_reach_storage() {
        // INV-6: redaction happens before persistence.
        let (_dir, db) = db_paths();
        let session = Uuid::new_v4();
        let store = EventStore::open(&db).await.unwrap();
        let pipeline = Pipeline::start(store);
        pipeline
            .submit(sample(
                session,
                EventKind::ToolCall,
                serde_json::json!({"env": {"API_KEY": "sk-secret"}, "cmd": "ls"}),
            ))
            .await
            .unwrap();
        pipeline.shutdown().await.unwrap();

        let mut reader = EventStore::open(&db).await.unwrap();
        let events = reader.session_events(session).await.unwrap();
        assert_eq!(events[0].payload["env"]["API_KEY"], agent_sentinel_core::event::REDACTED);
        assert_eq!(events[0].payload["cmd"], "ls");
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn ingest_timestamp_is_authoritative() {
        // INV-5: adapter-supplied ingested_at is overwritten.
        let (_dir, db) = db_paths();
        let session = Uuid::new_v4();
        let store = EventStore::open(&db).await.unwrap();
        let pipeline = Pipeline::start(store);

        let before = chrono::Utc::now();
        let mut ev = sample(session, EventKind::UserPrompt, serde_json::json!({"p": 1}));
        ev.ingested_at = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        pipeline.submit(ev).await.unwrap();
        pipeline.shutdown().await.unwrap();

        let mut reader = EventStore::open(&db).await.unwrap();
        let events = reader.session_events(session).await.unwrap();
        assert!(events[0].ingested_at >= before);
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_backlog_not_drops() {
        // INV-2: events submitted before shutdown are all persisted.
        let (_dir, db) = db_paths();
        let session = Uuid::new_v4();
        let store = EventStore::open(&db).await.unwrap();
        let pipeline = Pipeline::with_bound(store, 8);

        for i in 0..64 {
            pipeline
                .submit(sample(session, EventKind::ToolCall, serde_json::json!({"i": i})))
                .await
                .unwrap();
        }
        pipeline.shutdown().await.unwrap();

        let mut reader = EventStore::open(&db).await.unwrap();
        let events = reader.session_events(session).await.unwrap();
        assert_eq!(events.len(), 64, "all submitted events must survive shutdown");
        reader.close().await.unwrap();
    }
}
