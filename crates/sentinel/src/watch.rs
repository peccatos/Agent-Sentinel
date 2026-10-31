//! Spool watcher daemon: polls the spool directory, reads new
//! complete lines, maps them through the Claude adapter and feeds
//! the ingest pipeline.
//!
//! Polling (instead of inotify/notify) is a deliberate choice for
//! Phase 2: it is portable, works on network drives and OneDrive
//! paths (where the repo itself lives), and the spool is tiny. The
//! watch interval is 500ms — hook latency of half a second is fine
//! for observability.
//!
//! Idempotency: event ids are deterministic per spool line (UUIDv5),
//! so restarts and re-reads never duplicate stored events.

use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_sentinel_ingest::Pipeline;
use agent_sentinel_store::EventStore;
use tokio::io::AsyncReadExt;

use agent_sentinel_claude::{parse_spool_bytes, session_uuid_for_file};

pub struct Watcher {
    spool_dir: PathBuf,
    interval: Duration,
}

/// Per-file read state: we only read bytes beyond what we already
/// consumed. A torn tail is retried on the next tick.
struct FileProgress {
    consumed: u64,
    /// Buffer for the not-yet-complete trailing line.
    pending_tail: Vec<u8>,
}

impl Watcher {
    pub fn new(spool_dir: PathBuf, interval: Duration) -> Self {
        Self {
            spool_dir,
            interval,
        }
    }

    /// Runs until the process is stopped. Each tick: scan spool dir,
    /// read new bytes per file, parse complete lines, submit events.
    pub async fn run(&self, db: &Path) -> anyhow::Result<()> {
        let store = EventStore::open(db).await?;
        let pipeline = Pipeline::start(store);
        let mut progress: std::collections::HashMap<PathBuf, FileProgress> =
            std::collections::HashMap::new();
        let mut first = true;

        loop {
            if let Err(e) = self.tick(&pipeline, &mut progress).await {
                tracing::error!(error = %e, "watcher tick failed");
            }
            if first {
                let n: usize = progress.values().map(|p| p.pending_tail.len()).sum();
                let _ = n;
                first = false;
            }
            tokio::time::sleep(self.interval).await;
        }
    }

    async fn tick(
        &self,
        pipeline: &Pipeline,
        progress: &mut std::collections::HashMap<PathBuf, FileProgress>,
    ) -> anyhow::Result<()> {
        let mut entries = match tokio::fs::read_dir(&self.spool_dir).await {
            Ok(e) => e,
            Err(_) => return Ok(()), // spool dir doesn't exist yet — nothing to watch
        };

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let meta = entry.metadata().await?;
            if !meta.is_file() {
                continue;
            }

            let p = progress
                .entry(path.clone())
                .or_insert(FileProgress {
                    consumed: 0,
                    pending_tail: Vec::new(),
                });

            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(file = %path.display(), error = %e, "cannot open spool file");
                    continue;
                }
            };
            use tokio::io::AsyncSeekExt;
            file.seek(std::io::SeekFrom::Start(p.consumed)).await?;

            let mut new_bytes = Vec::new();
            file.read_to_end(&mut new_bytes).await?;
            if new_bytes.is_empty() {
                continue;
            }
            p.consumed += new_bytes.len() as u64;

            // Prepend any torn tail from the previous tick so a line
            // split across ticks still parses as one.
            let mut bytes = std::mem::take(&mut p.pending_tail);
            bytes.extend_from_slice(&new_bytes);

            let session_uuid = session_uuid_for_file(&path);
            let spool = parse_spool_bytes(&path, &bytes, session_uuid);

            // Keep the torn tail for the next tick.
            if let Some(from) = spool.torn_from {
                p.pending_tail = bytes[from..].to_vec();
            } else {
                p.pending_tail.clear();
            }

            for ev in spool.events {
                if let Err(e) = pipeline.submit(ev).await {
                    tracing::error!(error = %e, "pipeline submit failed");
                }
            }
        }
        Ok(())
    }
}
