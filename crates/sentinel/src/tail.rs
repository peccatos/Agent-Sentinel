//! Transcript tail daemon: an out-of-band observer of Claude Code.
//! Watches `~/.claude/projects/**.jsonl` and feeds new lines through
//! the same pipeline as the hook adapter. Claude Code is never
//! touched — if this daemon dies, the agent does not notice (that is
//! the entire point of the out-of-band design).
//!
//! Implementation mirrors `watch.rs` (spool watcher): polling with
//! per-file byte offsets, torn-tail deferral, idempotent line ids
//! (UUIDv5) so restarts never duplicate events.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_sentinel_ingest::Pipeline;
use agent_sentinel_store::EventStore;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use agent_sentinel_transcript::{
    find_transcripts, map_line, parse_transcript_bytes, session_uuid_for, TranscriptEvent,
};

pub struct TailWatcher {
    projects_dir: PathBuf,
    interval: Duration,
}

/// Per-file read state.
struct FileProgress {
    consumed: u64,
    /// Bytes of a torn trailing line, retried on the next tick.
    pending_tail: Vec<u8>,
    /// Last byte offset for which we have submitted events.
    session_uuid: Option<uuid::Uuid>,
}

impl TailWatcher {
    pub fn new(projects_dir: PathBuf, interval: Duration) -> Self {
        Self {
            projects_dir,
            interval,
        }
    }

    /// Runs until stopped. Each tick: scan projects tree, read new
    /// bytes per transcript, map and submit.
    pub async fn run(&self, db: &Path) -> anyhow::Result<()> {
        let store = EventStore::open(db).await?;
        let pipeline = Pipeline::start(store);
        let mut progress: HashMap<PathBuf, FileProgress> = HashMap::new();

        loop {
            if let Err(e) = self.tick(&pipeline, &mut progress).await {
                tracing::error!(error = %e, "tail tick failed");
            }
            tokio::time::sleep(self.interval).await;
        }
    }

    async fn tick(
        &self,
        pipeline: &Pipeline,
        progress: &mut HashMap<PathBuf, FileProgress>,
    ) -> anyhow::Result<()> {
        // Read dir tree synchronously: tiny, and avoids async borrow
        // gymnastics. Errors (dir missing) → nothing to observe.
        let files = {
            let dir = self.projects_dir.clone();
            tokio::task::spawn_blocking(move || find_transcripts(&dir))
                .await
                .map_err(|e| anyhow::anyhow!("join error: {e}"))??
        };

        for f in files {
            let p = progress.entry(f.path.clone()).or_insert(FileProgress {
                consumed: 0,
                pending_tail: Vec::new(),
                session_uuid: None,
            });

            let mut file = match tokio::fs::File::open(&f.path).await {
                Ok(file) => file,
                Err(e) => {
                    tracing::warn!(file = %f.path.display(), error = %e, "cannot open transcript");
                    continue;
                }
            };
            file.seek(std::io::SeekFrom::Start(p.consumed)).await?;
            let mut new_bytes = Vec::new();
            file.read_to_end(&mut new_bytes).await?;
            if new_bytes.is_empty() {
                continue;
            }
            p.consumed += new_bytes.len() as u64;

            let mut bytes = std::mem::take(&mut p.pending_tail);
            bytes.extend_from_slice(&new_bytes);

            // Session correlation: the transcript filename is the
            // Claude session id (same identity the hook spool uses).
            let session_key = f
                .path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let session_uuid = session_uuid_for(&session_key);
            p.session_uuid = Some(session_uuid);

            let parsed = parse_transcript_bytes(&bytes);
            if let Some(from) = parsed.torn_from {
                p.pending_tail = bytes[from..].to_vec();
            } else {
                p.pending_tail.clear();
            }

            let mut submitted = 0usize;
            for line in parsed.lines {
                if let TranscriptEvent::Events(evts) = map_line(session_uuid, &line.parsed) {
                    for mut ev in evts {
                        // Idempotent identity: the line's UUIDv5.
                        ev.event_id = line.line_id;
                        if let Err(e) = pipeline.submit(ev).await {
                            tracing::error!(error = %e, "pipeline submit failed");
                        }
                        submitted += 1;
                    }
                }
            }
            if submitted > 0 {
                tracing::info!(
                    file = %f.path.display(),
                    events = submitted,
                    "ingested transcript lines"
                );
            }
        }
        Ok(())
    }
}
