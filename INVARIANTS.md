# Pipeline Invariants

Every invariant below is enforced by at least one test in the workspace.
If you change pipeline behavior, run `cargo test` — an invariant violation
is a regression, not a warning.

| ID   | Invariant                                                            | Enforced by (crate::test) |
|------|----------------------------------------------------------------------|---------------------------|
| INV-1 | Ingestion never rejects an event because of its content. Unknown kinds/providers degrade to `Unknown`, never drop. | `core::event::unknown_kind_falls_back_instead_of_failing`, `store::corrupt_journal_tail_is_skipped_not_fatal` |
| INV-2 | Every accepted event is durably recorded. Backpressure blocks instead of dropping; shutdown drains the queue; crash before SQLite commit is recovered from the journal. | `ingest::shutdown_drains_backlog_not_drops`, `store::journal_replays_unflushed_events_on_reopen` |
| INV-3 | Event IDs are unique (UUIDv7). Duplicate delivery is idempotent, not an error. | `store::duplicate_insert_is_idempotent` |
| INV-4 | A session is closed by `SessionCompleted` or `SessionFailed`; the analyzer flags sessions that never close. | `core::lifecycle::happy_path`, `sentinel demo --anomalies` (manual) |
| INV-5 | `ingested_at` is assigned by the pipeline, never trusted from adapters. | `ingest::ingest_timestamp_is_authoritative` |
| INV-6 | Secrets are redacted before persistence; oversized fields are truncated before persistence. | `ingest::secrets_never_reach_storage`, `core::redact::*` |
| INV-7 | Unknown event kinds are preserved (payload kept verbatim after redaction), not mapped away. | `core::event::unknown_*` |

## Notes

## Analyzer guarantees (Phase 3)

| ID    | Guarantee                                                            | Enforced by |
|-------|----------------------------------------------------------------------|-------------|
| ANA-1 | **Zero false positives on healthy streams**: a fully-paired, well-ordered session produces an empty anomaly list. | `analyzer::tests::healthy_session_is_clean`, `healthy_sessions_always_clean_parametrized` |
| ANA-2 | Parallel identical tool calls (2× ToolCall: Read) are legal and never flagged. | `analyzer::tests::interleaved_tool_calls_pair_correctly` |
| ANA-3 | Every anomaly is explainable: message names the event index, kind and evidence. | contract of `Anomaly` |
| ANA-4 | Findings carry severity (info/warning/critical) and confidence (certain/probable/possible); heuristic findings are labeled as such. | `anomaly.rs` types |

Detector inventory: lifecycle FSM (missing start / restart / events
after close / unclosed), duplicate event ids, duplicate content
(with tool-parallelism guard), out-of-order timestamps, unpaired tool
calls, orphan tool results, orphan permission resolutions.

## Adapter-specific invariants (Phase 2)

| ID    | Invariant                                                             | Enforced by |
|-------|-----------------------------------------------------------------------|-------------|
| CLA-1 | The hook binary never fails the Claude Code hook chain (any internal error → exit 0). | design of `sentinel-hook` |
| CLA-2 | Spool ingestion is idempotent: re-reading the same spool line produces the same event id (UUIDv5) and no duplicates in storage. | `sentinel::hook_pipeline::reimport_is_idempotent` |
| CLA-3 | Torn spool lines (hook died mid-write) are deferred, and parse once completed. | `claude::spool::torn_trailing_line_is_deferred`, `sentinel::hook_pipeline::torn_write_is_recovered_when_completed` |
| CLA-4 | Unparsable spool lines are logged and skipped, never fatal. | `claude::spool::garbage_lines_are_skipped_not_fatal` |
| CLA-5 | Claude session strings map deterministically onto Sentinel session UUIDs (UUIDv5 namespace). | `claude::mapping::session_correlation_is_deterministic` |

## Transcript adapter invariants (Phase 2, out-of-band)

The transcript adapter is an **out-of-band observer**: it reads the
session transcripts Claude Code already writes
(`~/.claude/projects/<project>/<session>.jsonl`) and never touches
the agent process. If Sentinel dies, Claude Code does not notice.
This is the same philosophy as Codex `rollout-trace`.

| ID    | Invariant                                                              | Enforced by |
|-------|------------------------------------------------------------------------|-------------|
| TRA-1 | The adapter never modifies Claude Code state, config or process lifetime. | design: read-only file observation |
| TRA-2 | Transcript import is idempotent: line ids are UUIDv5 over (line bytes, block index); synthetic brackets are UUIDv5 over (session, tag). Re-import never duplicates. | `sentinel import-transcripts` double-run on real data |
| TRA-3 | A transcript line that cannot be parsed degrades to `Unknown` (INV-1); a *bookkeeping* entry (`mode`, `cost-state`, `api-request*`, `queue-operation`, `isMeta` user entries, …) is skipped — it is runtime plumbing, not agent behavior. | `core::event::unknown_kind_falls_back_instead_of_failing`, `mapping::bookkeeping_types_are_skipped` |
| TRA-4 | Parallel tool blocks in one message all become events (one per block, distinct ids). | `entry::parallel_tool_use_blocks_all_parsed`, `entry::parallel_tool_results_all_parsed` |
| TRA-5 | An `api_error` NEVER closes a session: real data shows sessions continuing after an exhausted retry chain. Closure is synthetic-only (end-of-file / quiescence). | `mapping::api_error_never_closes_the_session` |
| TRA-6 | Tool pairing in the analyzer prefers `payload.tool_call_id` (exact, parallel-safe); tool name is a fallback for adapters without ids. | `analyzer::detectors::tool_pairing` |
| TRA-7 | Sessions with no agent-behavior events (bookkeeping-only files from aborted launches) are not created. | `import_transcripts` first-pass activity check |

## Notes

- The write-ahead journal (`<db>.journal.jsonl`) is the durability
  boundary: an event is acknowledged only after its journal append
  succeeds. SQLite commit may lag; `EventStore::open` replays the
  journal and recovers anything not yet committed.
- Corrupt journal tail lines (torn writes from a mid-crash append) are
  skipped on recovery: the event was never acknowledged, so dropping
  it is correct with respect to INV-2.
- The hook spool (`%APPDATA%/agent-sentinel/spool` on Windows,
  `~/.local/share/agent-sentinel/spool` on Linux) holds **raw**
  payloads — redaction happens only in the pipeline, never in the
  hook. If the spool itself is a secret-exposure concern, point
  `SENTINEL_SPOOL_DIR` at an encrypted location.
