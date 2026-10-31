# Architecture

This document explains how Agent Sentinel is put together and why.
The one-paragraph version: adapters normalize provider-specific data
into a single event contract; a pipeline makes those events durable
through a write-ahead journal into SQLite; an analyzer checks the
resulting streams against lifecycle invariants. Every layer is
replaceable through the `AgentEvent` seam.

## The event contract (`crates/core`)

Everything that enters the system becomes an `AgentEvent`:

```rust
pub struct AgentEvent {
    pub event_id: Uuid,          // UUIDv7 (time-ordered, unique)
    pub session_id: Uuid,        // provider session, correlated
    pub timestamp: DateTime<Utc>,  // as reported by the adapter
    pub ingested_at: DateTime<Utc>, // assigned by the pipeline, never the adapter
    pub provider: AgentProvider,   // ClaudeCode | Codex | TestHarness | Unknown
    pub kind: EventKind,           // lifecycle classification
    pub name: Option<String>,      // tool name for tool events
    pub payload: serde_json::Value, // verbatim (after redaction)
    pub schema_version: u16,       // storage migration anchor
}
```

Design decisions worth knowing:

**Unknown is a value, not an error.** The format a provider writes is
its internal format and may change without notice. An adapter that
cannot map an entry emits `EventKind::Unknown` with the raw payload
preserved. Ingestion never fails because of event content (INV-1).
A future Claude Code entry type shows up in the data as `Unknown`
instead of breaking the pipeline — and can be promoted to a real kind
later, retroactively, from the stored payloads.

**Two timestamps, one authority.** `timestamp` is what the provider
said; `ingested_at` is when the pipeline accepted it. Only the
pipeline may set `ingested_at` (INV-5). Adapter timestamps are used
for reporting but never for ordering decisions — clock skew and
replayed history make them untrustworthy.

**Session correlation is deterministic.** Claude session strings are
mapped to UUIDs via UUIDv5 in a fixed namespace. The same Claude
session always maps to the same Sentinel session, which is what lets
the hook adapter and the transcript adapter — two independent sources
for the same session — converge on one identity.

**Redaction lives in the pipeline, not in adapters** (INV-6). One
choke point: every event passes `redact_event` before it reaches
storage. Sensitive keys (tokens, credentials, keys) are replaced,
oversized fields are truncated. A new adapter cannot forget to
redact because it does not do the redacting.

## Durability (`crates/store` + `crates/ingest`)

The SQLite file is not the durability boundary — the journal is:

```
adapter → pipeline queue (bounded) → journal append → ack
                                          │
                                          ▼ (async, may lag)
                                       SQLite commit
```

An event is acknowledged only after its append to
`<db>.journal.jsonl` succeeds. If the process dies between journal
append and SQLite commit, `EventStore::open` replays the journal and
recovers the unflushed events. A torn journal tail (a crash mid-append)
is skipped: that event was never acknowledged, so dropping it is
correct with respect to the durability contract.

The pipeline queue is **bounded with backpressure** (INV-2): when the
store is slow, producers block — events are never dropped to "keep
up". Shutdown drains the queue.

Inserts are idempotent on the event id (INV-3): duplicate delivery
is a no-op, not an error. This is what makes re-importing
transcripts safe.

## Adapters

Adapters are pure: raw provider data in, `AgentEvent`s out. They know
nothing about storage, the pipeline or the analyzer.

### Transcript adapter (`crates/transcript`) — recommended

Reads `~/.claude/projects/<project>/<session>.jsonl`, the transcripts
Claude Code writes on its own. Out-of-band by construction:

- the agent process is never touched, configured or even aware;
- observation survives Sentinel restarts (the files are append-only);
- all history is importable, not just future sessions.

Key behaviors, all learned from real transcript data:

- **One line → many events.** An assistant message with N `tool_use`
  blocks is N parallel tool calls; each becomes its own event. The
  naive one-event-per-line parser silently loses parallelism.
- **Bookkeeping is filtered, unknowns are kept.** `mode`,
  `cost-state`, `queue-operation`, `api-request*`, `isMeta` entries,
  slash-command plumbing — these are Claude Code's runtime state, not
  agent behavior; they are skipped. Anything else unmappable is kept
  as `Unknown`. The distinction matters: skipping *known* plumbing is
  noise reduction; skipping *unknown* entries would be data loss.
- **`api_error` never closes a session.** Transcripts record every
  retry as an `api_error` entry. Real sessions continue after a fully
  exhausted retry chain (observed: 10/10 retries, then 200+ normal
  events). Closure is synthesized from file state instead.
- **Synthetic session brackets.** Transcripts have no explicit
  session-start/close entries in every case, so the importer
  synthesizes them: a start before the stream when activity predates
  the `session_context` attachment, a completion at end-of-file.
  Synthetic events carry deterministic ids, so re-import stays
  idempotent.
- **Line identity is deterministic.** UUIDv5 over (line bytes, block
  index). Re-reading a file — after a crash, after a month — produces
  the same ids, and idempotent inserts turn that into a no-op.

### Hook adapter (`crates/claude`) — optional

A `sentinel-hook` binary that can be installed as a Claude Code hook.
It spools raw hook payloads to a JSONL spool directory; the `watch`
daemon drains the spool into the pipeline. Design constraints:

- the hook **never fails the Claude Code hook chain**: any internal
  error exits 0 (a broken observer must not break the agent);
- the spool holds **raw** payloads — redaction happens only in the
  pipeline;
- torn spool lines (hook died mid-write) are deferred and parsed once
  the line completes.

The hook path gives lower latency but requires modifying Claude Code
configuration. The transcript path is zero-touch; prefer it.

## Analyzer (`crates/analyzer`)

Pure functions over ordered event streams. Every finding is an
`Anomaly` with a kind, a severity, a confidence, a human-readable
message that names its evidence, and a machine-readable `details`
block.

The guarantee that shapes the whole crate: **a healthy stream
produces zero findings** (ANA-1). A detector that cannot distinguish
"broken" from "unusual but legal" is noise, not observability. This
is why:

- tool pairing matches by `tool_call_id` first (exact, parallel-safe),
  falling back to tool name only when ids are absent;
- parallel identical tool calls (2× `ToolCall: Read`) are legal and
  never flagged (ANA-2);
- duplicate-*content* findings are `Probable`, never `Certain` —
  identical events can have benign explanations;
- out-of-order timestamps are `Info` — clock skew is normal life.

## CLI (`crates/sentinel`)

Commands map to lifecycle: `import-transcripts` / `import-spool`
(one-shot ingestion), `tail` / `watch` (live daemons), `sessions` /
`report` / `audit` (reading and analysis). `demo` generates a
synthetic session through the *real* pipeline — it doubles as the
conformance harness seed and as a smoke test of the whole stack.

## Testing strategy

- **Unit tests per crate** pin the contract behaviors (90 total).
- **Property tests** (`proptest`) fuzz the parser: arbitrary garbage
  must never panic or error the pipeline, only produce `Unknown`.
- **Integration tests on real data**: the transcript crate's
  `real_transcripts` test runs against the developer's actual
  `~/.claude/projects` when present (silently skipped in CI) — it
  asserts no orphan tool results and full-line parsability on live
  data. Format changes in Claude Code fail the test suite before
  they fail a user.
- **Invariants as tests**: `INVARIANTS.md` maps every invariant to
  the test that enforces it. Changing pipeline behavior without
  updating the invariant (or breaking it silently) is a build break,
  not a code-review conversation.

## Extending: adding Codex

A Codex adapter is one new crate implementing the same shape:
parse `rollout-trace` / Codex hook output → `AgentEvent` with
`provider: Codex` and deterministic session correlation. The
pipeline, store and analyzer need no changes — unknown providers are
already a legal value, and the analyzer's detectors are
provider-agnostic by construction (their guards are keyed on event
kinds and payload fields, not provider identity).
