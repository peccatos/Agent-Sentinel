# Agent Sentinel

**Local agent observability & reliability runtime for AI coding agents.**

Rust · Tokio · SQLite · JSONL

Agent Sentinel watches what Claude Code actually does inside a session —
which tools ran, what they returned, which errors were retried, where
events were lost — and answers the question every agent user eventually
asks: *"what exactly happened there, and can I trust it?"*

It is not another chat viewer. It is an **evidence pipeline**: a
normalized event contract, a durable store, and an analyzer that flags
anomalies in agent sessions the same way a linter flags code.

```
$ sentinel report <session>

SESSION: dc03f2d1-…
PROVIDER: Claude Code
STATUS: Completed
EVENTS: 499

Timeline:
  [15:32:34] SessionStarted
  [15:32:34] UserPrompt
  [15:42:10] UserPrompt
  [15:42:11] Unknown            ← api_error, retry 1/10 (kept, not a close)
  …
  [16:14:02] ToolCall: Bash
  [16:14:03] ToolResult

Diagnostics:
  - 191 tool calls
  - anomalies (6):
    ! [Warning/Probable] ToolCall 'Bash' at #497 has no ToolResult
      — the result was lost or the tool hung
```

## How it observes (out-of-band, by design)

Claude Code already writes every session to a transcript file under
`~/.claude/projects/`. Agent Sentinel reads those files. That's it.

- **The agent is never modified.** No hooks, no config, no process
  interference. If Sentinel crashes, Claude Code does not even notice.
  (A hook-based adapter also exists in the codebase, but the
  transcript path is the recommended one.)
- **History included.** Everything already on disk can be imported —
  you don't start observing from zero.
- **Read-only.** The adapter cannot corrupt the agent's state by
  construction. Same philosophy as Codex `rollout-trace`.

The Codex adapter is planned (Phase 3 of the roadmap) with the same
out-of-band approach.

## Quick start

```bash
cargo build --release

# Index all existing Claude Code sessions (idempotent — rerun anytime)
./target/release/sentinel import-transcripts --db sentinel.db

# See what happened
./target/release/sentinel sessions --db sentinel.db
./target/release/sentinel report --db sentinel.db <session-id>
./target/release/sentinel audit --db sentinel.db --severity warning

# Live observation (runs until Ctrl-C)
./target/release/sentinel tail --db sentinel.db
```

`SENTINEL_PROJECTS_DIR` overrides the projects directory (default
`~/.claude/projects`).

## What the analyzer catches

| Detector | Flags |
|---|---|
| Lifecycle FSM | events before session start, double starts, activity after close, sessions that never close |
| Event ids | duplicate delivery (hard invariant violation at the source) |
| Event content | identical kind+name+payload repeated (retried writes, duplicated lines) |
| Timestamps | out-of-order events (clock skew, replay) |
| Tool pairing | `ToolCall` without `ToolResult` (result lost / tool hung) and vice versa — matched by `tool_call_id`, parallel-safe |
| Permissions | resolutions without requests |

Every finding carries **severity** (info / warning / critical) and
**confidence** (certain / probable / possible), and names the exact
event it points at. Healthy streams are guaranteed to produce zero
findings (ANA-1) — the test suite enforces it.

## Architecture

```
  Claude Code transcripts          Claude Code hooks (optional)
  ~/.claude/projects/**.jsonl      spool dir
          │                              │
          └──────────┬───────────────────┘
                     │
          Provider Adapters               ← crates/transcript, crates/claude
                     │
          Event Normalizer (redaction)     ← crates/ingest
                     │
          Durable Store (SQLite + WAL)     ← crates/store
                     │
          Reliability Analyzer             ← crates/analyzer
                     │
          CLI                              ← crates/sentinel
```

| Crate | Responsibility |
|---|---|
| `core` | The `AgentEvent` contract: UUIDv7 ids, providers, lifecycle kinds, secret redaction |
| `ingest` | Bounded-queue pipeline: backpressure instead of drops, ingestion-time authority, redaction before persistence |
| `store` | SQLite persistence with a write-ahead journal as the durability boundary; crash recovery by journal replay |
| `claude` | Hook-input adapter: parsing, session correlation, spool ingestion |
| `transcript` | Out-of-band transcript adapter: tolerant parsing, bookkeeping filtering, synthetic session bracketing |
| `analyzer` | Anomaly detectors over the event stream |
| `sentinel` | CLI binary and the `tail` / `watch` daemons |

Provider adapters know nothing about storage or analysis — the event
contract is the only seam. Adding a Codex adapter means adding one
crate, not touching the pipeline.

## Invariants

The pipeline's behavior is pinned by documented, test-enforced
invariants ([INVARIANTS.md](INVARIANTS.md)). The important ones:

- **Nothing is ever rejected for content.** Unknown event kinds,
  unknown providers, unparsable lines — everything degrades to
  `Unknown`, nothing is silently dropped.
- **Every accepted event is durably recorded** before it is
  acknowledged. Backpressure blocks instead of dropping; a crash
  before the SQLite commit is recovered from the journal.
- **Secrets are redacted before persistence** — one choke point in
  the pipeline, never in the adapters.
- **Import is idempotent.** Event ids are deterministic (UUIDv5 over
  line bytes); re-importing the same transcripts never duplicates.
- **`api_error` never closes a session.** Real data shows sessions
  continuing after a fully-exhausted retry chain; closure is always
  derived from file state, not from error entries.

90 tests across the workspace enforce this.

## Roadmap

- [x] **Phase 1 — Event foundation**: contract, pipeline, store
- [x] **Phase 2 — Claude Code adapter**: transcripts (out-of-band) + hooks
- [x] **Phase 3/4 — Reliability analyzer**: lifecycle, pairing, duplicates
- [ ] **Phase 5 — Policy engine**: TOML policies — forbidden commands, protected paths, required checks
- [ ] **Phase 6 — Developer interface**: TUI timeline
- [ ] **Codex adapter**: rollout-trace ingestion, same event contract
- [ ] Session graph reconstruction, execution replay, failure fingerprints

## License

MIT
