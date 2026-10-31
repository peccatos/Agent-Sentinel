//! Claude Code adapter: maps Claude Code hook payloads onto the
//! provider-agnostic event contract.
//!
//! Two parts:
//! - the `sentinel-hook` binary (see `src/bin/`) — installed as a
//!   Claude Code hook, it spools raw payloads to JSONL files;
//! - this lib — parsing/mapping of those payloads into
//!   [`AgentEvent`]s (Phase-2 scope: spool files; the daemon that
//!   watches and ingests them lives in the `sentinel` crate).

pub mod hook_input;
pub mod mapping;
pub mod spool;

pub use hook_input::{spool_dir, ClaudeHookName, HookInput};
pub use mapping::{claude_session_uuid, map_hook_input, ClaudeHookEvent};
pub use spool::{parse_spool_bytes, session_uuid_for_file};
