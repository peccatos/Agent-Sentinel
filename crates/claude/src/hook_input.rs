//! Raw Claude Code hook input (stdin JSON), parsed leniently.
//!
//! INV-1 applies at the adapter boundary too: a hook payload we do
//! not understand must not be dropped — it degrades to
//! `ClaudeHookEvent::Unknown` with the raw payload preserved.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Claude Code hook event names (the `hook_event_name` field).
/// Values observed in the wild map onto this set; anything else
/// degrades to `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ClaudeHookName {
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    Notification,
    Stop,
    SubagentStop,
    PreCompact,
    SessionStart,
    SessionEnd,
    Unknown,
}

/// A raw hook payload as Claude Code writes it to the hook's stdin.
///
/// Claude Code's exact field set varies by version; every field is
/// optional at parse time and validated/normalized in `mapping`.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct HookInput {
    /// Which hook fired, e.g. "PreToolUse".
    #[serde(default, alias = "hook_event_name")]
    pub hook_event_name: Option<String>,
    /// Opaque session identifier.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Opaque transcript path.
    #[serde(default)]
    pub transcript_path: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    /// Tool name for PreToolUse/PostToolUse.
    #[serde(default, alias = "tool_name")]
    pub tool_name: Option<String>,
    /// Tool input/output payload.
    #[serde(default, alias = "tool_input")]
    pub tool_input: Option<serde_json::Value>,
    #[serde(default, alias = "tool_response")]
    pub tool_response: Option<serde_json::Value>,
    /// Hook configurations / settings snapshot (usually redacted upstream).
    #[serde(default)]
    pub permission_mode: Option<String>,
    /// Everything else, preserved verbatim (INV-7).
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum HookParseError {
    #[error("input is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("input is not a JSON object: {0}")]
    NotAnObject(String),
}

impl HookInput {
    pub fn parse(json: &str) -> Result<Self, HookParseError> {
        let v: serde_json::Value =
            serde_json::from_str(json).map_err(|e| HookParseError::InvalidJson(e.to_string()))?;
        Self::from_value(v)
    }

    pub fn from_value(v: serde_json::Value) -> Result<Self, HookParseError> {
        let obj = match v {
            serde_json::Value::Object(map) => map,
            other => {
                return Err(HookParseError::NotAnObject(format!("{other:?}")));
            }
        };
        // Destructure through a round-trip so `#[serde(flatten)] extra`
        // picks up unknown fields.
        serde_json::from_value(serde_json::Value::Object(obj))
            .map_err(|e| HookParseError::InvalidJson(e.to_string()))
    }

    pub fn hook_name(&self) -> ClaudeHookName {
        self.hook_event_name
            .as_deref()
            .and_then(|s| serde_json::from_value(serde_json::json!(s)).ok())
            .unwrap_or(ClaudeHookName::Unknown)
    }

    /// The full raw payload, reconstructed (known + extra fields).
    pub fn raw(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(map) = &mut v {
            // The "Unknown" fallback shouldn't leak a bogus event name.
            if self.hook_event_name.is_none() {
                map.remove("hook_event_name");
            }
            if self.hook_event_name.is_some() {
                map.insert(
                    "hook_event_name".into(),
                    serde_json::json!(self.hook_event_name),
                );
            }
        }
        v
    }
}

impl ClaudeHookName {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClaudeHookName::UserPromptSubmit => "UserPromptSubmit",
            ClaudeHookName::PreToolUse => "PreToolUse",
            ClaudeHookName::PostToolUse => "PostToolUse",
            ClaudeHookName::Notification => "Notification",
            ClaudeHookName::Stop => "Stop",
            ClaudeHookName::SubagentStop => "SubagentStop",
            ClaudeHookName::PreCompact => "PreCompact",
            ClaudeHookName::SessionStart => "SessionStart",
            ClaudeHookName::SessionEnd => "SessionEnd",
            ClaudeHookName::Unknown => "Unknown",
        }
    }
}

/// Default spool directory: `<data-dir>/agent-sentinel/spool`.
/// Overridable via the `SENTINEL_SPOOL_DIR` env var (used by tests
/// and non-standard installs).
pub fn spool_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("SENTINEL_SPOOL_DIR") {
        return PathBuf::from(dir);
    }
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("agent-sentinel")
        .join("spool")
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRE_TOOL_USE: &str = r#"{
        "hook_event_name": "PreToolUse",
        "session_id": "abc123",
        "tool_name": "Bash",
        "tool_input": {"command": "cargo test"},
        "cwd": "/repo"
    }"#;

    #[test]
    fn parses_pre_tool_use() {
        let input = HookInput::parse(PRE_TOOL_USE).unwrap();
        assert_eq!(input.hook_name(), ClaudeHookName::PreToolUse);
        assert_eq!(input.tool_name.as_deref(), Some("Bash"));
        assert_eq!(input.session_id.as_deref(), Some("abc123"));
        assert_eq!(
            input.tool_input.as_ref().unwrap()["command"],
            "cargo test"
        );
    }

    #[test]
    fn unknown_hook_name_degrades_not_fails() {
        // INV-1: a hook event we've never seen must not break parsing.
        let input = HookInput::parse(
            r#"{"hook_event_name": "SomeNewFutureHook", "session_id": "s"}"#,
        )
        .unwrap();
        assert_eq!(input.hook_name(), ClaudeHookName::Unknown);
        // hook_event_name was present (but unknown); verify it round-trips.
        assert_eq!(
            input.hook_event_name.as_deref(),
            Some("SomeNewFutureHook")
        );
    }

    #[test]
    fn extra_fields_survive_roundtrip() {
        // INV-7: unknown fields are preserved verbatim.
        let input = HookInput::parse(
            r#"{"hook_event_name": "Stop", "session_id": "s", "stop_hook_active": true}"#,
        )
        .unwrap();
        assert_eq!(
            input.raw()["stop_hook_active"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn empty_input_is_an_object_not_an_error() {
        let input = HookInput::parse("{}").unwrap();
        assert_eq!(input.hook_name(), ClaudeHookName::Unknown);
        assert!(input.session_id.is_none());
    }

    #[test]
    fn non_object_input_is_rejected_cleanly() {
        assert!(matches!(
            HookInput::parse("[1,2,3]"),
            Err(HookParseError::NotAnObject(_))
        ));
    }
}
