//! Secret redaction. INV-6: secrets are removed **before** the event
//! reaches storage, at normalization time.

use crate::event::{AgentEvent, REDACTED};
use serde_json::{Map, Value};

/// Keys whose values are redacted on an **exact** (case-insensitive)
/// key match. Deliberately excludes bare "key" (too broad).
const SENSITIVE_KEYS: &[&str] = &[
    "api_key",
    "apikey",
    "authorization",
    "auth",
    "credentials",
    "credential",
    "private_key",
];

/// Key **fragments** that mark a key as sensitive when they appear as a
/// substring (case-insensitive). Unambiguous tokens only — "auth" is
/// exact-match-only so that fields like "author" are not clobbered.
/// Redaction errs on the side of over-redacting: a false positive
/// costs a field, a false negative leaks a secret.
const SENSITIVE_FRAGMENTS: &[&str] = &[
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
];

/// Max string length kept verbatim in payloads (512 KiB is plenty for
/// tool output; oversized blobs are truncated with a marker so the
/// pipeline cannot be blown up by a giant payload).
const MAX_FIELD_BYTES: usize = 512 * 1024;

const TRUNCATED: &str = "[TRUNCATED]";

/// Recursively redacts and truncates a JSON value in place-ish
/// (returns a new value; the input is never mutated).
pub fn redact_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                if is_sensitive_key(k) {
                    out.insert(k.clone(), Value::String(REDACTED.to_string()));
                } else {
                    out.insert(k.clone(), redact_value(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect()),
        Value::String(s) => {
            if s.len() > MAX_FIELD_BYTES {
                // Keep head, mark, and original length.
                let head = &s[..MAX_FIELD_BYTES];
                Value::String(format!("{TRUNCATED} {} bytes: {head}", s.len()))
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    SENSITIVE_KEYS.contains(&k.as_str())
        || SENSITIVE_FRAGMENTS.iter().any(|f| k.contains(f))
}

/// Redacts an event's payload (returns the event with payload replaced).
pub fn redact_event(mut ev: AgentEvent) -> AgentEvent {
    ev.payload = redact_value(&ev.payload);
    ev
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AgentProvider, EventKind};
    use serde_json::json;
    use uuid::Uuid;

    fn event_with(payload: Value) -> AgentEvent {
        AgentEvent::new(
            Uuid::new_v4(),
            AgentProvider::ClaudeCode,
            EventKind::ToolCall,
            payload,
        )
    }

    #[test]
    fn redacts_nested_sensitive_keys() {
        let ev = redact_event(event_with(json!({
            "tool": "Bash",
            "env": {"API_KEY": "sk-123", "PATH": "/usr/bin"},
            "nested": {"session": {"session_token": "t", "note": "ok"}}
        })));
        assert_eq!(ev.payload["env"]["API_KEY"], json!(REDACTED));
        assert_eq!(ev.payload["env"]["PATH"], json!("/usr/bin"));
        assert_eq!(ev.payload["nested"]["session"]["session_token"], json!(REDACTED));
        assert_eq!(ev.payload["nested"]["session"]["note"], json!("ok"));
    }

    #[test]
    fn auth_object_is_redacted_wholesale() {
        // "auth" as an exact key is sensitive even when it holds an
        // object — the whole subtree is replaced.
        let ev = redact_event(event_with(json!({"auth": {"type": "bearer"}})));
        assert_eq!(ev.payload["auth"], json!(REDACTED));
    }

    #[test]
    fn author_is_not_redacted() {
        // "auth" is exact-match-only: "author" must survive.
        let ev = redact_event(event_with(json!({"author": "Ada", "commit": "abc"})));
        assert_eq!(ev.payload["author"], json!("Ada"));
        assert_eq!(ev.payload["commit"], json!("abc"));
    }

    #[test]
    fn truncates_oversized_strings() {
        let big = "x".repeat(MAX_FIELD_BYTES + 100);
        let ev = redact_event(event_with(json!({ "output": big })));
        let s = ev.payload["output"].as_str().unwrap();
        assert!(s.starts_with(TRUNCATED));
        assert!(s.contains(&format!("{} bytes", MAX_FIELD_BYTES + 100)));
    }

    #[test]
    fn leaves_normal_payloads_untouched() {
        let payload = json!({"tool": "Edit", "file": "main.rs", "lines": 42});
        let ev = redact_event(event_with(payload.clone()));
        assert_eq!(ev.payload, payload);
    }

    #[test]
    fn key_match_is_case_insensitive() {
        let ev = redact_event(event_with(json!({"ApiToken": "x", "MY_PASSWORD": "y"})));
        assert_eq!(ev.payload["ApiToken"], json!(REDACTED));
        assert_eq!(ev.payload["MY_PASSWORD"], json!(REDACTED));
    }
}
