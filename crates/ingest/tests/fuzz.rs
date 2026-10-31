//! Property-based tests (INV-1, INV-2, INV-6): arbitrary JSON payloads
//! must flow through the pipeline without ever failing ingestion, and
//! secrets must never be persisted.
//!
//! proptest's `proptest!` macro does not compose with `#[tokio::test]`,
//! so we drive the proptest runner manually inside an async test.

use agent_sentinel_core::{AgentEvent, AgentProvider, EventKind};
use agent_sentinel_ingest::Pipeline;
use agent_sentinel_store::EventStore;
use proptest::prelude::*;
use uuid::Uuid;

const CASES: u32 = 64;

fn db_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fuzz.db");
    (dir, db)
}

/// Arbitrary JSON payloads, biased toward hostile shapes: deep
/// nesting, sensitive keys, nulls, arrays.
fn arb_payload() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(serde_json::Value::from),
        ".*".prop_map(serde_json::Value::String),
    ];
    leaf.prop_recursive(4, 16, 8, |inner| {
        prop_oneof![
            inner.clone().prop_map(|v| serde_json::json!({"field": v})),
            inner.clone().prop_map(|v| serde_json::json!({"token": v})),
            inner.clone().prop_map(|v| serde_json::json!({"API_KEY": v})),
            proptest::collection::vec(inner, 0..8).prop_map(serde_json::Value::Array),
        ]
    })
}

#[test]
fn arbitrary_payload_survives_pipeline() {
    // INV-1 + INV-2: any payload must be ingested and round-trip.
    // Each proptest case runs on its own single-threaded runtime.
    let result = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(CASES))
        .run(&arb_payload(), |payload| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TestCaseError::fail(e.to_string()))?
                .block_on(async { roundtrip_case(&payload).await })
                .map_err(TestCaseError::fail)
        });
    finish(result);
}

#[test]
fn sensitive_keys_never_survive() {
    // INV-6: no sensitive key retains a non-redacted value in storage.
    let result = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(CASES))
        .run(&arb_payload(), |payload| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TestCaseError::fail(e.to_string()))?
                .block_on(async { secrets_case(&payload).await })
                .map_err(TestCaseError::fail)
        });
    finish(result);
}

fn finish(result: Result<(), proptest::test_runner::TestError<serde_json::Value>>) {
    match result {
        Ok(()) => {}
        Err(e) => panic!("proptest failed: {e}"),
    }
}

async fn roundtrip_case(payload: &serde_json::Value) -> Result<(), String> {
    let (_dir, db) = db_path();
    let store = EventStore::open(&db).await.map_err(|e| e.to_string())?;
    let pipeline = Pipeline::start(store);
    let session = Uuid::new_v4();
    let ev = AgentEvent::new(
        session,
        AgentProvider::Unknown,
        EventKind::Unknown,
        payload.clone(),
    );
    let submit_ok = pipeline.submit(ev).await.is_ok();
    pipeline.shutdown().await.map_err(|e| e.to_string())?;

    let mut reader = EventStore::open(&db).await.map_err(|e| e.to_string())?;
    let events = reader
        .session_events(session)
        .await
        .map_err(|e| e.to_string())?;
    reader.close().await.map_err(|e| e.to_string())?;

    if !submit_ok {
        return Err("submit was rejected (INV-1 violation)".into());
    }
    if events.len() != 1 {
        return Err(format!("expected 1 stored event, got {}", events.len()));
    }
    Ok(())
}

async fn secrets_case(payload: &serde_json::Value) -> Result<(), String> {
    let (_dir, db) = db_path();
    let store = EventStore::open(&db).await.map_err(|e| e.to_string())?;
    let pipeline = Pipeline::start(store);
    let session = Uuid::new_v4();
    let ev = AgentEvent::new(session, AgentProvider::ClaudeCode, EventKind::ToolCall, payload.clone());
    pipeline.submit(ev).await.map_err(|e| e.to_string())?;
    pipeline.shutdown().await.map_err(|e| e.to_string())?;

    let mut reader = EventStore::open(&db).await.map_err(|e| e.to_string())?;
    let events = reader
        .session_events(session)
        .await
        .map_err(|e| e.to_string())?;
    reader.close().await.map_err(|e| e.to_string())?;
    let stored = events
        .first()
        .ok_or("no stored event")?
        .payload
        .clone();
    assert_no_secrets(&stored);
    Ok(())
}

fn assert_no_secrets(v: &serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map {
                let kl = k.to_ascii_lowercase();
                let sensitive = matches!(
                    kl.as_str(),
                    "api_key" | "apikey" | "token" | "authorization" | "auth" | "password" | "credential" | "credentials"
                ) || kl.contains("token")
                    || kl.contains("password")
                    || kl.contains("secret");
                if sensitive {
                    assert_eq!(
                        val, &serde_json::json!("[REDACTED]"),
                        "sensitive key {k:?} must be redacted, got {val}"
                    );
                } else {
                    assert_no_secrets(val);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(assert_no_secrets),
        _ => {}
    }
}
