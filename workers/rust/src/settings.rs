//! Everything a worker reads from its environment, parsed once at start-up.
//!
//! The environment is the "worker contract" defined in the backend (backend/app/contract.py). Kafka client tuning
//! (batching, acks, timeouts) is NOT decided here: the backend sends the same values to every language
//! (PRODUCER_CONF / CONSUMER_CONF), so comparisons between languages are fair.

use rdkafka::config::ClientConfig;
use serde_json::Value;
use std::sync::OnceLock;

use crate::logic::parse_edges;

pub struct Settings {
    /// "producer" or "consumer".
    pub role: String,
    /// Broker address.
    pub boot: String,
    /// The queue.
    pub topic: String,
    /// Id of this run; messages of other runs are ignored.
    pub run_id: String,
    /// Container name, identifies this worker in reports.
    pub worker_id: String,
    /// Where the once-per-second report is POSTed.
    pub report_url: String,
    /// Upper edge of each latency bucket in milliseconds; the backend sends them so they are defined only once.
    pub edges: Vec<f64>,
    /// How many messages one consumer handles at the same time (consumer only).
    pub concurrency: usize,
    /// "key": one user stays in one lane (in order); "none": any lane (consumer only).
    pub ordering: String,
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// Read a required environment variable or stop with a clear message.
pub fn required(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => panic!("Missing environment variable {name}"),
    }
}

/// Read the environment once. Called from main, so the unit tests need no environment.
pub fn init() {
    let settings = Settings {
        role: required("ROLE"),
        boot: required("KAFKA_BOOTSTRAP"),
        topic: required("TOPIC"),
        run_id: required("RUN_ID"),
        worker_id: std::env::var("WORKER_ID").unwrap_or_else(|_| "worker".to_string()),
        report_url: required("REPORT_URL"),
        edges: parse_edges(&required("HIST_EDGES")),
        ordering: std::env::var("ORDERING").unwrap_or_else(|_| "key".to_string()),
        concurrency: std::env::var("CONCURRENCY").ok().and_then(|v| v.parse().ok()).filter(|n| *n >= 1).unwrap_or(1),
    };
    let _ = SETTINGS.set(settings);
}

pub fn get() -> &'static Settings {
    SETTINGS.get().expect("settings are read in main")
}

/// The Kafka client config: the broker address plus the properties the backend sent as JSON (librdkafka names).
pub fn client_config(variable: &str) -> ClientConfig {
    let raw: Value = serde_json::from_str(&required(variable)).unwrap_or_else(|e| panic!("{variable} is not valid JSON: {e}"));
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &get().boot);
    for (key, value) in raw.as_object().expect("a JSON object") {
        let text = match value {
            Value::String(s) => s.clone(),
            other => other.to_string(), // numbers and booleans
        };
        config.set(key, text);
    }
    config
}