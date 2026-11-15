//! What a consumer counts about the messages it reads, and the report it sends once per second.

use rdkafka::consumer::{BaseConsumer, Consumer};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::logic::{bucket, Header, OrderCheck, SeqRanges};
use crate::reporting::{now_seconds, post};
use crate::settings::get;

/// Every counter of one consumer. The reporter thread reads it, so it lives behind a mutex.
pub struct Stats {
    received: u64,
    errors: u64,
    hist: Vec<u64>,
    offsets: HashMap<String, i64>,
    first_at: Option<f64>,
    last_at: Option<f64>,
    max_latency_ms: f64,
    seen: SeqRanges, // which message numbers arrived: the lab counts lost and repeated ones from it
    /// The messages of a user must be handled in the order they were sent.
    pub order: Arc<OrderCheck>,
    batch_busy_ms: f64, // time spent handling batches
    batch_wait_ms: f64, // of which some groups were done and the others still worked
}

impl Stats {
    pub fn new() -> Arc<Mutex<Stats>> {
        Arc::new(Mutex::new(Stats {
            received: 0,
            errors: 0,
            hist: vec![0; get().edges.len() + 1],
            offsets: HashMap::new(),
            first_at: None,
            last_at: None,
            max_latency_ms: 0.0,
            seen: SeqRanges::default(),
            order: Arc::new(OrderCheck::default()),
            batch_busy_ms: 0.0,
            batch_wait_ms: 0.0,
        }))
    }

    /// Remember how long a batch took and how much of it was spent waiting for the slowest group.
    pub fn add_batch(&mut self, busy_ms: f64, wait_ms: f64) {
        self.batch_busy_ms += busy_ms;
        self.batch_wait_ms += wait_ms;
    }

    pub fn count_error(&mut self) {
        self.errors += 1;
    }

    /// Count one message of this run that arrived at `arrived_ms` in `partition`.
    pub fn record(&mut self, header: &Header, partition: i32, offset: i64, arrived_ms: f64) {
        self.received += 1;
        self.first_at.get_or_insert(arrived_ms / 1000.0);
        self.last_at = Some(now_seconds());
        self.seen
            .observe(header.producer, partition as usize, header.seq);
        self.offsets.insert(partition.to_string(), offset + 1); // the next offset to read, used for the lag
        let latency = arrived_ms - header.sent_ms; // from "sent" to "received"
        self.hist[bucket(latency, &get().edges)] += 1;
        self.max_latency_ms = self.max_latency_ms.max(latency);
    }
}

/// Send the cumulative counters. The histogram goes as the change since the last report, then it is reset.
pub fn report(state: &Mutex<Stats>, consumer: &BaseConsumer, final_report: bool) {
    let mut assigned: Vec<i32> = consumer
        .assignment()
        .map(|list| list.elements().iter().map(|e| e.partition()).collect())
        .unwrap_or_default();
    assigned.sort_unstable();
    let mut s = state.lock().unwrap();
    let payload = json!({
        "runId": get().run_id, "worker": get().worker_id, "role": "consumer", "lang": "rust", "ts": now_seconds(),
        "ready": !assigned.is_empty(), "assigned": assigned, "received": s.received,
        "duplicates": s.seen.repeats, "ranges": s.seen.report(), "overflow": s.seen.overflow,
        "orderViolations": s.order.violations(), "batchBusyMs": (s.batch_busy_ms * 10.0).round() / 10.0,
        "batchWaitMs": (s.batch_wait_ms * 10.0).round() / 10.0, "errors": s.errors, "offsets": s.offsets,
        "hist": s.hist, "final": final_report, "firstAt": s.first_at, "lastAt": s.last_at,
        "maxLatencyMs": s.max_latency_ms,
    });
    s.hist.iter_mut().for_each(|b| *b = 0);
    drop(s);
    post(&payload);
}
