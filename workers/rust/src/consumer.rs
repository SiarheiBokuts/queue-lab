//! The consumer (the "handler"): reads messages in a consumer group, verifies them, runs the chosen scenario.

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::logic::{bucket, lane_for, now_ms, parse_header, SeqTracker};

/// Messages that may wait in front of one lane before the reading loop pauses.
const LANE_QUEUE: usize = 8;
use crate::reporting::{now_seconds, post, start_reporter, STOPPING};
use crate::settings::{client_config, get, required};

struct State {
    received: u64,
    errors: u64,
    hist: Vec<u64>,
    offsets: HashMap<String, i64>,
    first_at: Option<f64>,
    last_at: Option<f64>,
    max_latency_ms: f64,
    tracker: SeqTracker,
}

// @scenario-start
/// What the worker does with one message. Several run at the same time when the concurrency is above 1.
fn handle(scenario: &str, param: u64) {
    if scenario == "delay" && param > 0 {
        std::thread::sleep(Duration::from_millis(param)); // pretend to wait for a database or an API call
    } else if scenario == "cpu" && param > 0 {
        let mut hash = Sha256::digest(b"queuelab");
        for _ in 0..param {
            hash = Sha256::digest(hash); // burn CPU like parsing or encryption would
        }
        std::hint::black_box(hash);
    }
    // echo: do nothing, just count the message
}
// @scenario-end
pub fn run() {
    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "echo".to_string());
    let param: u64 = std::env::var("SCENARIO_PARAM").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let edges = &get().edges;
    let state = Arc::new(Mutex::new(State {
        received: 0,
        errors: 0,
        hist: vec![0; edges.len() + 1],
        offsets: HashMap::new(),
        first_at: None,
        last_at: None,
        max_latency_ms: 0.0,
        tracker: SeqTracker::default(), // detects gaps and duplicates per stream
    }));

    // Offset reset, commit and timeout settings come from the backend (the same values for every language).
    let mut config = client_config("CONSUMER_CONF");
    config.set("group.id", required("GROUP_ID"));
    let consumer: Arc<BaseConsumer> = Arc::new(config.create().expect("could not create the consumer"));
    consumer.subscribe(&[&get().topic]).expect("could not subscribe");

    // Send cumulative counters. The histogram is sent as a delta and reset after each report.
    let report = {
        let (state, consumer) = (state.clone(), consumer.clone());
        move |final_report: bool| {
            let mut assigned: Vec<i32> = consumer
                .assignment()
                .map(|list| list.elements().iter().map(|e| e.partition()).collect())
                .unwrap_or_default();
            assigned.sort_unstable();
            let mut s = state.lock().unwrap();
            let payload = json!({
                "runId": get().run_id, "worker": get().worker_id, "role": "consumer", "lang": "rust", "ts": now_seconds(),
                "ready": !assigned.is_empty(), "assigned": assigned, "received": s.received,
                "duplicates": s.tracker.duplicates, "gaps": s.tracker.gaps, "errors": s.errors, "offsets": s.offsets,
                "hist": s.hist, "final": final_report, "firstAt": s.first_at, "lastAt": s.last_at,
                "maxLatencyMs": s.max_latency_ms,
            });
            s.hist.iter_mut().for_each(|b| *b = 0);
            drop(s);
            post(&payload);
        }
    };
    let periodic = report.clone();
    start_reporter(move || periodic(false), || true);

    // Production style: ONE loop reads from Kafka and hands every message to a "lane". There are `concurrency` lanes, each
    // is a thread that handles its messages one after another, so up to `concurrency` messages are handled at the same
    let lane_count = get().concurrency;
    let mut lanes = Vec::new();
    let mut threads = Vec::new();
    if lane_count > 1 && scenario != "echo" {
        for _ in 0..lane_count {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(LANE_QUEUE);
            lanes.push(sender);
            let scenario = scenario.clone();
            threads.push(std::thread::spawn(move || {
                while receiver.recv().is_ok() {
                    handle(&scenario, param);
                }
            }));
        }
    }
    let mut next_lane = 0usize;
    let mut dispatch = |key: &[u8]| {
        if lanes.is_empty() {
            handle(&scenario, param); // concurrency 1 (or nothing to wait for): one at a time
            return;
        }
        let index = if get().ordering == "key" { lane_for(key, lanes.len()) } else { next_lane % lanes.len() };
        next_lane += 1;
        let _ = lanes[index].send(()); // blocks while that lane is full
    };
    while !STOPPING.load(Ordering::Relaxed) {
        let Some(polled) = consumer.poll(Duration::from_millis(100)) else { continue };
        let arrived = now_ms();
        let message = match polled {
            Ok(message) => message,
            Err(_) => {
                state.lock().unwrap().errors += 1;
                continue;
            }
        };
        let Some(header) = message.payload().and_then(|p| parse_header(p, &get().run_id)) else {
            continue; // a leftover from another run: ignore it
        };
        {
            let mut s = state.lock().unwrap();
            s.received += 1;
            s.first_at.get_or_insert(arrived / 1000.0);
            s.last_at = Some(now_seconds());
            s.tracker.observe(header.producer, message.partition() as usize, header.seq); // counts gaps (lost) and duplicates
            s.offsets.insert(message.partition().to_string(), message.offset() + 1); // the next offset to read, used for lag
            let latency = arrived - header.sent_ms; // time from "sent" to "received", in milliseconds
            let slot = bucket(latency, edges);
            s.hist[slot] += 1;
            s.max_latency_ms = s.max_latency_ms.max(latency);
        }
        dispatch(message.key().unwrap_or(b""));
    }
    lanes.clear(); // no more work: each lane finishes what it holds and stops
    for thread in threads {
        let _ = thread.join();
    }
    report(true);
}
