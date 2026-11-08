//! The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::ClientContext;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::logic::{build_header, now_ms, pacing_chunk, pacing_delay, user_key};
use crate::reporting::{now_seconds, post, start_reporter, STOPPING};
use crate::settings::{client_config, get, required};

/// Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing.
const POOL_BYTES: usize = 1 << 20;

#[derive(Default)]
struct Counters {
    delivered: AtomicU64,
    errors: AtomicU64,
}

/// Called by the client for every message once the broker confirmed or rejected it.
struct Context(Arc<Counters>);

impl ClientContext for Context {}

impl ProducerContext for Context {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        let counter = if result.is_ok() { &self.0.delivered } else { &self.0.errors };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct State {
    sent: u64,
    done: bool,
    started: Option<f64>,
    finished: Option<f64>,
}

fn number<T: std::str::FromStr>(name: &str) -> T {
    required(name).parse().unwrap_or_else(|_| panic!("{name} is not a number"))
}

pub fn run() {
    let total: u64 = number("MESSAGES"); // this producer's share of the messages
    let size: usize = number("MSG_SIZE");
    let nparts: usize = number("PARTITIONS");
    let users: usize = number("USERS"); // how many different users (message keys) the messages are spread over
    let index: usize = number("PRODUCER_INDEX"); // which producer this is (0, 1, ...) when several share the load
    let rate: f64 = number("RATE"); // this producer's target messages per second; 0 = as fast as possible
    let random_payload = required("PAYLOAD") == "random"; // incompressible data instead of a repeated filler

    let counters = Arc::new(Counters::default());
    let state = Arc::new(Mutex::new(State::default()));
    // Batching, acks, queue size and compression come from the backend (the same values for every language).
    let producer: BaseProducer<Context> = client_config("PRODUCER_CONF")
        .create_with_context(Context(counters.clone()))
        .expect("could not create the producer");

    let mut pool = vec![0u8; if random_payload { POOL_BYTES + size } else { 0 }];
    if random_payload {
        getrandom::fill(&mut pool).expect("random bytes");
    }

    let report = {
        let (state, counters) = (state.clone(), counters.clone());
        move || {
            let s = state.lock().unwrap();
            post(&json!({
                "runId": get().run_id, "worker": get().worker_id, "role": "producer", "lang": "rust", "ts": now_seconds(),
                "sent": s.sent, "delivered": counters.delivered.load(Ordering::Relaxed),
                "errors": counters.errors.load(Ordering::Relaxed), "done": s.done, "started": s.started, "finished": s.finished,
            }));
        }
    };
    start_reporter(report.clone(), {
        let state = state.clone();
        move || !state.lock().unwrap().done
    });

    // @producer-start
    let mut seqs = vec![0u64; nparts]; // each partition has its own sequence counter for this producer, starting at 0
    let begin = Instant::now();
    state.lock().unwrap().started = Some(now_seconds());
    let check_every = pacing_chunk(rate, 500) as u64; // how often to look at the clock and serve delivery callbacks
    for i in 0..total {
        if STOPPING.load(Ordering::Relaxed) {
            break;
        }
        let part = (i as usize + index) % nparts; // round robin; producers start on different partitions so the load stays even
        let mut payload = build_header(&get().run_id, index, part, seqs[part], now_ms());
        seqs[part] += 1;
        let fill = size.saturating_sub(payload.len()); // pad up to the requested message size
        if random_payload {
            let offset = (i as usize * 997) % POOL_BYTES;
            payload.extend_from_slice(&pool[offset..offset + fill]);
        } else {
            payload.resize(payload.len() + fill, b'x');
        }
        let key = user_key(part, i as usize, nparts, users); // the user this message belongs to
        let mut record = BaseRecord::<str, Vec<u8>>::to(&get().topic).partition(part as i32).key(key.as_str()).payload(&payload);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    producer.poll(Duration::from_millis(50)); // the local queue is full: let the client drain it, then retry
                    record = returned;
                }
                Err((error, _)) => panic!("produce failed: {error}"),
            }
        }
        state.lock().unwrap().sent = i + 1;
        if (i + 1) % check_every == 0 {
            producer.poll(Duration::ZERO); // serve delivery callbacks regularly
            let wait = pacing_delay(i + 1, rate, begin.elapsed()); // ahead of the target rate? then wait
            if !wait.is_zero() {
                std::thread::sleep(wait);
            }
        }
    }
    // @producer-end
    let _ = producer.flush(Duration::from_secs(60)); // wait until every message is confirmed by the broker
    {
        let mut s = state.lock().unwrap();
        s.finished = Some(now_seconds());
        s.done = true;
    }
    report();
}