//! The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".
//!
//! The file reads top to bottom in the same groups as the producer of every other language:
//!  1. settings      what this producer was asked to do
//!  2. counters      what the report shows, shared by the loop, the delivery callbacks and the reporter
//!  3. client        the Kafka producer and its delivery reports
//!  4. message       how one message is built and handed to the client
//!  5. run           the steps in order (the sending loop is the part shown on the Code page)

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::ClientContext;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::logic::{build_header, now_ms, pacing_chunk, pacing_delay, user_key};
use crate::reporting::{now_seconds, post, start_reporter, STOPPING};
use crate::settings::{client_config, get, required};

/// Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing.
const POOL_BYTES: usize = 1 << 20;

fn env_int(name: &str) -> usize {
    match required(name).parse() {
        Ok(n) => n,
        Err(_) => panic!("{name} is not a number"),
    }
}

// ---- 1. settings -------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct ProducerSettings {
    total: usize,         // this producer's share of the messages
    size: usize,          // bytes per message
    partitions: usize,    // partitions of the queue
    index: usize,         // which producer this is (0, 1, ...) when several share the load
    rate: f64,            // this producer's target messages per second; 0 = as fast as possible
    random_payload: bool, // incompressible data instead of a repeated filler
    users: usize,         // how many different users (message keys) the messages are spread over
}

fn read_settings() -> ProducerSettings {
    let rate = match required("RATE").parse() {
        Ok(rate) => rate,
        Err(_) => panic!("RATE is not a number"),
    };
    ProducerSettings {
        total: env_int("MESSAGES"),
        size: env_int("MSG_SIZE"),
        partitions: env_int("PARTITIONS"),
        index: env_int("PRODUCER_INDEX"),
        rate,
        random_payload: required("PAYLOAD") == "random",
        users: env_int("USERS"),
    }
}

// ---- 2. counters -------------------------------------------------------------------------------------------------

#[derive(Default)]
struct CounterValues {
    sent: usize,
    delivered: usize,
    errors: usize,
    done: bool,
    started: Option<f64>,  // seconds since 1970; None = not yet
    finished: Option<f64>, // seconds since 1970; None = not yet
}

/// The numbers of the report. One lock guards them all, because the loop, the delivery callbacks and the reporter
/// each run on their own thread.
#[derive(Default)]
struct Counters {
    values: Mutex<CounterValues>,
}

impl Counters {
    fn set_sent(&self, n: usize) {
        self.values.lock().unwrap().sent = n;
    }

    /// Called for every message once the broker confirmed or rejected it.
    fn confirmed(&self, failed: bool) {
        let mut values = self.values.lock().unwrap();
        if failed {
            values.errors += 1;
        } else {
            values.delivered += 1;
        }
    }

    fn start(&self) {
        self.values.lock().unwrap().started = Some(now_seconds());
    }

    fn finish(&self) {
        let mut values = self.values.lock().unwrap();
        values.finished = Some(now_seconds());
        values.done = true;
    }

    fn running(&self) -> bool {
        !self.values.lock().unwrap().done
    }

    fn report(&self) {
        let payload = {
            let values = self.values.lock().unwrap();
            json!({
                "runId": get().run_id, "worker": get().worker_id, "role": "producer", "lang": "rust", "ts": now_seconds(),
                "sent": values.sent, "delivered": values.delivered, "errors": values.errors, "done": values.done,
                "started": values.started, "finished": values.finished,
            })
        }; // the lock is released here, so the HTTP request never holds up the sending loop
        post(&payload);
    }
}

// ---- 3. client ---------------------------------------------------------------------------------------------------

/// Called by the client for every message once the broker confirmed or rejected it (the delivery report).
struct DeliveryReports {
    counters: Arc<Counters>,
}

impl ClientContext for DeliveryReports {}

impl ProducerContext for DeliveryReports {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        self.counters.confirmed(result.is_err());
    }
}

fn new_kafka_producer(counters: Arc<Counters>) -> BaseProducer<DeliveryReports> {
    // Batching, acks, queue size and compression come from the backend (the same values for every language).
    client_config("PRODUCER_CONF")
        .create_with_context(DeliveryReports { counters })
        .expect("could not create the producer")
}

// ---- 4. message --------------------------------------------------------------------------------------------------

/// One message, ready to hand to the client.
struct Message {
    part: usize,
    key: String,
    payload: Vec<u8>,
}

/// Builds message number i of this producer: which partition, which user, and the bytes to send.
/// It owns what has to be remembered between messages: the sequence counter of every partition.
struct MessageMaker {
    cfg: ProducerSettings,
    seqs: Vec<u64>, // each partition has its own sequence counter for this producer, starting at 0
    pool: Vec<u8>, // the data random padding is cut from (empty when the padding is a repeated filler)
}

impl MessageMaker {
    fn new(cfg: &ProducerSettings) -> MessageMaker {
        let mut pool = Vec::new();
        if cfg.random_payload {
            pool = vec![0u8; POOL_BYTES + cfg.size];
            getrandom::fill(&mut pool).expect("random bytes");
        }
        MessageMaker {
            cfg: cfg.clone(),
            seqs: vec![0; cfg.partitions],
            pool,
        }
    }

    fn make(&mut self, i: usize) -> Message {
        let part = (i + self.cfg.index) % self.cfg.partitions; // round robin; producers start on different partitions so the load stays even
        let header = build_header(
            &get().run_id,
            self.cfg.index,
            part,
            self.seqs[part],
            now_ms(),
        );
        self.seqs[part] += 1;
        Message {
            part,
            key: user_key(part, i, self.cfg.partitions, self.cfg.users), // the user this message belongs to
            payload: self.pad(header, i),
        }
    }

    /// The header (who sent it, where, which number, when) padded up to the requested message size.
    fn pad(&self, header: Vec<u8>, i: usize) -> Vec<u8> {
        let mut payload = header;
        let fill = self.cfg.size.saturating_sub(payload.len());
        if self.cfg.random_payload {
            let offset = (i * 997) % POOL_BYTES;
            payload.extend_from_slice(&self.pool[offset..offset + fill]);
        } else {
            payload.resize(payload.len() + fill, b'x');
        }
        payload
    }
}

/// Hands a message to the client; when the local queue is full it waits for the client to drain it.
fn produce(producer: &BaseProducer<DeliveryReports>, message: &Message) {
    let mut record = BaseRecord::<str, [u8]>::to(&get().topic)
        .partition(message.part as i32)
        .key(message.key.as_str())
        .payload(message.payload.as_slice());
    loop {
        match producer.send(record) {
            Ok(()) => return,
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                producer.poll(Duration::from_millis(50));
                record = returned;
            }
            Err((error, _)) => panic!("produce failed: {error}"),
        }
    }
}

// ---- 5. run ------------------------------------------------------------------------------------------------------

pub fn run() {
    let cfg = read_settings();
    let counters = Arc::new(Counters::default());
    let producer = new_kafka_producer(counters.clone());
    let mut maker = MessageMaker::new(&cfg);
    start_reporter(
        {
            let counters = counters.clone();
            move || counters.report()
        },
        {
            let counters = counters.clone();
            move || counters.running()
        },
    );

    // @producer-start
    let begin = Instant::now();
    counters.start();
    let check_every = pacing_chunk(cfg.rate, 500); // how often to look at the clock and serve delivery callbacks
    for i in 0..cfg.total {
        if STOPPING.load(Ordering::Relaxed) {
            break;
        }
        let message = maker.make(i); // message number i: its partition, its user and its bytes
        produce(&producer, &message);
        counters.set_sent(i + 1);
        if (i + 1) % check_every == 0 {
            producer.poll(Duration::ZERO); // serve delivery callbacks regularly
            let wait = pacing_delay((i + 1) as u64, cfg.rate, begin.elapsed()); // ahead of the target rate? then wait
            if !wait.is_zero() {
                std::thread::sleep(wait);
            }
        }
    }
    // @producer-end
    let _ = producer.flush(Duration::from_secs(60)); // wait until every message is confirmed by the broker
    counters.finish();
    counters.report();
}
