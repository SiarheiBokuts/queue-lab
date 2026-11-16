//! The consumer (the "handler"): reads messages in batches, verifies them and runs the chosen scenario.
//!
//! The same loop is written in every language of the lab, with the same steps and names:
//!
//!   read_batch -> group_by_user -> run_batch (handle_group for every user at the same time) -> commit
//!
//! A batch is at most `concurrency` messages. The messages of one user are always handled one after another, like in
//! production; the messages of different users are handled at the same time. The next batch is read when the whole
//! batch is done: the client has already fetched the next messages in the background, and starting early could let a
//! message of a busy user overtake an older one.

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::logic::{batch_timing, group_by_user, now_ms, parse_header, Item, OrderCheck};
use crate::reporting::{start_reporter, STOPPING};
use crate::settings::{client_config, get, required};
use crate::stats::{report, Stats};

/// A counter of free places: at most `cpu_threads` messages compute at once, like in every other language.
struct Slots {
    free: Mutex<usize>,
    freed: Condvar,
}

impl Slots {
    fn new(places: usize) -> Self {
        Slots {
            free: Mutex::new(places),
            freed: Condvar::new(),
        }
    }

    fn take(&self) {
        let mut free = self.free.lock().unwrap();
        while *free == 0 {
            free = self.freed.wait(free).unwrap();
        }
        *free -= 1;
    }

    fn give_back(&self) {
        *self.free.lock().unwrap() += 1;
        self.freed.notify_one();
    }
}

/// Wait at most `batch_wait_ms` for a message, then take up to `concurrency` messages that have already arrived.
/// It never waits to fill the batch.
fn read_batch<'a>(consumer: &'a BaseConsumer, stats: &Mutex<Stats>) -> Vec<BorrowedMessage<'a>> {
    let mut batch = Vec::new();
    let mut wait = Duration::from_millis(get().batch_wait_ms);
    while batch.len() < get().concurrency {
        match consumer.poll(wait) {
            Some(Ok(message)) => {
                batch.push(message);
                wait = Duration::ZERO; // after the first message only take what is already there
            }
            Some(Err(_)) => stats.lock().unwrap().count_error(),
            None => break,
        }
    }
    batch
}

// @scenario-start
/// What the worker does with one message. The same three scenarios in every language:
/// echo = nothing, the message is only counted; delay = wait, like a call to a database;
/// json = parse a JSON document and write it back, like a service reading a request.
fn handle(scenario: &str, param: u64, cpu: &Slots) {
    if scenario == "delay" && param > 0 {
        std::thread::sleep(Duration::from_millis(param)); // wait param milliseconds and do nothing else
    } else if scenario == "json" && param > 0 {
        cpu.take(); // only one message per core may work at once, so no language takes the cores of another
        let mut text = get().json_doc.clone();
        for _ in 0..param {
            // parse the document and write it back, param times
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(value) => text = value.to_string(),
                Err(_) => break,
            }
        }
        std::hint::black_box(text);
        cpu.give_back();
    }
    // echo: nothing to do
}
// @scenario-end

/// Handle the messages of one user one after another.
fn handle_group(
    group: &[Item],
    scenario: &str,
    param: u64,
    cpu: &Slots,
    order: &OrderCheck,
) -> Instant {
    for item in group {
        order.check(item);
        handle(scenario, param, cpu);
    }
    Instant::now() // the time this group was done
}

/// Handle all groups at the same time and wait until every one of them is done.
///
/// Returns how long the batch took and how long of that some groups were done while others still worked (milliseconds).
fn run_batch(
    groups: &[Vec<Item>],
    scenario: &str,
    param: u64,
    cpu: &Slots,
    order: &OrderCheck,
) -> (f64, f64) {
    let started = Instant::now();
    let done: Vec<Instant> = std::thread::scope(|scope| {
        let running: Vec<_> = groups
            .iter()
            .map(|group| scope.spawn(move || handle_group(group, scenario, param, cpu, order)))
            .collect();
        running.into_iter().map(|h| h.join().unwrap()).collect()
    });
    batch_timing(started, &done)
}

/// Tell Kafka that everything read so far is handled. A crash before this repeats the batch.
fn commit(consumer: &BaseConsumer) {
    if let Err(error) = consumer.commit_consumer_state(CommitMode::Sync) {
        eprintln!("commit failed: {error}");
    }
}

pub fn run() {
    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "echo".to_string());
    let param: u64 = std::env::var("SCENARIO_PARAM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let stats = Stats::new();

    // Offset reset, commit and timeout settings come from the backend (the same values for every language).
    let mut config = client_config("CONSUMER_CONF");
    config.set("group.id", required("GROUP_ID"));
    let consumer: Arc<BaseConsumer> =
        Arc::new(config.create().expect("could not create the consumer"));
    consumer
        .subscribe(&[&get().topic])
        .expect("could not subscribe");

    let periodic = (stats.clone(), consumer.clone());
    start_reporter(move || report(&periodic.0, &periodic.1, false), || true);

    let cpu = Slots::new(get().cpu_threads);
    let order = stats.lock().unwrap().order.clone();
    while !STOPPING.load(Ordering::Relaxed) {
        let batch = read_batch(&consumer, &stats);
        if batch.is_empty() {
            continue;
        }
        let arrived = now_ms();
        let mut items: Vec<Item> = Vec::new();
        for message in &batch {
            let Some(header) = message
                .payload()
                .and_then(|p| parse_header(p, &get().run_id))
            else {
                continue; // a leftover from another run: ignore it
            };
            stats
                .lock()
                .unwrap()
                .record(&header, message.partition(), message.offset(), arrived);
            items.push(Item {
                key: message.key().unwrap_or(b"").to_vec(),
                producer: header.producer,
                seq: header.seq,
            });
        }
        let (busy_ms, wait_ms) = run_batch(&group_by_user(items), &scenario, param, &cpu, &order);
        stats.lock().unwrap().add_batch(busy_ms, wait_ms);
        commit(&consumer);
    }
    report(&stats, &consumer, true);
}
