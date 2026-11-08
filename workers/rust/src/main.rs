//! queue-lab worker (Rust): the entry point. One image, two roles, chosen by the ROLE variable.
//!
//! - consumer.rs   the handler: reads messages, verifies them, runs the processing scenario
//! - producer.rs   sends the messages
//! - settings.rs   everything read from the environment (the worker contract, see backend/app/contract.py)
//! - reporting.rs  once-per-second progress reports to the backend
//! - logic.rs      pure helpers: message format, latency buckets, loss and duplicate detection (unit tested)

mod consumer;
mod logic;
mod producer;
mod reporting;
mod settings;

use std::sync::atomic::Ordering;

fn main() {
    settings::init();
    ctrlc::set_handler(|| reporting::STOPPING.store(true, Ordering::Relaxed)).expect("signal handler"); // docker stop sends SIGTERM
    let s = settings::get();
    println!("{} {} starting", s.role, s.worker_id);
    if s.role == "consumer" {
        consumer::run();
    } else {
        producer::run();
    }
}