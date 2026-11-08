//! Progress reporting shared by the producer and the consumer: the stop flag and the HTTP report.

use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Set on SIGTERM so loops can finish and send a final report.
pub static STOPPING: AtomicBool = AtomicBool::new(false);

static AGENT: OnceLock<ureq::Agent> = OnceLock::new();

/// POST a progress report to the backend. A failed report is logged and skipped, never fatal.
pub fn post(payload: &Value) {
    let agent = AGENT.get_or_init(|| ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(3))).build().into());
    if let Err(error) = agent.post(&crate::settings::get().report_url).send_json(payload) {
        println!("report failed: {error}");
    }
}

/// Call `send` once per second on a background thread until the worker stops.
pub fn start_reporter<F, K>(send: F, keep_going: K)
where
    F: Fn() + Send + 'static,
    K: Fn() -> bool + Send + 'static,
{
    std::thread::spawn(move || {
        while !STOPPING.load(Ordering::Relaxed) && keep_going() {
            std::thread::sleep(Duration::from_secs(1));
            send();
        }
    });
}

/// Seconds since the epoch, as the backend expects timestamps.
pub fn now_seconds() -> f64 {
    crate::logic::now_ms() / 1000.0
}