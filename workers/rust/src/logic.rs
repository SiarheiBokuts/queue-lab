//! Pure helpers of the Rust worker: no Kafka, no network, no environment. Easy to unit test.
//!
//! Message format (shared with the other workers):
//!
//! ```text
//! <run>|<producer>|<partition>|<sequence>|<sentAtMs>|xxxxxxxx...
//! ```
//!
//! - run: id of the run, so leftovers from earlier runs are ignored
//! - producer: which producer sent it (several producers can share the load)
//! - partition: partition the message was sent to
//! - sequence: counter that starts at 0 per producer and partition; gaps and repeats reveal loss and duplicates
//! - sentAtMs: producer wall-clock time in milliseconds; the consumer subtracts it from "now" to get the latency
//! - padding: fills the message up to the size chosen in the UI

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The header is far shorter than this; we never look at the (possibly large) padding to read it.
pub const HEADER_MAX_BYTES: usize = 96;

/// Parse the HIST_EDGES value, e.g. "0.5,1,2,5", into bucket edges in milliseconds.
pub fn parse_edges(raw: &str) -> Vec<f64> {
    raw.split(',').filter_map(|part| part.trim().parse().ok()).collect()
}

/// The histogram bucket a latency falls into: bucket i holds latencies <= edges[i]; the last bucket (index
/// edges.len()) holds everything above the last edge. Counting messages per bucket keeps reports small.
pub fn bucket(latency_ms: f64, edges: &[f64]) -> usize {
    edges.iter().position(|edge| latency_ms <= *edge).unwrap_or(edges.len())
}

/// Header of a message; the producer appends padding after it.
pub fn build_header(run: &str, producer: usize, partition: usize, seq: u64, sent_ms: f64) -> Vec<u8> {
    format!("{run}|{producer}|{partition}|{seq}|{sent_ms:.3}|").into_bytes()
}

/// What `parse_header` reads from the start of a message.
#[derive(Debug, PartialEq)]
pub struct Header {
    pub producer: usize,
    pub partition: usize,
    pub seq: u64,
    pub sent_ms: f64,
}

/// Read the start of a message. None if it is malformed or not part of `expected_run`.
pub fn parse_header(value: &[u8], expected_run: &str) -> Option<Header> {
    let head = &value[..value.len().min(HEADER_MAX_BYTES)];
    let text = String::from_utf8_lossy(head);
    let parts: Vec<&str> = text.splitn(6, '|').collect();
    if parts.len() < 6 || parts[0] != expected_run {
        return None;
    }
    Some(Header {
        producer: parts[1].parse().ok()?,
        partition: parts[2].parse().ok()?,
        seq: parts[3].parse().ok()?,
        sent_ms: parts[4].parse().ok()?,
    })
}

/// Detects lost and duplicated messages from per-stream sequence numbers.
///
/// Within one partition Kafka preserves order, so after sequence N of a stream (one producer, one partition) the
/// next message of that stream must be N + 1. A higher number means messages were skipped (gap); a number we
/// already saw (or lower) means a duplicate. The first message seen on a stream only sets the baseline, because a
/// worker may join mid-run after a rebalance.
#[derive(Default)]
pub struct SeqTracker {
    last: HashMap<(usize, usize), u64>,
    pub duplicates: u64,
    /// Total number of skipped sequence numbers.
    pub gaps: u64,
}

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Ok,
    Gap,
    Duplicate,
}

impl SeqTracker {
    /// Record one message of the stream (producer, partition).
    pub fn observe(&mut self, producer: usize, partition: usize, seq: u64) -> Verdict {
        match self.last.insert((producer, partition), seq) {
            None => Verdict::Ok,
            Some(last) if seq == last + 1 => Verdict::Ok,
            Some(last) if seq <= last => {
                self.last.insert((producer, partition), last); // a duplicate does not move the stream
                self.duplicates += 1;
                Verdict::Duplicate
            }
            Some(last) => {
                self.gaps += seq - last - 1;
                Verdict::Gap
            }
        }
    }
}

/// How long a producer must wait so that `sent` messages take at least sent / rate seconds. Zero means "go on".
/// A rate of 0 means "as fast as possible", and a producer behind schedule never waits.
/// Message key of the user a message belongs to, like a payment of one user. A user always lands in the same partition
/// (user number % partitions == partition), so Kafka keeps that user's messages in order.
pub fn user_key(part: usize, i: usize, nparts: usize, users: usize) -> String {
    let per_partition = (users / nparts).max(1);
    format!("user-{}", part + nparts * ((i / nparts) % per_partition))
}

/// The lane (0 .. lanes-1) that handles messages with this key. The same key always gets the same lane (FNV-1a hash).
pub fn lane_for(key: &[u8], lanes: usize) -> usize {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in key {
        hash = (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193);
    }
    hash as usize % lanes
}
pub fn pacing_delay(sent: u64, rate: f64, elapsed: Duration) -> Duration {
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(sent as f64 / rate).saturating_sub(elapsed)
}

/// How many messages to send between two pacing checks: about 10 ms worth, so the rate stays smooth.
pub fn pacing_chunk(rate: f64, max_chunk: usize) -> usize {
    if rate <= 0.0 {
        return max_chunk;
    }
    ((rate / 100.0) as usize).clamp(1, max_chunk)
}

/// Wall-clock time in milliseconds with sub-millisecond precision.
pub fn now_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EDGES: [f64; 5] = [0.5, 1.0, 2.0, 5.0, 10.0];

    #[test]
    fn a_user_always_lands_in_the_same_partition() {
        let mut seen = std::collections::HashMap::new();
        for i in 0..500usize {
            let part = i % 6;
            let key = user_key(part, i, 6, 60);
            assert_eq!(*seen.entry(key).or_insert(part), part);
        }
        assert_eq!(seen.len(), 60);
    }

    #[test]
    fn the_same_key_always_gets_the_same_lane() {
        assert_eq!(lane_for(b"user-7", 16), lane_for(b"user-7", 16));
        assert!((0..200).all(|n| lane_for(format!("user-{n}").as_bytes(), 8) < 8));
    }
    #[test]
    fn parses_edges() {
        assert_eq!(parse_edges(" 0.5, 1 ,2,"), vec![0.5, 1.0, 2.0]);
    }

    #[test]
    fn bucket_boundaries() {
        for (latency, want) in [(0.0, 0), (0.5, 0), (0.51, 1), (1.0, 1), (4.9, 3), (5.0, 3), (10.0, 4), (10.1, 5), (99999.0, 5)] {
            assert_eq!(bucket(latency, &EDGES), want, "latency {latency}");
        }
    }

    #[test]
    fn header_round_trip() {
        let mut message = build_header("abc12345", 2, 3, 42, 1794147580123.456);
        message.extend(vec![b'x'; 100]);
        assert_eq!(parse_header(&message, "abc12345"), Some(Header { producer: 2, partition: 3, seq: 42, sent_ms: 1794147580123.456 }));
    }

    #[test]
    fn header_of_another_run_is_ignored() {
        assert_eq!(parse_header(&build_header("other", 0, 0, 0, 1.0), "abc12345"), None);
    }

    #[test]
    fn malformed_headers_are_rejected() {
        for garbage in ["", "no separators", "a|b|c", "run|0|x|y|z|pad", "run|0|1|2|notanumber|pad"] {
            assert_eq!(parse_header(garbage.as_bytes(), "run"), None, "{garbage}");
        }
    }

    #[test]
    fn in_order_sequence_is_ok() {
        let mut t = SeqTracker::default();
        for s in 0..5 {
            assert_eq!(t.observe(0, 0, s), Verdict::Ok);
        }
        assert_eq!(t.gaps + t.duplicates, 0);
    }

    #[test]
    fn gap_and_duplicate_are_counted() {
        let mut t = SeqTracker::default();
        t.observe(0, 1, 0);
        assert_eq!(t.observe(0, 1, 4), Verdict::Gap);
        assert_eq!(t.gaps, 3);
        assert_eq!(t.observe(0, 1, 4), Verdict::Duplicate);
        assert_eq!(t.duplicates, 1);
        assert_eq!(t.observe(0, 1, 5), Verdict::Ok);
    }

    #[test]
    fn producers_do_not_clash() {
        let mut t = SeqTracker::default();
        for s in 0..3 {
            assert_eq!(t.observe(0, 5, s), Verdict::Ok);
            assert_eq!(t.observe(1, 5, s), Verdict::Ok);
        }
        assert_eq!(t.gaps + t.duplicates, 0);
    }

    #[test]
    fn first_message_only_sets_the_baseline() {
        let mut t = SeqTracker::default();
        assert_eq!(t.observe(0, 2, 500), Verdict::Ok);
        assert_eq!(t.gaps, 0);
    }

    #[test]
    fn pacing() {
        assert_eq!(pacing_delay(1_000_000, 0.0, Duration::from_millis(1)), Duration::ZERO);
        assert_eq!(pacing_delay(1000, 1000.0, Duration::from_millis(250)), Duration::from_millis(750));
        assert_eq!(pacing_delay(1000, 1000.0, Duration::from_secs(2)), Duration::ZERO);
        assert_eq!(pacing_chunk(0.0, 1000), 1000);
        assert_eq!(pacing_chunk(10_000.0, 1000), 100);
        assert_eq!(pacing_chunk(5_000_000.0, 1000), 1000);
        assert_eq!(pacing_chunk(50.0, 1000), 1);
    }
}