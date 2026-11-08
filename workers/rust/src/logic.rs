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

/// Ranges kept per stream; beyond this the worker only counts (the report says how many it did not list).
pub const MAX_RANGES: usize = 1000;

/// Remembers which message numbers arrived, as ranges, so the lab can count lost and repeated messages exactly.
///
/// Every message has a number per stream (one sender, one partition). Numbers that arrive one after another make one
/// range (`0-4999`), so a run without losses is one range per stream. A number that is already inside a range is a
/// repeat. The lab merges the ranges of all workers: what was sent and is in nobody's ranges is lost, what is counted
/// more than once is repeated (for example after a worker restarts and its batch is read again).
#[derive(Default)]
pub struct SeqRanges {
    streams: HashMap<(usize, usize), Vec<(u64, u64)>>,
    /// Numbers this worker saw twice.
    pub repeats: u64,
    /// Numbers not listed because a stream hit MAX_RANGES.
    pub overflow: u64,
}

impl SeqRanges {
    /// Remember one message number. Returns false when this worker has seen it before.
    pub fn observe(&mut self, producer: usize, partition: usize, seq: u64) -> bool {
        let ranges = self.streams.entry((producer, partition)).or_default();
        if let Some(last) = ranges.last_mut() {
            if seq == last.1 + 1 {
                last.1 = seq; // the usual case: the next number
                return true;
            }
        }
        if ranges.iter().any(|&(start, end)| start <= seq && seq <= end) {
            self.repeats += 1;
            return false;
        }
        if ranges.len() >= MAX_RANGES {
            self.overflow += 1;
            return true;
        }
        ranges.push((seq, seq));
        true
    }

    /// The ranges per stream, sorted and merged, keyed "producer:partition".
    pub fn report(&self) -> HashMap<String, Vec<(u64, u64)>> {
        let mut out = HashMap::new();
        for ((producer, partition), ranges) in &self.streams {
            let mut sorted = ranges.clone();
            sorted.sort_unstable();
            let mut merged: Vec<(u64, u64)> = Vec::new();
            for (start, end) in sorted {
                match merged.last_mut() {
                    Some(previous) if start <= previous.1 + 1 => previous.1 = previous.1.max(end),
                    _ => merged.push((start, end)),
                }
            }
            out.insert(format!("{producer}:{partition}"), merged);
        }
        out
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

/// Group messages by their key (the user). Returns, for each user, the positions of its messages in the batch; the
/// order inside a group is the order they were sent in.
pub fn group_by_user(keys: &[&[u8]]) -> Vec<Vec<usize>> {
    let mut index: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (position, key) in keys.iter().enumerate() {
        let group = *index.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[group].push(position);
    }
    groups
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
    fn messages_are_grouped_by_user_and_keep_their_order() {
        let keys: Vec<&[u8]> = vec![b"user-1", b"user-2", b"user-1", b"user-1"];
        assert_eq!(group_by_user(&keys), vec![vec![0, 2, 3], vec![1]]);
        assert!(group_by_user(&[]).is_empty());
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
    fn numbers_one_after_another_make_one_range() {
        let mut seen = SeqRanges::default();
        assert!((0..5).all(|s| seen.observe(0, 0, s)));
        assert_eq!(seen.report()["0:0"], vec![(0, 4)]);
        assert_eq!(seen.repeats + seen.overflow, 0);
    }

    #[test]
    fn a_skipped_number_leaves_a_gap_between_two_ranges() {
        let mut seen = SeqRanges::default();
        for s in [0, 1, 2, 5, 6] {
            seen.observe(0, 0, s);
        }
        assert_eq!(seen.report()["0:0"], vec![(0, 2), (5, 6)]); // 3 and 4 are lost
    }

    #[test]
    fn a_number_seen_twice_is_a_repeat() {
        let mut seen = SeqRanges::default();
        for s in 0..3 {
            seen.observe(1, 0, s);
        }
        assert!(!seen.observe(1, 0, 1));
        assert!(!seen.observe(1, 0, 0));
        assert_eq!(seen.repeats, 2);
    }

    #[test]
    fn a_replayed_batch_fills_a_gap_and_the_ranges_are_merged() {
        let mut seen = SeqRanges::default();
        for s in [0, 1, 4, 5] {
            seen.observe(0, 0, s);
        }
        assert!(seen.observe(0, 0, 2) && seen.observe(0, 0, 3));
        assert_eq!(seen.report()["0:0"], vec![(0, 5)]);
    }

    #[test]
    fn streams_of_different_senders_and_partitions_do_not_clash() {
        let mut seen = SeqRanges::default();
        for s in 0..3 {
            assert!(seen.observe(0, 5, s) && seen.observe(1, 5, s) && seen.observe(0, 6, s));
        }
        assert_eq!(seen.repeats, 0);
        assert_eq!(seen.report().len(), 3);
    }

    #[test]
    fn a_stream_with_too_many_gaps_stops_listing_but_keeps_counting() {
        let mut seen = SeqRanges::default();
        for s in (0..(MAX_RANGES as u64 * 2 + 10)).step_by(2) {
            seen.observe(0, 0, s);
        }
        assert_eq!(seen.report()["0:0"].len(), MAX_RANGES);
        assert!(seen.overflow > 0);
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