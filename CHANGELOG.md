# Changelog

## 1.0.0 (2026-11-08)

First release.

- Workers and senders in Python, Node.js, Java, Go and Rust behind one contract, with identical Kafka client settings.
- Run a test from the web page: queue, partitions, workers, senders, load (up to 50M messages, target rate, size,
  compression), scenario (echo, delay, CPU).
- Compare languages: the same test per language in random order, with a scored winner.
- Workers handle many messages at once, in order per user.
- Live dashboard, run report, comparison, code page, settings.
- Tests for the backend, the frontend and every worker language.