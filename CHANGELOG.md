# Changelog

## 1.0.0 (2026-11-08)

First release.

- Workers and senders in Python, Node.js, Java, Go and Rust behind one contract, with identical Kafka client settings.
- Run a test from the web page: queue, partitions, workers, senders, load (up to 50M messages, target rate, size,
  compression), scenario (echo, delay, CPU).
- Compare languages: the same test per language in random order, every number side by side, no score and no winner.
- Every worker runs the same loop: read a batch (up to N messages), group it by user, handle the groups at the same
  time (the messages of one user one after another), commit the offsets, read the next batch.
- Lost and repeated messages are counted exactly: workers report the message numbers they received as ranges and the
  backend merges them.
- A JSON scenario: every message is parsed and written back with the standard JSON library of the language.
- First-visit welcome page: Docker found, its cores and memory, choice of the memory budget.
- Live dashboard, run report, comparison, code page, settings.
- Tests for the backend, the frontend and every worker language.
