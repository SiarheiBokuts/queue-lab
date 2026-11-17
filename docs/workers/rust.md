# Rust worker

`workers/rust` · [back to the overview](README.md)

- **Client:** the `rdkafka` crate 0.39 (a Rust API over librdkafka, built from source with CMake), `serde_json` for
  JSON. **Runtime:** Rust 1.99, release build.
- **Why this client:** it is the usual Kafka client of Rust services and it shares librdkafka (and the settings) with
  Python, Node.js and Go.

## How it works

- **Producer.** `BaseProducer::send()` with a delivery callback; `poll(0)` serves the callbacks regularly and a full queue
  (`QueueFull`) waits 50 ms in `poll` and tries again. The unconfirmed limit goes to the client as it is.
- **Consumer.** `BaseConsumer::poll()` waits for the first message, then takes what has already arrived. The groups of
  users run on a **pool of threads that live as long as the worker**; the batch hands every group to the pool and waits
  for all of them. The offsets are committed after the batch (`CommitMode::Sync`).

## What is special in Rust, and why

- **A thread pool, not async.** Rust has no built-in concurrency for this; the choice is threads or an async runtime.
  We use plain threads and the standard library, so the code stays short and has no framework to learn. The pool grows
  to the largest batch seen (at most the batch size) and the threads wait for the next batch.
- **The first version started a thread for every group of every batch.** It was correct, but it used almost the whole
  processor (105% of a 115% share) and kept the worker at about 36,000 messages per second in echo, while Go used 30%
  for several times more. The pool removed that cost: about 2.5 times faster with a tenth of the processor. Messages,
  order and numbers did not change. It is the example in the [overview](README.md) of a worker bug that looked like a
  language difference.
- **A semaphore for the `json` scenario.** A counter of free places (mutex and condition variable) limits the work to the
  whole cores of the container, like the other languages.
- **A slow first build.** librdkafka is compiled from source, so the first image takes minutes; later runs reuse it.

## Why we think this is fair

Standard client, shared settings, plain threads, and the same steps as the other languages. We fixed the thread cost
because it was our code and not Rust. We did not remove the wait described below, because we do not know yet whether it
comes from Rust, from the crate or from our use of it, and until we know, a slow run of Rust is not a fact about Rust.

## Known gaps (open)

- **The speed in echo changes a lot from run to run.** The same setup (5 workers, 1 million messages of 256 bytes, one Go
  sender, one machine) run three times in turn gave Rust 55,000, 89,000 and 235,000 messages per second, and Go 234,000,
  234,000 and 235,000. So Rust can match Go, but sometimes it does not. The slow runs are the ones where Rust falls behind
  the sender (the backlog peaked near 450,000 messages against about 175,000 in the fast run) and then drains the backlog
  slowly. A measurement inside a slow worker shows that about 90% of its time is the wait for the first message of a
  batch: the client queue runs dry about 17 times a second and `poll` waits the full 50 ms, while the other 99 `poll`
  calls of a batch cost nothing. Why the queue runs dry in Rust and not in Go is not found. We would like to know it.
  The experiments still to do are an asynchronous commit and another way to read from the client.
- **So one run of Rust in echo says little.** Run it several times before you believe a gap in either direction. The
  `delay` scenario is close to Go (about 18,600 against 20,500 messages per second).
