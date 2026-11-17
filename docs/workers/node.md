# Node.js worker

`workers/node` · [back to the overview](README.md)

- **Client:** `@confluentinc/kafka-javascript` 1.11.0, built on librdkafka, used through its KafkaJS-style API.
  **Runtime:** Node.js 24, TypeScript compiled before the image is built.
- **Why this client:** it is Confluent's own client for Node.js, the one that shares librdkafka (and the settings) with
  Python, Go and Rust. The KafkaJS API is what most Node.js services know.

## How it works

- **Producer.** Node.js sends in batches: `send()` takes an array of messages (about 10 ms of the target rate, at most
  1000) and settles when the broker answered for the whole batch. The shared limit of unconfirmed data becomes the number
  of batches that may wait (`batchPlan`, at least two, so the connection is never idle).
- **Consumer.** `eachBatch` gives a batch of one partition, up to `js.consumer.max.batch.size` messages (the batch size
  of the lab). The groups of users are `async` functions run with `Promise.all` on the event loop, and the offsets are
  committed after the batch with `commitOffsets`.

## What is special in Node.js, and why

- **One event-loop thread.** The groups are not threads but promises, which is how Node.js does concurrency. `delay`
  waits without blocking; work that computes would block everything.
- **Threads for the `json` scenario.** A pool of `worker_threads` (one per core, at most the CPU budget) does the
  parsing, so the reading thread stays free. It is created only for that scenario.
- **A batch of one partition.** The client hands over the messages of one partition at a time; the other languages read
  from all their partitions. A worker with several partitions therefore reads them one after another.
- **The sender holds data twice.** A sent batch lives in the buffers of Node.js until the broker confirms it and again in
  the client queue. For big messages the sender needs more memory than the others; the limit is 32 MB, not 64, because
  64 MB ran out of the 256 MB of a sender with 900 KB messages.
- **Node.js 24, not 26.** The client has no prebuilt binary for 26 yet and building it from source fails.

## Why we think this is fair

Batched sending is how Node.js is used with this client: sending a message at a time would measure a style nobody uses.
We keep the same limit of unconfirmed data and the same Kafka settings, so the batching is the only thing that is
different, and it is written down here and in the README.

## Known gaps

- It reads one partition per batch and sends in batches (see above).
- In the `json` scenario the thread pool adds a hop, like the process pool of Python.
