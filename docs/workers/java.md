# Java worker

`workers/java` · [back to the overview](README.md)

- **Client:** the official Apache `kafka-clients` 4.4.0, with Jackson 2.23.0 for JSON. **Runtime:** Java 25 (Temurin),
  heap set to half of the container memory.
- **Why this client:** it is the standard client of Java and the one every Java service uses. It is **not** librdkafka,
  so it is the only worker whose client is another program than the other four.

## How it works

- **Producer.** `KafkaProducer.send()` with a callback that counts the delivery. The unconfirmed limit becomes
  `buffer.memory` (the 32 MB).
- **Consumer.** `poll()` returns up to `max.poll.records` (the batch size of the lab) records. The groups of users run on
  virtual threads, one per group (`Executors.newVirtualThreadPerTaskExecutor`), and the batch is committed with
  `commitSync()`.

## What is special in Java, and why

- **Settings are translated, not decided.** The backend sends the settings with librdkafka names. `KafkaConf.java`
  renames the few that differ (`fetch.wait.max.ms` becomes `fetch.max.wait.ms`, `fetch.message.max.bytes` becomes
  `max.partition.fetch.bytes`) and turns the two that count messages (`batch.num.messages`, the unconfirmed limit) into
  bytes with the message size. No value is chosen there, and the translation has unit tests.
- **A batch is at most 1 MB.** librdkafka never puts more than `batch.size` (1,000,000 bytes) in a batch, whatever the
  number of messages says. Java got the same cap. Without it, big messages made a batch of up to 16 MB per partition
  and filled the whole buffer, so the sender stalled.
- **No read-ahead queue.** The Apache client reads one fetch at a time and has no `queued.min.messages`. This is the
  one known gap in the Kafka settings.
- **Virtual threads.** They are the modern way to run many waiting tasks in Java 21 and later, the same idea as a
  goroutine.
- **The heap is 50% of the memory.** The rest is for compression code, class data and thread stacks. A heap of 75% made a
  256 MB sender run out of memory.
- **The `json` scenario uses Jackson**, the usual library, behind a semaphore of whole cores like the others.

## Why we think this is fair

Using the official Java client means the numbers are what a Java service gets, and the only place where we shape its
settings is the translation, which follows the librdkafka meaning. The missing read-ahead is a real property of the
client and is written down, not worked around with a second thread of our own.

## Known gaps

- No read-ahead queue; a different client from the other four.
- The JIT compiler warms up during the first seconds of a run, and that is counted. A short run and a long one may
  not show the same speed; we have not measured how much it matters.
