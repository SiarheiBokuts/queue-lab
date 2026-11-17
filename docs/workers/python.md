# Python worker

`workers/python` · [back to the overview](README.md)

- **Client:** `confluent-kafka` 2.16.1, a binding of librdkafka (the C client). **Runtime:** Python 3.14.
- **Why this client:** it is the one most Python services use, and it shares librdkafka (and so the settings and their
  meaning) with Node.js, Go and Rust.

## How it works

- **Producer.** `Producer.produce()` hands a message to the client, `poll(0)` serves the delivery callbacks now and then,
  and `flush(60)` waits at the end until the broker confirmed everything. When the client queue is full it waits
  50 ms in `poll` and tries again. The sender's unconfirmed limit is passed to the client as it is
  (`queue.buffering.max.*`).
- **Consumer.** `consume(1, timeout)` waits for the first message, then `consume(N-1, 0)` takes what has already arrived,
  so a batch never waits to fill. The groups of users are handled by a `ThreadPoolExecutor` with one thread per possible
  group. Offsets are committed after every batch with `commit(asynchronous=False)`.

## What is special in Python, and why

- **Threads for the groups.** In the `echo` and `delay` scenarios a message does nothing or sleeps, so threads (which
  share one interpreter lock) are enough and are the usual way to do it.
- **Processes for the `json` scenario.** Even the C-written `json` module holds the interpreter lock while it works, so
  threads would compute one message at a time. The worker sends the work to a `ProcessPoolExecutor` with one process
  per core (at most the CPU budget, the same in every language), which keeps the reading thread free, like a service
  that sends heavy work to other processes would. This costs the interprocess traffic, and it is part of the price of
  Python.
- **Python 3.14, not 3.15.** `confluent-kafka` has no prebuilt wheel for 3.15 yet and the slim image has no compiler.
  The worker moves up when a release with a wheel exists.

## Why we think this is fair

It is the normal librdkafka binding with the shared settings, no native extension of our own and no special tuning. The
costs that Python really has (the interpreter lock, the process hop for CPU work) are not hidden: they show in the CPU
and in the speed. A faster approach would need a different kind of program (for example several consumer processes),
which is a different test.

## Known gaps

- The `json` scenario also measures the process hop, not only the JSON library.
