# Change: one batch loop for the consumer in every language

## Why
Workers handled messages with lanes (a reading loop plus N queues), written a little differently in each language.
It is hard to explain and hard to compare. A production service usually does this instead:

1. read a batch of at most N messages (whatever has arrived, no waiting to fill it);
2. group the batch by user (the message key);
3. handle the groups at the same time, the messages of one group one after another;
4. commit the offsets and read the next batch.

The size of the batch is also the limit of messages handled at the same time, so one number in the form
(`concurrency`, default 30) controls both.

## What changes
- Every consumer (Python, Node.js, Java, Go, Rust) runs the same loop: `read batch -> group by key -> handle -> commit`.
- The lanes and their queues are removed; so are `LANE_QUEUE` and the lane hash.
- Offsets are committed by the worker after a batch is handled (`enable.auto.commit=false`), so a crash repeats a
  batch instead of losing it.
- The order setting is removed. A production service always keeps the messages of one user in order, so the batch is
  always grouped by user; there is no "any order" mode. `ORDERING` leaves the worker contract, the form and the
  workers.
- The CPU scenario is a plain loop that adds up squares (`total = (total + i * i) % 1000`), the same in every language.

## Decision: the next batch starts when the whole batch is done
We wait for the slowest group before reading again. Reasons:
- The client already fetches the next messages into its own queue meanwhile, so reading the next batch is instant.
- Starting early would let a message of a user who is still busy run beside the older ones and break the order. To
  prevent that, the worker would have to track busy users and queue behind them, which is the lane design again,
  and committing offsets would need a "first message not yet handled" per partition.
- It is the simplest rule to explain and to keep identical in five languages.

The cost: when one user sends many slow messages in a row, the other users of the same batch wait for them. The lab
shows it as idle time at the end of a batch. If this proves to matter, a later change can start the next batch early
for users that are not busy.

## Same settings in every language
All Kafka client settings come from `backend/app/config.py` and are passed to every worker; a worker decides none of
them. Known gap that cannot be removed: the Java client (Apache) has no read-ahead queue (`queued.*`), it reads one
fetch at a time, and it has a JVM heap on top of the client memory. Everything else is identical. The README states
that the lab uses one broker, so it is closer to production in load and batching than in replication.

## One source of truth, one structure
- **Numbers.** Everything a worker could tune comes from the backend in the environment: the batch size
  (`concurrency`), the longest wait for a batch (50 ms, `fetch.wait.max.ms`), the ordering, the Kafka client settings.
  Today Python and Java read up to 500 messages per call and the others one; after this change every language asks for
  "up to N, waiting at most W", with the same N and W. No language keeps a number of its own.
- **Code.** Every consumer has the same units, with the same names and the same order of steps, so the five files can
  be read side by side:

  | Unit | What it does | Python / Go / Rust / Node.js / Java |
  |---|---|---|
  | `read_batch` | asks the client for up to N messages, waits at most W | `stats` and `consumer` files |
  | `group_by_user` | groups the batch by the message key, keeps the order inside a group | `consumer` |
  | `handle` | the scenario for one message (echo, delay, cpu), between `@scenario` markers | `consumer` |
  | `handle_group` | handles the messages of one group one after another | `consumer` |
  | `run_batch` | starts all groups at the same time, waits for all | `consumer` |
  | `commit` | commits the offsets of the batch | `consumer` |
  | `ConsumerStats` | counts, checks sequence numbers, builds the report | its own file |

- **Comments.** The same comment sits above the same unit in every language (one short sentence in plain words, then
  only what is special about the language). A contract test checks that every consumer defines these units, so a
  language cannot drift away silently.

## Impact
Specs: `worker-containers`, `load-runs`. Code: the five `consumer` files, `config.py` (commit settings),
`KafkaConf.java`, `contract.py`/`catalog.py` (concurrency text), the contract test, README.
