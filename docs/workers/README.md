# The five workers: how they are built and why the comparison is fair

queue-lab runs the same job in Python, Node.js, Java, Go and Rust and shows what it costs: speed, latency, CPU, memory,
network. A number is only worth showing if a difference between two languages comes from the languages and not from the
code around them. This page says what is the same in every worker, what is allowed to differ, and why we think the
result is honest. One page per language says what is special there:
[Python](python.md) · [Node.js](node.md) · [Java](java.md) · [Go](go.md) · [Rust](rust.md).

These are our opinions and our choices, not a proof. The author is not an expert in all five languages, so a choice here
may be wrong. If you see one, an issue or a pull request is welcome.

## What is the same in every worker

- **The contract.** A worker is a container that reads its job from environment variables (`backend/app/contract.py`),
  does the steps below and sends one report per second. `backend/tests/test_contract.py` checks that all five read the
  same variables and agree on the shared names.
- **The steps and their names.** A producer reads its settings, makes a message, sends it, counts it and keeps the pace.
  A consumer reads a batch, groups it by user, handles the groups at the same time (a user's messages one after another)
  and commits. They are written in the same order with the same names and comments, in the idiom of each language. The
  Code page shows them side by side.
- **The Kafka settings.** All client settings (`acks`, `linger.ms`, batch size, `fetch.*`, read-ahead, manual commit)
  live in `backend/app/config.py` and are sent to every worker unchanged. A worker never decides a Kafka value.
- **The machine.** Every sender and worker container gets the same memory limit and the same share of the cores (cores
  minus one kept for Kafka, divided equally). A worker computes as many messages at once as it has whole cores, so a
  language with more threads cannot take the cores of another.
- **The memory guard of a sender.** One limit of data the broker has not confirmed (300,000 messages or 32 MB, the smaller
  one) for all five.
- **The measuring.** CPU, memory and network come from Docker, the same way for every container, not from the workers.
  The languages run one after another, in random order, with a pause.
- **The count.** Every message has a number per sender and partition, so lost, repeated and out-of-order messages are
  counted, not guessed. A faster worker that loses messages shows it.
- **No verdicts.** The report and the comparison show numbers and the setup behind them. No score, no winner.

## What is allowed to differ, and why

We use the **standard Kafka client of each language and its normal way of working**. We do not tune one language to look
better, and we do not add a library that only one language has. That means some differences are real parts of the
language, and we write them down instead of hiding them:

| Difference | Where | Why it stays |
|---|---|---|
| Python, Node.js, Go and Rust use librdkafka (the C client); Java uses the Apache client | all | Java's own client is the standard one there. Its names and a few settings are translated, see [Java](java.md) |
| Node.js sends in batches and reads one partition per batch | Node.js | That is how its client is used; see [Node.js](node.md) |
| Java has no read-ahead queue | Java | The Apache client reads one fetch at a time |
| Python runs the work of a message in processes, Node.js in threads | `json` scenario | One thread of Python or Node.js computes on one core at a time |
| The JSON library is the standard one of the language | `json` scenario | It compares the libraries as much as the languages |

## How to read a result

- The **echo** scenario (a message is only counted) is the closest to a measure of the client and the language. Start
  there. The `delay` scenario waits, so most workers look alike. The `json` scenario depends on the JSON library as much
  as on the language, so differences there can be large and mean less.
- **Run it more than once.** The same setup can give very different speeds in echo from one run to the next (we saw
  55,000 and 235,000 messages per second from the same Rust worker). One run is a sample, not a result.
- A number is true for **this setup on this machine**. Another message size, batch size or machine can change the order.
- **A difference can be a bug in a worker.** The Rust worker once started a thread for every group of every batch and
  used its whole processor for it; it looked like "Rust is slow in echo" until the CPU per message was compared with Go
  ([Rust](rust.md)). If one language is far from the others, look at its CPU and at the code before you believe it.

## Where the code is

`workers/python`, `workers/node`, `workers/java`, `workers/go`, `workers/rust`. Each has a `Dockerfile` (the image) and a
`Dockerfile.test` (formatting, lints and unit tests). What is built and how to run the checks is in
[CONTRIBUTING.md](../../CONTRIBUTING.md).
