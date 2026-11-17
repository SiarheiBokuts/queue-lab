# Go worker

`workers/go` · [back to the overview](README.md)

- **Client:** `confluent-kafka-go` v2.16.1, a Go API over librdkafka (it needs cgo). **Runtime:** Go 1.24 module, built
  with the Go 1.27 image.
- **Why this client:** it is Confluent's supported client for Go and shares librdkafka (and the settings) with Python,
  Node.js and Rust. Pure-Go clients exist and are popular, but they would be a different client with other defaults, and
  the comparison of languages would turn into a comparison of clients.

## How it works

- **Producer.** `Produce()` hands a message to the client; a goroutine reads `producer.Events()` and counts every
  delivery report (the broker confirmed or rejected the message). The unconfirmed limit goes to the client as it is
  (`queue.buffering.max.*`).
- **Consumer.** `Poll()` waits for the first message, then takes what has already arrived (wait 0). Every group of users
  is a goroutine; a `WaitGroup` waits for all of them, and `Commit()` is synchronous after the batch.

## What is special in Go, and why

- **Goroutines for the groups.** They are the normal way to run many tasks at once in Go, and they are cheap, so a
  batch with a hundred groups costs little. Goroutines run on all cores, so a counting channel (`cpuSlots`) limits the
  `json` scenario to the whole cores of the container, like the other languages.
- **The standard library for JSON.** `encoding/json` is written in Go; it is not the fastest JSON code, but it is what a Go
  service uses.
- **cgo.** The client is C code, so the image is built with `CGO_ENABLED=1` and runs on Debian slim.

## Why we think this is fair

It is the supported client with the shared settings and no special tuning, and the concurrency is the one a Go developer
would write. We did not use a second client or lower-level tricks to get a better number.

## Known gaps

- The `json` scenario measures `encoding/json` as much as Go.
- The client is a binding over C: the CPU of the C part (fetching, batching) is counted with the Go code in the same
  container.
