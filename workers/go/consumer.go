package main

// The consumer (the "handler"): reads messages in a consumer group, verifies them, runs the chosen scenario.

import (
	"crypto/sha256"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// laneQueue is how many messages may wait in front of one lane before the reading loop pauses.
const laneQueue = 8

func runConsumer() {
	scenario := os.Getenv("SCENARIO")
	if scenario == "" {
		scenario = "echo"
	}
	param, _ := strconv.Atoi(os.Getenv("SCENARIO_PARAM"))
	tracker := newSeqTracker() // detects gaps and duplicates per stream

	var mu sync.Mutex
	var received, errs int64
	hist := make([]int64, len(edgesMs)+1)
	offsets := map[string]int64{}
	assigned := map[int32]bool{}
	var firstAt, lastAt, maxLatency float64

	// Offset reset, commit and timeout settings come from the backend (the same values for every language).
	conf := kafkaConf("CONSUMER_CONF")
	conf["bootstrap.servers"] = boot
	conf["group.id"] = required("GROUP_ID")
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		panic(err)
	}
	err = consumer.SubscribeTopics([]string{topic}, func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions: // Kafka gave us partitions (called again after every rebalance)
			mu.Lock()
			for _, p := range e.Partitions {
				assigned[p.Partition] = true
			}
			mu.Unlock()
			return c.Assign(e.Partitions)
		case kafka.RevokedPartitions: // Kafka took partitions away, usually because another worker joined or left
			mu.Lock()
			for _, p := range e.Partitions {
				delete(assigned, p.Partition)
			}
			mu.Unlock()
			return c.Unassign()
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// sendReport sends cumulative counters. The histogram is sent as a delta and reset after each report.
	sendReport := func(final bool) {
		mu.Lock()
		parts := make([]int, 0, len(assigned))
		for p := range assigned {
			parts = append(parts, int(p))
		}
		sort.Ints(parts)
		histCopy := append([]int64(nil), hist...)
		for i := range hist {
			hist[i] = 0
		}
		payload := map[string]interface{}{
			"runId": runID, "worker": workerID(), "role": "consumer", "lang": "go", "ts": float64(time.Now().UnixNano()) / 1e9,
			"ready": len(parts) > 0, "assigned": parts, "received": received, "duplicates": tracker.duplicates,
			"gaps": tracker.gaps, "errors": errs, "offsets": offsets, "hist": histCopy, "final": final,
			"firstAt": nilIfZero(firstAt), "lastAt": nilIfZero(lastAt), "maxLatencyMs": maxLatency,
		}
		mu.Unlock()
		post(payload)
	}
	startReporter(func() { sendReport(false) }, func() bool { return true })

	seed := []byte("queuelab") // starting value of the hash chain used by the cpu scenario
	// @scenario-start
	// handle is what the worker does with one message. Several run at the same time when concurrency is above 1.
	handle := func() {
		if scenario == "delay" && param > 0 {
			time.Sleep(time.Duration(param) * time.Millisecond) // pretend to wait for a database or an API call
		} else if scenario == "cpu" && param > 0 {
			h := sha256.Sum256(seed)
			for i := 0; i < param; i++ {
				h = sha256.Sum256(h[:]) // burn CPU like parsing or encryption would
			}
		}
		// echo: do nothing, just count the message
	}
	// @scenario-end

	// Production style: ONE loop reads from Kafka and hands every message to a "lane". There are `concurrency` lanes, each
	// is a goroutine that handles its messages one after another, so up to `concurrency` messages are handled at the same
	var lanes []chan struct{}
	var running sync.WaitGroup
	if concurrency > 1 && scenario != "echo" {
		for n := 0; n < concurrency; n++ {
			lane := make(chan struct{}, laneQueue)
			lanes = append(lanes, lane)
			running.Add(1)
			go func() {
				defer running.Done()
				for range lane {
					handle()
				}
			}()
		}
	}
	nextLane := 0
	dispatch := func(key []byte) {
		if len(lanes) == 0 {
			handle() // concurrency 1 (or nothing to wait for): one at a time
			return
		}
		index := nextLane % len(lanes)
		nextLane++
		if ordering == "key" {
			index = laneFor(key, len(lanes))
		}
		lanes[index] <- struct{}{} // blocks while that lane is full
	}
	for !stopping.Load() {
		event := consumer.Poll(100)
		message, ok := event.(*kafka.Message)
		if !ok {
			if _, isError := event.(kafka.Error); isError {
				mu.Lock()
				errs++
				mu.Unlock()
			}
			continue
		}
		arrived := nowMs()
		head, valid := parseHeader(message.Value, runID)
		if !valid {
			continue // a leftover from another run: ignore it
		}
		mu.Lock()
		received++
		if firstAt == 0 {
			firstAt = arrived / 1000
		}
		lastAt = float64(time.Now().UnixNano()) / 1e9
		tracker.observe(head.producer, int(message.TopicPartition.Partition), head.seq) // counts gaps (lost) and duplicates
		offsets[strconv.Itoa(int(message.TopicPartition.Partition))] = int64(message.TopicPartition.Offset) + 1 // next offset, used for lag
		latency := arrived - head.sentMs // time from "sent" to "received", in milliseconds
		hist[bucket(latency, edgesMs)]++
		if latency > maxLatency {
			maxLatency = latency
		}
		mu.Unlock()
		dispatch(message.Key)
	}
	for _, lane := range lanes {
		close(lane) // no more work: each lane finishes what it holds and stops
	}
	running.Wait() // let the messages already taken finish before the final report
	sendReport(true)
	consumer.Close() // leave the group cleanly so the partitions are reassigned at once
}
