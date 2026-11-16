package main

// What a consumer counts about the messages it reads, and the report it sends once per second.

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// consumerStats holds every counter of one consumer. The reporter goroutine reads it, so all access goes through mu.
type consumerStats struct {
	mu         sync.Mutex
	received   int64
	errors     int64
	hist       []int64
	offsets    map[string]int64 // the next offset to read per partition, used to work out the lag
	assigned   map[int32]bool
	firstAt    float64 // seconds since the epoch
	lastAt     float64
	maxLatency float64
	seen       *seqRanges  // which message numbers arrived: the lab counts lost and repeated ones from it
	order      *orderCheck // the messages of a user must be handled in the order they were sent
	batchBusy  float64     // milliseconds spent handling batches
	batchWait  float64     // of which some groups were done and the others still worked
}

func newConsumerStats() *consumerStats {
	return &consumerStats{
		hist:     make([]int64, len(edgesMs)+1),
		offsets:  map[string]int64{},
		assigned: map[int32]bool{},
		seen:     newSeqRanges(),
		order:    newOrderCheck(),
	}
}

func (s *consumerStats) assign(partitions []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range partitions {
		s.assigned[p] = true
	}
}

func (s *consumerStats) revoke(partitions []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range partitions {
		delete(s.assigned, p)
	}
}

// addBatch remembers how long a batch took and how much of it was spent waiting for the slowest group.
func (s *consumerStats) addBatch(busyMs, waitMs float64) {
	s.mu.Lock()
	s.batchBusy += busyMs
	s.batchWait += waitMs
	s.mu.Unlock()
}

func (s *consumerStats) countError() {
	s.mu.Lock()
	s.errors++
	s.mu.Unlock()
}

// record counts one message of this run that arrived at arrivedMs.
func (s *consumerStats) record(head header, partition int32, offset int64, arrivedMs float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received++
	if s.firstAt == 0 {
		s.firstAt = arrivedMs / 1000
	}
	s.lastAt = float64(time.Now().UnixNano()) / 1e9
	s.seen.observe(head.producer, int(partition), head.seq)
	s.offsets[strconv.Itoa(int(partition))] = offset + 1
	latency := arrivedMs - head.sentMs // from "sent" to "received"
	s.hist[bucket(latency, edgesMs)]++
	if latency > s.maxLatency {
		s.maxLatency = latency
	}
}

// report sends the cumulative counters. The histogram goes as the change since the last report, then it is reset.
func (s *consumerStats) report(final bool) {
	s.mu.Lock()
	parts := make([]int, 0, len(s.assigned))
	for p := range s.assigned {
		parts = append(parts, int(p))
	}
	sort.Ints(parts)
	histCopy := append([]int64(nil), s.hist...)
	for i := range s.hist {
		s.hist[i] = 0
	}
	payload := map[string]interface{}{
		"runId": runID, "worker": workerID(), "role": "consumer", "lang": "go", "ts": float64(time.Now().UnixNano()) / 1e9,
		"ready": len(parts) > 0, "assigned": parts, "received": s.received, "duplicates": s.seen.repeats,
		"ranges": s.seen.report(), "overflow": s.seen.overflow,
		"orderViolations": s.order.violations, "batchBusyMs": math.Round(s.batchBusy*10) / 10, "batchWaitMs": math.Round(s.batchWait*10) / 10, "errors": s.errors, "offsets": s.offsets, "hist": histCopy, "final": final,
		"firstAt": nilIfZero(s.firstAt), "lastAt": nilIfZero(s.lastAt), "maxLatencyMs": s.maxLatency,
	}
	s.mu.Unlock()
	post(payload)
}
