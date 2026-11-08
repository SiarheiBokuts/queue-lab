package main

// Pure helpers of the Go worker: no Kafka, no network, no environment. Easy to unit test.
//

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

// headerMaxBytes: the header is far shorter than this; we never look at the (possibly large) padding to read it.
const headerMaxBytes = 96

// parseEdges parses the HIST_EDGES value, e.g. "0.5,1,2,5", into bucket edges in milliseconds.
func parseEdges(raw string) []float64 {
	var edges []float64
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			if v, err := strconv.ParseFloat(part, 64); err == nil {
				edges = append(edges, v)
			}
		}
	}
	return edges
}

// bucket returns the histogram bucket a latency falls into: bucket i holds latencies <= edges[i]; the last bucket
// (index len(edges)) holds everything above the last edge. Counting messages per bucket keeps reports small.
func bucket(latencyMs float64, edges []float64) int {
	for i, edge := range edges {
		if latencyMs <= edge {
			return i
		}
	}
	return len(edges)
}

// buildHeader is the header of a message; the producer appends padding after it.
func buildHeader(run string, producer, partition int, seq int64, sentMs float64) []byte {
	return []byte(fmt.Sprintf("%s|%d|%d|%d|%.3f|", run, producer, partition, seq, sentMs))
}

// header is what parseHeader reads from the start of a message.
type header struct {
	producer, partition int
	seq                 int64
	sentMs              float64
}

// parseHeader reads the start of a message. ok is false if it is malformed or not part of expectedRun.
func parseHeader(head []byte, expectedRun string) (h header, ok bool) {
	if len(head) > headerMaxBytes {
		head = head[:headerMaxBytes]
	}
	parts := strings.SplitN(string(head), "|", 6)
	if len(parts) < 6 || parts[0] != expectedRun {
		return h, false
	}
	producer, err1 := strconv.Atoi(parts[1])
	partition, err2 := strconv.Atoi(parts[2])
	seq, err3 := strconv.ParseInt(parts[3], 10, 64)
	sentMs, err4 := strconv.ParseFloat(parts[4], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || producer < 0 || partition < 0 || seq < 0 {
		return h, false
	}
	return header{producer, partition, seq, sentMs}, true
}

// seqTracker detects lost and duplicated messages from per-stream sequence numbers.
//
type seqTracker struct {
	last       map[[2]int]int64
	duplicates int64
	gaps       int64 // total number of skipped sequence numbers
}

func newSeqTracker() *seqTracker { return &seqTracker{last: map[[2]int]int64{}} }

// observe records one message of the stream (producer, partition) and returns "ok", "gap" or "duplicate".
func (t *seqTracker) observe(producer, partition int, seq int64) string {
	key := [2]int{producer, partition}
	last, seen := t.last[key]
	if !seen || seq == last+1 {
		t.last[key] = seq
		return "ok"
	}
	if seq <= last {
		t.duplicates++
		return "duplicate"
	}
	t.gaps += seq - last - 1
	t.last[key] = seq
	return "gap"
}

// pacingDelay is how long a producer must wait so that `sent` messages take at least sent / rate seconds. Zero
// means "go on". A rate of 0 means "as fast as possible", and a producer behind schedule never waits.
func pacingDelay(sent int, rate float64, elapsed time.Duration) time.Duration {
	if rate <= 0 {
		return 0
	}
	wait := time.Duration(float64(sent)/rate*float64(time.Second)) - elapsed
	if wait < 0 {
		return 0
	}
	return wait
}

// pacingChunk is how many messages to send between two pacing checks: about 10 ms worth, so the rate stays smooth.
func pacingChunk(rate float64, maxChunk int) int {
	if rate <= 0 {
		return maxChunk
	}
	chunk := int(rate / 100)
	if chunk < 1 {
		return 1
	}
	if chunk > maxChunk {
		return maxChunk
	}
	return chunk
}

// nowMs is the wall-clock time in milliseconds with sub-millisecond precision.
func nowMs() float64 { return float64(time.Now().UnixNano()) / 1e6 }
// userKey is the message key of the user a message belongs to, like a payment of one user. A user always lands in the
// same partition (user number % partitions == partition), so Kafka keeps that user's messages in order.
func userKey(part, i, nparts, users int) []byte {
	perPartition := users / nparts
	if perPartition < 1 {
		perPartition = 1
	}
	return []byte("user-" + strconv.Itoa(part+nparts*((i/nparts)%perPartition)))
}

// laneFor is the lane (0 .. lanes-1) that handles messages with this key. The same key always gets the same lane.
func laneFor(key []byte, lanes int) int {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(lanes))
}
