package main

// Pure helpers of the Go worker: no Kafka, no network, no environment. Easy to unit test.
//

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
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

// maxRanges is how many ranges are kept per stream; beyond it the worker only counts (the report says how many it did
// not list).
const maxRanges = 1000

// seqRanges remembers which message numbers arrived, as ranges, so the lab can count lost and repeated messages
// exactly.
//
// Every message has a number per stream (one sender, one partition). Numbers that arrive one after another make one
// range (0-4999), so a run without losses is one range per stream. A number that is already inside a range is a
// repeat. The lab merges the ranges of all workers: what was sent and is in nobody's ranges is lost, what is counted
// more than once is repeated (for example after a worker restarts and its batch is read again).
type seqRanges struct {
	streams  map[[2]int][][2]int64
	repeats  int64 // numbers this worker saw twice
	overflow int64 // numbers not listed because a stream hit maxRanges
}

func newSeqRanges() *seqRanges { return &seqRanges{streams: map[[2]int][][2]int64{}} }

// observe remembers one message number. It returns false when this worker has seen it before.
func (s *seqRanges) observe(producer, partition int, seq int64) bool {
	key := [2]int{producer, partition}
	ranges := s.streams[key]
	if n := len(ranges); n > 0 && seq == ranges[n-1][1]+1 {
		ranges[n-1][1] = seq // the usual case: the next number
		return true
	}
	for _, r := range ranges {
		if r[0] <= seq && seq <= r[1] {
			s.repeats++
			return false
		}
	}
	if len(ranges) >= maxRanges {
		s.overflow++
		return true
	}
	s.streams[key] = append(ranges, [2]int64{seq, seq})
	return true
}

// report returns the ranges per stream, sorted and merged, keyed "producer:partition".
func (s *seqRanges) report() map[string][][2]int64 {
	out := map[string][][2]int64{}
	for key, ranges := range s.streams {
		sorted := append([][2]int64(nil), ranges...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
		var merged [][2]int64
		for _, r := range sorted {
			if n := len(merged); n > 0 && r[0] <= merged[n-1][1]+1 {
				if r[1] > merged[n-1][1] {
					merged[n-1][1] = r[1]
				}
			} else {
				merged = append(merged, r)
			}
		}
		out[strconv.Itoa(key[0])+":"+strconv.Itoa(key[1])] = merged
	}
	return out
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

// item is what the handling step needs to know about a message: whose it is and which one it is.
type item struct {
	key      []byte // the user
	producer int    // which sender
	seq      int64  // the number of the message in its stream
}

// groupByUser groups the items by their key (the user). The order inside a group is the order they were sent in.
func groupByUser(items []item) [][]item {
	index := map[string]int{}
	var groups [][]item
	for _, it := range items {
		g, seen := index[string(it.key)]
		if !seen {
			g = len(groups)
			index[string(it.key)] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], it)
	}
	return groups
}

// batchTiming returns how long a batch took, and how long of that some groups were done while others still worked
// (milliseconds). A batch without groups (all its messages belonged to another run) took no time and waited for nobody.
func batchTiming(started time.Time, done []time.Time) (busyMs, waitMs float64) {
	if len(done) == 0 {
		return 0, 0
	}
	first, last := done[0], done[0]
	for _, t := range done {
		if t.Before(first) {
			first = t
		}
		if t.After(last) {
			last = t
		}
	}
	return float64(last.Sub(started)) / 1e6, float64(last.Sub(first)) / 1e6
}

// orderCheck checks that the messages of one user are handled in the order they were sent.
//
// For every (user, sender) it remembers the number of the message handled last. A message with a lower number than that
// was handled too late: it is counted as an order violation. A well built consumer never has one.
type orderCheck struct {
	mu         sync.Mutex
	last       map[orderKey]int64
	violations int64
}

type orderKey struct {
	user     string
	producer int
}

func newOrderCheck() *orderCheck { return &orderCheck{last: map[orderKey]int64{}} }

// check remembers that the item is handled now. It returns false when it came after a later message of the same user.
func (o *orderCheck) check(it item) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := orderKey{string(it.key), it.producer}
	if last, seen := o.last[key]; seen && it.seq < last {
		o.violations++
		return false
	}
	o.last[key] = it.seq
	return true
}
