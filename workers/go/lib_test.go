package main

import (
	"testing"
	"time"
)

var edges = []float64{0.5, 1, 2, 5, 10}

func TestParseEdges(t *testing.T) {
	got := parseEdges(" 0.5, 1 ,2,")
	if len(got) != 3 || got[0] != 0.5 || got[2] != 2 {
		t.Fatalf("unexpected edges %v", got)
	}
}

func TestBucketBoundaries(t *testing.T) {
	cases := map[float64]int{0: 0, 0.5: 0, 0.51: 1, 1: 1, 4.9: 3, 5: 3, 10: 4, 10.1: 5, 99999: 5}
	for latency, want := range cases {
		if got := bucket(latency, edges); got != want {
			t.Errorf("bucket(%v) = %d, want %d", latency, got, want)
		}
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	msg := append(buildHeader("abc12345", 2, 3, 42, 1794147580123.456), make([]byte, 100)...)
	h, ok := parseHeader(msg, "abc12345")
	if !ok || h.producer != 2 || h.partition != 3 || h.seq != 42 || h.sentMs != 1794147580123.456 {
		t.Fatalf("round trip failed: %+v %v", h, ok)
	}
}

func TestHeaderOfAnotherRunIsIgnored(t *testing.T) {
	if _, ok := parseHeader(buildHeader("other", 0, 0, 0, 1), "abc12345"); ok {
		t.Fatal("a message of another run must be ignored")
	}
}

func TestMalformedHeadersAreRejected(t *testing.T) {
	for _, garbage := range []string{"", "no separators", "a|b|c", "run|0|x|y|z|pad", "run|0|1|2|notanumber|pad"} {
		if _, ok := parseHeader([]byte(garbage), "run"); ok {
			t.Errorf("%q should be rejected", garbage)
		}
	}
}

func TestNumbersOneAfterAnotherMakeOneRange(t *testing.T) {
	seen := newSeqRanges()
	for s := int64(0); s < 5; s++ {
		if !seen.observe(0, 0, s) {
			t.Fatalf("seq %d should be new", s)
		}
	}
	r := seen.report()["0:0"]
	if len(r) != 1 || r[0] != [2]int64{0, 4} || seen.repeats != 0 {
		t.Fatalf("unexpected ranges %v", r)
	}
}

func TestASkippedNumberLeavesAGap(t *testing.T) {
	seen := newSeqRanges()
	for _, s := range []int64{0, 1, 2, 5, 6} {
		seen.observe(0, 0, s)
	}
	r := seen.report()["0:0"]
	if len(r) != 2 || r[0] != [2]int64{0, 2} || r[1] != [2]int64{5, 6} {
		t.Fatalf("3 and 4 should be in no range: %v", r)
	}
}

func TestANumberSeenTwiceIsARepeat(t *testing.T) {
	seen := newSeqRanges()
	for s := int64(0); s < 3; s++ {
		seen.observe(1, 0, s)
	}
	if seen.observe(1, 0, 1) || seen.observe(1, 0, 0) || seen.repeats != 2 {
		t.Fatal("two repeats expected")
	}
}

func TestAReplayedBatchFillsAGapAndRangesMerge(t *testing.T) {
	seen := newSeqRanges()
	for _, s := range []int64{0, 1, 4, 5} {
		seen.observe(0, 0, s)
	}
	for _, s := range []int64{2, 3} {
		if !seen.observe(0, 0, s) {
			t.Fatalf("%d was missing, it is new", s)
		}
	}
	r := seen.report()["0:0"]
	if len(r) != 1 || r[0] != [2]int64{0, 5} {
		t.Fatalf("expected one merged range: %v", r)
	}
}

func TestStreamsOfDifferentSendersDoNotClash(t *testing.T) {
	seen := newSeqRanges()
	for s := int64(0); s < 3; s++ {
		if !seen.observe(0, 5, s) || !seen.observe(1, 5, s) || !seen.observe(0, 6, s) {
			t.Fatal("streams must be independent")
		}
	}
	if len(seen.report()) != 3 || seen.repeats != 0 {
		t.Fatal("three streams, no repeats expected")
	}
}

func TestTooManyGapsStopListingButKeepCounting(t *testing.T) {
	seen := newSeqRanges()
	for s := int64(0); s < maxRanges*2+10; s += 2 {
		seen.observe(0, 0, s)
	}
	if len(seen.report()["0:0"]) != maxRanges || seen.overflow == 0 {
		t.Fatal("the list is capped and the rest is counted")
	}
}
func TestPacing(t *testing.T) {
	if pacingDelay(1_000_000, 0, time.Millisecond) != 0 {
		t.Error("no rate means no waiting")
	}
	if got := pacingDelay(1000, 1000, 250*time.Millisecond); got != 750*time.Millisecond {
		t.Errorf("ahead of the rate: %v", got)
	}
	if pacingDelay(1000, 1000, 2*time.Second) != 0 {
		t.Error("behind schedule never waits")
	}
	for rate, want := range map[float64]int{0: 1000, 10_000: 100, 5_000_000: 1000, 50: 1} {
		if got := pacingChunk(rate, 1000); got != want {
			t.Errorf("pacingChunk(%v) = %d, want %d", rate, got, want)
		}
	}
}
func TestAUserAlwaysLandsInTheSamePartition(t *testing.T) {
	seen := map[string]int{}
	for i := 0; i < 500; i++ {
		part := i % 6
		key := string(userKey(part, i, 6, 60))
		if p, ok := seen[key]; ok && p != part {
			t.Fatalf("%s moved from partition %d to %d", key, p, part)
		}
		seen[key] = part
	}
	if len(seen) != 60 {
		t.Fatalf("expected 60 users, got %d", len(seen))
	}
}

func TestMessagesAreGroupedByUserAndKeepTheirOrder(t *testing.T) {
	items := []item{
		{[]byte("user-1"), 0, 1}, {[]byte("user-2"), 0, 2}, {[]byte("user-1"), 0, 3}, {[]byte("user-1"), 0, 4},
	}
	groups := groupByUser(items)
	if len(groups) != 2 || len(groups[0]) != 3 || groups[0][0].seq != 1 || groups[0][1].seq != 3 || groups[0][2].seq != 4 ||
		len(groups[1]) != 1 || groups[1][0].seq != 2 {
		t.Fatalf("unexpected groups: %v", groups)
	}
	if len(groupByUser(nil)) != 0 {
		t.Fatal("an empty batch has no groups")
	}
}

func TestABatchTakesAsLongAsItsSlowestGroup(t *testing.T) {
	start := time.Unix(100, 0)
	busy, wait := batchTiming(start, []time.Time{start.Add(500 * time.Millisecond), start.Add(2 * time.Second), start.Add(time.Second)})
	if busy != 2000 || wait != 1500 {
		t.Fatalf("busy %v wait %v", busy, wait)
	}
	if busy, wait = batchTiming(start, []time.Time{start.Add(400 * time.Millisecond)}); busy != 400 || wait != 0 {
		t.Fatal("one group: nobody waits")
	}
}

func TestABatchWithoutGroupsTakesNoTimeAndDoesNotPanic(t *testing.T) {
	if busy, wait := batchTiming(time.Now(), nil); busy != 0 || wait != 0 {
		t.Fatal("all messages belonged to another run")
	}
}

func TestAUserHandledInOrderHasNoViolation(t *testing.T) {
	order := newOrderCheck()
	for _, seq := range []int64{1, 2, 3, 3, 10} { // a repeat of 3 is not out of order
		if !order.check(item{[]byte("user-1"), 0, seq}) {
			t.Fatalf("seq %d should be in order", seq)
		}
	}
	if order.violations != 0 {
		t.Fatal("no violation expected")
	}
}

func TestAMessageHandledAfterALaterOneIsAViolation(t *testing.T) {
	order := newOrderCheck()
	order.check(item{[]byte("user-1"), 0, 5})
	if order.check(item{[]byte("user-1"), 0, 4}) || order.violations != 1 {
		t.Fatal("one violation expected")
	}
}

func TestUsersAndSendersAreCheckedIndependently(t *testing.T) {
	order := newOrderCheck()
	order.check(item{[]byte("user-1"), 0, 9})
	if !order.check(item{[]byte("user-2"), 0, 1}) || !order.check(item{[]byte("user-1"), 1, 1}) || order.violations != 0 {
		t.Fatal("another user or another sender is not out of order")
	}
}
