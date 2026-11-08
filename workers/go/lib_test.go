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

func TestSeqTrackerInOrder(t *testing.T) {
	tr := newSeqTracker()
	for s := int64(0); s < 5; s++ {
		if v := tr.observe(0, 0, s); v != "ok" {
			t.Fatalf("seq %d: %s", s, v)
		}
	}
	if tr.gaps+tr.duplicates != 0 {
		t.Fatal("nothing should be flagged")
	}
}

func TestSeqTrackerGapAndDuplicate(t *testing.T) {
	tr := newSeqTracker()
	tr.observe(0, 1, 0)
	if v := tr.observe(0, 1, 4); v != "gap" || tr.gaps != 3 {
		t.Fatalf("gap: %s %d", v, tr.gaps)
	}
	if v := tr.observe(0, 1, 4); v != "duplicate" || tr.duplicates != 1 {
		t.Fatalf("duplicate: %s %d", v, tr.duplicates)
	}
}

func TestProducersDoNotClash(t *testing.T) {
	tr := newSeqTracker()
	for s := int64(0); s < 3; s++ {
		if tr.observe(0, 5, s) != "ok" || tr.observe(1, 5, s) != "ok" {
			t.Fatal("streams of different producers must be independent")
		}
	}
}

func TestFirstMessageOnlySetsTheBaseline(t *testing.T) {
	tr := newSeqTracker()
	if tr.observe(0, 2, 500) != "ok" || tr.gaps != 0 {
		t.Fatal("a late joiner must not count 500 lost messages")
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

func TestTheSameKeyAlwaysGetsTheSameLane(t *testing.T) {
	if laneFor([]byte("user-7"), 16) != laneFor([]byte("user-7"), 16) {
		t.Fatal("the lane must be stable")
	}
	for n := 0; n < 200; n++ {
		if lane := laneFor([]byte("user-"+string(rune('a'+n%26))), 8); lane < 0 || lane >= 8 {
			t.Fatalf("lane %d out of range", lane)
		}
	}
}
