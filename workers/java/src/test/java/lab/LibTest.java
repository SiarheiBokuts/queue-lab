package lab;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class LibTest {
  private static final double[] EDGES = {0.5, 1, 2, 5, 10};

  @Test
  void parsesEdges() {
    assertArrayEquals(new double[] {0.5, 1, 2}, Lib.parseEdges(" 0.5, 1 ,2,"));
  }

  @Test
  void bucketBoundaries() {
    double[][] cases = {
      {0, 0}, {0.5, 0}, {0.51, 1}, {1, 1}, {4.9, 3}, {5, 3}, {10, 4}, {10.1, 5}, {99999, 5}
    };
    for (double[] c : cases) assertEquals((int) c[1], Lib.bucket(c[0], EDGES), "latency " + c[0]);
  }

  @Test
  void headerRoundTrip() {
    byte[] header = Lib.buildHeader("abc12345", 2, 3, 42, 1794147580123.456);
    byte[] message = new byte[header.length + 100];
    System.arraycopy(header, 0, message, 0, header.length);
    var parsed = Lib.parseHeader(message, "abc12345").orElseThrow();
    assertEquals(new Lib.Header(2, 3, 42, 1794147580123.456), parsed);
  }

  @Test
  void headerOfAnotherRunIsIgnored() {
    assertTrue(Lib.parseHeader(Lib.buildHeader("other", 0, 0, 0, 1), "abc12345").isEmpty());
  }

  @Test
  void malformedHeadersAreRejected() {
    for (String garbage :
        new String[] {
          "", "no separators", "a|b|c", "run|0|x|y|z|pad", "run|0|1|2|notanumber|pad"
        }) {
      assertTrue(
          Lib.parseHeader(garbage.getBytes(StandardCharsets.ISO_8859_1), "run").isEmpty(), garbage);
    }
  }

  @Test
  void numbersOneAfterAnotherMakeOneRange() {
    var seen = new Lib.SeqRanges();
    for (long s = 0; s < 5; s++) assertTrue(seen.observe(0, 0, s));
    assertEquals(java.util.List.of(java.util.List.of(0L, 4L)), seen.report().get("0:0"));
    assertEquals(0, seen.repeats + seen.overflow);
  }

  @Test
  void aSkippedNumberLeavesAGapBetweenTwoRanges() {
    var seen = new Lib.SeqRanges();
    for (long s : new long[] {0, 1, 2, 5, 6}) seen.observe(0, 0, s);
    assertEquals(
        java.util.List.of(java.util.List.of(0L, 2L), java.util.List.of(5L, 6L)),
        seen.report().get("0:0"));
  }

  @Test
  void aNumberSeenTwiceIsARepeat() {
    var seen = new Lib.SeqRanges();
    for (long s = 0; s < 3; s++) seen.observe(1, 0, s);
    assertTrue(!seen.observe(1, 0, 1));
    assertTrue(!seen.observe(1, 0, 0));
    assertEquals(2, seen.repeats);
  }

  @Test
  void aReplayedBatchFillsAGapAndTheRangesAreMerged() {
    var seen = new Lib.SeqRanges();
    for (long s : new long[] {0, 1, 4, 5}) seen.observe(0, 0, s);
    assertTrue(seen.observe(0, 0, 2) && seen.observe(0, 0, 3));
    assertEquals(java.util.List.of(java.util.List.of(0L, 5L)), seen.report().get("0:0"));
  }

  @Test
  void streamsOfDifferentSendersAndPartitionsDoNotClash() {
    var seen = new Lib.SeqRanges();
    for (long s = 0; s < 3; s++)
      assertTrue(seen.observe(0, 5, s) && seen.observe(1, 5, s) && seen.observe(0, 6, s));
    assertEquals(0, seen.repeats);
    assertEquals(3, seen.report().size());
  }

  @Test
  void aStreamWithTooManyGapsStopsListingButKeepsCounting() {
    var seen = new Lib.SeqRanges();
    for (long s = 0; s < Lib.MAX_RANGES * 2L + 10; s += 2) seen.observe(0, 0, s);
    assertEquals(Lib.MAX_RANGES, seen.report().get("0:0").size());
    assertTrue(seen.overflow > 0);
  }

  @Test
  void pacing() {
    assertEquals(0, Lib.pacingDelayNanos(1_000_000, 0, 1_000_000));
    assertEquals(750_000_000L, Lib.pacingDelayNanos(1000, 1000, 250_000_000L));
    assertEquals(0, Lib.pacingDelayNanos(1000, 1000, 2_000_000_000L));
    assertEquals(1000, Lib.pacingChunk(0, 1000));
    assertEquals(100, Lib.pacingChunk(10_000, 1000));
    assertEquals(1000, Lib.pacingChunk(5_000_000, 1000));
    assertEquals(1, Lib.pacingChunk(50, 1000));
  }

  @Test
  void kafkaSettingsAreRenamedOrConvertedForTheJavaClient() {
    Properties p =
        KafkaConf.translateOnly(
            Map.of(
                "fetch.wait.max.ms",
                50,
                "batch.num.messages",
                10000,
                "queue.buffering.max.messages",
                300000,
                "queue.buffering.max.kbytes",
                32768,
                "acks",
                "1",
                "enable.auto.commit",
                true),
            256);
    assertEquals("50", p.get("fetch.max.wait.ms").toString());
    assertEquals(
        "1048576",
        KafkaConf.translateOnly(Map.of("fetch.message.max.bytes", 1048576), 256)
            .get("max.partition.fetch.bytes")
            .toString());
    assertEquals(
        "1000000",
        p.get("batch.size").toString()); // 10000 x 256 bytes, cut to the 1 MB of the others
    assertEquals(
        "25600",
        KafkaConf.translateOnly(Map.of("batch.num.messages", 100), 256)
            .get("batch.size")
            .toString()); // under the cap
    assertEquals(
        String.valueOf(32L * 1024 * 1024),
        p.get("buffer.memory").toString()); // 32768 kB, like the others
    assertEquals("1", p.get("acks"));
    assertEquals("true", p.get("enable.auto.commit"));
  }

  @Test
  void aUserAlwaysLandsInTheSamePartition() {
    var seen = new java.util.HashMap<String, Integer>();
    for (int i = 0; i < 500; i++) {
      int part = i % 6;
      String key = new String(Lib.userKey(part, i, 6, 60), StandardCharsets.US_ASCII);
      assertEquals(part, seen.computeIfAbsent(key, k -> part));
    }
    assertEquals(60, seen.size());
  }

  @Test
  void messagesAreGroupedByUserAndKeepTheirOrder() {
    var items =
        java.util.List.of(
            item("user-1", 0, 1), item("user-2", 0, 2), item("user-1", 0, 3), item("user-1", 0, 4));
    var groups = Lib.groupByUser(items);
    assertEquals(2, groups.size());
    assertEquals(java.util.List.of(1L, 3L, 4L), groups.get(0).stream().map(Lib.Item::seq).toList());
    assertEquals(java.util.List.of(2L), groups.get(1).stream().map(Lib.Item::seq).toList());
    assertTrue(Lib.groupByUser(java.util.List.of()).isEmpty());
  }

  private static Lib.Item item(String user, int producer, long seq) {
    return new Lib.Item(user.getBytes(), producer, seq);
  }

  @Test
  void aBatchTakesAsLongAsItsSlowestGroup() {
    var timing =
        Lib.batchTiming(
            1_000_000_000L, java.util.List.of(1_500_000_000L, 3_000_000_000L, 2_000_000_000L));
    assertEquals(2000.0, timing[0], 0.001);
    assertEquals(1500.0, timing[1], 0.001);
    assertEquals(
        0.0,
        Lib.batchTiming(0L, java.util.List.of(400_000_000L))[1],
        0.001); // one group: nobody waits
  }

  @Test
  void aBatchWithoutGroupsTakesNoTimeAndDoesNotCrash() {
    var timing = Lib.batchTiming(5L, java.util.List.of());
    assertEquals(0.0, timing[0] + timing[1], 0.0); // all messages belonged to another run
  }

  @Test
  void aUserHandledInOrderHasNoViolation() {
    var order = new Lib.OrderCheck();
    for (long seq : new long[] {1, 2, 3, 3, 10})
      assertTrue(order.check(item("user-1", 0, seq))); // a repeat is fine
    assertEquals(0, order.violations());
  }

  @Test
  void aMessageHandledAfterALaterOneIsAViolation() {
    var order = new Lib.OrderCheck();
    order.check(item("user-1", 0, 5));
    assertTrue(!order.check(item("user-1", 0, 4)));
    assertEquals(1, order.violations());
  }

  @Test
  void usersAndSendersAreCheckedIndependently() {
    var order = new Lib.OrderCheck();
    order.check(item("user-1", 0, 9));
    assertTrue(order.check(item("user-2", 0, 1)) && order.check(item("user-1", 1, 1)));
    assertEquals(0, order.violations());
  }
}
