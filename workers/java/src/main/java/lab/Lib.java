package lab;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pure helpers of the Java worker: no Kafka, no network, no environment. Easy to unit test.
 */
public final class Lib {
  private Lib() {}

  /** The header is far shorter than this; we never look at the (possibly large) padding to read it. */
  static final int HEADER_MAX_BYTES = 96;

  /** Parse the HIST_EDGES value, e.g. "0.5,1,2,5", into bucket edges in milliseconds. */
  public static double[] parseEdges(String raw) {
    List<Double> edges = new ArrayList<>();
    for (String part : raw.split(",")) {
      if (!part.isBlank()) edges.add(Double.parseDouble(part.trim()));
    }
    return edges.stream().mapToDouble(Double::doubleValue).toArray();
  }

  /**
   * The histogram bucket a latency falls into: bucket i holds latencies &lt;= edges[i]; the last bucket (index
   * edges.length) holds everything above the last edge. Counting messages per bucket keeps reports small.
   */
  public static int bucket(double latencyMs, double[] edges) {
    for (int i = 0; i < edges.length; i++) {
      if (latencyMs <= edges[i]) return i;
    }
    return edges.length;
  }

  /** Header of a message; the producer appends padding after it. */
  public static byte[] buildHeader(String run, int producer, int partition, long seq, double sentMs) {
    return String.format(java.util.Locale.ROOT, "%s|%d|%d|%d|%.3f|", run, producer, partition, seq, sentMs)
        .getBytes(StandardCharsets.ISO_8859_1);
  }

  /** What {@link #parseHeader} reads from the start of a message. */
  public record Header(int producer, int partition, long seq, double sentMs) {}

  /** Read the start of a message. Empty if it is malformed or not part of {@code expectedRun}. */
  public static Optional<Header> parseHeader(byte[] value, String expectedRun) {
    String head = new String(value, 0, Math.min(value.length, HEADER_MAX_BYTES), StandardCharsets.ISO_8859_1);
    String[] parts = head.split("\\|", 6);
    if (parts.length < 6 || !parts[0].equals(expectedRun)) return Optional.empty();
    try {
      int producer = Integer.parseInt(parts[1]);
      int partition = Integer.parseInt(parts[2]);
      long seq = Long.parseLong(parts[3]);
      double sentMs = Double.parseDouble(parts[4]);
      if (producer < 0 || partition < 0 || seq < 0) return Optional.empty();
      return Optional.of(new Header(producer, partition, seq, sentMs));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /** What {@link SeqTracker#observe} decided about a message. */
  public enum Verdict { OK, GAP, DUPLICATE }

  /**
   * Detects lost and duplicated messages from per-stream sequence numbers.
   *
   * <p>Within one partition Kafka preserves order, so after sequence N of a stream (one producer, one partition)
   * the next message of that stream must be N + 1. A higher number means messages were skipped (gap); a number we
   * already saw (or lower) means a duplicate. The first message seen on a stream only sets the baseline, because a
   * worker may join mid-run after a rebalance.
   */
  public static final class SeqTracker {
    private final Map<Long, Long> last = new HashMap<>();
    public long duplicates = 0;
    /** Total number of skipped sequence numbers. */
    public long gaps = 0;

    public Verdict observe(int producer, int partition, long seq) {
      long stream = ((long) producer << 32) | partition;
      Long previous = last.get(stream);
      if (previous == null || seq == previous + 1) {
        last.put(stream, seq);
        return Verdict.OK;
      }
      if (seq <= previous) {
        duplicates++;
        return Verdict.DUPLICATE;
      }
      gaps += seq - previous - 1;
      last.put(stream, seq);
      return Verdict.GAP;
    }
  }

  /**
   * How many nanoseconds a producer must wait so that {@code sent} messages take at least sent / rate seconds.
   * Zero means "go on". A rate of 0 means "as fast as possible", and a producer behind schedule never waits.
   */
  /**
   * Message key of the user a message belongs to, like a payment of one user. A user always lands in the same partition
   * (user number % partitions == partition), so Kafka keeps that user's messages in order.
   */
  public static byte[] userKey(int part, long i, int nparts, int users) {
    int perPartition = Math.max(1, users / nparts);
    long user = part + (long) nparts * ((i / nparts) % perPartition);
    return ("user-" + user).getBytes(StandardCharsets.US_ASCII);
  }

  /** The lane (0 .. lanes-1) that handles messages with this key. The same key always gets the same lane (FNV-1a). */
  public static int laneFor(byte[] key, int lanes) {
    int hash = 0x811c9dc5;
    for (byte b : key) {
      hash ^= (b & 0xff);
      hash *= 0x01000193;
    }
    return Math.floorMod(hash, lanes);
  }

  public static long pacingDelayNanos(long sent, double rate, long elapsedNanos) {
    if (rate <= 0) return 0;
    return Math.max(0, (long) (sent / rate * 1e9) - elapsedNanos);
  }

  /** How many messages to send between two pacing checks: about 10 ms worth, so the rate stays smooth. */
  public static int pacingChunk(double rate, int maxChunk) {
    if (rate <= 0) return maxChunk;
    return Math.max(1, Math.min(maxChunk, (int) (rate / 100)));
  }

  /** Wall-clock time in milliseconds with sub-millisecond precision. */
  public static double nowMs() {
    java.time.Instant now = java.time.Instant.now();
    return now.getEpochSecond() * 1000.0 + now.getNano() / 1e6;
  }
}
