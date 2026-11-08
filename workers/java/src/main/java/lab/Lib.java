package lab;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

  /** Ranges kept per stream; beyond this the worker only counts (the report says how many it did not list). */
  public static final int MAX_RANGES = 1000;

  /**
   * Remembers which message numbers arrived, as ranges, so the lab can count lost and repeated messages exactly.
   *
   * <p>Every message has a number per stream (one sender, one partition). Numbers that arrive one after another make
   * one range ({@code 0-4999}), so a run without losses is one range per stream. A number that is already inside a
   * range is a repeat. The lab merges the ranges of all workers: what was sent and is in nobody's ranges is lost, what
   * is counted more than once is repeated (for example after a worker restarts and its batch is read again).
   */
  public static final class SeqRanges {
    private final Map<String, List<long[]>> streams = new HashMap<>();
    /** Numbers this worker saw twice. */
    public long repeats = 0;
    /** Numbers not listed because a stream hit {@link #MAX_RANGES}. */
    public long overflow = 0;

    /** Remember one message number. Returns false when this worker has seen it before. */
    public boolean observe(int producer, int partition, long seq) {
      List<long[]> ranges = streams.computeIfAbsent(producer + ":" + partition, k -> new ArrayList<>());
      if (!ranges.isEmpty()) {
        long[] last = ranges.get(ranges.size() - 1);
        if (seq == last[1] + 1) {
          last[1] = seq; // the usual case: the next number
          return true;
        }
      }
      for (long[] range : ranges) {
        if (range[0] <= seq && seq <= range[1]) {
          repeats++;
          return false;
        }
      }
      if (ranges.size() >= MAX_RANGES) {
        overflow++;
        return true;
      }
      ranges.add(new long[] {seq, seq});
      return true;
    }

    /** The ranges per stream, sorted and merged, keyed "producer:partition". */
    public Map<String, List<List<Long>>> report() {
      Map<String, List<List<Long>>> out = new HashMap<>();
      streams.forEach((key, ranges) -> {
        List<long[]> sorted = new ArrayList<>(ranges);
        sorted.sort((x, y) -> Long.compare(x[0], y[0]));
        List<List<Long>> merged = new ArrayList<>();
        for (long[] range : sorted) {
          List<Long> previous = merged.isEmpty() ? null : merged.get(merged.size() - 1);
          if (previous != null && range[0] <= previous.get(1) + 1) {
            previous.set(1, Math.max(previous.get(1), range[1]));
          } else {
            merged.add(new ArrayList<>(List.of(range[0], range[1])));
          }
        }
        out.put(key, merged);
      });
      return out;
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

  /**
   * Group messages by their key (the user). Returns, for each user, the positions of its messages in the batch; the
   * order inside a group is the order they were sent in.
   */
  public static List<List<Integer>> groupByUser(List<byte[]> keys) {
    Map<String, List<Integer>> groups = new LinkedHashMap<>();
    for (int position = 0; position < keys.size(); position++) {
      groups.computeIfAbsent(new String(keys.get(position), StandardCharsets.ISO_8859_1), k -> new ArrayList<>()).add(position);
    }
    return new ArrayList<>(groups.values());
  }

  /**
   * How many nanoseconds a producer must wait so that {@code sent} messages take at least sent / rate seconds.
   * Zero means "go on". A rate of 0 means "as fast as possible", and a producer behind schedule never waits.
   */
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