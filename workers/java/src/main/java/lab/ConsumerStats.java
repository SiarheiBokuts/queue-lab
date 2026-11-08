package lab;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * What a consumer counts about the messages it reads, and the report it sends once per second.
 *
 * <p>Every method is synchronized, because the reporter thread reads the counters while the batch loop writes them.
 */
final class ConsumerStats {
  private long received;
  private long errors;
  private final long[] hist = new long[Settings.EDGES.length + 1];
  private final Map<String, Long> offsets = new HashMap<>(); // the next offset to read per partition, for the lag
  private final TreeSet<Integer> assigned = new TreeSet<>();
  private Double firstAt; // seconds since the epoch
  private Double lastAt;
  private double maxLatencyMs;
  private final Lib.SeqRanges seen = new Lib.SeqRanges(); // which message numbers arrived: the lab counts lost and repeated ones from it

  /** Kafka gave us partitions (also called again after every rebalance). */
  synchronized void assign(Collection<Integer> partitions) {
    assigned.addAll(partitions);
  }

  /** Kafka took partitions away, usually because another worker joined or left. */
  synchronized void revoke(Collection<Integer> partitions) {
    assigned.removeAll(partitions);
  }

  /** Count a read that failed. */
  synchronized void countError() {
    errors++;
  }

  /** Count one message of this run that arrived at {@code arrivedMs} in {@code partition}. */
  synchronized void record(Lib.Header header, int partition, long offset, double arrivedMs) {
    received++;
    if (firstAt == null) firstAt = arrivedMs / 1000.0;
    lastAt = Reporting.nowSeconds();
    seen.observe(header.producer(), partition, header.seq());
    offsets.put(Integer.toString(partition), offset + 1);
    double latency = arrivedMs - header.sentMs(); // from "sent" to "received", in milliseconds
    hist[Lib.bucket(latency, Settings.EDGES)]++;
    maxLatencyMs = Math.max(maxLatencyMs, latency);
  }

  /** Send the cumulative counters. The histogram goes as the change since the last report, then it is reset. */
  void report(boolean finalReport) {
    Map<String, Object> payload = new HashMap<>();
    synchronized (this) {
      payload.put("runId", Settings.RUN);
      payload.put("worker", Settings.WORKER_ID);
      payload.put("role", "consumer");
      payload.put("lang", "java");
      payload.put("ts", Reporting.nowSeconds());
      payload.put("ready", !assigned.isEmpty());
      payload.put("assigned", List.copyOf(assigned));
      payload.put("received", received);
      payload.put("duplicates", seen.repeats);
      payload.put("ranges", seen.report());
      payload.put("overflow", seen.overflow);
      payload.put("errors", errors);
      payload.put("offsets", new HashMap<>(offsets));
      payload.put("hist", hist.clone());
      payload.put("final", finalReport);
      payload.put("firstAt", firstAt);
      payload.put("lastAt", lastAt);
      payload.put("maxLatencyMs", maxLatencyMs);
      Arrays.fill(hist, 0);
    }
    Reporting.post(payload);
  }
}
