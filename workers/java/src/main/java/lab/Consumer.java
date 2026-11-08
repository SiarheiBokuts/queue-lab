package lab;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/** The consumer (the "handler"): reads messages in a consumer group, verifies them, runs the chosen scenario. */
final class Consumer {
  private Consumer() {}

  private static final class State {
    long received;
    long errors;
    long[] hist = new long[Settings.EDGES.length + 1];
    Map<String, Long> offsets = new HashMap<>();
    TreeSet<Integer> assigned = new TreeSet<>();
    Double firstAt;
    Double lastAt;
    double maxLatencyMs;
    Lib.SeqTracker tracker = new Lib.SeqTracker(); // detects gaps and duplicates per stream
  }

  /** Messages that may wait in front of one lane before the reading loop pauses. */
  static final int LANE_QUEUE = 8;

  static void run() throws Exception {
    String scenario = System.getenv().getOrDefault("SCENARIO", "echo");
    int param = Integer.parseInt(System.getenv().getOrDefault("SCENARIO_PARAM", "0"));
    State state = new State();

    // Offset reset, commit and timeout settings come from the backend (the same values for every language).
    var properties = KafkaConf.fromEnvironment("CONSUMER_CONF", 0);
    properties.put("group.id", Settings.required("GROUP_ID"));
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    var consumer = new KafkaConsumer<byte[], byte[]>(properties);
    consumer.subscribe(List.of(Settings.TOPIC), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // Kafka gave us partitions
        synchronized (state) {
          partitions.forEach(p -> state.assigned.add(p.partition()));
        }
      }

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // Kafka took partitions away
        synchronized (state) {
          partitions.forEach(p -> state.assigned.remove(p.partition()));
        }
      }
    });

    // Send cumulative counters. The histogram is sent as a delta and reset after each report.
    java.util.function.Consumer<Boolean> report = finalReport -> {
      Map<String, Object> payload = new HashMap<>();
      synchronized (state) {
        payload.put("runId", Settings.RUN);
        payload.put("worker", Settings.WORKER_ID);
        payload.put("role", "consumer");
        payload.put("lang", "java");
        payload.put("ts", Reporting.nowSeconds());
        payload.put("ready", !state.assigned.isEmpty());
        payload.put("assigned", List.copyOf(state.assigned));
        payload.put("received", state.received);
        payload.put("duplicates", state.tracker.duplicates);
        payload.put("gaps", state.tracker.gaps);
        payload.put("errors", state.errors);
        payload.put("offsets", new HashMap<>(state.offsets));
        payload.put("hist", state.hist.clone());
        payload.put("final", finalReport);
        payload.put("firstAt", state.firstAt);
        payload.put("lastAt", state.lastAt);
        payload.put("maxLatencyMs", state.maxLatencyMs);
        java.util.Arrays.fill(state.hist, 0);
      }
      Reporting.post(payload);
    };
    Reporting.startReporter(() -> report.accept(false), () -> true);

    // @scenario-start
    // What the worker does with one message. Several run at the same time when the concurrency is above 1.
    Runnable handle = () -> {
      try {
        if (scenario.equals("delay") && param > 0) {
          Thread.sleep(param); // pretend to wait for a database or an API call
        } else if (scenario.equals("cpu") && param > 0) {
          MessageDigest sha = MessageDigest.getInstance("SHA-256"); // one per message: a digest is not thread-safe
          byte[] hash = sha.digest("queuelab".getBytes());
          for (int i = 0; i < param; i++) hash = sha.digest(hash); // burn CPU like parsing or encryption would
        }
        // echo: do nothing, just count the message
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      }
    };
    // @scenario-end

    // Production style: ONE loop reads from Kafka and hands every message to a "lane". There are CONCURRENCY lanes, each
    // is a virtual thread that handles its messages one after another, so up to CONCURRENCY messages are handled at the
    int laneCount = Settings.CONCURRENCY;
    boolean laned = laneCount > 1 && !scenario.equals("echo");
    Object token = new Object();
    Object stop = new Object();
    var lanes = new java.util.ArrayList<java.util.concurrent.BlockingQueue<Object>>();
    var threads = new java.util.ArrayList<Thread>();
    for (int n = 0; laned && n < laneCount; n++) {
      var lane = new java.util.concurrent.ArrayBlockingQueue<Object>(LANE_QUEUE);
      lanes.add(lane);
      threads.add(Thread.ofVirtual().start(() -> {
        try {
          while (lane.take() != stop) handle.run();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }));
    }
    int[] nextLane = {0};
    java.util.function.Consumer<byte[]> dispatch = key -> {
      if (!laned) {
        handle.run(); // one at a time, or nothing to wait for
        return;
      }
      int index = Settings.ORDERING.equals("key") ? Lib.laneFor(key, laneCount) : nextLane[0]++ % laneCount;
      try {
        lanes.get(index).put(token); // blocks while that lane is full
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      while (!Reporting.stopping) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
          double arrived = Lib.nowMs();
          var header = Lib.parseHeader(record.value(), Settings.RUN);
          if (header.isEmpty()) continue; // a leftover from another run: ignore it
          var h = header.get();
          synchronized (state) {
            state.received++;
            if (state.firstAt == null) state.firstAt = arrived / 1000.0;
            state.lastAt = Reporting.nowSeconds();
            state.tracker.observe(h.producer(), record.partition(), h.seq()); // counts gaps (lost) and duplicates
            state.offsets.put(Integer.toString(record.partition()), record.offset() + 1); // next offset, used for lag
            double latency = arrived - h.sentMs(); // time from "sent" to "received", in milliseconds
            state.hist[Lib.bucket(latency, Settings.EDGES)]++;
            state.maxLatencyMs = Math.max(state.maxLatencyMs, latency);
          }
          dispatch.accept(record.key() == null ? new byte[0] : record.key());
        }
      }
    } finally {
      for (var lane : lanes) lane.put(stop); // no more work: each lane finishes what it holds and stops
      for (Thread thread : threads) thread.join(); // let the messages already taken finish before the final report      report.accept(true);
      consumer.close(); // leave the group cleanly so the partitions are reassigned at once
    }
  }
}
