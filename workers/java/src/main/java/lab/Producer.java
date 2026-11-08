package lab;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done". */
final class Producer {
  private Producer() {}

  /** Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing. */
  private static final int POOL_BYTES = 1 << 20;

  private static final class State {
    long sent;
    boolean done;
    Double started;
    Double finished;
  }

  static void run() throws Exception {
    long total = Long.parseLong(Settings.required("MESSAGES")); // this producer's share of the messages
    int size = Integer.parseInt(Settings.required("MSG_SIZE"));
    int nparts = Integer.parseInt(Settings.required("PARTITIONS"));
    int index = Integer.parseInt(Settings.required("PRODUCER_INDEX")); // which producer this is (0, 1, ...)
    double rate = Double.parseDouble(Settings.required("RATE")); // target messages per second; 0 = as fast as possible
    boolean randomPayload = Settings.required("PAYLOAD").equals("random"); // incompressible data, not a repeated filler
    int users = Integer.parseInt(Settings.required("USERS")); // how many different users (message keys) the messages are spread over

    AtomicLong delivered = new AtomicLong();
    AtomicLong errors = new AtomicLong();
    State state = new State();

    // Batching, acks, queue size and compression come from the backend (the same values for every language).
    var properties = KafkaConf.fromEnvironment("PRODUCER_CONF", size);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    var producer = new KafkaProducer<byte[], byte[]>(properties);

    byte[] pool = new byte[randomPayload ? POOL_BYTES + size : 0];
    new Random().nextBytes(pool);

    Runnable report = () -> {
      Map<String, Object> payload = new HashMap<>();
      synchronized (state) {
        payload.put("runId", Settings.RUN);
        payload.put("worker", Settings.WORKER_ID);
        payload.put("role", "producer");
        payload.put("lang", "java");
        payload.put("ts", Reporting.nowSeconds());
        payload.put("sent", state.sent);
        payload.put("delivered", delivered.get());
        payload.put("errors", errors.get());
        payload.put("done", state.done);
        payload.put("started", state.started);
        payload.put("finished", state.finished);
      }
      Reporting.post(payload);
    };
    Reporting.startReporter(report, () -> {
      synchronized (state) {
        return !state.done;
      }
    });

    // @producer-start
    long[] seqs = new long[nparts]; // each partition has its own sequence counter for this producer, starting at 0
    long begin = System.nanoTime();
    synchronized (state) {
      state.started = Reporting.nowSeconds();
    }
    int checkEvery = Lib.pacingChunk(rate, 500); // how often to look at the clock
    for (long i = 0; i < total && !Reporting.stopping; i++) {
      int part = (int) ((i + index) % nparts); // round robin; producers start on different partitions so the load stays even
      byte[] header = Lib.buildHeader(Settings.RUN, index, part, seqs[part]++, Lib.nowMs());
      int fill = Math.max(0, size - header.length); // pad up to the requested message size
      byte[] payload = new byte[header.length + fill];
      System.arraycopy(header, 0, payload, 0, header.length);
      if (randomPayload) {
        System.arraycopy(pool, (int) ((i * 997) % POOL_BYTES), payload, header.length, fill);
      } else {
        java.util.Arrays.fill(payload, header.length, payload.length, (byte) 'x');
      }
      producer.send(new ProducerRecord<>(Settings.TOPIC, part, Lib.userKey(part, i, nparts, users), payload), (metadata, error) -> {
        if (error != null) errors.incrementAndGet();
        else delivered.incrementAndGet();
      });
      synchronized (state) {
        state.sent = i + 1;
      }
      if ((i + 1) % checkEvery == 0) {
        long wait = Lib.pacingDelayNanos(i + 1, rate, System.nanoTime() - begin); // ahead of the target rate? then wait
        if (wait > 0) java.util.concurrent.locks.LockSupport.parkNanos(wait);
      }
    }
    // @producer-end
    producer.flush(); // wait until every message is confirmed by the broker
    synchronized (state) {
      state.finished = Reporting.nowSeconds();
      state.done = true;
    }
    report.run();
    producer.close();
  }
}
