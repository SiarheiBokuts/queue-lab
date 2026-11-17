package lab;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The producer: sends its share of the messages over all partitions, at the requested rate, then
 * reports "done".
 *
 * <p>The file reads top to bottom in the same groups as the producer of every other language:
 *
 * <ol>
 *   <li>settings: what this producer was asked to do
 *   <li>counters: what the report shows, shared by the loop, the delivery callbacks and the
 *       reporter
 *   <li>client: the Kafka producer and its delivery reports
 *   <li>message: how one message is built and handed to the client
 *   <li>run: the steps in order (the sending loop is the part shown on the Code page)
 * </ol>
 */
final class Producer {
  private Producer() {}

  /**
   * Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing.
   */
  private static final int POOL_BYTES = 1 << 20;

  // ---- 1. settings -------------------------------------------------------------------------

  /**
   * @param total this producer's share of the messages
   * @param size bytes per message
   * @param partitions partitions of the queue
   * @param index which producer this is (0, 1, ...) when several share the load
   * @param rate this producer's target messages per second; 0 = as fast as possible
   * @param randomPayload incompressible data instead of a repeated filler
   * @param users how many different users (message keys) the messages are spread over
   */
  record ProducerSettings(
      long total,
      int size,
      int partitions,
      int index,
      double rate,
      boolean randomPayload,
      int users) {}

  static ProducerSettings readSettings() {
    return new ProducerSettings(
        Long.parseLong(Settings.required("MESSAGES")),
        Integer.parseInt(Settings.required("MSG_SIZE")),
        Integer.parseInt(Settings.required("PARTITIONS")),
        Integer.parseInt(Settings.required("PRODUCER_INDEX")),
        Double.parseDouble(Settings.required("RATE")),
        Settings.required("PAYLOAD").equals("random"),
        Integer.parseInt(Settings.required("USERS")));
  }

  // ---- 2. counters -------------------------------------------------------------------------

  /** Every method locks the object, so the loop, the delivery callbacks and the reporter agree. */
  static final class Counters {
    private long sent;
    private long delivered;
    private long errors;
    private boolean done;
    private Double started; // seconds since 1970; null = not yet
    private Double finished;

    synchronized void setSent(long n) {
      sent = n;
    }

    /** Called for every message once the broker confirmed or rejected it. */
    synchronized void confirmed(boolean failed) {
      if (failed) {
        errors++;
      } else {
        delivered++;
      }
    }

    synchronized void start() {
      started = Reporting.nowSeconds();
    }

    synchronized void finish() {
      finished = Reporting.nowSeconds();
      done = true;
    }

    synchronized boolean running() {
      return !done;
    }

    void report() {
      Map<String, Object> payload = new HashMap<>();
      synchronized (this) {
        payload.put("runId", Settings.RUN);
        payload.put("worker", Settings.WORKER_ID);
        payload.put("role", "producer");
        payload.put("lang", "java");
        payload.put("ts", Reporting.nowSeconds());
        payload.put("sent", sent);
        payload.put("delivered", delivered);
        payload.put("errors", errors);
        payload.put("done", done);
        payload.put("started", started);
        payload.put("finished", finished);
      }
      Reporting.post(payload);
    }
  }

  // ---- 3. client ---------------------------------------------------------------------------

  static KafkaProducer<byte[], byte[]> newKafkaProducer(ProducerSettings cfg) {
    // Batching, acks, queue size and compression come from the backend (the same values for every
    // language).
    var properties = KafkaConf.fromEnvironment("PRODUCER_CONF", cfg.size());
    properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return new KafkaProducer<>(properties);
  }

  /** The delivery report: the broker confirmed or rejected a message. */
  static Callback deliveryCallback(Counters counters) {
    return (metadata, error) -> counters.confirmed(error != null);
  }

  // ---- 4. message --------------------------------------------------------------------------

  /** One message, ready to send: the partition it goes to, its user key and its bytes. */
  record Message(int partition, byte[] key, byte[] value) {}

  /**
   * Builds message number i of this producer: which partition, which user, and the bytes to send.
   * It owns what has to be remembered between messages: the sequence counter of every partition.
   */
  static final class MessageMaker {
    private final ProducerSettings cfg;
    // Each partition has its own sequence counter for this producer, starting at 0.
    private final long[] seqs;
    // The data random padding is cut from (empty when the padding is a repeated filler).
    private final byte[] pool;

    MessageMaker(ProducerSettings cfg) {
      this.cfg = cfg;
      this.seqs = new long[cfg.partitions()];
      if (cfg.randomPayload()) {
        this.pool = new byte[POOL_BYTES + cfg.size()];
        new Random().nextBytes(pool);
      } else {
        this.pool = new byte[0];
      }
    }

    Message make(long i) {
      // Round robin; producers start on different partitions so the load stays even.
      int part = (int) ((i + cfg.index()) % cfg.partitions());
      byte[] header = Lib.buildHeader(Settings.RUN, cfg.index(), part, seqs[part], Lib.nowMs());
      seqs[part]++;
      byte[] key = Lib.userKey(part, i, cfg.partitions(), cfg.users());
      return new Message(part, key, pad(header, i));
    }

    /**
     * The header (who sent it, where, which number, when) padded up to the requested message size.
     */
    private byte[] pad(byte[] header, long i) {
      int fill = Math.max(0, cfg.size() - header.length);
      byte[] payload = new byte[header.length + fill];
      System.arraycopy(header, 0, payload, 0, header.length);
      if (cfg.randomPayload()) {
        int offset = (int) ((i * 997) % POOL_BYTES);
        System.arraycopy(pool, offset, payload, header.length, fill);
      } else {
        Arrays.fill(payload, header.length, payload.length, (byte) 'x');
      }
      return payload;
    }
  }

  /** Hands a message to the client; the client blocks by itself while its local queue is full. */
  static void send(KafkaProducer<byte[], byte[]> producer, Message message, Callback onDelivery) {
    producer.send(
        new ProducerRecord<>(Settings.TOPIC, message.partition(), message.key(), message.value()),
        onDelivery);
  }

  // ---- 5. run ------------------------------------------------------------------------------

  static void run() throws Exception {
    ProducerSettings cfg = readSettings();
    Counters counters = new Counters();
    KafkaProducer<byte[], byte[]> producer = newKafkaProducer(cfg);
    Callback onDelivery = deliveryCallback(counters);
    MessageMaker maker = new MessageMaker(cfg);
    Reporting.startReporter(counters::report, counters::running);

    // @producer-start
    long begin = System.nanoTime();
    counters.start();
    int checkEvery = Lib.pacingChunk(cfg.rate(), 500); // how often to look at the clock
    for (long i = 0; i < cfg.total() && !Reporting.stopping; i++) {
      var message = maker.make(i); // message number i: its partition, its user and its bytes
      send(producer, message, onDelivery);
      counters.setSent(i + 1);
      if ((i + 1) % checkEvery == 0) {
        // Ahead of the target rate? Then wait.
        long wait = Lib.pacingDelayNanos(i + 1, cfg.rate(), System.nanoTime() - begin);
        if (wait > 0) {
          LockSupport.parkNanos(wait);
        }
      }
    }
    // @producer-end
    producer.flush(); // wait until every message is confirmed by the broker
    counters.finish();
    counters.report();
    producer.close();
  }
}
