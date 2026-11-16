package lab;

/**
 * Everything a worker reads from its environment, parsed once when this class is first used.
 *
 * <p>The environment is the "worker contract" defined in the backend (backend/app/contract.py).
 * Kafka client tuning (batching, acks, timeouts) is NOT decided here: the backend sends the same
 * values to every language (PRODUCER_CONF / CONSUMER_CONF), so comparisons between languages are
 * fair.
 */
final class Settings {
  private Settings() {}

  /** Read a required environment variable or stop with a clear message. */
  static String required(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty())
      throw new IllegalStateException("Missing environment variable " + name);
    return value;
  }

  static final String ROLE = required("ROLE"); // "producer" or "consumer"
  static final String BOOT = required("KAFKA_BOOTSTRAP"); // broker address
  static final String TOPIC = required("TOPIC"); // the queue
  static final String RUN =
      required("RUN_ID"); // id of this run; messages of other runs are ignored
  static final String WORKER_ID =
      System.getenv().getOrDefault("WORKER_ID", "worker"); // identifies this worker
  static final String REPORT_URL =
      required("REPORT_URL"); // where the once-per-second report is POSTed

  /**
   * Upper edge of each latency bucket in milliseconds; the backend sends them so they are defined
   * only once.
   */
  static final double[] EDGES = Lib.parseEdges(required("HIST_EDGES"));

  // How many messages one consumer may compute at the same time (the same budget in every
  // language).
  /** The document of the "json" scenario, the same bytes in every language (consumer only). */
  static final String JSON_DOC = System.getenv().getOrDefault("JSON_DOC", "{}");

  static final int CPU_THREADS =
      Math.max(1, Integer.parseInt(System.getenv().getOrDefault("CPU_THREADS", "1")));

  /** The batch size: messages one consumer reads and handles at the same time (consumer only). */
  static final int CONCURRENCY =
      Math.max(1, Integer.parseInt(System.getenv().getOrDefault("CONCURRENCY", "1")));

  /** The longest a consumer waits for a batch to fill (consumer only). */
  static final int BATCH_WAIT_MS =
      Math.max(1, Integer.parseInt(System.getenv().getOrDefault("BATCH_WAIT_MS", "50")));
}
