package lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * The consumer (the "handler"): reads messages in batches, verifies them and runs the chosen
 * scenario.
 *
 * <p>The same loop is written in every language of the lab, with the same steps and names:
 *
 * <pre>readBatch -> groupByUser -> runBatch (handleGroup for every user at the same time) -> commit
 * </pre>
 *
 * <p>A batch is at most CONCURRENCY messages. The messages of one user are always handled one after
 * another, like in production; the messages of different users are handled at the same time. The
 * next batch is read when the whole batch is done: the client has already fetched the next messages
 * in the background, and starting early could let a message of a busy user overtake an older one.
 */
final class Consumer {
  private Consumer() {}

  private static volatile long
      sink; // the result of the json scenario goes here so the work is not optimized away
  private static final ObjectMapper MAPPER =
      new ObjectMapper(); // Jackson, the usual JSON library of Java services
  private static final String SCENARIO = System.getenv().getOrDefault("SCENARIO", "echo");
  private static final int PARAM =
      Integer.parseInt(System.getenv().getOrDefault("SCENARIO_PARAM", "0"));
  private static final Semaphore CPU_SLOTS = new Semaphore(Settings.CPU_THREADS);

  /**
   * Ask Kafka for up to CONCURRENCY messages (max.poll.records) and wait at most BATCH_WAIT_MS;
   * take what arrived.
   */
  static List<ConsumerRecord<byte[], byte[]>> readBatch(KafkaConsumer<byte[], byte[]> consumer) {
    var batch = new ArrayList<ConsumerRecord<byte[], byte[]>>();
    consumer.poll(Duration.ofMillis(Settings.BATCH_WAIT_MS)).forEach(batch::add);
    return batch;
  }

  // @scenario-start
  /**
   * What the worker does with one message. The same three scenarios in every language: echo =
   * nothing, the message is only counted; delay = wait, like a call to a database; json = parse a
   * JSON document and write it back, like a service reading a request.
   */
  static void handle() throws Exception {
    if (SCENARIO.equals("delay") && PARAM > 0) {
      Thread.sleep(PARAM); // wait PARAM milliseconds and do nothing else
    } else if (SCENARIO.equals("json") && PARAM > 0) {
      CPU_SLOTS
          .acquire(); // only one message per core may work at once, so no language takes the cores
      // of another
      try {
        String text = Settings.JSON_DOC;
        for (int i = 0; i < PARAM; i++) { // parse the document and write it back, PARAM times
          text = MAPPER.writeValueAsString(MAPPER.readTree(text));
        }
        sink = text.length(); // a volatile write, so the JIT cannot drop the work
      } finally {
        CPU_SLOTS.release();
      }
    }
    // echo: nothing to do
  }

  // @scenario-end

  /** Handle the messages of one user one after another. Returns the time the group was done. */
  static long handleGroup(List<Lib.Item> group, Lib.OrderCheck order) throws Exception {
    for (Lib.Item item : group) {
      order.check(item);
      handle();
    }
    return System.nanoTime(); // the time this group was done
  }

  /**
   * Handle all groups at the same time and wait until every one of them is done. Returns how long
   * the batch took and how long of that some groups were done while others still worked
   * (milliseconds).
   */
  static double[] runBatch(List<List<Lib.Item>> groups, Lib.OrderCheck order) throws Exception {
    long started = System.nanoTime();
    var done = new ArrayList<Long>();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var running = new ArrayList<Future<Long>>();
      for (var group : groups) running.add(executor.submit(() -> handleGroup(group, order)));
      for (var future : running) done.add(future.get());
    }
    return Lib.batchTiming(started, done);
  }

  /** Tell Kafka that everything read so far is handled. A crash before this repeats the batch. */
  static void commit(KafkaConsumer<byte[], byte[]> consumer) {
    consumer.commitSync();
  }

  static void run() throws Exception {
    var stats = new ConsumerStats();

    // Offset reset, commit and timeout settings come from the backend (the same values for every
    // language).
    var properties = KafkaConf.fromEnvironment("CONSUMER_CONF", 0);
    properties.put("group.id", Settings.required("GROUP_ID"));
    properties.put("max.poll.records", Integer.toString(Settings.CONCURRENCY)); // the batch size
    properties.put(
        "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(
        "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    var consumer = new KafkaConsumer<byte[], byte[]>(properties);
    consumer.subscribe(
        List.of(Settings.TOPIC),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            stats.assign(partitions.stream().map(TopicPartition::partition).toList());
          }

          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            stats.revoke(partitions.stream().map(TopicPartition::partition).toList());
          }
        });
    Reporting.startReporter(() -> stats.report(false), () -> true);

    try {
      while (!Reporting.stopping) {
        List<ConsumerRecord<byte[], byte[]>> batch;
        try {
          batch = readBatch(consumer);
        } catch (org.apache.kafka.common.KafkaException e) {
          stats.countError();
          continue;
        }
        if (batch.isEmpty()) continue;
        double arrived = Lib.nowMs();
        var items = new ArrayList<Lib.Item>();
        for (ConsumerRecord<byte[], byte[]> record : batch) {
          var header = Lib.parseHeader(record.value(), Settings.RUN);
          if (header.isEmpty()) continue; // a leftover from another run: ignore it
          stats.record(header.get(), record.partition(), record.offset(), arrived);
          items.add(
              new Lib.Item(
                  record.key() == null ? new byte[0] : record.key(),
                  header.get().producer(),
                  header.get().seq()));
        }
        var timing = runBatch(Lib.groupByUser(items), stats.order);
        stats.addBatch(timing[0], timing[1]);
        commit(consumer);
      }
    } finally {
      stats.report(true);
      consumer.close(); // leave the group cleanly so the partitions are reassigned at once
    }
  }
}
