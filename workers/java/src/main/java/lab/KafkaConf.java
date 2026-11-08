package lab;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Turns the Kafka client settings of the backend into properties of the official Java client.
 *
 * <p>The backend sends the settings once, for every language, with librdkafka names (the C client that Python,
 * Node.js, Go and Rust use). Most names are identical in the Java client; the few that differ are only renamed here,
 * and the two that count messages instead of bytes are converted using the message size. Values are never decided here.
 */
final class KafkaConf {
  private KafkaConf() {}

  private static final long MIB = 1024L * 1024L;

  /** The settings in the environment variable, translated, plus the broker address. */
  static Properties fromEnvironment(String variable, int messageSize) {
    Map<String, Object> raw;
    try {
      raw = new ObjectMapper().readValue(Settings.required(variable), new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      throw new IllegalStateException(variable + " is not valid JSON: " + e.getMessage(), e);
    }
    Properties properties = translateOnly(raw, messageSize);
    properties.put("bootstrap.servers", Settings.BOOT);
    return properties;
  }

  /** Pure translation (no environment), unit tested. */
  static Properties translateOnly(Map<String, Object> raw, int messageSize) {
    Map<String, Object> out = new HashMap<>();
    for (Map.Entry<String, Object> entry : raw.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      switch (key) {
        case "fetch.wait.max.ms" -> out.put("fetch.max.wait.ms", value);
        case "fetch.message.max.bytes" -> out.put("max.partition.fetch.bytes", value);
        case "batch.num.messages" ->
            out.put("batch.size", (int) Math.min(16 * MIB, ((Number) value).longValue() * messageSize));
        case "queue.buffering.max.messages" ->
            out.put("buffer.memory", Math.max(32 * MIB, Math.min(64 * MIB, ((Number) value).longValue() * messageSize)));
        default -> out.put(key, value);
      }
    }
    Properties properties = new Properties();
    out.forEach((k, v) -> properties.put(k, v instanceof Boolean || v instanceof Number ? v.toString() : v));
    return properties;
  }
}
