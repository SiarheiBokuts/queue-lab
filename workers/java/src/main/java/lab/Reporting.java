package lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Progress reporting shared by the producer and the consumer: the stop flag and the HTTP report. */
final class Reporting {
  private Reporting() {}

  /** Set on SIGTERM so loops can finish and send a final report. */
  static volatile boolean stopping = false;

  // HTTP/1.1 on purpose: the default tries an HTTP/2 upgrade, which the backend does not speak, and loses the body.
  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(3))
      .build();
  private static final ObjectMapper JSON = new ObjectMapper();

  /** POST a progress report to the backend. A failed report is logged and skipped, never fatal. */
  static void post(Map<String, Object> payload) {
    try {
      HttpRequest request = HttpRequest.newBuilder(URI.create(Settings.REPORT_URL))
          .timeout(Duration.ofSeconds(3))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload)))
          .build();
      CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    } catch (Exception e) {
      System.out.println("report failed: " + e.getMessage());
    }
  }

  /** Call {@code send} once per second on a background thread until the worker stops. */
  static void startReporter(Runnable send, BooleanSupplier keepGoing) {
    Thread thread = new Thread(() -> {
      while (!stopping && keepGoing.getAsBoolean()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
        send.run();
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  /** Seconds since the epoch, as the backend expects timestamps. */
  static double nowSeconds() {
    return Lib.nowMs() / 1000.0;
  }
}
