package lab;

/**
 * queue-lab worker (Java): the entry point. One image, two roles, chosen by the ROLE variable.
 */
public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    Thread main = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> { // docker stop sends SIGTERM
      Reporting.stopping = true;
      try {
        main.join(5000); // let the loop finish and send its final report
      } catch (InterruptedException ignored) {
        // exiting anyway
      }
    }));
    System.out.println(Settings.ROLE + " " + Settings.WORKER_ID + " starting");
    if (Settings.ROLE.equals("consumer")) Consumer.run();
    else Producer.run();
  }
}
