package demo;

/** Integrated standalone application bootstrap.
 * Maintains the original engine implementation under ActivationWorkerService until
 * its production-oriented refactoring and Spring Boot adapter are delivered.
 */
public final class JamesApplication {
  private JamesApplication() {}
  public static void main(String[] args) throws Exception {
    new ActivationWorkerService().start();
  }
}
