package demo;

/** Queue transport lifecycle. Business transitions remain in the PostgreSQL ledger. */
interface DeliveryBackend extends AutoCloseable {
  @FunctionalInterface interface WorkHandler {void handle(String requestId) throws Exception;}
  @FunctionalInterface interface Clock {long now() throws java.sql.SQLException;}
  void start(String instance) throws Exception;
  void dispatcher();
  void restoreConsumers();
  int publishedCount();
  boolean isOpen();
  @Override void close();
}
