package demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** PostgreSQL-only durable queue alternative to RabbitMQ.
 * The existing transactional outbox is the queue. A worker claims one due hint
 * with SKIP LOCKED, commits the claim, then invokes exactly the same business
 * engine as the AMQP consumer. Leases and ledger CAS tolerate redelivery.
 */
final class PostgresDelivery implements DeliveryBackend {
  private static final long LEASE_MS = 30_000;
  private static final long POLL_MS = 200;
  private final JdbcRabbitLedger ledger;
  private final DeliveryBackend.WorkHandler work;
  private final DeliveryBackend.Clock clock;
  private final int concurrency;
  private final AtomicInteger published = new AtomicInteger();
  private volatile boolean active;
  private ExecutorService workers;

  PostgresDelivery(JdbcRabbitLedger ledger, DeliveryBackend.WorkHandler work,
                   DeliveryBackend.Clock clock) {
    this.ledger=ledger;
    this.work=work;
    this.clock=clock;
    String configured=System.getenv().getOrDefault("ACTIVATION_PG_WORKERS", "4");
    try {concurrency=Integer.parseInt(configured);}
    catch(NumberFormatException e){throw new IllegalArgumentException("Invalid ACTIVATION_PG_WORKERS",e);}
    if(concurrency<1||concurrency>64)throw new IllegalArgumentException("ACTIVATION_PG_WORKERS must be 1..64");
  }

  record Claim(String nid,String rid,String token,long epoch){}

  /** Atomic claim uses the PostgreSQL clock and never holds a row lock during HTTP I/O. */
  private Claim claim() throws SQLException {
    try(Connection db=ledger.connect()) {
      db.setAutoCommit(false);
      try {
        String nid, rid;
        try(PreparedStatement select=db.prepareStatement("""
            SELECT o.nid,o.rid
            FROM p9_outbox o JOIN p9_requests r ON r.rid=o.rid
            WHERE o.published_ms IS NULL
              AND o.due_ms <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT
              AND o.token_until <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT
              AND r.stage IN ('READY','WAITING')
              AND r.due_ms <= (EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT
            ORDER BY o.due_ms,o.nid LIMIT 1
            FOR UPDATE OF o SKIP LOCKED
            """);ResultSet rs=select.executeQuery()) {
          if(!rs.next()) {db.rollback();return null;}
          nid=rs.getString(1);rid=rs.getString(2);
        }
        String token=UUID.randomUUID().toString();
        long epoch;
        try(PreparedStatement update=db.prepareStatement("""
            UPDATE p9_outbox SET token=?,token_until=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT+?,
              pub_epoch=pub_epoch+1 WHERE nid=? AND published_ms IS NULL
            RETURNING pub_epoch
            """)) {
          update.setString(1,token);update.setLong(2,LEASE_MS);update.setString(3,nid);
          try(ResultSet rs=update.executeQuery()){
            if(!rs.next()){db.rollback();return null;}
            epoch=rs.getLong(1);
          }
        }
        db.commit();
        return new Claim(nid,rid,token,epoch);
      }catch(SQLException | RuntimeException ex){db.rollback();throw ex;}
    }
  }

  @Override public synchronized void start(String instance){
    if(active)return;
    active=true;
    workers=Executors.newFixedThreadPool(concurrency,Thread.ofVirtual().name("pg-delivery-",0).factory());
    for(int i=0;i<concurrency;i++)workers.submit(this::worker);
    System.out.println("ACTIVATION_DELIVERY_BACKEND postgres workers="+concurrency+" instance="+instance);
  }

  private void worker(){
    while(active && !Thread.currentThread().isInterrupted()){
      try {
        Claim next=claim();
        if(next==null){Thread.sleep(POLL_MS);continue;}
        try {
          work.handle(next.rid());
          var outcome=ledger.markPublished(next.nid(),next.token(),next.epoch(),clock.now());
          if(outcome==JdbcRabbitLedger.Outcome.COMMITTED)published.incrementAndGet();
          else System.err.println("ACTIVATION_PG_MARK "+outcome+" nid="+next.nid());
        }catch(Exception e){
          // No ACK on uncertain state: the claim lease expires and the durable
          // outbox entry becomes eligible again. The watchdog repairs lost owners.
          System.err.println("ACTIVATION_PG_DELIVERY_ERROR nid="+next.nid()+" error="+e);
          if(active)Thread.sleep(POLL_MS);
        }
      }catch(InterruptedException e){Thread.currentThread().interrupt();break;
      }catch(Exception e){
        System.err.println("ACTIVATION_PG_POLL_ERROR "+e);
        try{Thread.sleep(POLL_MS);}catch(InterruptedException stop){Thread.currentThread().interrupt();break;}
      }
    }
  }

  // PostgreSQL workers do their own bounded polling; RabbitMQ needs a publisher tick.
  @Override public void dispatcher(){}
  @Override public void restoreConsumers(){}
  @Override public int publishedCount(){return published.get();}
  @Override public boolean isOpen(){return active && workers!=null && !workers.isShutdown();}
  @Override public synchronized void close(){
    active=false;
    if(workers!=null){
      workers.shutdownNow();
      try{workers.awaitTermination(3,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}
      workers=null;
    }
  }
}
