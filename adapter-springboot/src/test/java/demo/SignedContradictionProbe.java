package demo;
import java.nio.file.Path;
import java.util.UUID;

/** TEST-ONLY adapter deliberately invokes production verification/journaling code.
 * The laboratory issuer knowingly signs an incorrect REJECTED snapshot after
 * having created an ACTIVATED external effect. A valid signature is NOT truth.
 */
public final class SignedContradictionProbe {
  public static void main(String[] args) throws Exception {
    if(args.length!=4)throw new IllegalArgumentException("Expected dispatch id, key, activate URL, lookup URL");
    var db=new JdbcRabbitLedger(System.getenv("ACTIVATION_JDBC"),System.getenv("ACTIVATION_DB_USER"),System.getenv("ACTIVATION_DB_PASSWORD"));
    var gateway=new ProviderHttpGateway(args[2],args[3],true);
    var signed=gateway.lookup(args[1]);
    if(signed==null || signed.status()!=422 || signed.attestation()==null)
      throw new IllegalStateException("deliberately contrary signed provider response unavailable");
    var verifier=new SignedProviderProof(Path.of(System.getenv("SIGNED_EVIDENCE_PUBLIC_KEY_PEM")),
        System.getenv("SIGNED_EVIDENCE_EXPECTED_ISSUER"),System.getenv("SIGNED_EVIDENCE_EXPECTED_KID"),2500);
    var result=db.observeSignedLookup(UUID.randomUUID().toString(),args[0],signed.status(),
        signed.attestation(),signed.challenge(),verifier);
    if(result!=JdbcRabbitLedger.Outcome.COMMITTED)
      throw new IllegalStateException("contrary provider report not journaled: "+result);
    System.out.println("SIGNED_CONTRADICTION_PERSISTED");
  }
}
