package v57.channel;

/**
 * TLS settings for {@code quic://} addresses (used by the optional {@code channel-java-quic} module). Without any,
 * the server certificate must be trusted by the JVM's default trust store and valid for the address host.
 */
public final class QuicOptions {
  private final String ca;
  private final String fingerprint;
  private final String serverName;

  public QuicOptions() {
    this(null, null, null);
  }

  private QuicOptions(String ca, String fingerprint, String serverName) {
    this.ca = ca;
    this.fingerprint = fingerprint;
    this.serverName = serverName;
  }

  /** Trusts the PEM certificate(s) {@code pem} (a self-signed server certificate or a CA) instead of the system roots. */
  public QuicOptions withCa(String pem) {
    return new QuicOptions(pem, fingerprint, serverName);
  }

  /**
   * Accepts only the server certificate with this SHA-256 fingerprint (hex, colons optional) and skips the other
   * checks.
   */
  public QuicOptions withFingerprint(String sha256) {
    return new QuicOptions(ca, sha256, serverName);
  }

  /** The name sent (SNI) and checked against the certificate instead of the address host. */
  public QuicOptions withServerName(String name) {
    return new QuicOptions(ca, fingerprint, name);
  }

  public String ca() {
    return ca;
  }

  public String fingerprint() {
    return fingerprint;
  }

  public String serverName() {
    return serverName;
  }
}
