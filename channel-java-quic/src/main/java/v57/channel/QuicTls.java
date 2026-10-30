package v57.channel;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/** Certificates and keys from PEM, and the client's check of the server certificate. */
final class QuicTls {
  private static final Pattern IPV4 = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");

  private QuicTls() {
  }

  static List<X509Certificate> certificates(String pem) {
    try {
      var factory = java.security.cert.CertificateFactory.getInstance("X.509");
      var result = new ArrayList<X509Certificate>();
      for (var certificate : factory.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)))) {
        result.add((X509Certificate) certificate);
      }
      if (result.isEmpty()) {
        throw new ChannelException("no certificate in PEM");
      }
      return result;
    } catch (CertificateException error) {
      throw new ChannelException("invalid certificate PEM", error);
    }
  }

  /** An unencrypted PKCS#8 key ({@code BEGIN PRIVATE KEY}), EC or RSA. */
  static PrivateKey privateKey(String pem) {
    var begin = "-----BEGIN PRIVATE KEY-----";
    var start = pem.indexOf(begin);
    var end = pem.indexOf("-----END PRIVATE KEY-----");
    if (start < 0 || end < start) {
      throw new ChannelException("the key must be an unencrypted PKCS#8 PEM (BEGIN PRIVATE KEY); convert it with "
          + "openssl pkcs8 -topk8 -nocrypt -in key.pem");
    }
    var der = Base64.getMimeDecoder().decode(pem.substring(start + begin.length(), end));
    for (var algorithm : List.of("EC", "RSA")) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
      } catch (GeneralSecurityException ignored) {
      }
    }
    throw new ChannelException("unsupported private key: only EC and RSA keys work");
  }

  static KeyStore keyStore(String certificatePem, String keyPem, String alias, char[] password) {
    try {
      var store = KeyStore.getInstance("PKCS12");
      store.load(null, null);
      store.setKeyEntry(alias, privateKey(keyPem), password, certificates(certificatePem).toArray(new X509Certificate[0]));
      return store;
    } catch (GeneralSecurityException | java.io.IOException error) {
      throw new ChannelException("invalid certificate or key", error);
    }
  }

  /** SHA-256 fingerprint from hex, colons and spaces optional */
  static byte[] fingerprint(String text) {
    var hex = text.replace(":", "").replace(" ", "");
    if (hex.length() != 64) {
      throw new ChannelException("invalid certificate fingerprint: expected 32 hex bytes");
    }
    try {
      return HexFormat.of().parseHex(hex);
    } catch (IllegalArgumentException error) {
      throw new ChannelException("invalid certificate fingerprint", error);
    }
  }

  static boolean isIpLiteral(String host) {
    return host.indexOf(':') >= 0 || IPV4.matcher(host).matches();
  }

  /** Checks the server certificate: a pinned fingerprint, or a chain trusted by {@code ca} (or the JVM) for {@code name}. */
  static X509TrustManager serverTrust(QuicOptions options, String name) {
    if (options.fingerprint() != null) {
      return new PinnedTrust(fingerprint(options.fingerprint()));
    }
    try {
      var factory = TrustManagerFactory.getInstance("PKIX");
      if (options.ca() != null) {
        var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        var index = 0;
        for (var certificate : certificates(options.ca())) {
          store.setCertificateEntry("ca" + index++, certificate);
        }
        factory.init(store);
      } else {
        factory.init((KeyStore) null);
      }
      for (var manager : factory.getTrustManagers()) {
        if (manager instanceof X509TrustManager x509) {
          return new VerifiedTrust(x509, name);
        }
      }
      throw new ChannelException("no X.509 trust manager");
    } catch (GeneralSecurityException | java.io.IOException error) {
      throw new ChannelException("invalid quic trust settings", error);
    }
  }

  private abstract static class ServerOnly implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      throw new CertificateException("client certificates aren't used");
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private static final class PinnedTrust extends ServerOnly {
    private final byte[] pin;

    PinnedTrust(byte[] pin) {
      this.pin = pin;
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      if (chain == null || chain.length == 0) {
        throw new CertificateException("no server certificate");
      }
      try {
        var digest = MessageDigest.getInstance("SHA-256").digest(chain[0].getEncoded());
        if (!MessageDigest.isEqual(digest, pin)) {
          throw new CertificateException("certificate fingerprint mismatch");
        }
      } catch (java.security.NoSuchAlgorithmException error) {
        throw new CertificateException(error);
      }
    }
  }

  private static final class VerifiedTrust extends ServerOnly {
    private final X509TrustManager chainCheck;
    private final String name;

    VerifiedTrust(X509TrustManager chainCheck, String name) {
      this.chainCheck = chainCheck;
      this.name = name;
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      if (chain == null || chain.length == 0) {
        throw new CertificateException("no server certificate");
      }
      chainCheck.checkServerTrusted(chain, "UNKNOWN");
      if (!matches(chain[0], name)) {
        throw new CertificateException("certificate is not valid for " + name);
      }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return chainCheck.getAcceptedIssuers();
    }
  }

  /** RFC 6125: the subject alternative names (DNS with a leftmost wildcard, or IP), the CN only when there are none. */
  static boolean matches(X509Certificate certificate, String name) {
    var ip = isIpLiteral(name);
    try {
      var names = certificate.getSubjectAlternativeNames();
      if (names == null) {
        return !ip && name.equalsIgnoreCase(commonName(certificate));
      }
      for (var entry : names) {
        var type = (Integer) entry.get(0);
        var value = entry.get(1) instanceof String text ? text : null;
        if (value == null) {
          continue;
        }
        if (ip && type == 7 && sameAddress(value, name)) {
          return true;
        }
        if (!ip && type == 2 && dnsMatches(value, name)) {
          return true;
        }
      }
      return false;
    } catch (CertificateParsingException error) {
      return false;
    }
  }

  private static boolean dnsMatches(String pattern, String name) {
    var host = name.toLowerCase(Locale.ROOT);
    var dns = pattern.toLowerCase(Locale.ROOT);
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }
    if (dns.startsWith("*.")) {
      var dot = host.indexOf('.');
      return dot > 0 && host.substring(dot + 1).equals(dns.substring(2));
    }
    return host.equals(dns);
  }

  private static boolean sameAddress(String a, String b) {
    try {
      return isIpLiteral(a) && InetAddress.getByName(strip(a)).equals(InetAddress.getByName(strip(b)));
    } catch (UnknownHostException error) {
      return false;
    }
  }

  static String strip(String host) {
    return host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
  }

  private static String commonName(X509Certificate certificate) {
    try {
      String cn = null;
      for (var rdn : new LdapName(certificate.getSubjectX500Principal().getName()).getRdns()) {
        if (rdn.getType().equalsIgnoreCase("CN")) {
          cn = rdn.getValue().toString();
        }
      }
      return cn;
    } catch (InvalidNameException error) {
      return null;
    }
  }
}
