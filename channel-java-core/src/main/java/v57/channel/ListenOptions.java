package v57.channel;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ListenOptions<State> {
  private final Function<Map<String, String>, State> stateFactory;
  private final BiConsumer<State, Sender<State>> onConnect;
  private final BiConsumer<State, Sender<State>> onDisconnect;
  private final int maxPayload;
  private final String certificatePem;
  private final String keyPem;

  public ListenOptions() {
    this(null, null, null);
  }

  public ListenOptions(
      Function<Map<String, String>, State> stateFactory,
      BiConsumer<State, Sender<State>> onConnect,
      BiConsumer<State, Sender<State>> onDisconnect
  ) {
    this(stateFactory, onConnect, onDisconnect, WebSocketSupport.DEFAULT_MAX_PAYLOAD, null, null);
  }

  private ListenOptions(
      Function<Map<String, String>, State> stateFactory,
      BiConsumer<State, Sender<State>> onConnect,
      BiConsumer<State, Sender<State>> onDisconnect,
      int maxPayload,
      String certificatePem,
      String keyPem
  ) {
    if (maxPayload <= 0) {
      throw new IllegalArgumentException("maxPayload must be positive");
    }
    this.stateFactory = stateFactory;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.maxPayload = maxPayload;
    this.certificatePem = certificatePem;
    this.keyPem = keyPem;
  }

  /** Returns a copy that drops connections sending a message larger than {@code bytes} (default 16 MiB). */
  public ListenOptions<State> withMaxPayload(int bytes) {
    return new ListenOptions<>(stateFactory, onConnect, onDisconnect, bytes, certificatePem, keyPem);
  }

  /**
   * Returns a copy with the TLS certificate chain and its PKCS#8 private key (both PEM), which
   * {@link Channel#listenQuic} requires.
   */
  public ListenOptions<State> withCertificate(String certificatePem, String keyPem) {
    return new ListenOptions<>(stateFactory, onConnect, onDisconnect, maxPayload, certificatePem, keyPem);
  }

  public Function<Map<String, String>, State> stateFactory() {
    return stateFactory;
  }

  public BiConsumer<State, Sender<State>> onConnect() {
    return onConnect;
  }

  public BiConsumer<State, Sender<State>> onDisconnect() {
    return onDisconnect;
  }

  public int maxPayload() {
    return maxPayload;
  }

  public String certificatePem() {
    return certificatePem;
  }

  public String keyPem() {
    return keyPem;
  }
}
