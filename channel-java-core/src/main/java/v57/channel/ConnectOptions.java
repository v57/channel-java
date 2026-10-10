package v57.channel;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ConnectOptions<State> {
  private final State state;
  private final Map<String, String> headers;
  private final int maxPayload;

  public ConnectOptions() {
    this(null, Map.of());
  }

  public ConnectOptions(State state, Map<String, String> headers) {
    this(state, headers, WebSocketSupport.DEFAULT_MAX_PAYLOAD);
  }

  private ConnectOptions(State state, Map<String, String> headers, int maxPayload) {
    if (maxPayload <= 0) {
      throw new IllegalArgumentException("maxPayload must be positive");
    }
    this.state = state;
    this.headers = headers == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(headers));
    this.maxPayload = maxPayload;
  }

  /** Returns a copy that disconnects when the server sends a websocket message larger than {@code bytes} (default 16 MiB). */
  public ConnectOptions<State> withMaxPayload(int bytes) {
    return new ConnectOptions<>(state, headers, bytes);
  }

  public State state() {
    return state;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public int maxPayload() {
    return maxPayload;
  }
}
