package v57.channel;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ConnectOptions<State> {
  private final State state;
  private final Map<String, String> headers;

  public ConnectOptions() {
    this(null, Map.of());
  }

  public ConnectOptions(State state, Map<String, String> headers) {
    this.state = state;
    this.headers = headers == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(headers));
  }

  public State state() {
    return state;
  }

  public Map<String, String> headers() {
    return headers;
  }
}
