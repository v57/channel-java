package v57.channel;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ListenOptions<State> {
  private final Function<Map<String, String>, State> stateFactory;
  private final BiConsumer<State, Sender<State>> onConnect;
  private final BiConsumer<State, Sender<State>> onDisconnect;
  private final int maxPayload;

  public ListenOptions() {
    this(null, null, null);
  }

  public ListenOptions(
      Function<Map<String, String>, State> stateFactory,
      BiConsumer<State, Sender<State>> onConnect,
      BiConsumer<State, Sender<State>> onDisconnect
  ) {
    this(stateFactory, onConnect, onDisconnect, WebSocketSupport.DEFAULT_MAX_PAYLOAD);
  }

  private ListenOptions(
      Function<Map<String, String>, State> stateFactory,
      BiConsumer<State, Sender<State>> onConnect,
      BiConsumer<State, Sender<State>> onDisconnect,
      int maxPayload
  ) {
    if (maxPayload <= 0) {
      throw new IllegalArgumentException("maxPayload must be positive");
    }
    this.stateFactory = stateFactory;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.maxPayload = maxPayload;
  }

  /** Returns a copy that drops connections sending a websocket message larger than {@code bytes} (default 16 MiB). */
  public ListenOptions<State> withMaxPayload(int bytes) {
    return new ListenOptions<>(stateFactory, onConnect, onDisconnect, bytes);
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
}
