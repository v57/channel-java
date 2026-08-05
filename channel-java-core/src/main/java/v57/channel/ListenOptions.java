package v57.channel;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ListenOptions<State> {
  private final Function<Map<String, String>, State> stateFactory;
  private final BiConsumer<State, Sender<State>> onConnect;
  private final BiConsumer<State, Sender<State>> onDisconnect;

  public ListenOptions() {
    this(null, null, null);
  }

  public ListenOptions(
      Function<Map<String, String>, State> stateFactory,
      BiConsumer<State, Sender<State>> onConnect,
      BiConsumer<State, Sender<State>> onDisconnect
  ) {
    this.stateFactory = stateFactory;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
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
}
