package v57.channel;

public interface ClientTransportProvider {
  <State> ChannelClient<State> connect(Channel<State> channel, String address, ConnectOptions<State> options);

  /** Whether this transport handles addresses with {@code scheme} (lowercase, for example {@code ws} or {@code quic}). */
  default boolean supports(String scheme) {
    return "ws".equals(scheme);
  }
}
