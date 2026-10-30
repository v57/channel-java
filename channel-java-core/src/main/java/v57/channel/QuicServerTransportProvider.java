package v57.channel;

/** Implemented by the optional {@code channel-java-quic} module. */
public interface QuicServerTransportProvider {
  <State> ChannelServer<State> listenQuic(Channel<State> channel, String host, int port, ListenOptions<State> options);
}
