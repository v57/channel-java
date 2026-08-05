package v57.channel;

public interface ServerTransportProvider {
  <State> ChannelServer<State> listen(Channel<State> channel, String host, int port, ListenOptions<State> options);
}
