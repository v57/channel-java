package v57.channel;

public interface ClientTransportProvider {
  <State> ChannelClient<State> connect(Channel<State> channel, String address, ConnectOptions<State> options);
}
