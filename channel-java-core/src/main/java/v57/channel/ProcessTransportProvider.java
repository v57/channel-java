package v57.channel;

public interface ProcessTransportProvider {
  <State> ChannelProcess<State> process(Channel<State> channel, State state);
}
