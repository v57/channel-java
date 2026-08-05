package v57.channel;

public final class ChannelProcess<State> implements AutoCloseable {
  private final Sender<State> sender;
  private final Peer<State> peer;
  private final Pollable pollable;
  private final Runnable stopAction;

  ChannelProcess(Peer<State> peer, Pollable pollable, Runnable stopAction) {
    this.sender = new Sender<>(peer);
    this.peer = peer;
    this.pollable = pollable;
    this.stopAction = stopAction;
  }

  public Sender<State> sender() {
    return sender;
  }

  public boolean poll() {
    try {
      var progressed = pollable.poll();
      return peer.pollLocalStreams() || progressed;
    } catch (Exception error) {
      throw new ChannelException("process poll failed", error);
    }
  }

  public boolean isConnected() {
    return peer.isConnected();
  }

  public boolean isRemoteSubscribed(String topic) {
    return peer.isRemoteSubscribed(topic);
  }

  public void stop() {
    peer.stop();
    stopAction.run();
  }

  @Override
  public void close() {
    stop();
  }
}
