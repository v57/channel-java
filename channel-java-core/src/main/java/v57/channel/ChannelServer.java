package v57.channel;

public final class ChannelServer<State> implements AutoCloseable {
  private final Pollable pollable;
  private final Runnable stopAction;
  private final int port;

  ChannelServer(Pollable pollable, Runnable stopAction, int port) {
    this.pollable = pollable;
    this.stopAction = stopAction;
    this.port = port;
  }

  public boolean poll() {
    try {
      return pollable.poll();
    } catch (Exception error) {
      throw new ChannelException("server poll failed", error);
    }
  }

  public int port() {
    return port;
  }

  public void stop() {
    stopAction.run();
  }

  @Override
  public void close() {
    stop();
  }
}
