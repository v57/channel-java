package v57.channel;

import java.util.ArrayDeque;

public final class ProcessTransportModule implements ProcessTransportProvider {
  @Override
  public <State> ChannelProcess<State> process(Channel<State> channel, State state) {
    var transport = new ProcessLoopbackTransport();
    var peer = new Peer<>(channel, state, transport);
    transport.attach(peer);
    PumpRegistry.register(transport);

    return new ChannelProcess<>(peer, transport, () -> {
      PumpRegistry.unregister(transport);
      transport.close();
    });
  }

  private static final class ProcessLoopbackTransport implements Transport, Pollable {
    private final ArrayDeque<String> queue = new ArrayDeque<>();
    private Peer<?> peer;
    private boolean open = true;

    void attach(Peer<?> peer) {
      this.peer = peer;
    }

    @Override
    public synchronized void queueFrame(String payload) {
      if (!open) {
        return;
      }
      queue.addLast(payload);
    }

    @Override
    public synchronized void close() {
      open = false;
      queue.clear();
    }

    @Override
    public synchronized boolean isConnected() {
      return open;
    }

    @Override
    public boolean isProcess() {
      return true;
    }

    @Override
    public boolean poll() {
      var localPeer = peer;
      if (localPeer == null || !open) {
        return false;
      }

      var progressed = localPeer.pollLocalStreams();
      while (true) {
        String payload;
        synchronized (this) {
          payload = queue.pollFirst();
        }
        if (payload == null) {
          break;
        }
        progressed = true;
        localPeer.handlePayload(payload);
        progressed = localPeer.pollLocalStreams() || progressed;
      }
      return progressed;
    }
  }
}
