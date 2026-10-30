package v57.channel;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicConnection;

/** {@code quic://host:port} for {@link Channel#connect}: one QUIC connection, from its own UDP socket, per client. */
public final class QuicClientTransportModule implements ClientTransportProvider {
  private static final AtomicInteger IDS = new AtomicInteger();

  static {
    // The client checks the certificate itself (QuicTls) and so turns kwik's own check off; kwik would print a
    // warning about that.
    if (System.getProperty("tech.kwik.core.no-security-warnings") == null) {
      System.setProperty("tech.kwik.core.no-security-warnings", "true");
    }
  }

  @Override
  public boolean supports(String scheme) {
    return "quic".equals(scheme);
  }

  @Override
  public <State> ChannelClient<State> connect(Channel<State> channel, String address, ConnectOptions<State> options) {
    var safeOptions = options == null ? new ConnectOptions<State>() : options;
    var target = Target.parse(address);
    var tls = safeOptions.quic();
    var serverName = tls.serverName() != null ? tls.serverName() : target.host();
    var trust = QuicTls.serverTrust(tls, serverName);

    QuicClientConnection connection;
    try {
      // Each connection gets its own UDP socket (kwik's default): Network.framework servers take one connection
      // per client address and port.
      var builder = QuicClientConnection.newBuilder()
          .host(serverName)
          .port(target.port())
          .applicationProtocol(QuicFrames.ALPN)
          .version(QuicConnection.QuicVersion.V1)
          .connectTimeout(Duration.ofSeconds(10))
          .maxIdleTimeout(Duration.ofSeconds(QuicFrames.IDLE_TIMEOUT_SECONDS))
          .maxOpenPeerInitiatedBidirectionalStreams(0)
          .maxOpenPeerInitiatedUnidirectionalStreams(0)
          .defaultStreamReceiveBufferSize(4_000_000L)
          .noServerCertificateCheck()
          .customTrustManager(trust);
      if (!serverName.equals(target.host())) {
        builder.proxy(target.host());
      }
      connection = builder.build();
    } catch (java.io.IOException | RuntimeException error) {
      throw new ChannelException("failed to create quic client for " + address, error);
    }

    var transport = new ClientTransport(connection, safeOptions.headers(), safeOptions.maxPayload());
    var peer = new Peer<>(channel, safeOptions.state(), transport);
    transport.attach(peer);
    PumpRegistry.register(transport);
    transport.start();

    return new ChannelClient<>(peer, transport, () -> {
      PumpRegistry.unregister(transport);
      transport.close();
    });
  }

  private record Target(String host, int port) {
    static Target parse(String address) {
      try {
        var uri = new URI(java.util.Objects.requireNonNull(address, "address"));
        if (!"quic".equalsIgnoreCase(uri.getScheme())) {
          throw new ChannelException("only quic:// is supported");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
          throw new ChannelException("quic host is required");
        }
        if (uri.getPort() <= 0) {
          throw new ChannelException("quic port is required");
        }
        return new Target(QuicTls.strip(uri.getHost()), uri.getPort());
      } catch (URISyntaxException error) {
        throw new ChannelException("invalid quic address", error);
      }
    }
  }

  private static final class ClientTransport implements Transport, Pollable {
    private final QuicClientConnection connection;
    private final QuicPipe pipe;
    private final int id = IDS.incrementAndGet();
    private Peer<?> peer;
    private Thread connector;
    private volatile boolean open = true;
    private volatile boolean streamReady;

    ClientTransport(QuicClientConnection connection, Map<String, String> headers, int maxPayload) {
      this.connection = connection;
      this.pipe = new QuicPipe(maxPayload, true, connection::close);
      connection.setConnectionListener(event -> pipe.close());
      // The hello goes first: the WebSocket upgrade headers, as {"hello": {name: value}}
      var hello = new LinkedHashMap<String, Object>();
      hello.put("hello", headers == null ? Map.of() : headers);
      pipe.send(JsonCodec.encode(hello));
    }

    void attach(Peer<?> peer) {
      this.peer = peer;
    }

    void start() {
      // kwik's connect() blocks for the handshake; messages queue up meanwhile, like on a connecting WebSocket
      connector = new Thread(() -> {
        try {
          connection.connect();
          var stream = connection.createStream(true);
          synchronized (this) {
            if (!open) {
              connection.close();
              return;
            }
            pipe.start(stream, "channel-quic-client-" + id);
            streamReady = true;
          }
        } catch (Exception error) {
          pipe.close();
          connection.close();
        }
      }, "channel-quic-connect-" + id);
      connector.setDaemon(true);
      connector.start();
    }

    @Override
    public void queueFrame(String payload) {
      if (open) {
        pipe.send(payload);
      }
    }

    @Override
    public void close() {
      synchronized (this) {
        if (!open) {
          return;
        }
        open = false;
      }
      pipe.close();
      connection.close();
      if (connector != null) {
        connector.interrupt();
      }
    }

    @Override
    public boolean isConnected() {
      return open && streamReady && !pipe.ended();
    }

    @Override
    public boolean isOpen() {
      // Connecting counts as open: calls wait for the handshake instead of failing at once
      return open && (!pipe.ended() || pipe.hasInbox());
    }

    @Override
    public boolean poll() {
      if (!open) {
        return false;
      }
      var localPeer = peer;
      var ended = pipe.ended();
      var progressed = false;
      try {
        for (var payload = pipe.poll(); payload != null; payload = pipe.poll()) {
          progressed = true;
          if (localPeer != null) {
            localPeer.handlePayload(payload);
          }
        }
        if (localPeer != null && streamReady && open) {
          progressed = localPeer.pollLocalStreams() || progressed;
        }
      } catch (RuntimeException | StackOverflowError error) {
        // A broken server (undecodable message) only disconnects this client
        fail();
        return true;
      }
      if (ended && !pipe.hasInbox()) {
        fail();
        return true;
      }
      return progressed;
    }

    private void fail() {
      var localPeer = peer;
      if (localPeer != null && open) {
        localPeer.stop("disconnected");
        return;
      }
      close();
    }
  }
}
