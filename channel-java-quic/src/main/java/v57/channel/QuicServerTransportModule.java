package v57.channel;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import tech.kwik.agent15.TlsConstants;
import tech.kwik.agent15.engine.TlsServerEngineFactory;
import tech.kwik.agent15.engine.TlsSession;
import tech.kwik.agent15.engine.TlsSessionRegistry;
import tech.kwik.agent15.engine.impl.TlsState;
import tech.kwik.agent15.extension.ClientHelloPreSharedKeyExtension;
import tech.kwik.agent15.handshake.NewSessionTicketMessage;
import tech.kwik.core.ConnectionListener;
import tech.kwik.core.ConnectionTerminatedEvent;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.log.NullLogger;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;

/** {@link Channel#listenQuic}: the channel's routes over QUIC, driven by {@link ChannelServer#poll()} like listen(). */
public final class QuicServerTransportModule implements QuicServerTransportProvider {
  private static final AtomicInteger IDS = new AtomicInteger();

  @Override
  public <State> ChannelServer<State> listenQuic(Channel<State> channel, String host, int port, ListenOptions<State> options) {
    var safeOptions = options == null ? new ListenOptions<State>() : options;
    if (safeOptions.certificatePem() == null || safeOptions.keyPem() == null) {
      throw new ChannelException("listenQuic needs a certificate and key: ListenOptions.withCertificate(certPem, keyPem)");
    }
    var server = new QuicServerLoop<>(channel, safeOptions, host, port);
    PumpRegistry.register(server);
    return new ChannelServer<>(server, () -> {
      PumpRegistry.unregister(server);
      server.stop();
    }, server.port());
  }

  private static final class QuicServerLoop<State> implements Pollable, ApplicationProtocolConnectionFactory {
    private final Channel<State> channel;
    private final ListenOptions<State> options;
    private final DatagramSocket socket;
    private final ServerConnector connector;
    /** Connections kwik accepted, handed over to the pumping thread */
    private final ConcurrentLinkedQueue<Connection<State>> accepted = new ConcurrentLinkedQueue<>();
    private final LinkedHashMap<Connection<State>, Boolean> connections = new LinkedHashMap<>();
    private volatile boolean open = true;

    QuicServerLoop(Channel<State> channel, ListenOptions<State> options, String host, int port) {
      this.channel = channel;
      this.options = options;
      var password = UUID.randomUUID().toString().toCharArray();
      var keyStore = QuicTls.keyStore(options.certificatePem(), options.keyPem(), "channel", password);
      try {
        socket = new DatagramSocket(new InetSocketAddress(host, port));
      } catch (SocketException error) {
        throw new ChannelException("failed to start quic server on " + host + ":" + port, error);
      }
      try {
        var config = ServerConnectionConfig.builder()
            .maxIdleTimeoutInSeconds(QuicFrames.IDLE_TIMEOUT_SECONDS)
            .maxOpenPeerInitiatedBidirectionalStreams(1)
            .maxOpenPeerInitiatedUnidirectionalStreams(0)
            .maxBidirectionalStreamBufferSize(4_000_000)
            .maxConnectionBufferSize(4_000_000)
            .build();
        connector = ServerConnector.builder()
            .withSocket(socket)
            .withKeyStore(keyStore, "channel", password)
            // Connections start as QUIC v1. kwik always moves to the client's preferred version (RFC 9368 compatible
            // negotiation) and Bun's client prefers v2, so the connector must accept v2 packets too.
            .withSupportedVersions(List.of(QuicConnection.QuicVersion.V1, QuicConnection.QuicVersion.V2))
            .withConfiguration(config)
            .withLogger(new NullLogger())
            .build();
        disableResumption(connector);
        connector.registerApplicationProtocol(QuicFrames.ALPN, this);
        connector.start();
      } catch (Exception error) {
        socket.close();
        throw error instanceof ChannelException channelError ? channelError
            : new ChannelException("failed to start quic server: " + error.getMessage(), error);
      }
    }

    int port() {
      return socket.getLocalPort();
    }

    // One bidirectional stream per connection, opened by the client, and no unidirectional ones
    @Override
    public int maxConcurrentPeerInitiatedBidirectionalStreams() {
      return 1;
    }

    @Override
    public long maxTotalPeerInitiatedBidirectionalStreams() {
      return 1;
    }

    @Override
    public int maxConcurrentPeerInitiatedUnidirectionalStreams() {
      return 0;
    }

    @Override
    public long maxTotalPeerInitiatedUnidirectionalStreams() {
      return 0;
    }

    @Override
    public ApplicationProtocolConnection createConnection(String protocol, QuicConnection quicConnection) {
      var connection = new Connection<>(this, quicConnection);
      if (!open) {
        quicConnection.close();
        return connection;
      }
      accepted.add(connection);
      return connection;
    }

    void stop() {
      if (!open) {
        return;
      }
      open = false;
      for (var connection : List.copyOf(connections.keySet())) {
        connection.shutdown();
      }
      connections.clear();
      for (var connection = accepted.poll(); connection != null; connection = accepted.poll()) {
        connection.quic.close();
      }
      connector.close();
      socket.close();
    }

    @Override
    public boolean poll() {
      if (!open) {
        return false;
      }
      var progressed = false;
      for (var connection = accepted.poll(); connection != null; connection = accepted.poll()) {
        connections.put(connection, Boolean.TRUE);
        progressed = true;
      }
      for (var connection : List.copyOf(connections.keySet())) {
        // Contain per-connection failures (no hello, rejected state, invalid JSON) to that connection
        try {
          progressed = connection.poll() || progressed;
        } catch (RuntimeException | StackOverflowError error) {
          connection.shutdown();
          progressed = true;
        }
      }
      return progressed;
    }

    void remove(Connection<State> connection) {
      connections.remove(connection);
    }
  }

  private static final class Connection<State> implements ApplicationProtocolConnection, ConnectionListener {
    private final QuicServerLoop<State> server;
    private final QuicConnection quic;
    private final QuicPipe pipe;
    private final AtomicBoolean streamAccepted = new AtomicBoolean();
    private final int id = IDS.incrementAndGet();
    private volatile boolean quicClosed;
    private Peer<State> peer;
    private State state;
    private boolean open = true;
    private boolean disconnectCallbackInvoked;

    Connection(QuicServerLoop<State> server, QuicConnection quic) {
      this.server = server;
      this.quic = quic;
      this.pipe = new QuicPipe(server.options.maxPayload(), false, quic::close);
      quic.setConnectionListener(this);
    }

    @Override
    public void acceptPeerInitiatedStream(QuicStream stream) {
      // Only the client's first bidirectional stream carries the connection; any other is refused
      if (stream.isBidirectional() && stream.isClientInitiatedBidirectional() && streamAccepted.compareAndSet(false, true)) {
        pipe.start(stream, "channel-quic-server-" + id);
        return;
      }
      stream.abortReading(0);
      stream.resetStream(0);
    }

    @Override
    public void disconnected(ConnectionTerminatedEvent event) {
      quicClosed = true;
      pipe.close();
    }

    boolean poll() {
      if (!open) {
        return false;
      }
      var ended = quicClosed || pipe.ended();
      var progressed = false;
      for (var payload = pipe.poll(); payload != null && open; payload = pipe.poll()) {
        progressed = true;
        if (peer == null) {
          hello(payload);
        } else {
          peer.handlePayload(payload);
          peer.pollLocalStreams();
        }
      }
      if (open && peer != null) {
        progressed = peer.pollLocalStreams() || progressed;
      }
      if (open && ended && !pipe.hasInbox()) {
        shutdown();
        return true;
      }
      return progressed;
    }

    /** The first frame: {"hello": {header: value}}, used like the WebSocket upgrade request headers */
    private void hello(String payload) {
      var message = JsonCodec.decode(payload);
      if (!(message instanceof Map<?, ?> map) || !(map.get("hello") instanceof Map<?, ?> hello)) {
        throw new ChannelException("the first quic frame isn't a hello");
      }
      var headers = new LinkedHashMap<String, String>();
      for (var entry : hello.entrySet()) {
        if (entry.getKey() instanceof String name && entry.getValue() instanceof String value) {
          headers.put(name.toLowerCase(Locale.ROOT), value);
        }
      }
      var factory = server.options.stateFactory();
      state = factory == null ? null : factory.apply(headers);
      peer = new Peer<>(server.channel, state, new ServerTransport<>(this));
      var onConnect = server.options.onConnect();
      if (onConnect != null) {
        try {
          onConnect.accept(state, new Sender<>(peer));
        } catch (Throwable ignored) {
        }
      }
    }

    void queue(String payload) {
      if (open) {
        pipe.send(payload);
      }
    }

    boolean isOpen() {
      return open && !quicClosed;
    }

    /** Closes the connection and, once it was connected, stops the peer and calls onDisconnect. */
    void shutdown() {
      if (!closeTransportOnly()) {
        return;
      }
      if (peer != null) {
        peer.stop();
        invokeDisconnectCallback();
      }
    }

    private boolean closeTransportOnly() {
      if (!open) {
        return false;
      }
      open = false;
      server.remove(this);
      pipe.close();
      quic.close();
      return true;
    }

    private void invokeDisconnectCallback() {
      if (disconnectCallbackInvoked) {
        return;
      }
      disconnectCallbackInvoked = true;
      var onDisconnect = server.options.onDisconnect();
      if (onDisconnect == null) {
        return;
      }
      try {
        onDisconnect.accept(state, new Sender<>(peer));
      } catch (Throwable ignored) {
      }
    }

    private static final class ServerTransport<State> implements Transport {
      private final Connection<State> connection;

      ServerTransport(Connection<State> connection) {
        this.connection = connection;
      }

      @Override
      public void queueFrame(String payload) {
        connection.queue(payload);
      }

      @Override
      public void close() {
        connection.closeTransportOnly();
        connection.invokeDisconnectCallback();
      }

      @Override
      public boolean isConnected() {
        return connection.isOpen();
      }
    }
  }

  /**
   * kwik issues session tickets and accepts 0-RTT data with them, and has no setting for either. A replayed 0-RTT
   * request could run twice, so the server's TLS engine factory gets a session registry that never issues or
   * accepts a ticket: no resumption, no 0-RTT. Fails closed when kwik's internals change.
   */
  private static void disableResumption(ServerConnector connector) {
    try {
      var factoryField = connector.getClass().getDeclaredField("tlsEngineFactory");
      factoryField.setAccessible(true);
      var factory = (TlsServerEngineFactory) factoryField.get(connector);
      var registryField = TlsServerEngineFactory.class.getDeclaredField("tlsSessionRegistry");
      registryField.setAccessible(true);
      registryField.set(factory, NoSessions.INSTANCE);
    } catch (ReflectiveOperationException | RuntimeException error) {
      connector.close();
      throw new ChannelException("can't turn off 0-RTT in this kwik version", error);
    }
  }

  private enum NoSessions implements TlsSessionRegistry {
    INSTANCE;

    @Override
    public NewSessionTicketMessage createNewSessionTicketMessage(
        byte ticketNonce, TlsConstants.CipherSuite selectedCipher, TlsState tlsState, String selectedApplicationLayerProtocol) {
      return null;
    }

    @Override
    public NewSessionTicketMessage createNewSessionTicketMessage(
        byte ticketNonce, TlsConstants.CipherSuite selectedCipher, TlsState tlsState, String selectedApplicationLayerProtocol,
        Long maxEarlyDataSize, byte[] data) {
      return null;
    }

    @Override
    public Integer selectIdentity(List<ClientHelloPreSharedKeyExtension.PskIdentity> identities, TlsConstants.CipherSuite selectedCipher) {
      return null;
    }

    @Override
    public TlsSession useSession(ClientHelloPreSharedKeyExtension.PskIdentity pskIdentity) {
      return null;
    }

    @Override
    public byte[] peekSessionData(ClientHelloPreSharedKeyExtension.PskIdentity pskIdentity) {
      return null;
    }

    @Override
    public void shutdown() {
    }
  }
}
