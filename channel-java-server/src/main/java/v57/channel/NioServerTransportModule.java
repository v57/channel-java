package v57.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class NioServerTransportModule implements ServerTransportProvider {
  @Override
  public <State> ChannelServer<State> listen(Channel<State> channel, String host, int port, ListenOptions<State> options) {
    var safeOptions = options == null ? new ListenOptions<State>() : options;
    var server = NioServerLoop.open(channel, host, port, safeOptions);
    PumpRegistry.register(server);
    return new ChannelServer<>(server, () -> {
      PumpRegistry.unregister(server);
      server.stop();
    }, server.port());
  }

  private static final class NioServerLoop<State> implements Pollable {
    private final Channel<State> channel;
    private final ListenOptions<State> options;
    private final Selector selector;
    private final ServerSocketChannel serverSocket;
    private final LinkedHashMap<SocketChannel, Connection<State>> connections = new LinkedHashMap<>();
    private boolean open = true;

    private NioServerLoop(
        Channel<State> channel,
        ListenOptions<State> options,
        Selector selector,
        ServerSocketChannel serverSocket
    ) {
      this.channel = channel;
      this.options = options;
      this.selector = selector;
      this.serverSocket = serverSocket;
    }

    static <State> NioServerLoop<State> open(Channel<State> channel, String host, int port, ListenOptions<State> options) {
      try {
        var selector = Selector.open();
        var serverSocket = ServerSocketChannel.open();
        serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverSocket.configureBlocking(false);
        serverSocket.bind(new InetSocketAddress(host, port));
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        return new NioServerLoop<>(channel, options, selector, serverSocket);
      } catch (IOException error) {
        throw new ChannelException("failed to start websocket server", error);
      }
    }

    int port() {
      try {
        return ((InetSocketAddress) serverSocket.getLocalAddress()).getPort();
      } catch (IOException error) {
        throw new ChannelException("failed to resolve server port", error);
      }
    }

    void stop() {
      if (!open) {
        return;
      }
      open = false;

      for (var connection : new ArrayList<>(connections.values())) {
        connection.shutdown(true);
      }
      connections.clear();

      try {
        serverSocket.close();
      } catch (IOException ignored) {
      }
      try {
        selector.close();
      } catch (IOException ignored) {
      }
    }

    @Override
    public boolean poll() {
      if (!open) {
        return false;
      }

      var progressed = false;
      try {
        selector.selectNow();
        var iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
          var key = iterator.next();
          iterator.remove();
          if (!key.isValid()) {
            continue;
          }
          if (key.channel() == serverSocket) {
            progressed = acceptConnections() || progressed;
            continue;
          }

          @SuppressWarnings("unchecked")
          var connection = (Connection<State>) key.attachment();
          if (connection == null) {
            continue;
          }
          if (key.isReadable()) {
            progressed = connection.read() || progressed;
          }
          if (!key.isValid()) {
            continue;
          }
          if (key.isWritable()) {
            progressed = connection.write() || progressed;
          }
        }

        for (var connection : new ArrayList<>(connections.values())) {
          progressed = connection.pollLocalStreams() || progressed;
          if (!connection.isOpen()) {
            continue;
          }
          if (connection.hasOutbound()) {
            connection.enableWrite();
          } else {
            connection.disableWrite();
          }
        }
      } catch (IOException error) {
        throw new ChannelException("server poll failed", error);
      }
      return progressed;
    }

    private boolean acceptConnections() throws IOException {
      var progressed = false;
      while (true) {
        var socket = serverSocket.accept();
        if (socket == null) {
          return progressed;
        }
        socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socket.configureBlocking(false);
        var key = socket.register(selector, SelectionKey.OP_READ);
        var connection = new Connection<>(this, socket, key);
        key.attach(connection);
        connections.put(socket, connection);
        progressed = true;
      }
    }

    private void remove(SocketChannel socket) {
      connections.remove(socket);
    }
  }

  private static final class Connection<State> {
    private final NioServerLoop<State> server;
    private final SocketChannel socket;
    private final SelectionKey key;
    private final ArrayDeque<ByteBuffer> outbound = new ArrayDeque<>();
    private final ByteBuffer readBuffer = ByteBuffer.allocate(16_384);

    private byte[] inbound = new byte[16_384];
    private int inboundLength;
    private Peer<State> peer;
    private State state;
    private boolean handshakeComplete;
    private boolean open = true;
    private boolean disconnectCallbackInvoked;

    private Connection(NioServerLoop<State> server, SocketChannel socket, SelectionKey key) {
      this.server = server;
      this.socket = socket;
      this.key = key;
    }

    boolean isOpen() {
      return open;
    }

    boolean hasOutbound() {
      return !outbound.isEmpty();
    }

    void enableWrite() {
      if (!open || !key.isValid()) {
        return;
      }
      key.interestOps(key.interestOps() | SelectionKey.OP_WRITE | SelectionKey.OP_READ);
    }

    void disableWrite() {
      if (!open || !key.isValid()) {
        return;
      }
      key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    boolean pollLocalStreams() {
      if (peer == null || !open) {
        return false;
      }
      var progressed = peer.pollLocalStreams();
      if (progressed && hasOutbound()) {
        enableWrite();
      }
      return progressed;
    }

    boolean read() {
      if (!open) {
        return false;
      }

      var progressed = false;
      while (true) {
        readBuffer.clear();
        int read;
        try {
          read = socket.read(readBuffer);
        } catch (IOException error) {
          shutdown(true);
          return true;
        }

        if (read < 0) {
          shutdown(true);
          return true;
        }
        if (read == 0) {
          break;
        }

        readBuffer.flip();
        ensureInboundCapacity(inboundLength + read);
        readBuffer.get(inbound, inboundLength, read);
        inboundLength += read;
        progressed = true;
      }

      if (!handshakeComplete) {
        progressed = finishHandshakeIfReady() || progressed;
      }
      if (handshakeComplete && open) {
        progressed = decodeFrames() || progressed;
      }
      return progressed;
    }

    boolean write() {
      if (!open) {
        return false;
      }

      var progressed = false;
      while (true) {
        var frame = outbound.peekFirst();
        if (frame == null) {
          disableWrite();
          return progressed;
        }

        try {
          var written = socket.write(frame);
          if (written > 0) {
            progressed = true;
          }
        } catch (IOException error) {
          shutdown(true);
          return true;
        }

        if (frame.hasRemaining()) {
          enableWrite();
          return progressed;
        }

        outbound.pollFirst();
      }
    }

    void shutdown(boolean notifyPeer) {
      if (!open) {
        return;
      }
      open = false;

      try {
        key.cancel();
      } catch (Throwable ignored) {
      }
      server.remove(socket);

      try {
        socket.close();
      } catch (IOException ignored) {
      }

      if (notifyPeer && peer != null) {
        peer.stop();
        invokeDisconnectCallback();
      }
    }

    private boolean finishHandshakeIfReady() {
      var headerEnd = findHttpHeaderEnd(inbound, inboundLength);
      if (headerEnd < 0) {
        return false;
      }

      var requestText = new String(inbound, 0, headerEnd, StandardCharsets.US_ASCII);
      consume(headerEnd);

      var request = parseHandshake(requestText);
      var clientKey = request.headers().get("sec-websocket-key");
      if (clientKey == null || clientKey.isBlank()) {
        throw new ChannelException("missing Sec-WebSocket-Key");
      }

      var response = "HTTP/1.1 101 Switching Protocols\r\n"
          + "Upgrade: websocket\r\n"
          + "Connection: Upgrade\r\n"
          + "Sec-WebSocket-Accept: " + WebSocketSupport.acceptFor(clientKey) + "\r\n"
          + "\r\n";
      outbound.addLast(ByteBuffer.wrap(response.getBytes(StandardCharsets.US_ASCII)));
      enableWrite();

      state = server.options.stateFactory() == null ? null : server.options.stateFactory().apply(request.stateHeaders());
      var transport = new ServerConnectionTransport<>(this);
      peer = new Peer<>(server.channel, state, transport);
      handshakeComplete = true;

      var onConnect = server.options.onConnect();
      if (onConnect != null) {
        try {
          onConnect.accept(state, new Sender<>(peer));
        } catch (Throwable ignored) {
        }
      }
      return true;
    }

    private boolean decodeFrames() {
      var progressed = false;
      while (inboundLength > 0) {
        var frame = WebSocketSupport.decode(inbound, inboundLength);
        if (frame == null) {
          return progressed;
        }

        consume(frame.consumed());
        progressed = true;

        switch (frame.opcode()) {
          case 0x1 -> {
            peer.handlePayload(frame.text());
            progressed = peer.pollLocalStreams() || progressed;
          }
          case 0x8 -> {
            shutdown(true);
            return true;
          }
          case 0x9 -> {
            outbound.addLast(WebSocketSupport.pongFrame(frame.payload(), false));
            enableWrite();
          }
          case 0xA -> {
          }
          default -> throw new ChannelException("unsupported websocket opcode: " + frame.opcode());
        }
      }
      return progressed;
    }

    private void queueText(String payload) {
      if (!open) {
        return;
      }
      outbound.addLast(WebSocketSupport.textFrame(payload, false));
      enableWrite();
    }

    private void closeTransportOnly() {
      if (!open) {
        return;
      }
      open = false;
      try {
        key.cancel();
      } catch (Throwable ignored) {
      }
      server.remove(socket);
      try {
        socket.close();
      } catch (IOException ignored) {
      }
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

    private HandshakeRequest parseHandshake(String requestText) {
      var lines = requestText.split("\r\n");
      if (lines.length == 0) {
        throw new ChannelException("invalid websocket handshake");
      }

      var requestLine = lines[0].split(" ");
      if (requestLine.length < 2) {
        throw new ChannelException("invalid websocket request line");
      }

      var headers = new LinkedHashMap<String, String>();
      for (var i = 1; i < lines.length; i += 1) {
        var line = lines[i];
        var idx = line.indexOf(':');
        if (idx <= 0) {
          continue;
        }
        var name = line.substring(0, idx).trim();
        var value = line.substring(idx + 1).trim();
        headers.put(name.toLowerCase(Locale.ROOT), value);
      }

      var merged = new LinkedHashMap<String, String>();
      applyQueryParams(merged, requestLine[1]);
      for (var entry : headers.entrySet()) {
        merged.put(entry.getKey(), entry.getValue());
      }

      return new HandshakeRequest(headers, merged);
    }

    private void applyQueryParams(Map<String, String> merged, String target) {
      var queryIndex = target.indexOf('?');
      if (queryIndex < 0 || queryIndex == target.length() - 1) {
        return;
      }
      var query = target.substring(queryIndex + 1);
      for (var part : query.split("&")) {
        if (part.isEmpty()) {
          continue;
        }
        var idx = part.indexOf('=');
        if (idx < 0) {
          merged.put(urlDecode(part), "");
        } else {
          merged.put(urlDecode(part.substring(0, idx)), urlDecode(part.substring(idx + 1)));
        }
      }
    }

    private String urlDecode(String value) {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private void ensureInboundCapacity(int required) {
      if (required <= inbound.length) {
        return;
      }
      var next = inbound.length;
      while (next < required) {
        next *= 2;
      }
      var grown = new byte[next];
      System.arraycopy(inbound, 0, grown, 0, inboundLength);
      inbound = grown;
    }

    private void consume(int bytes) {
      if (bytes >= inboundLength) {
        inboundLength = 0;
        return;
      }
      System.arraycopy(inbound, bytes, inbound, 0, inboundLength - bytes);
      inboundLength -= bytes;
    }

    private int findHttpHeaderEnd(byte[] bytes, int length) {
      for (var i = 0; i <= length - 4; i += 1) {
        if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n') {
          return i + 4;
        }
      }
      return -1;
    }
  }

  private record HandshakeRequest(Map<String, String> headers, Map<String, String> stateHeaders) {
  }

  private static final class ServerConnectionTransport<State> implements Transport {
    private final Connection<State> connection;

    private ServerConnectionTransport(Connection<State> connection) {
      this.connection = connection;
    }

    @Override
    public void queueFrame(String payload) {
      connection.queueText(payload);
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
