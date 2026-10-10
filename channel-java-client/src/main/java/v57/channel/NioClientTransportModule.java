package v57.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class NioClientTransportModule implements ClientTransportProvider {
  @Override
  public <State> ChannelClient<State> connect(Channel<State> channel, String address, ConnectOptions<State> options) {
    var safeOptions = options == null ? new ConnectOptions<State>() : options;
    var target = ClientTarget.parse(address);
    var transport = ClientSocketTransport.open(target, safeOptions.headers(), safeOptions.maxPayload());
    var peer = new Peer<>(channel, safeOptions.state(), transport);
    transport.attach(peer);
    PumpRegistry.register(transport);

    return new ChannelClient<>(peer, transport, () -> {
      PumpRegistry.unregister(transport);
      transport.close();
    });
  }

  private record ClientTarget(String host, int port, String pathWithQuery, String hostHeader) {
    static ClientTarget parse(String address) {
      try {
        var uri = new URI(Objects.requireNonNull(address, "address"));
        if (!"ws".equalsIgnoreCase(uri.getScheme())) {
          throw new ChannelException("only ws:// is supported");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
          throw new ChannelException("websocket host is required");
        }

        var port = uri.getPort() > 0 ? uri.getPort() : 80;
        var path = uri.getRawPath();
        if (path == null || path.isEmpty()) {
          path = "/";
        }
        if (uri.getRawQuery() != null && !uri.getRawQuery().isEmpty()) {
          path += "?" + uri.getRawQuery();
        }

        var hostHeader = uri.getHost() + (port == 80 ? "" : ":" + port);
        return new ClientTarget(uri.getHost(), port, path, hostHeader);
      } catch (URISyntaxException error) {
        throw new ChannelException("invalid websocket address", error);
      }
    }
  }

  private static final class ClientSocketTransport implements Transport, Pollable {
    private final ClientTarget target;
    private final Map<String, String> headers;
    private final SocketChannel socket;
    private final ArrayDeque<ByteBuffer> outbound = new ArrayDeque<>();
    private final ByteBuffer readBuffer = ByteBuffer.allocate(16_384);
    private final ArrayDeque<String> pendingText = new ArrayDeque<>();
    private final int maxPayload;
    private final WebSocketSupport.MessageAssembler assembler;

    private byte[] inbound = new byte[16_384];
    private int inboundLength;
    private Peer<?> peer;
    private boolean open = true;
    private boolean tcpConnected;
    private boolean handshakeSent;
    private boolean handshakeComplete;
    private final String clientKey = WebSocketSupport.clientKey();

    private ClientSocketTransport(ClientTarget target, Map<String, String> headers, SocketChannel socket, boolean tcpConnected, int maxPayload) {
      this.target = target;
      this.headers = headers;
      this.socket = socket;
      this.tcpConnected = tcpConnected;
      this.maxPayload = maxPayload;
      this.assembler = new WebSocketSupport.MessageAssembler(maxPayload);
    }

    static ClientSocketTransport open(ClientTarget target, Map<String, String> headers, int maxPayload) {
      try {
        var socket = SocketChannel.open();
        socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socket.configureBlocking(false);
        var connected = socket.connect(new InetSocketAddress(target.host(), target.port()));
        return new ClientSocketTransport(target, headers == null ? Map.of() : Map.copyOf(headers), socket, connected, maxPayload);
      } catch (IOException error) {
        throw new ChannelException("failed to connect websocket client", error);
      }
    }

    void attach(Peer<?> peer) {
      this.peer = peer;
    }

    @Override
    public synchronized void queueFrame(String payload) {
      if (!open) {
        return;
      }
      pendingText.addLast(payload);
    }

    @Override
    public synchronized void close() {
      if (!open) {
        return;
      }
      open = false;
      outbound.clear();
      pendingText.clear();
      try {
        socket.close();
      } catch (IOException ignored) {
      }
    }

    @Override
    public synchronized boolean isConnected() {
      return open && handshakeComplete && socket.isOpen();
    }

    @Override
    public synchronized boolean isOpen() {
      return open && socket.isOpen();
    }

    @Override
    public boolean poll() {
      if (!isOpen()) {
        return false;
      }
      try {
        return pollOnce();
      } catch (RuntimeException | StackOverflowError error) {
        // A hostile or broken server (bad handshake, malformed frame, undecodable message) only
        // disconnects this client: pending calls settle with an error instead of the exception
        // escaping into whoever happens to be pumping.
        fail(error instanceof HandshakeException ? error.getMessage() : "disconnected");
        return true;
      }
    }

    private boolean pollOnce() {
      var localPeer = peer;
      var progressed = finishConnect();
      progressed = sendHandshakeIfReady() || progressed;
      progressed = flushOutbound() || progressed;
      if (tcpConnected) {
        progressed = readInbound() || progressed;
      }
      if (!handshakeComplete) {
        progressed = finishHandshakeIfReady() || progressed;
      }
      progressed = flushPendingFrames() || progressed;
      if (handshakeComplete) {
        progressed = decodeFrames() || progressed;
      }
      if (localPeer != null && handshakeComplete && isOpen()) {
        progressed = localPeer.pollLocalStreams() || progressed;
      }
      progressed = flushOutbound() || progressed;
      return progressed;
    }

    private void validateHandshake(String headersText, String clientKey) {
      var lines = headersText.split("\r\n");
      if (lines.length == 0 || !lines[0].contains(" 101")) {
        var status = lines.length == 0 ? "" : lines[0].trim();
        if (status.length() > 80) {
          status = status.substring(0, 80);
        }
        throw new HandshakeException("websocket upgrade failed: " + status);
      }

      var headers = new LinkedHashMap<String, String>();
      for (var i = 1; i < lines.length; i += 1) {
        var line = lines[i];
        var idx = line.indexOf(':');
        if (idx <= 0) {
          continue;
        }
        headers.put(line.substring(0, idx).trim().toLowerCase(), line.substring(idx + 1).trim());
      }

      var accept = headers.get("sec-websocket-accept");
      if (!WebSocketSupport.acceptFor(clientKey).equals(accept)) {
        throw new HandshakeException("invalid websocket accept header");
      }
    }

    private boolean finishConnect() {
      if (tcpConnected) {
        return false;
      }
      try {
        tcpConnected = socket.finishConnect();
        return tcpConnected;
      } catch (IOException error) {
        handleDisconnect();
        return true;
      }
    }

    private boolean sendHandshakeIfReady() {
      if (!tcpConnected || handshakeSent || !open) {
        return false;
      }

      var request = new StringBuilder(256);
      request.append("GET ").append(target.pathWithQuery()).append(" HTTP/1.1\r\n");
      request.append("Host: ").append(target.hostHeader()).append("\r\n");
      request.append("Upgrade: websocket\r\n");
      request.append("Connection: Upgrade\r\n");
      request.append("Sec-WebSocket-Key: ").append(clientKey).append("\r\n");
      request.append("Sec-WebSocket-Version: 13\r\n");
      for (var entry : headers.entrySet()) {
        if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null) {
          continue;
        }
        request.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
      }
      request.append("\r\n");

      outbound.addLast(ByteBuffer.wrap(request.toString().getBytes(StandardCharsets.US_ASCII)));
      handshakeSent = true;
      return true;
    }

    private boolean finishHandshakeIfReady() {
      if (!handshakeSent) {
        return false;
      }
      var headerEnd = findHttpHeaderEnd(inbound, inboundLength);
      if (headerEnd < 0) {
        if (inboundLength > WebSocketSupport.MAX_HANDSHAKE_BYTES) {
          throw new HandshakeException("websocket upgrade failed: response head too large");
        }
        return false;
      }

      var headersText = new String(inbound, 0, headerEnd, StandardCharsets.US_ASCII);
      validateHandshake(headersText, clientKey);
      consume(headerEnd);
      handshakeComplete = true;
      return true;
    }

    private boolean flushPendingFrames() {
      if (!handshakeComplete) {
        return false;
      }
      var progressed = false;
      while (true) {
        String payload;
        synchronized (this) {
          payload = pendingText.pollFirst();
        }
        if (payload == null) {
          return progressed;
        }
        outbound.addLast(WebSocketSupport.textFrame(payload, true));
        progressed = true;
      }
    }

    private boolean flushOutbound() {
      if (!tcpConnected) {
        return false;
      }
      var progressed = false;
      while (true) {
        ByteBuffer frame;
        synchronized (this) {
          frame = outbound.peekFirst();
        }
        if (frame == null) {
          return progressed;
        }

        try {
          var written = socket.write(frame);
          if (written > 0) {
            progressed = true;
          }
        } catch (IOException error) {
          handleDisconnect();
          return true;
        }

        if (frame.hasRemaining()) {
          return progressed;
        }

        synchronized (this) {
          outbound.pollFirst();
        }
      }
    }

    private boolean readInbound() {
      var progressed = false;
      while (true) {
        readBuffer.clear();
        int read;
        try {
          read = socket.read(readBuffer);
        } catch (IOException error) {
          handleDisconnect();
          return true;
        }

        if (read < 0) {
          handleDisconnect();
          return true;
        }
        if (read == 0) {
          return progressed;
        }

        readBuffer.flip();
        ensureInboundCapacity(inboundLength + read);
        readBuffer.get(inbound, inboundLength, read);
        inboundLength += read;
        progressed = true;
        // Let decodeFrames consume (or reject) what is buffered before reading more.
        if (inboundLength >= readLimit()) {
          return progressed;
        }
      }
    }

    private int readLimit() {
      return (int) Math.min(Integer.MAX_VALUE / 2, (long) maxPayload + WebSocketSupport.MAX_HANDSHAKE_BYTES);
    }

    private boolean decodeFrames() {
      var progressed = false;
      while (inboundLength > 0 && isOpen()) {
        var raw = WebSocketSupport.decode(inbound, inboundLength, maxPayload);
        if (raw == null) {
          return progressed;
        }

        consume(raw.consumed());
        progressed = true;
        var frame = assembler.accept(raw);
        if (frame == null) {
          continue;
        }

        switch (frame.opcode()) {
          // Binary messages are decoded as UTF-8 JSON too (same as channel-kotlin).
          case 0x1, 0x2 -> {
            if (handshakeComplete && peer != null) {
              peer.handlePayload(frame.text());
            }
          }
          case 0x8 -> {
            handleDisconnect();
            return true;
          }
          case 0x9 -> {
            synchronized (this) {
              if (open) {
                outbound.addLast(WebSocketSupport.pongFrame(frame.payload(), true));
              }
            }
          }
          case 0xA -> {
          }
          default -> throw new ChannelException("unsupported websocket opcode: " + frame.opcode());
        }
      }
      return progressed;
    }

    private void handleDisconnect() {
      fail("disconnected");
    }

    private void fail(String reason) {
      var localPeer = peer;
      if (localPeer != null && isOpen()) {
        localPeer.stop(reason);
        return;
      }
      close();
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

  /** Upgrade rejected by the server; its message is surfaced to pending calls. */
  private static final class HandshakeException extends RuntimeException {
    HandshakeException(String message) {
      super(message);
    }
  }
}
