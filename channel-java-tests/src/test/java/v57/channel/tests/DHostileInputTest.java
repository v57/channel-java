package v57.channel.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import v57.channel.Channel;
import v57.channel.ChannelServer;
import v57.channel.ListenOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression tests for AUDIT.md section 2.2/3.1 and AUDIT2.md section 1.5/3 (hostile peers). */
public class DHostileInputTest {
  private static final String HANDSHAKE = "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
      + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

  // ---------- server side ----------

  @Test
  @Timeout(60)
  void serverSurvivesHostileMessagesFramesAndHandshakes() throws Exception {
    try (var running = new PolledServer()) {
      var port = running.server.port();
      var deep = "[".repeat(100_000) + "]".repeat(100_000);
      List<byte[]> hostileMessages = List.of(
          frame(0x1, true, "not json"),
          frame(0x1, true, "null"),
          frame(0x1, true, "123"),
          frame(0x1, true, "\"str\""),
          frame(0x1, true, "[null, 1]"),
          frame(0x1, true, "{\"id\":\"abc\",\"path\":\"echo\"}"),
          frame(0x1, true, "{\"cancel\":\"abc\"}"),
          frame(0x1, true, deep),
          frame(0x3, true, "{}"),
          frame(0x0, true, "orphan continuation"),
          lengthHeader(0x7FFF_FFFF_FFFF_FFFFL),
          lengthHeader(-1L),
          lengthHeader(1L << 30)
      );
      for (var message : hostileMessages) {
        try (var socket = rawConnect(port)) {
          socket.getOutputStream().write(message);
          socket.getOutputStream().flush();
          readUntilClosedOrTimeout(socket);
        }
        assertEquals("{\"id\":1,\"body\":5}", echo(port), "server stopped answering after a hostile message");
      }

      for (var handshake : List.of("GET / HTTP/1.1\r\nHost: x\r\n\r\n", "garbage\r\n\r\n")) {
        try (var socket = new Socket()) {
          socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
          socket.setSoTimeout(2000);
          socket.getOutputStream().write(handshake.getBytes(StandardCharsets.US_ASCII));
          readUntilClosedOrTimeout(socket);
        }
        assertEquals("{\"id\":1,\"body\":5}", echo(port), "server stopped answering after a bad handshake");
      }
      assertFalse(running.failed.get(), "server poll() threw");
    }
  }

  @Test
  @Timeout(60)
  void serverClosesEndlessHandshakeAndOversizedFrame() throws Exception {
    try (var running = new PolledServer()) {
      var port = running.server.port();
      try (var socket = new Socket()) {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        socket.setSoTimeout(5000);
        var chunk = ("X-Junk: " + "a".repeat(1000) + "\r\n").getBytes(StandardCharsets.US_ASCII);
        var closed = false;
        try {
          socket.getOutputStream().write("GET / HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII));
          for (var i = 0; i < 2_000; i += 1) {
            socket.getOutputStream().write(chunk);
          }
        } catch (IOException expected) {
          closed = true;
        }
        if (!closed) {
          closed = readUntilClosedOrTimeout(socket);
        }
        assertTrue(closed, "2 MB handshake without terminator must close the connection");
      }

      try (var socket = rawConnect(port)) {
        socket.getOutputStream().write(lengthHeader(1L << 30));
        assertTrue(readUntilClosedOrTimeout(socket), "a declared 1 GiB frame must close the connection");
      }
      assertEquals("{\"id\":1,\"body\":5}", echo(port));
    }
  }

  @Test
  @Timeout(60)
  void serverAcceptsFragmentedAndBinaryMessages() throws Exception {
    try (var running = new PolledServer()) {
      var port = running.server.port();
      try (var socket = rawConnect(port)) {
        var out = socket.getOutputStream();
        out.write(frame(0x1, false, "{\"id\":1,"));
        out.write(frame(0x9, true, "ping"));
        out.write(frame(0x0, false, "\"path\":\"echo\","));
        out.write(frame(0x0, true, "\"body\":5}"));
        out.flush();
        assertEquals("ping", readTextFrame(socket.getInputStream(), 0xA));
        assertEquals("{\"id\":1,\"body\":5}", readTextFrame(socket.getInputStream(), 0x1));

        out.write(frame(0x2, true, "{\"id\":2,\"path\":\"echo\",\"body\":6}"));
        out.flush();
        assertEquals("{\"id\":2,\"body\":6}", readTextFrame(socket.getInputStream(), 0x1));
      }
    }
  }

  // ---------- client side ----------

  @Test
  @Timeout(60)
  void clientSettlesCallsWhenTheServerMisbehaves() throws Exception {
    var deep = "[".repeat(100_000) + "]".repeat(100_000);
    record Case(String name, byte[] reply, String expected) {
    }
    var cases = List.of(
        new Case("badjson", serverFrame(0x1, true, "{oops"), "disconnected"),
        new Case("deepjson", serverFrame(0x1, true, "{\"id\":1,\"body\":" + deep + "}"), "disconnected"),
        new Case("len 2^63-1", serverLength(0x7FFF_FFFF_FFFF_FFFFL), "disconnected"),
        new Case("len 2^64-1", serverLength(-1L), "disconnected"),
        new Case("unknown opcode", serverFrame(0x3, true, "{}"), "disconnected")
    );
    for (var c : cases) {
      try (var hostile = new HostileServer(c.reply)) {
        try (var client = new Channel<Void>().connect("ws://127.0.0.1:" + hostile.port() + "/")) {
          var result = client.sender().send(Integer.class, "echo", 5);
          assertFalse(result.isOk(), c.name + " should fail");
          assertEquals(c.expected, result.remoteErr(), c.name);
        }
      }
    }

    var fragmented = concat(serverFrame(0x1, false, "{\"id\":1,"), serverFrame(0x0, true, "\"body\":5}"));
    for (var reply : List.of(fragmented, serverFrame(0x2, true, "{\"id\":1,\"body\":5}"))) {
      try (var hostile = new HostileServer(reply)) {
        try (var client = new Channel<Void>().connect("ws://127.0.0.1:" + hostile.port() + "/")) {
          assertEquals(5, client.sender().send(Integer.class, "echo", 5).requireValue());
        }
      }
    }

    try (var hostile = new HostileServer(null)) {
      try (var client = new Channel<Void>().connect("ws://127.0.0.1:" + hostile.port() + "/")) {
        var result = client.sender().send(Integer.class, "echo", 5);
        assertEquals("websocket upgrade failed: HTTP/1.1 500 Internal Server Error", result.remoteErr());
      }
    }
  }

  @Test
  @Timeout(60)
  void clientTypedDecodeFailureSettlesTheCall() throws Exception {
    try (var hostile = new HostileServer(serverFrame(0x1, true, "{\"id\":1,\"body\":\"not a number\"}"))) {
      try (var client = new Channel<Void>().connect("ws://127.0.0.1:" + hostile.port() + "/")) {
        var result = client.sender().send(Integer.class, "echo", 5);
        assertEquals("invalid response body", result.remoteErr());
      }
    }
  }

  // ---------- helpers ----------

  private static final class PolledServer implements AutoCloseable {
    final ChannelServer<Void> server;
    final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;

    PolledServer() {
      server = new Channel<Void>()
          .post("echo", Integer.class, Integer.class, request -> request.body())
          .listen("127.0.0.1", 0, new ListenOptions<Void>());
      thread = Thread.ofPlatform().name("polled-server").start(() -> {
        while (running.get()) {
          try {
            if (!server.poll()) {
              TestSupport.sleep(1);
            }
          } catch (Throwable error) {
            failed.set(true);
            return;
          }
        }
      });
    }

    @Override
    public void close() throws Exception {
      running.set(false);
      thread.join(5000);
      server.close();
    }
  }

  /** One-shot raw websocket server: completes the handshake (or answers 500 when reply is null), then sends reply. */
  private static final class HostileServer implements AutoCloseable {
    private final ServerSocket listener = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
    private final Thread thread;

    HostileServer(byte[] reply) throws IOException {
      thread = Thread.ofPlatform().daemon().start(() -> {
        try (var socket = listener.accept()) {
          var in = socket.getInputStream();
          var head = readHead(in);
          var out = socket.getOutputStream();
          if (reply == null) {
            out.write("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Thread.sleep(500);
            return;
          }
          var key = head.lines().filter(l -> l.toLowerCase().startsWith("sec-websocket-key:")).findFirst().orElseThrow()
              .substring(18).trim();
          out.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
              + "Sec-WebSocket-Accept: " + accept(key) + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
          readClientFrame(in);
          out.write(reply);
          out.flush();
          Thread.sleep(2000);
        } catch (Exception ignored) {
        }
      });
    }

    int port() {
      return listener.getLocalPort();
    }

    @Override
    public void close() throws Exception {
      listener.close();
      thread.interrupt();
      thread.join(3000);
    }
  }

  private static String accept(String key) throws Exception {
    var digest = java.security.MessageDigest.getInstance("SHA-1")
        .digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII));
    return Base64.getEncoder().encodeToString(digest);
  }

  private static String readHead(InputStream in) throws IOException {
    var head = new StringBuilder();
    while (!head.toString().endsWith("\r\n\r\n")) {
      var b = in.read();
      if (b < 0) {
        throw new IOException("eof in head");
      }
      head.append((char) b);
    }
    return head.toString();
  }

  private static void readClientFrame(InputStream in) throws IOException {
    in.readNBytes(1);
    var len = in.read() & 0x7F;
    if (len == 126) {
      len = ((in.read() & 0xFF) << 8) | (in.read() & 0xFF);
    } else if (len == 127) {
      len = (int) java.nio.ByteBuffer.wrap(in.readNBytes(8)).getLong();
    }
    in.readNBytes(4 + len);
  }

  private static Socket rawConnect(int port) throws IOException {
    var socket = new Socket();
    socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
    socket.setSoTimeout(5000);
    socket.getOutputStream().write(HANDSHAKE.getBytes(StandardCharsets.US_ASCII));
    var head = readHead(socket.getInputStream());
    assertTrue(head.startsWith("HTTP/1.1 101"), head);
    return socket;
  }

  private static String echo(int port) throws IOException {
    try (var socket = rawConnect(port)) {
      socket.getOutputStream().write(frame(0x1, true, "{\"id\":1,\"path\":\"echo\",\"body\":5}"));
      return readTextFrame(socket.getInputStream(), 0x1);
    }
  }

  private static String readTextFrame(InputStream in, int expectedOpcode) throws IOException {
    var first = in.read();
    assertEquals(expectedOpcode, first & 0x0F);
    var len = in.read() & 0x7F;
    if (len == 126) {
      len = ((in.read() & 0xFF) << 8) | (in.read() & 0xFF);
    }
    return new String(in.readNBytes(len), StandardCharsets.UTF_8);
  }

  /** Returns true when the peer closed the socket (EOF/reset) within the read timeout. */
  private static boolean readUntilClosedOrTimeout(Socket socket) {
    try {
      var in = socket.getInputStream();
      var buffer = new byte[4096];
      while (in.read(buffer) >= 0) {
        // drain
      }
      return true;
    } catch (java.net.SocketTimeoutException timeout) {
      return false;
    } catch (IOException reset) {
      return true;
    }
  }

  /** Masked client frame. */
  private static byte[] frame(int opcode, boolean fin, String text) {
    return encode(opcode, fin, text.getBytes(StandardCharsets.UTF_8), true);
  }

  private static byte[] serverFrame(int opcode, boolean fin, String text) {
    return encode(opcode, fin, text.getBytes(StandardCharsets.UTF_8), false);
  }

  private static byte[] encode(int opcode, boolean fin, byte[] payload, boolean masked) {
    var out = new ByteArrayOutputStream();
    out.write((fin ? 0x80 : 0) | opcode);
    var maskBit = masked ? 0x80 : 0;
    if (payload.length < 126) {
      out.write(maskBit | payload.length);
    } else if (payload.length <= 0xFFFF) {
      out.write(maskBit | 126);
      out.write(payload.length >>> 8);
      out.write(payload.length & 0xFF);
    } else {
      out.write(maskBit | 127);
      out.writeBytes(java.nio.ByteBuffer.allocate(8).putLong(payload.length).array());
    }
    if (masked) {
      var mask = new byte[]{1, 2, 3, 4};
      out.writeBytes(mask);
      for (var i = 0; i < payload.length; i += 1) {
        out.write(payload[i] ^ mask[i % 4]);
      }
    } else {
      out.writeBytes(payload);
    }
    return out.toByteArray();
  }

  private static byte[] lengthHeader(long length) {
    var buffer = java.nio.ByteBuffer.allocate(14);
    buffer.put((byte) 0x81).put((byte) (0x80 | 127)).putLong(length).put(new byte[]{1, 2, 3, 4});
    return buffer.array();
  }

  private static byte[] serverLength(long length) {
    var buffer = java.nio.ByteBuffer.allocate(10);
    buffer.put((byte) 0x81).put((byte) 127).putLong(length);
    return buffer.array();
  }

  private static byte[] concat(byte[] a, byte[] b) {
    var out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
