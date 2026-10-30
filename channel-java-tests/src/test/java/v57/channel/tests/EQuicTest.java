package v57.channel.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import v57.channel.Channel;
import v57.channel.ChannelClient;
import v57.channel.ChannelException;
import v57.channel.ChannelServer;
import v57.channel.ChannelStreams;
import v57.channel.ConnectOptions;
import v57.channel.JsonCodec;
import v57.channel.ListenOptions;
import v57.channel.QuicOptions;
import v57.channel.Subscription;
import v57.channel.TypeRef;

/** QUIC transport (channel-java-quic): Java client and server, the wire rules, and interop with the TypeScript reference. */
public class EQuicTest {
  /** The TypeScript reference's test certificate (channel/tests/quic): localhost, 127.0.0.1 and ::1 */
  private static final String FINGERPRINT =
      "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B";
  private static final TypeRef<Object> DYNAMIC = TypeRef.raw(Object.class);
  private static final TypeRef<Void> VOID = TypeRef.of(Void.class);

  private static File certDir;
  private static String cert;
  private static String key;

  @BeforeAll
  static void readCertificate() throws IOException {
    try {
      certDir = new File(TsServerFixture.resolveChannelRoot(), "tests/quic");
    } catch (IllegalStateException error) {
      Assumptions.abort("the TypeScript reference isn't next to this checkout");
    }
    cert = Files.readString(new File(certDir, "cert.pem").toPath());
    key = Files.readString(new File(certDir, "key.pem").toPath());
  }

  // MARK: Java client and server

  private static final class Fixture implements AutoCloseable {
    /** No snapshot on subscribe (the default acknowledges with the request, which a TypeScript client takes as an event) */
    final Subscription<String, String> ticks = new Subscription<>(
        TypeRef.of(String.class), TypeRef.of(String.class), topic -> topic, topic -> null);
    final AtomicInteger connected = new AtomicInteger();
    final AtomicInteger disconnected = new AtomicInteger();
    final ChannelServer<Map<String, String>> server;

    Fixture() {
      var channel = new Channel<Map<String, String>>()
          .post("hello", String.class, request -> "world")
          .post("echo", DYNAMIC, DYNAMIC, VOID, request -> request.body())
          .post("name", String.class, request -> request.state().get("x-name"))
          .post("mirror", String.class, request -> request.sender().send(String.class, "hello").requireValue())
          .post("disconnect", String.class, request -> {
            request.sender().stop();
            return "dropped";
          })
          .stream("count", Integer.class, request -> ChannelStreams.fromIterable(List.of(0, 1, 2)))
          .post("tick", Void.class, request -> {
            ticks.send("a", "tick");
            return null;
          })
          .events(Map.of("ticks", ticks));
      server = channel.listenQuic(0, new ListenOptions<Map<String, String>>(
          headers -> headers,
          (state, sender) -> connected.incrementAndGet(),
          (state, sender) -> disconnected.incrementAndGet()
      ).withCertificate(cert, key));
    }

    int port() {
      return server.port();
    }

    String address() {
      return "quic://127.0.0.1:" + server.port();
    }

    @Override
    public void close() {
      server.stop();
    }
  }

  private static Channel<Void> clientChannel() {
    return new Channel<Void>().post("hello", String.class, request -> "client world");
  }

  private static ConnectOptions<Void> trusted() {
    return new ConnectOptions<Void>().withQuic(new QuicOptions().withCa(cert));
  }

  @Test
  void clientAndServer() {
    try (var f = new Fixture();
        var client = clientChannel().connect(f.address(),
            new ConnectOptions<Void>(null, Map.of("X-Name", "java")).withQuic(new QuicOptions().withCa(cert)))) {
      assertEquals("world", client.sender().send(String.class, "hello").requireValue());
      assertTrue(client.isConnected());

      var body = new LinkedHashMap<String, Object>();
      body.put("list", Arrays.asList(1L, "two", null));
      body.put("text", "é".repeat(20000));
      assertEquals(body, client.sender().send(DYNAMIC, "echo", body, null).requireValue());

      // Hello headers become the state, names lowercased
      assertEquals("java", client.sender().send(String.class, "name").requireValue());
      // The server calls the client back
      assertEquals("client world", client.sender().send(String.class, "mirror").requireValue());

      try (var values = client.sender().values(Integer.class, "count")) {
        assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, null));
      }

      var events = new AtomicInteger();
      var subscription = client.sender().subscribe(String.class, "ticks", "a", event -> events.incrementAndGet());
      assertTrue(subscription.isOk());
      f.ticks.send("a", "tick");
      f.ticks.send("b", "not for us");
      f.ticks.send("a", "tick");
      TestSupport.waitFor(() -> events.get() == 2, Duration.ofSeconds(5), "events not received", client::poll);
      subscription.handle().cancel();
      pump(Duration.ofMillis(100), client::poll);
      f.ticks.send("a", "after cancel");
      pump(Duration.ofMillis(100), client::poll);
      assertEquals(2, events.get());

      for (var i = 0; i < 500; i += 1) {
        assertEquals((long) i, client.sender().send(DYNAMIC, "echo", i, null).requireValue());
      }
      // A message over 1 MiB: several flow-control windows
      var large = "x".repeat(3 * 1024 * 1024);
      assertEquals(large, client.sender().send(DYNAMIC, "echo", large, null).requireValue());

      assertEquals(1, f.connected.get());
      client.stop();
      TestSupport.waitFor(() -> f.disconnected.get() == 1, Duration.ofSeconds(5), "no disconnect", f.server::poll);
      TestSupport.assertRemoteErr(client.sender().send(String.class, "hello"), "disconnected");
    }
  }

  @Test
  void eachClientHasItsOwnConnection() {
    try (var f = new Fixture();
        var a = clientChannel().connect(f.address(), new ConnectOptions<Void>(null, Map.of("x-name", "a")).withQuic(new QuicOptions().withCa(cert)));
        var b = clientChannel().connect(f.address(), new ConnectOptions<Void>(null, Map.of("x-name", "b")).withQuic(new QuicOptions().withCa(cert)))) {
      assertEquals("a", a.sender().send(String.class, "name").requireValue());
      assertEquals("b", b.sender().send(String.class, "name").requireValue());
      assertEquals(2, f.connected.get());
    }
  }

  @Test
  void certificateTrust() {
    try (var f = new Fixture()) {
      var pin = FINGERPRINT.replace(":", "").toLowerCase();
      try (var pinned = clientChannel().connect(f.address(), new ConnectOptions<Void>().withQuic(new QuicOptions().withFingerprint(pin)))) {
        assertEquals("world", pinned.sender().send(String.class, "hello").requireValue());
      }
      try (var byName = clientChannel().connect(f.address(),
          new ConnectOptions<Void>().withQuic(new QuicOptions().withCa(cert).withServerName("localhost")))) {
        assertEquals("world", byName.sender().send(String.class, "hello").requireValue());
      }

      var refused = Map.of(
          "system roots", new QuicOptions(),
          "wrong pin", new QuicOptions().withFingerprint("00".repeat(32)),
          "name not in the certificate", new QuicOptions().withCa(cert).withServerName("example.com")
      );
      for (var entry : refused.entrySet()) {
        try (var client = clientChannel().connect(f.address(), new ConnectOptions<Void>().withQuic(entry.getValue()))) {
          var result = client.sender().send(String.class, "hello");
          assertFalse(result.isOk(), entry.getKey() + ": connected");
          assertEquals("disconnected", result.remoteErr(), entry.getKey());
          assertFalse(client.isConnected(), entry.getKey());
        }
      }
      assertEquals(2, f.connected.get());

      assertThrows(ChannelException.class,
          () -> clientChannel().connect(f.address(), new ConnectOptions<Void>().withQuic(new QuicOptions().withFingerprint("nope"))));
      assertThrows(ChannelException.class, () -> clientChannel().connect("quic://127.0.0.1", trusted()));
    }
  }

  @Test
  void listenQuicNeedsACertificate() {
    assertThrows(ChannelException.class, () -> clientChannel().listenQuic(0, new ListenOptions<>()));
    assertThrows(ChannelException.class, () -> clientChannel().listenQuic(0, new ListenOptions<Void>().withCertificate(cert, "not a key")));
  }

  @Test
  void aDroppedConnectionFailsCallsLikeTheWebSocketClient() {
    try (var f = new Fixture(); var client = clientChannel().connect(f.address(), trusted())) {
      assertEquals("world", client.sender().send(String.class, "hello").requireValue());
      // The server stops the connection without answering
      TestSupport.assertRemoteErr(client.sender().send(String.class, "disconnect"), "disconnected");
      TestSupport.waitFor(() -> !client.isConnected(), Duration.ofSeconds(5), "still connected", client::poll);
      TestSupport.waitFor(() -> f.disconnected.get() == 1, Duration.ofSeconds(5), "no disconnect", f.server::poll);
    }

    var f = new Fixture();
    try (var client = clientChannel().connect(f.address(), trusted())) {
      assertEquals("world", client.sender().send(String.class, "hello").requireValue());
      f.close();
      TestSupport.waitFor(() -> !client.isConnected(), Duration.ofSeconds(5), "still connected after the server stopped", client::poll);
      TestSupport.assertRemoteErr(client.sender().send(String.class, "hello"), "disconnected");
    }

    // Stop fails calls while it still connects (nothing listens there)
    var client = clientChannel().connect("quic://127.0.0.1:9", trusted());
    client.stop();
    TestSupport.assertRemoteErr(client.sender().send(String.class, "hello"), "disconnected");
  }

  // MARK: Wire rules with raw frames

  private static byte[] frame(String text) {
    var payload = text.getBytes(StandardCharsets.UTF_8);
    var out = new ByteArrayOutputStream();
    var n = payload.length;
    if (n < 64) {
      out.write(n);
    } else if (n < 16384) {
      out.write(0x40 | (n >> 8));
      out.write(n & 0xff);
    } else {
      out.write(0x80 | (n >>> 24));
      out.write((n >> 16) & 0xff);
      out.write((n >> 8) & 0xff);
      out.write(n & 0xff);
    }
    out.writeBytes(payload);
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    var out = new ByteArrayOutputStream();
    for (var part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  private static String readFrame(InputStream input) throws IOException {
    while (true) {
      var first = input.read();
      if (first < 0) {
        return null;
      }
      var size = 1 << (first >> 6);
      long length = first & 0x3f;
      for (var i = 1; i < size; i += 1) {
        length = (length << 8) | input.read();
      }
      if (length > 0) {
        return new String(input.readNBytes((int) length), StandardCharsets.UTF_8);
      }
    }
  }

  private record Raw(QuicClientConnection connection, QuicStream stream, AtomicBoolean closed) {
  }

  /** Dials the server without the Channel client */
  private static Raw raw(Fixture f) throws IOException {
    var connection = QuicClientConnection.newBuilder()
        .host("127.0.0.1")
        .port(f.port())
        .applicationProtocol("channel/1")
        .version(QuicConnection.QuicVersion.V1)
        .connectTimeout(Duration.ofSeconds(5))
        .noServerCertificateCheck()
        .build();
    var closed = new AtomicBoolean();
    connection.setConnectionListener(event -> closed.set(true));
    connection.connect();
    return new Raw(connection, connection.createStream(true), closed);
  }

  private static void pump(Duration duration, TestSupport.Poller... pollers) {
    var deadline = System.nanoTime() + duration.toNanos();
    while (System.nanoTime() < deadline) {
      var progressed = false;
      for (var poller : pollers) {
        progressed = poller.poll() || progressed;
      }
      if (!progressed) {
        TestSupport.sleep(1);
      }
    }
  }

  private static <T> T await(CompletableFuture<T> future, Fixture f) throws Exception {
    TestSupport.waitFor(future::isDone, Duration.ofSeconds(5), "timed out", f.server::poll);
    return future.get();
  }

  @Test
  void rawFramesKeepalivesAndBatches() throws Exception {
    try (var f = new Fixture()) {
      var raw = raw(f);
      try {
        var output = raw.stream().getOutputStream();
        output.write(concat(
            frame("{\"hello\":{\"X-Name\":\"raw\",\"ignored\":1}}"),
            new byte[] {0},
            frame("[{\"id\":1,\"path\":\"name\"},{\"id\":2,\"path\":\"hello\"}]")
        ));
        output.flush();
        var responses = CompletableFuture.supplyAsync(() -> {
          try {
            var input = raw.stream().getInputStream();
            var bodies = new LinkedHashMap<Object, Object>();
            while (bodies.size() < 2) {
              var message = (Map<?, ?>) JsonCodec.decode(readFrame(input));
              bodies.put(message.get("id"), message.get("body"));
            }
            return bodies;
          } catch (IOException error) {
            throw new RuntimeException(error);
          }
        });
        var bodies = await(responses, f);
        assertEquals("raw", bodies.get(1L));
        assertEquals("world", bodies.get(2L));
      } finally {
        raw.connection().close();
      }
    }
  }

  @Test
  void oneStreamPerConnection() throws Exception {
    try (var f = new Fixture()) {
      var raw = raw(f);
      try {
        raw.stream().getOutputStream().write(frame("{\"hello\":{}}"));
        raw.stream().getOutputStream().flush();
        pump(Duration.ofMillis(100), f.server::poll);
        // The server grants no credit for a second bidirectional stream or any unidirectional one: opening blocks
        for (var bidirectional : new boolean[] {true, false}) {
          var opening = new Thread(() -> {
            try {
              raw.connection().createStream(bidirectional);
            } catch (Exception ignored) {
            }
          });
          opening.setDaemon(true);
          opening.start();
          opening.join(500);
          assertTrue(opening.isAlive(), (bidirectional ? "a second" : "a unidirectional") + " stream opened");
          opening.interrupt();
        }
        assertEquals(1, f.connected.get());
        // Resumption is off, so no client gets 0-RTT: the server issues no session ticket
        assertTrue(raw.connection().getNewSessionTickets().isEmpty());
      } finally {
        raw.connection().close();
      }
    }
  }

  @Test
  void badFirstFramesCloseTheConnection() throws Exception {
    var cases = new LinkedHashMap<String, byte[]>();
    cases.put("a first frame that isn't a hello", frame("{\"id\":1,\"path\":\"hello\"}"));
    cases.put("invalid JSON", concat(frame("{\"hello\":{}}"), frame("{nope")));
    // 16 MiB + 1, with no payload behind it
    cases.put("a frame over the limit", concat(frame("{\"hello\":{}}"), new byte[] {(byte) 0x81, 0, 0, 1}));
    try (var f = new Fixture()) {
      for (var entry : cases.entrySet()) {
        var raw = raw(f);
        try {
          raw.stream().getOutputStream().write(entry.getValue());
          raw.stream().getOutputStream().flush();
          TestSupport.waitFor(() -> raw.closed().get() || !raw.connection().isConnected(), Duration.ofSeconds(3),
              entry.getKey() + ": still open", f.server::poll);
        } finally {
          raw.connection().close();
        }
      }
      // The hello opened two of them, and both were disconnected
      TestSupport.waitFor(() -> f.disconnected.get() == 2, Duration.ofSeconds(3), "disconnects", f.server::poll);
      assertEquals(2, f.connected.get());
    }
  }

  // MARK: Interop with the TypeScript reference

  /** The fixture's port: `CHANNEL_TEST_PORT` bun tests/server.ts serves QUIC on UDP next to its WebSocket on TCP. */
  private static int tsPort() {
    var configured = System.getenv("CHANNEL_TEST_PORT");
    return configured == null || configured.isBlank() ? 2105 : Integer.parseInt(configured.trim());
  }

  private static boolean tcpOpen(int port) {
    try (var socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
      return true;
    } catch (IOException error) {
      return false;
    }
  }

  @Test
  void interopTypeScriptServer() {
    var port = tsPort();
    Assumptions.assumeTrue(tcpOpen(port), "the TypeScript fixture isn't running on 127.0.0.1:" + port);

    var names = new Subscription<>(String.class, String.class);
    var channel = clientChannel()
        .stream("stream/values", Integer.class, request -> ChannelStreams.fromIterable(List.of(0, 1, 2)))
        .events(Map.of("names", names));
    try (var client = channel.connect("quic://127.0.0.1:" + port,
        new ConnectOptions<Void>(null, Map.of("X-Port", "java")).withQuic(new QuicOptions().withCa(cert)))) {
      assertEquals("world", client.sender().send(String.class, "hello").requireValue());
      assertTrue(client.isConnected());
      assertEquals(Map.of("a", List.of(1L, "b")), client.sender().send(DYNAMIC, "echo", Map.of("a", List.of(1, "b")), null).requireValue());
      assertEquals("client world", client.sender().send(String.class, "mirror").requireValue());
      assertEquals("quic", client.sender().send(String.class, "auth", Map.of("name", "quic")).requireValue());
      assertEquals("quic", client.sender().send(String.class, "auth/name").requireValue());
      var headers = (Map<?, ?>) client.sender().send("v2/headers").requireValue();
      assertEquals("java", headers.get("x-port"));

      try (var values = client.sender().values(Integer.class, "mirror/stream")) {
        assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, null));
      }

      // The fixture subscribes to this client's names/1 and cancels after the first event
      assertTrue(client.sender().send(Void.class, "mirror/events").isOk());
      TestSupport.waitFor(() -> client.isRemoteSubscribed("names/1"), Duration.ofSeconds(3), "never subscribed", client::poll);
      names.send("1", "h");
      TestSupport.waitFor(() -> !client.isRemoteSubscribed("names/1"), Duration.ofSeconds(3), "never unsubscribed", client::poll);

      assertFalse(client.sender().send("v2/error/code").isOk());
      assertNull(client.sender().send("v2/drops/reset").requireValue());
      // The fixture drops the connection without answering
      TestSupport.assertRemoteErr(client.sender().send("v2/drop"), "disconnected");
      TestSupport.waitFor(() -> !client.isConnected(), Duration.ofSeconds(3), "still connected", client::poll);
    }
  }

  @Test
  void interopTypeScriptClient() throws Exception {
    var script = new File("src/test/ts/quic-client.ts").getAbsoluteFile();
    Assumptions.assumeTrue(script.isFile(), "missing " + script);
    var bun = System.getenv("BUN");
    try (var f = new Fixture()) {
      var process = new ProcessBuilder(bun == null || bun.isBlank() ? "bun" : bun, script.getPath(),
          String.valueOf(f.port()), new File(certDir, "cert.pem").getPath())
          .redirectError(ProcessBuilder.Redirect.INHERIT)
          .start();
      var output = CompletableFuture.supplyAsync(() -> {
        try {
          return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException error) {
          throw new RuntimeException(error);
        }
      });
      try {
        TestSupport.waitFor(() -> !process.isAlive(), Duration.ofSeconds(30), "the bun client didn't finish", f.server::poll);
      } finally {
        process.destroyForcibly();
      }
      assertEquals(0, process.waitFor(5, TimeUnit.SECONDS) ? process.exitValue() : -1, "bun exit code");
      var result = (Map<?, ?>) JsonCodec.decode(output.get().trim());
      var expected = new LinkedHashMap<String, Object>();
      expected.put("hello", "world");
      expected.put("echo", Map.of("a", List.of(1L, "b")));
      expected.put("name", "ts");
      expected.put("mirror", "ts world");
      expected.put("values", List.of(0L, 1L, 2L));
      expected.put("event", "tick");
      assertEquals(expected, new LinkedHashMap<>(result));
      TestSupport.waitFor(() -> f.disconnected.get() == f.connected.get(), Duration.ofSeconds(5), "connections left open", f.server::poll);
      assertEquals(1, f.connected.get());
    }
  }
}
