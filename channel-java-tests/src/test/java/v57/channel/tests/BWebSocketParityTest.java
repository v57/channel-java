package v57.channel.tests;

import org.junit.jupiter.api.Test;
import v57.channel.Channel;
import v57.channel.ChannelStreams;
import v57.channel.ConnectOptions;
import v57.channel.ListenOptions;
import v57.channel.Subscription;
import v57.channel.TypeRef;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BWebSocketParityTest {
  private static final TypeRef<Void> VOID = TypeRef.of(Void.class);
  private static final TypeRef<List<Integer>> INT_LIST = new TypeRef<>() {
  };

  @Test
  void websocketTypedApiMergedRoutesStateFactoryAndDisconnect() {
    var updates = new Subscription<>(String.class, String.class);
    var disconnects = new AtomicInteger();

    var merged = new Channel<ServerState>()
        .post("hello", String.class, request -> "merged");

    var serverChannel = new Channel<ServerState>()
        .post("state/check", Boolean.class, request -> request.state().headerWins())
        .post("api/hello", String.class, request -> "api")
        .post("mirror", String.class, request -> request.sender().send(String.class, "hello").requireValue())
        .post("payload/list", VOID, INT_LIST, VOID, request -> List.of(1, 2, 3))
        .postOther(
            path -> path.startsWith("other/post/"),
            VOID,
            TypeRef.of(String.class),
            VOID,
            request -> request.path()
        )
        .stream("api/numbers", Integer.class, request -> ChannelStreams.fromIterable(List.of(9)))
        .streamOther(
            path -> path.startsWith("other/stream/"),
            VOID,
            TypeRef.of(Integer.class),
            VOID,
            request -> ChannelStreams.fromIterable(List.of(7, 8))
        )
        .events(Map.of("updates", updates))
        .merge(merged, "merged");

    var listenOptions = new ListenOptions<>(
        headers -> new ServerState("header".equals(headers.get("name"))),
        null,
        (state, sender) -> disconnects.incrementAndGet()
    );

    try (var server = serverChannel.listen("127.0.0.1", 0, listenOptions)) {
      var clientChannel = new Channel<ClientState>()
          .post("hello", String.class, request -> "client world");

      var connectOptions = new ConnectOptions<>(new ClientState(), Map.of("name", "header"));
      try (var client = clientChannel.connect("ws://127.0.0.1:" + server.port() + "/?name=query", connectOptions)) {
        assertEquals(true, client.sender().send(Boolean.class, "state/check").requireValue());
        assertEquals("other/post/demo", client.sender().send(String.class, "other/post/demo").requireValue());
        assertEquals("api", client.sender().send(String.class, "api/hello").requireValue());
        assertEquals("merged", client.sender().send(String.class, "merged/hello").requireValue());
        assertEquals("client world", client.sender().send(String.class, "mirror").requireValue());

        var payload = client.sender().send(INT_LIST, "payload/list", null, null);
        assertTrue(payload.isOk());
        assertEquals(List.of(1, 2, 3), payload.requireValue());

        try (var otherValues = client.sender().values(Integer.class, "other/stream/demo")) {
          assertEquals(List.of(7, 8), TestSupport.readIntValues(otherValues, null));
        }

        try (var apiValues = client.sender().values(Integer.class, "api/numbers")) {
          assertEquals(List.of(9), TestSupport.readIntValues(apiValues, null));
        }

        var eventCount = new AtomicInteger();
        var subscription = client.sender().subscribe(String.class, "updates", "1", event -> {
          if ("hello".equals(event)) {
            eventCount.incrementAndGet();
          }
        });
        assertTrue(subscription.isOk());

        updates.send("1", "hello");
        TestSupport.waitFor(
            () -> eventCount.get() == 1,
            Duration.ofSeconds(2),
            "client did not receive websocket event",
            server::poll,
            client::poll
        );

        subscription.handle().cancel();
        updates.send("1", "ignored");
        for (int i = 0; i < 12; i += 1) {
          server.poll();
          client.poll();
        }
        assertEquals(1, eventCount.get());

        client.stop();
        TestSupport.waitFor(
            () -> disconnects.get() == 1,
            Duration.ofSeconds(2),
            "disconnect callback not observed",
            server::poll
        );
      }
    }
  }

  @Test
  void websocketClientHandlesSerialSendsWithoutWriterThread() {
    var serverChannel = new Channel<ServerState>()
        .post("echo", Integer.class, Integer.class, request -> request.body());

    try (var server = serverChannel.listen(0, new ListenOptions<>())) {
      try (var client = new Channel<ClientState>().connect("ws://127.0.0.1:" + server.port() + "/")) {
        for (int i = 0; i < 256; i += 1) {
          assertEquals(i, client.sender().send(Integer.class, "echo", i).requireValue());
        }
      }
    }
  }

  private record ServerState(boolean headerWins) {
  }

  private static final class ClientState {
  }
}
