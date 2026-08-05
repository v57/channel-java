package v57.channel.tests;

import org.junit.jupiter.api.Test;
import v57.channel.CallResult;
import v57.channel.Channel;
import v57.channel.ChannelStream;
import v57.channel.ChannelStreams;
import v57.channel.StreamNext;
import v57.channel.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CTsInteropTest {
  @Test
  void javaClientInteroperatesWithTsFixtureServer() {
    try (var fixture = new TsServerFixture()) {
      fixture.start();

      var names = new Subscription<>(String.class, String.class);
      var valuesSent = new AtomicInteger();

      var channel = new Channel<Void>()
          .post("hello", String.class, request -> "client world")
          .stream("stream/values", Integer.class, request -> ChannelStreams.fromIterable(List.of(0, 1, 2)))
          .stream("stream/cancel", Integer.class, request -> new ChannelStream<>() {
            private int next;
            private boolean closed;

            @Override
            public StreamNext<Integer> next() {
              if (closed || next >= 10) {
                return StreamNext.done();
              }
              valuesSent.incrementAndGet();
              return StreamNext.item(next++);
            }

            @Override
            public void close() {
              closed = true;
            }
          })
          .events(Map.of("names", names));

      try (var client = channel.connect("ws://127.0.0.1:2049/")) {
        assertEquals("world", client.sender().send(String.class, "hello").requireValue());
        assertEquals(0.1337, client.sender().send(Double.class, "echo", 0.1337).requireValue(), 0.000001);

        CallResult<Void> empty = client.sender().send(Void.class, "empty");
        assertTrue(empty.isOk());
        assertNull(empty.requireValue());

        TestSupport.assertRemoteErr(client.sender().send(String.class, "notfound"), "api not found");

        var missingSubscription = client.sender().subscribe(String.class, "notfound", "1", event -> {
        });
        assertTrue(!missingSubscription.isOk());
        assertEquals("subscription not found", missingSubscription.remoteErr());

        try (var values = client.sender().values(Integer.class, "stream/values")) {
          assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, null));
        }

        assertEquals("client world", client.sender().send(String.class, "mirror").requireValue());

        CallResult<Void> mirrorEvents = client.sender().send(Void.class, "mirror/events");
        assertTrue(mirrorEvents.isOk());
        assertNull(mirrorEvents.requireValue());

        TestSupport.waitFor(
            () -> client.isRemoteSubscribed("names/1"),
            Duration.ofSeconds(2),
            "ts server never subscribed to client events",
            client::poll
        );
        names.send("1", "hello");
        TestSupport.waitFor(
            () -> !client.isRemoteSubscribed("names/1"),
            Duration.ofSeconds(2),
            "ts server never unsubscribed from client events",
            client::poll
        );

        try (var values = client.sender().values(Integer.class, "mirror/stream")) {
          assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, null));
        }

        try (var values = client.sender().values(Integer.class, "mirror/stream/cancel")) {
          assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, 2));
        }
        assertTrue(valuesSent.get() >= 3);

        try (var authClient = new Channel<Void>().connect("ws://127.0.0.1:2049/")) {
          assertEquals("tester", authClient.sender().send(String.class, "auth", Map.of("name", "tester")).requireValue());
          assertEquals("tester", authClient.sender().send(String.class, "auth/name").requireValue());
        }

        try (var authClient = new Channel<Void>().connect("ws://127.0.0.1:2049/")) {
          TestSupport.assertRemoteErr(authClient.sender().send(String.class, "auth/name"), "unauthorized");
        }
      }
    }
  }
}
