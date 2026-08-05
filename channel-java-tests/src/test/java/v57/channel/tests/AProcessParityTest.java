package v57.channel.tests;

import org.junit.jupiter.api.Test;
import v57.channel.CallResult;
import v57.channel.Channel;
import v57.channel.ChannelStream;
import v57.channel.ChannelStreams;
import v57.channel.StreamNext;
import v57.channel.Subscription;
import v57.channel.TypeRef;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AProcessParityTest {
  private static final TypeRef<Void> VOID = TypeRef.of(Void.class);
  private static final TypeRef<List<Integer>> INT_LIST = new TypeRef<>() {
  };

  @Test
  void processSendSuccessErrorTypedAndGeneric() {
    var channel = new Channel<TestState>()
        .post("hello", String.class, request -> "world")
        .post("echo", Integer.class, Integer.class, request -> request.body())
        .post("fail", String.class, request -> {
          throw new IllegalStateException("failed");
        })
        .post("payload/list", VOID, INT_LIST, VOID, request -> List.of(1, 2, 3))
        .stream("stream/values", Integer.class, request -> ChannelStreams.fromIterable(List.of(0, 1, 2)));

    try (var process = channel.process(new TestState())) {
      CallResult<String> hello = process.sender().send(String.class, "hello");
      assertTrue(hello.isOk());
      assertEquals("world", hello.requireValue());

      CallResult<Integer> echo = process.sender().send(Integer.class, "echo", 42);
      assertTrue(echo.isOk());
      assertEquals(42, echo.requireValue());

      TestSupport.assertRemoteErr(process.sender().send(String.class, "fail"), "failed");

      var payload = process.sender().send(INT_LIST, "payload/list", null, null);
      assertTrue(payload.isOk());
      assertEquals(List.of(1, 2, 3), payload.requireValue());

      try (var values = process.sender().values(Integer.class, "stream/values")) {
        assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, null));
      }
    }
  }

  @Test
  void processSubscriptionCancelAndStreamCancel() {
    var events = new Subscription<>(String.class, String.class);
    var streamValuesSent = new AtomicInteger();

    var channel = new Channel<TestState>()
        .events(Map.of("updates", events))
        .stream("stream/cancel", Integer.class, request -> new ChannelStream<>() {
          private int next;
          private boolean closed;

          @Override
          public StreamNext<Integer> next() {
            if (closed || next >= 10) {
              return StreamNext.done();
            }
            streamValuesSent.incrementAndGet();
            return StreamNext.item(next++);
          }

          @Override
          public void close() {
            closed = true;
          }
        });

    try (var process = channel.process(new TestState())) {
      var count = new AtomicInteger();
      var subscribed = process.sender().subscribe(String.class, "updates", "1", event -> {
        if (String.valueOf(event).contains("event")) {
          count.incrementAndGet();
        }
      });

      assertTrue(subscribed.isOk());
      TestSupport.waitFor(
          () -> process.isRemoteSubscribed("updates/1"),
          Duration.ofSeconds(1),
          "process never registered remote topic",
          process::poll
      );

      events.send("1", "event 1");
      TestSupport.waitFor(() -> count.get() == 1, Duration.ofSeconds(1), "expected process event", process::poll);

      subscribed.handle().cancel();
      TestSupport.waitFor(
          () -> !process.isRemoteSubscribed("updates/1"),
          Duration.ofSeconds(1),
          "process never removed remote topic",
          process::poll
      );

      events.send("1", "event 2");
      for (int i = 0; i < 8; i += 1) {
        process.poll();
      }
      assertEquals(1, count.get());

      try (var values = process.sender().values(Integer.class, "stream/cancel")) {
        assertEquals(List.of(0, 1, 2), TestSupport.readIntValues(values, 2));
      }
      assertEquals(10, streamValuesSent.get());
    }
  }

  private static final class TestState {
  }
}
