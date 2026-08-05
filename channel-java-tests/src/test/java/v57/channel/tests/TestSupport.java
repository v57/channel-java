package v57.channel.tests;

import v57.channel.CallResult;
import v57.channel.StreamNext;
import v57.channel.Values;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TestSupport {
  private TestSupport() {
  }

  @FunctionalInterface
  interface Poller {
    boolean poll();
  }

  static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }

  static void waitFor(BooleanSupplier condition, Duration timeout, String message, Poller... pollers) {
    var deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      if (condition.getAsBoolean()) {
        return;
      }

      var progressed = false;
      for (var poller : pollers) {
        progressed = poller.poll() || progressed;
      }
      if (!progressed) {
        sleep(1);
      }
    }
    throw new AssertionError(message);
  }

  static <T> void assertRemoteErr(CallResult<T> result, String expected) {
    assertFalse(result.isOk());
    assertEquals(expected, result.remoteErr());
  }

  static List<Integer> readIntValues(Values<Integer> values, Integer breakOn) {
    var items = new ArrayList<Integer>();
    while (true) {
      StreamNext<Integer> next = values.next();
      if (next.isDone()) {
        return items;
      }
      assertTrue(next.isItem(), () -> "unexpected stream state: " + next.kind());
      items.add(next.item());
      if (breakOn != null && breakOn.equals(next.item())) {
        values.close();
        return items;
      }
    }
  }
}
