package v57.channel.bench;

import v57.channel.Channel;
import v57.channel.JsonCodec;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BenchmarkProcessMain {
  private BenchmarkProcessMain() {
  }

  public static void main(String[] args) {
    var totalRequests = optionalLongEnv("BENCH_TOTAL_REQUESTS", -1);
    var durationMs = totalRequests >= 0 ? 0 : Math.max(1, optionalIntEnv("BENCH_DURATION_MS", 1_000));
    var running = new AtomicBoolean(true);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

    var channel = new Channel<Void>()
        .post("count", Integer.class, request -> 1);

    try (var process = channel.process(null)) {
      emit(Map.of("type", "ready", "pid", ProcessHandle.current().pid()));

      var completed = 0L;
      var errors = 0L;
      var startedAt = System.nanoTime();

      if (totalRequests >= 0) {
        while (running.get() && completed + errors < totalRequests) {
          var result = process.sender().send(Integer.class, "count");
          if (result.isOk()) {
            completed += 1;
          } else {
            errors += 1;
          }
        }
      } else {
        var deadline = startedAt + durationMs * 1_000_000L;
        while (running.get() && System.nanoTime() < deadline) {
          var result = process.sender().send(Integer.class, "count");
          if (result.isOk()) {
            completed += 1;
          } else {
            errors += 1;
          }
        }
      }

      var elapsedSeconds = Math.max(0.000001, (System.nanoTime() - startedAt) / 1_000_000_000.0);
      emit(Map.of(
          "type", "result",
          "completed", completed,
          "errors", errors,
          "elapsedSeconds", elapsedSeconds
      ));
    }
  }

  private static void emit(Map<String, ?> payload) {
    try {
      var out = new FileOutputStream(FileDescriptor.out);
      out.write((JsonCodec.encode(payload) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (Throwable ignored) {
    }
  }

  private static long optionalLongEnv(String key, long fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Long.parseLong(value.trim());
  }

  private static int optionalIntEnv(String key, int fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Integer.parseInt(value.trim());
  }
}
