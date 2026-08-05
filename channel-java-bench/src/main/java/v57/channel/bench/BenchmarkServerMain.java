package v57.channel.bench;

import sun.misc.Signal;
import v57.channel.Channel;
import v57.channel.ChannelServer;
import v57.channel.JsonCodec;
import v57.channel.ListenOptions;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class BenchmarkServerMain {
  private static final boolean DEFAULT_WARMUP_ENABLED = true;
  private static final long DEFAULT_WARMUP_REQUESTS = 50_000;
  private static final int DEFAULT_WARMUP_CLIENTS = 2;
  private static final int DEFAULT_WARMUP_ROUNDS = 1;

  private static final AtomicLong REQUESTS = new AtomicLong();
  private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
  private static final AtomicBoolean EMITTED = new AtomicBoolean(false);
  private static volatile ChannelServer<Void> SERVER;

  private BenchmarkServerMain() {
  }

  public static void main(String[] args) {
    var port = requiredIntEnv("BENCH_PORT");

    installTermHandler();
    Runtime.getRuntime().addShutdownHook(new Thread(BenchmarkServerMain::shutdownAndEmit));

    try {
      var channel = new Channel<Void>()
          .post("text/count", Object.class, Integer.class, request -> {
            REQUESTS.incrementAndGet();
            return textCount(request.body());
          });

      SERVER = channel.listen("127.0.0.1", port, new ListenOptions<>());
      runWarmupIfEnabled(port);
      REQUESTS.set(0);
      emit(Map.of("type", "ready", "pid", ProcessHandle.current().pid(), "port", port));

      while (RUNNING.get()) {
        if (SERVER == null || !SERVER.poll()) {
          LockSupport.parkNanos(100_000L);
        }
      }
    } finally {
      shutdownAndEmit();
    }
  }

  private static int textCount(Object body) {
    if (body instanceof Map<?, ?> payload) {
      var text = payload.get("text");
      if (text instanceof String value) {
        return value.length();
      }
    }
    return 0;
  }

  private static void runWarmupIfEnabled(int port) {
    if (!optionalBooleanEnv("BENCH_WARMUP_ENABLED", DEFAULT_WARMUP_ENABLED)) {
      return;
    }

    var totalRequests = optionalLongEnv("BENCH_WARMUP_REQUESTS", DEFAULT_WARMUP_REQUESTS);
    var clientsCount = optionalIntEnv("BENCH_WARMUP_CLIENTS", DEFAULT_WARMUP_CLIENTS);
    var rounds = optionalIntEnv("BENCH_WARMUP_ROUNDS", DEFAULT_WARMUP_ROUNDS);
    if (totalRequests <= 0 || clientsCount <= 0 || rounds <= 0) {
      return;
    }

    var javaBinary = Path.of(
        System.getProperty("java.home"),
        "bin",
        System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java"
    ).toString();
    var classPath = System.getProperty("java.class.path");

    for (int i = 0; i < rounds; i += 1) {
      Process process = null;
      try {
        var builder = new ProcessBuilder(
            javaBinary,
            "-cp",
            classPath,
            BenchmarkClientMain.class.getName()
        );
        var env = builder.environment();
        env.put("BENCH_HOST", "127.0.0.1");
        env.put("BENCH_PORT", Integer.toString(port));
        env.put("BENCH_CLIENTS", Integer.toString(clientsCount));
        env.put("BENCH_TOTAL_REQUESTS", Long.toString(totalRequests));
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);

        process = builder.start();
        if (!process.waitFor(90, TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (Throwable ignored) {
        if (process != null) {
          try {
            process.destroyForcibly();
          } catch (Throwable ignoredDestroy) {
          }
        }
      }
    }
  }

  private static void installTermHandler() {
    try {
      Signal.handle(new Signal("TERM"), signal -> {
        shutdownAndEmit();
        System.exit(0);
      });
    } catch (Throwable ignored) {
    }
  }

  private static void shutdownAndEmit() {
    RUNNING.set(false);

    var local = SERVER;
    SERVER = null;
    if (local != null) {
      try {
        local.stop();
      } catch (Throwable ignored) {
      }
    }

    if (!EMITTED.compareAndSet(false, true)) {
      return;
    }
    emit(Map.of("type", "result", "requests", REQUESTS.get()));
  }

  private static void emit(Map<String, ?> payload) {
    try {
      var out = new FileOutputStream(FileDescriptor.out);
      out.write((JsonCodec.encode(payload) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (Throwable ignored) {
    }
  }

  private static int requiredIntEnv(String key) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("missing env: " + key);
    }
    return Integer.parseInt(value.trim());
  }

  private static int optionalIntEnv(String key, int fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Integer.parseInt(value.trim());
  }

  private static long optionalLongEnv(String key, long fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Long.parseLong(value.trim());
  }

  private static boolean optionalBooleanEnv(String key, boolean fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return switch (value.trim().toLowerCase()) {
      case "1", "true", "yes", "on" -> true;
      case "0", "false", "no", "off" -> false;
      default -> fallback;
    };
  }
}
