package v57.channel.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class TsServerFixture implements AutoCloseable {
  private static final File ROOT = resolveChannelRoot();

  private static File resolveChannelRoot() {
    var configured = System.getenv("CHANNEL_TS_ROOT");
    if (configured != null && !configured.isBlank()) {
      return new File(configured);
    }

    // Prefer the sibling of this repo (monorepo layout), falling back to the
    // historical absolute path used by earlier checkouts.
    var fromRepo = new File(System.getProperty("user.dir")).getParentFile();
    var candidates = new File[] {
      fromRepo == null ? null : new File(fromRepo, "channel"),
      new File("/Users/v57/github/channels/channel"),
      new File("/Users/dimas/github/hub/channels/channel"),
    };
    for (var candidate : candidates) {
      if (candidate != null && new File(candidate, "tests/server.ts").isFile()) {
        return candidate;
      }
    }
    return new File("/Users/dimas/github/hub/channels/channel");
  }

  private Process process;
  private final StringBuilder logs = new StringBuilder();
  private final AtomicBoolean running = new AtomicBoolean(false);

  void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    try {
      var bun = resolveBun();
      var builder = new ProcessBuilder(bun, "tests/server.ts");
      builder.directory(ROOT);
      builder.redirectErrorStream(true);
      process = builder.start();

      Thread.ofVirtual().name("ts-server-log").start(() -> {
        try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            synchronized (logs) {
              logs.append(line).append('\n');
            }
          }
        } catch (Throwable ignored) {
        }
      });

      waitForPort("127.0.0.1", 2049, Duration.ofSeconds(20));
    } catch (Throwable error) {
      close();
      throw new RuntimeException("failed to start tests/server.ts\n" + logs(), error);
    }
  }

  private String resolveBun() throws Exception {
    var configured = System.getenv("BUN");
    if (configured != null && !configured.isBlank()) {
      return configured;
    }

    var probe = new ProcessBuilder("/bin/zsh", "-lc", "command -v bun").start();
    if (!probe.waitFor(5, TimeUnit.SECONDS) || probe.exitValue() != 0) {
      throw new IllegalStateException("bun was not found on PATH");
    }
    try (var reader = new BufferedReader(new InputStreamReader(probe.getInputStream(), StandardCharsets.UTF_8))) {
      var line = reader.readLine();
      if (line == null || line.isBlank()) {
        throw new IllegalStateException("bun was not found on PATH");
      }
      return line.trim();
    }
  }

  private void waitForPort(String host, int port, Duration timeout) {
    TestSupport.waitFor(() -> {
      try (var socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), 100);
        return true;
      } catch (Exception ignored) {
        return false;
      }
    }, timeout, "timed out waiting for ts server on " + host + ":" + port + "\n" + logs());
  }

  private String logs() {
    synchronized (logs) {
      return logs.toString();
    }
  }

  @Override
  public void close() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    if (process == null) {
      return;
    }

    process.destroy();
    try {
      if (!process.waitFor(3, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor(2, TimeUnit.SECONDS);
      }
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }
}
