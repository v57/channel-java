package v57.channel.bench;

import sun.misc.Signal;
import v57.channel.Channel;
import v57.channel.ChannelClient;
import v57.channel.JsonCodec;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class BenchmarkClientMain {
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final Map<String, String> TEXT_COUNT_BODY = Map.of("text", "hello");

  private final String host;
  private final int port;
  private final int clientsCount;
  private final int rpsPerClient;
  private final long totalRequests;
  private final ArrayList<ChannelClient<Void>> clients = new ArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicBoolean emitted = new AtomicBoolean(false);

  private long completed;
  private long failed;

  private BenchmarkClientMain(String host, int port, int clientsCount, int rpsPerClient, long totalRequests) {
    this.host = host;
    this.port = port;
    this.clientsCount = clientsCount;
    this.rpsPerClient = rpsPerClient;
    this.totalRequests = totalRequests;
  }

  public static void main(String[] args) {
    var host = envOrDefault("BENCH_HOST", "127.0.0.1");
    var port = requiredIntEnv("BENCH_PORT");
    var clients = Math.max(1, requiredIntEnv("BENCH_CLIENTS"));
    var totalRequests = optionalLongEnv("BENCH_TOTAL_REQUESTS", -1);
    var rpsPerClient = totalRequests >= 0 ? 0 : Math.max(1, requiredIntEnv("BENCH_RPS_PER_CLIENT"));

    var app = new BenchmarkClientMain(host, port, clients, rpsPerClient, totalRequests);
    app.run();
  }

  private void run() {
    installTermHandler();
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownAndEmit));

    var address = "ws://" + host + ":" + port + "/";
    try {
      for (int i = 0; i < clientsCount; i += 1) {
        clients.add(new Channel<Void>().connect(address));
      }

      waitUntilConnected();
      emit(Map.of(
          "type", "ready",
          "pid", ProcessHandle.current().pid(),
          "host", host,
          "port", port,
          "clients", clientsCount
      ));

      var startedAt = System.nanoTime();
      if (totalRequests >= 0) {
        runFixedRequests();
      } else {
        runRateLimited();
      }

      var elapsedSeconds = Math.max(0.000001, (System.nanoTime() - startedAt) / 1_000_000_000.0);
      emitResult(elapsedSeconds);
    } finally {
      closeClients();
    }
  }

  private void waitUntilConnected() {
    var deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CONNECT_TIMEOUT_MS);
    while (System.nanoTime() < deadline) {
      pumpClients();
      if (connectedClients() == clients.size()) {
        return;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    throw new IllegalStateException("timeout waiting for benchmark clients to connect");
  }

  private void runFixedRequests() {
    var index = 0;
    while (running.get() && completed + failed < totalRequests) {
      dispatchOne(index);
      index = (index + 1) % clients.size();
    }
  }

  private void runRateLimited() {
    var totalRps = (long) clientsCount * rpsPerClient;
    var intervalNanos = Math.max(1L, 1_000_000_000L / Math.max(1L, totalRps));
    var nextSendAt = System.nanoTime();
    var index = 0;

    while (running.get()) {
      dispatchOne(index);
      index = (index + 1) % clients.size();
      nextSendAt += intervalNanos;

      var sleepNanos = nextSendAt - System.nanoTime();
      if (sleepNanos > 0) {
        LockSupport.parkNanos(sleepNanos);
      }
    }
  }

  private void dispatchOne(int clientIndex) {
    var result = clients.get(clientIndex).sender().send(Integer.class, "text/count", TEXT_COUNT_BODY);
    if (result.isOk()) {
      completed += 1;
    } else {
      failed += 1;
    }
  }

  private boolean pumpClients() {
    var progressed = false;
    for (var client : clients) {
      progressed = client.poll() || progressed;
    }
    return progressed;
  }

  private long connectedClients() {
    return clients.stream().filter(ChannelClient::isConnected).count();
  }

  private void closeClients() {
    for (var client : clients) {
      try {
        client.stop();
      } catch (Throwable ignored) {
      }
    }
    clients.clear();
  }

  private void shutdownAndEmit() {
    running.set(false);
    emitResult(0);
  }

  private void emitResult(double elapsedSeconds) {
    if (!emitted.compareAndSet(false, true)) {
      return;
    }
    emit(Map.of(
        "type", "result",
        "completed", completed,
        "responsesReceived", completed,
        "requestsSent", completed + failed,
        "failed", failed,
        "elapsedSeconds", elapsedSeconds
    ));
  }

  private void installTermHandler() {
    try {
      Signal.handle(new Signal("TERM"), signal -> {
        shutdownAndEmit();
        System.exit(0);
      });
    } catch (Throwable ignored) {
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

  private static String envOrDefault(String key, String fallback) {
    var value = System.getenv(key);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  private static int requiredIntEnv(String key) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("missing env: " + key);
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
}
