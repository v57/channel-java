package v57.channel.bench;

import v57.channel.JsonCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class BenchmarkMain {
  private static final String HOST = "127.0.0.1";
  private static final int[] STAGES = new int[]{1, 10, 100, 1000};
  private static final int DEFAULT_STAGE_MS = 5000;
  private static final int DEFAULT_RPS_PER_CLIENT = 1000;

  private BenchmarkMain() {
  }

  public static void main(String[] args) throws Exception {
    var stageMs = intEnv("BENCH_STAGE_MS", DEFAULT_STAGE_MS);
    var rpsPerClient = intEnv("BENCH_RPS_PER_CLIENT", DEFAULT_RPS_PER_CLIENT);

    var results = new ArrayList<Result>();

    for (var clients : STAGES) {
      ManagedProcess server = null;
      ManagedProcess client = null;

      try {
        var port = randomPort();

        server = ManagedProcess.start(
            "server",
            BenchmarkServerMain.class.getName(),
            Map.of("BENCH_PORT", Integer.toString(port))
        );

        client = ManagedProcess.start(
            "client",
            BenchmarkClientMain.class.getName(),
            Map.of(
                "BENCH_HOST", HOST,
                "BENCH_PORT", Integer.toString(port),
                "BENCH_CLIENTS", Integer.toString(clients),
                "BENCH_RPS_PER_CLIENT", Integer.toString(rpsPerClient)
            )
        );

        server.awaitJsonType("ready", Duration.ofSeconds(30));
        client.awaitJsonType("ready", Duration.ofSeconds(30));

        Thread.sleep(stageMs);

        var clientUsage = Usage.sample(client.process().pid());
        var serverUsage = Usage.sample(server.process().pid());

        client.sigterm();
        server.sigterm();

        var serverResult = server.awaitJsonType("result", Duration.ofSeconds(30));
        var requests = asLong(serverResult.get("requests"));
        var rps = round(requests / (stageMs / 1000.0));

        results.add(new Result(rps, clients, clientUsage, serverUsage));

        client.awaitExit(Duration.ofSeconds(30));
        server.awaitExit(Duration.ofSeconds(30));

        System.out.printf(
            "clients=%d rps=%.2f client(cpu=%.2f mem=%.2f) server(cpu=%.2f mem=%.2f)%n",
            clients,
            rps,
            clientUsage.cpu(),
            clientUsage.mem(),
            serverUsage.cpu(),
            serverUsage.mem()
        );
      } finally {
        stopQuietly(client);
        stopQuietly(server);
      }
    }

    var out = writeJson(results);
    System.out.println("benchmark written to " + out.toAbsolutePath());
  }

  private static Path writeJson(List<Result> results) throws IOException {
    var root = new LinkedHashMap<String, Object>();
    root.put("language", "java");

    var list = new ArrayList<Map<String, Object>>();
    for (var row : results) {
      var item = new LinkedHashMap<String, Object>();
      item.put("rps", row.rps());
      item.put("clients", row.clients());

      var client = new LinkedHashMap<String, Object>();
      client.put("cpu", row.client().cpu());
      client.put("mem", row.client().mem());
      item.put("client", client);

      var server = new LinkedHashMap<String, Object>();
      server.put("cpu", row.server().cpu());
      server.put("mem", row.server().mem());
      item.put("server", server);

      list.add(item);
    }

    root.put("results", list);

    var output = resolveOutputPath();
    Files.writeString(output, JsonCodec.encode(root), StandardCharsets.UTF_8);
    return output;
  }

  private static Path resolveOutputPath() {
    var cwd = Path.of("").toAbsolutePath().normalize();
    var fileName = cwd.getFileName();
    if (fileName != null && "channel-java-bench".equals(fileName.toString()) && cwd.getParent() != null) {
      return cwd.getParent().resolve("benchmark.json");
    }
    return cwd.resolve("benchmark.json");
  }

  private static int randomPort() throws IOException {
    try (var socket = new java.net.ServerSocket()) {
      socket.bind(new java.net.InetSocketAddress(HOST, 0));
      return socket.getLocalPort();
    }
  }

  private static long asLong(Object value) {
    if (value instanceof Integer i) {
      return i.longValue();
    }
    if (value instanceof Long l) {
      return l;
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    if (value instanceof String s) {
      return Long.parseLong(s);
    }
    throw new IllegalStateException("invalid numeric value: " + value);
  }

  private static double round(double value) {
    return Math.round(value * 100.0) / 100.0;
  }

  private static int intEnv(String key, int fallback) {
    var value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return Integer.parseInt(value.trim());
  }

  private static void stopQuietly(ManagedProcess process) {
    if (process == null) {
      return;
    }
    try {
      process.forceStop();
    } catch (Throwable ignored) {
    }
  }

  private record Usage(double cpu, double mem) {
    static Usage sample(long pid) {
      try {
        var process = new ProcessBuilder(
            "ps",
            "-p",
            Long.toString(pid),
            "-o",
            "%cpu=",
            "-o",
            "rss="
        ).start();

        try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          var line = reader.readLine();
          process.waitFor(2, TimeUnit.SECONDS);
          if (line == null || line.isBlank()) {
            return new Usage(0, 0);
          }
          var parts = line.trim().split("\\s+");
          if (parts.length < 2) {
            return new Usage(0, 0);
          }

          var cpu = Double.parseDouble(parts[0]);
          var rssKb = Double.parseDouble(parts[1]);
          return new Usage(round(cpu), round(rssKb / 1024.0));
        }
      } catch (Throwable ignored) {
        return new Usage(0, 0);
      }
    }
  }

  private record Result(double rps, int clients, Usage client, Usage server) {
  }

  private static final class ManagedProcess {
    private final String name;
    private final Process process;
    private final LinkedBlockingQueue<String> stdoutLines = new LinkedBlockingQueue<>();

    private ManagedProcess(String name, Process process) {
      this.name = name;
      this.process = process;

      Thread.ofVirtual().name("bench-" + name + "-stdout").start(() -> {
        try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            stdoutLines.offer(line);
          }
        } catch (IOException ignored) {
        }
      });

      Thread.ofVirtual().name("bench-" + name + "-stderr").start(() -> {
        try (var reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.err.println("[" + name + "] " + line);
          }
        } catch (IOException ignored) {
        }
      });
    }

    static ManagedProcess start(String name, String mainClass, Map<String, String> env) throws IOException {
      var javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
      var classPath = System.getProperty("java.class.path");

      var command = new ArrayList<String>();
      command.add(javaBinary);
      command.add("-cp");
      command.add(classPath);
      command.add(mainClass);

      var builder = new ProcessBuilder(command);
      builder.environment().putAll(env);

      return new ManagedProcess(name, builder.start());
    }

    Process process() {
      return process;
    }

    void sigterm() {
      if (process.isAlive()) {
        try {
          var killer = new ProcessBuilder("kill", "-TERM", Long.toString(process.pid())).start();
          killer.waitFor(2, TimeUnit.SECONDS);
        } catch (Throwable ignored) {
          process.destroy();
        }
      }
    }

    Map<String, Object> awaitJsonType(String type, Duration timeout) throws InterruptedException {
      var deadline = System.nanoTime() + timeout.toNanos();

      while (System.nanoTime() < deadline) {
        var remaining = deadline - System.nanoTime();
        var line = stdoutLines.poll(Math.max(1, remaining), TimeUnit.NANOSECONDS);
        if (line == null) {
          continue;
        }

        var decoded = JsonCodec.decode(line);
        if (!(decoded instanceof Map<?, ?> map)) {
          continue;
        }

        var eventType = map.get("type");
        if (type.equals(String.valueOf(eventType))) {
          @SuppressWarnings("unchecked")
          var cast = (Map<String, Object>) map;
          return cast;
        }
      }

      var alive = process.isAlive();
      var exit = alive ? "alive" : ("exit=" + process.exitValue());
      var leftovers = new ArrayList<String>();
      stdoutLines.drainTo(leftovers);
      throw new IllegalStateException(
          name + " timeout waiting for event type: " + type + " (" + exit + ") leftovers=" + leftovers
      );
    }

    void awaitExit(Duration timeout) throws InterruptedException {
      if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException(name + " did not exit in time");
      }
    }

    void forceStop() throws InterruptedException {
      if (!process.isAlive()) {
        return;
      }

      process.destroy();
      if (!process.waitFor(2, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor(2, TimeUnit.SECONDS);
      }
    }
  }
}
