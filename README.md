<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Java implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Streaming values are supported out of the box.

# Installation

Gradle (multi-module, group `v57.channel`):

```kotlin
implementation("v57.channel:channel-java-core:VERSION")
implementation("v57.channel:channel-java-client:VERSION")
```

# Usage

```java
// Client
import v57.channel.Channel;
import v57.channel.client.ChannelClient;

var client = ChannelClient.connect(new Channel<Void>(), 2049);
String response = client.send(String.class, "hello", "hi").get();

try (var values = client.values(String.class, "stream/values")) {
  while (true) {
    var next = values.next().get();
    if (next.done()) break;
    System.out.println(next.value());
  }
}
```

```java
// Server
import v57.channel.Channel;
import v57.channel.server.ChannelServer;

var server = ChannelServer.listen(
    new Channel<Void>().post("hello", body -> "world"),
    2049
);
```

# Modules
- `channel-java-core`
- `channel-java-client`
- `channel-java-server`
- `channel-java-process`
- `channel-java-tests`
- `channel-java-bench`

# Stack
- JSON codec: `DSL-JSON`
- WebSocket client/server: `Netty`
- Build: `Gradle` multi-module, Java toolchain `21`

# Scripts
- `scripts/test` - runs full test suite (TS-server interop tests first, then Java-server tests)
- `scripts/benchmark` - runs process + Java WS + TS WS benchmarks
- `scripts/coverage` - runs tests and outputs LCOV report to `coverage/lcov.info`

# Quick run
```bash
./scripts/test
./scripts/benchmark
./scripts/coverage
```

# Environment
If Java is not configured on PATH, scripts default to:
- `/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`

Required tools:
- JDK 21
- Bun (for TS-server interop tests)
- lcov (for coverage output)

# Interop target
Integration tests use:
- `../channel/tests/server.ts`
- WebSocket endpoint: `127.0.0.1:2049`
