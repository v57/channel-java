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
// Server: single-threaded, drive it with poll()
import v57.channel.Channel;

var server = new Channel<Void>()
    .post("hello", String.class, request -> "world")
    .listen(2049);

while (true) {
  if (!server.poll()) Thread.onSpinWait();
}
```

```java
// Client: send() and values().next() block and pump the connection themselves
import v57.channel.Channel;
import v57.channel.StreamNext;

try (var client = new Channel<Void>().connect(2049)) {
  var result = client.sender().send(String.class, "hello", "hi"); // CallResult<String>
  if (result.isOk()) System.out.println(result.value());
  else System.err.println(result.remoteErr());

  try (var values = client.sender().values(Integer.class, "stream/values")) {
    while (true) {
      StreamNext<Integer> next = values.next();
      if (!next.isItem()) break; // done or remoteErr
      System.out.println(next.item());
    }
  }
}
```

Limits: websocket messages (single or fragmented) above 16 MiB and HTTP upgrade heads above 64 KiB drop the
connection. Change the message cap with `new ListenOptions<>(...).withMaxPayload(bytes)` /
`new ConnectOptions<>(...).withMaxPayload(bytes)`. A peer that sends a malformed frame, invalid JSON or JSON nested
deeper than 512 levels is disconnected; on the client, pending calls then fail with `disconnected`.

# Modules
- `channel-java-core`
- `channel-java-client`
- `channel-java-server`
- `channel-java-process`
- `channel-java-tests`
- `channel-java-bench`

# Stack
- JSON codec: `DSL-JSON`
- WebSocket client/server: hand-written on `java.nio` (no Netty)
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
