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

# Dependencies

[dsl-json](https://github.com/ngs-doo/dsl-json) (`dsl-json`, `dsl-json-java8`) for JSON. The WebSocket client and
server are written on Java NIO. The core module also declares `slf4j-api`, which the code doesn't use yet.

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

# QUIC transport (experimental)

Channel over QUIC, as in the TypeScript reference (`channel/docs/quic.md`): the same messages over one QUIC stream,
TLS always on, WebSocket stays the default. It lives in its own module, `channel-java-quic`, so only programs that add
it depend on [kwik](https://github.com/ptrd/kwik) (pure Java QUIC with its own TLS 1.3, LGPL-3.0); `channel-java-core`,
`-client` and `-server` don't.

```kotlin
implementation("v57.channel:channel-java-quic:VERSION")
```

```java
// Server: PEM certificate chain and unencrypted PKCS#8 key (BEGIN PRIVATE KEY), plus the WebSocket server's options
var server = new Channel<Map<String, String>>()
    .post("hello", String.class, request -> "world")
    .listenQuic(4433, new ListenOptions<Map<String, String>>(headers -> headers, null, null).withCertificate(certPem, keyPem));
// listenQuic("0.0.0.0", 4433, options) for other interfaces; drive it with server.poll() like listen()

// Client: quic://host:port goes to the QUIC module, ws:// to the WebSocket one
var client = new Channel<Void>().connect("quic://127.0.0.1:4433",
    new ConnectOptions<Void>(null, Map.of("x-name", "java")).withQuic(new QuicOptions().withCa(certPem)));
client.sender().send(String.class, "hello");
```

Certificates (`QuicOptions`):
- nothing: the server certificate must be trusted by the JVM's default trust store;
- `withCa(pem)`: trust this self-signed certificate or CA instead;
- `withFingerprint("DD:D2:37:…")`: accept only the certificate with this SHA-256 fingerprint (hex, colons optional) and
  skip the other checks;
- `withServerName(name)`: the name sent and checked instead of the address host.

The address host (a DNS name or an IP address) must be in the certificate's subject alternative names. The client
dials each connection from its own UDP socket. The headers go in the first frame (`{"hello": {...}}`) and the server
uses them, lowercased, like the WebSocket upgrade headers. Frames above `withMaxPayload` (16 MiB) close the connection;
idle connections close after 30 s, and the client sends a keepalive every 15 s. Like the WebSocket client, the QUIC
client doesn't reconnect: when the connection drops, pending calls fail with `disconnected`. 0-RTT is off: the server
issues no session tickets. Which languages support QUIC: see `channel/docs/quic.md`.

# Modules
- `channel-java-core`
- `channel-java-client`
- `channel-java-server`
- `channel-java-process`
- `channel-java-quic` (optional QUIC client and server)
- `channel-java-tests`
- `channel-java-bench`

# Stack
- JSON codec: `DSL-JSON`
- WebSocket client/server: hand-written on `java.nio` (no Netty)
- Build: `Gradle` multi-module, Java toolchain `21`

# Scripts
- `scripts/test` - runs full test suite (TS-server interop tests first, then Java-server tests, then QUIC). The QUIC
  interop test calls the TS fixture on `CHANNEL_TEST_PORT` (default 2105) and skips when it isn't running there
- `scripts/benchmark` - runs process + Java WS + TS WS benchmarks
- `scripts/coverage` - runs tests and outputs LCOV report to `coverage/lcov.info`

# Quick run
```bash
./scripts/test
./scripts/benchmark
./scripts/coverage
```

# Environment
In the Hub checkout the scripts run in its toolchain container (`../../HubTools/hub-run`), which has
all of these. Elsewhere they use `JAVA_HOME`, or `java` on PATH.

Required tools:
- JDK 21
- Bun (for TS-server interop tests)
- lcov (for coverage output)

# Interop target
Integration tests use:
- `../channel/tests/server.ts`
- WebSocket endpoint: `127.0.0.1:2049`
- QUIC endpoint: UDP `127.0.0.1:$CHANNEL_TEST_PORT` (default 2105), started with
  `CHANNEL_TEST_PORT=2105 bun tests/server.ts`
