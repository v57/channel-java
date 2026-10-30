plugins {
  `java-library`
}

dependencies {
  api(project(":channel-java-core"))
  // Pure-Java QUIC (client and server) with its own TLS 1.3 stack (agent15): no native code, no Netty
  implementation("tech.kwik:kwik:0.12.1")
  // kwik's TLS engine, used directly to turn off session resumption (and so 0-RTT) on the server
  implementation("tech.kwik:agent15:4.0")

  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
