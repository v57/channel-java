plugins {
  application
}

dependencies {
  implementation(project(":channel-java-core"))
  implementation(project(":channel-java-client"))
  implementation(project(":channel-java-server"))
  implementation(project(":channel-java-process"))
}

application {
  mainClass.set("v57.channel.bench.BenchmarkMain")
}
