plugins {
  `java-library`
}

tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs.add("-Adsljson.unknown=ALLOW")
}

dependencies {
  api("com.dslplatform:dsl-json:1.10.0")
  implementation("com.dslplatform:dsl-json-java8:1.10.0")
  implementation("org.slf4j:slf4j-api:2.0.17")
  annotationProcessor("com.dslplatform:dsl-json-java8:1.10.0")
}
