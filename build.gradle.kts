plugins {
  base
}

allprojects {
  group = "v57.channel"
  version = "0.1.0"

  repositories {
    mavenCentral()
  }
}

subprojects {
  plugins.withId("java") {
    extensions.configure<JavaPluginExtension> {
      toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
      }
    }

    tasks.withType<Test>().configureEach {
      useJUnitPlatform()
      testLogging {
        events("failed", "skipped")
      }
    }

    dependencies {
      "testImplementation"(platform("org.junit:junit-bom:5.10.2"))
      "testImplementation"("org.junit.jupiter:junit-jupiter")
    }
  }
}
