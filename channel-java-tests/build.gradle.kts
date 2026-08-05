plugins {
  `java-library`
  jacoco
}

dependencies {
  implementation(project(":channel-java-core"))
  implementation(project(":channel-java-client"))
  implementation(project(":channel-java-server"))
  implementation(project(":channel-java-process"))

  testImplementation(platform("org.junit:junit-bom:5.10.2"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation("org.junit.platform:junit-platform-suite:1.10.2")
}

val coveredProjects = listOf(
  project(":channel-java-core"),
  project(":channel-java-client"),
  project(":channel-java-server"),
  project(":channel-java-process"),
)

tasks.test {
  useJUnitPlatform()
  finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
  dependsOn(tasks.test)

  reports {
    xml.required.set(true)
    csv.required.set(false)
    html.required.set(true)
  }

  classDirectories.setFrom(
    coveredProjects.map {
      it.layout.buildDirectory.dir("classes/java/main")
    },
  )

  sourceDirectories.setFrom(
    coveredProjects.map {
      it.layout.projectDirectory.dir("src/main/java")
    },
  )

  executionData.setFrom(layout.buildDirectory.file("jacoco/test.exec"))
}
