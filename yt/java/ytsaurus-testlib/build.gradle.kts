plugins {
    `java-library`
}

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(project(":yt:java:annotations"))
    api(project(":yt:java:ytsaurus-testlib:src:main:proto"))
    api("com.google.protobuf:protobuf-java:3.25.5")
    api("com.google.code.findbugs:jsr305:3.0.2")
    api("org.hamcrest:hamcrest-core:2.2")
    api("org.testcontainers:testcontainers:1.17.0")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


