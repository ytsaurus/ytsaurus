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
    api("javax.persistence:persistence-api:1.0")
    implementation(project(":yt:java:ytsaurus-testlib")) {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "junit", module = "junit")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-annotations")
        exclude(group = "org.hamcrest", module = "hamcrest-core")
    }
    api("com.google.protobuf:protobuf-java:3.25.5")
    api("com.fasterxml.jackson.core:jackson-core:2.11.3")
    api("com.fasterxml.jackson.core:jackson-databind:2.11.3")
    api("junit:junit:4.13")
    api("org.apache.logging.log4j:log4j-core:2.13.1")
    api("org.apache.logging.log4j:log4j-slf4j-impl:2.13.1")
    api("org.hamcrest:hamcrest:2.2")
    api("org.testcontainers:testcontainers:1.17.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-annotations")
        exclude(group = "junit", module = "junit")
    }
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


