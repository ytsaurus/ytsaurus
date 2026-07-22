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
    api(project(":yt:java:flow:flow-server"))
    api(project(":yt:java:flow:flow-runner"))
    api("javax.persistence:persistence-api:1.0")
    api("com.google.protobuf:protobuf-java:4.33.0")
    api("com.google.protobuf:protobuf-java-util:4.33.0")
    api("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.1")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


