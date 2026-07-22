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
    api(project(":yt:java:flow:flow-core"))
    api(project(":yt:java:flow:flow-server"))
    api("com.google.protobuf:protobuf-java:4.33.0")
    api("com.google.protobuf:protobuf-java-util:4.33.0")
    api("io.grpc:grpc-api:1.78.0")
    api("io.grpc:grpc-context:1.78.0")
    api("io.grpc:grpc-stub:1.78.0")
    api("io.grpc:grpc-protobuf:1.78.0")
    api("io.grpc:grpc-protobuf-lite:1.78.0")
    api("io.grpc:grpc-netty-shaded:1.78.0")
    api("io.grpc:grpc-services:1.78.0")
    api("com.google.api.grpc:proto-google-common-protos:2.63.1")
    api("com.google.guava:guava:33.5.0-jre")
    api("com.google.guava:failureaccess:1.0.3")
    api("com.google.j2objc:j2objc-annotations:3.1")
    api("com.google.errorprone:error_prone_annotations:2.44.0")
    api("com.google.code.gson:gson:2.12.1")
    api("com.google.code.findbugs:jsr305:3.0.2")
    api("io.micrometer:micrometer-core:1.16.3")
    api("io.micrometer:micrometer-commons:1.16.3")
    api("io.micrometer:micrometer-observation:1.16.3")
    api("org.slf4j:slf4j-api:2.0.17")
    api("org.apache.logging.log4j:log4j-api:2.25.3")
    api("org.apache.logging.log4j:log4j-core:2.25.3")
    api("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.3")
    api("org.lz4:lz4-java:1.6.0")
    api("org.jspecify:jspecify:1.0.0")
    api("com.github.ben-manes.caffeine:caffeine:3.2.3")
    api("org.springframework.boot:spring-boot-starter-log4j2:4.0.2")
    api("org.springframework.boot:spring-boot-autoconfigure:4.0.2")
    api("org.springframework:spring-context:7.0.3")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


