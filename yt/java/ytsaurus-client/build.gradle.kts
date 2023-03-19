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

dependencies{
    api("io.dropwizard.metrics:metrics-core:3.1.2")
    api(project(":yt:java:annotations"))
    api(project(":yt:java:skiff"))
    api(project(":yt:java:type-info"))
    api(project(":yt:java:ytsaurus-client-core"))
    api(project(":yt:yt_proto:yt:client"))
    api("com.google.protobuf:protobuf-java:3.21.12")
    api("org.slf4j:slf4j-api:1.7.7")
    api("io.netty:netty-buffer:4.1.42.Final")
    api("io.netty:netty-codec:4.1.42.Final")
    api("io.netty:netty-common:4.1.42.Final")
    api("io.netty:netty-handler:4.1.42.Final")
    api("io.netty:netty-transport:4.1.42.Final")
    api("io.netty:netty-transport-native-epoll:4.1.42.Final:linux-x86_64")
    api("org.lz4:lz4-java:1.6.0")
    testImplementation("javax.persistence:persistence-api:1.0")
    testImplementation("com.google.protobuf:protobuf-java:3.21.12")
    testImplementation("junit:junit:4.13")
    testImplementation("org.apache.logging.log4j:log4j-core:2.13.1")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.1")
}

