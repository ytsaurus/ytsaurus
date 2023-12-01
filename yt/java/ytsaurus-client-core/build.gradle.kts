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
    api(project(":yt:java:type-info"))
    api(project(":yt:java:yson"))
    api(project(":yt:java:yson-tree"))
    api(project(":yt:yt_proto:yt:core"))
    api(project(":yt:yt_proto:yt:formats"))
    api("com.google.protobuf:protobuf-java:3.22.5")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


