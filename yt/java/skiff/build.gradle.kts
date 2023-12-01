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
    api(project(":yt:java:yson-tree"))
    api(project(":yt:java:ytsaurus-client-core"))
    api(""com.google.protobuf:protobuf-java:3.22.5"")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


