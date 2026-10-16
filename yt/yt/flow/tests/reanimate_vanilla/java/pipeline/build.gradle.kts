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

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

tasks.withType<Javadoc>().configureEach {
    (options as CoreJavadocOptions).addStringOption("source", "21")
}

dependencies {
    api(project(":yt:java:flow:flow-runner"))
    api(project(":yt:java:flow:flow-spring-boot-starter"))
    api("javax.persistence:persistence-api:1.0")
    api("com.google.protobuf:protobuf-java:4.33.0")
    api("com.google.protobuf:protobuf-java-util:4.33.0")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


