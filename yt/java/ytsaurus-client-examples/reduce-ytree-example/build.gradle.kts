plugins {
    `application`
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("tech.ytsaurus.example.ExampleReduceYTree")
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    implementation(project(":yt:java:annotations"))
    implementation(project(":yt:java:ytsaurus-client"))
    implementation(""javax.persistence:persistence-api:1.0"")
    implementation(""com.google.protobuf:protobuf-java:3.22.5"")
    implementation(""org.apache.logging.log4j:log4j-slf4j-impl:2.13.1"")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


