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
    api(""junit:junit:4.13"")
    api(""org.apache.logging.log4j:log4j-core:2.13.1"")
    api(""org.apache.logging.log4j:log4j-slf4j-impl:2.13.1"")
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


