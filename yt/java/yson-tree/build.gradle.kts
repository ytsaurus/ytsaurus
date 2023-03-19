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
    api(project(":yt:java:annotations"))
    api(project(":yt:java:yson"))
    testImplementation("junit:junit:4.13")
    testImplementation("org.apache.logging.log4j:log4j-core:2.13.1")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.1")
}

