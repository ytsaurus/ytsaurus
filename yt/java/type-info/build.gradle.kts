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
}

