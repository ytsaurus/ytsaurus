plugins {
    `java-library`
    `maven-publish`
    `signing`
}

group = "tech.ytsaurus"
version = project.properties["version"]

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

tasks.withType<Javadoc>().configureEach {
    (options as CoreJavadocOptions).addStringOption("source", "17")
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.2")
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
    api("org.lz4:lz4-java:1.6.0")
    api("org.jspecify:jspecify:1.0.0")
    api("com.beust:jcommander:1.82")
    testImplementation(project(":yt:java:flow:flow-core"))
    testImplementation(project(":yt:java:flow:flow-test-utils"))
    testImplementation("javax.persistence:persistence-api:1.0")
    testImplementation("com.google.protobuf:protobuf-java:4.33.0")
    testImplementation("com.google.protobuf:protobuf-java-util:4.33.0")
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testImplementation("org.mockito:mockito-core:5.21.0")
    testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.1")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.test {
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
}


// Flow ships on its own flow/X.Y.Z tag at its own version. The Java SDK modules it depends
// on keep -Pversion, so the POM points at a released ytsaurus-client.
version = project.properties["flowVersion"]

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "flow-runner"
            from(components["java"])

            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("YTsaurus Flow Java SDK runner")
                description.set("Pipeline lifecycle management for YTsaurus Flow")
                url.set("https://github.com/ytsaurus/ytsaurus")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("timoninmaxim")
                        email.set("timoninmaxim@ytsaurus.tech")
                        organization.set("YTsaurus")
                        organizationUrl.set("https://ytsaurus.tech")
                    }
                    developer {
                        id.set("sergeypozdeev")
                        email.set("sergeypozdeev@ytsaurus.tech")
                        organization.set("YTsaurus")
                        organizationUrl.set("https://ytsaurus.tech")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/ytsaurus/ytsaurus.git")
                    developerConnection.set("scm:git:ssh://github.com/ytsaurus/ytsaurus.git")
                    url.set("https://github.com/ytsaurus/ytsaurus")
                }
            }
        }
    }
}

// The remote repository is declared only when the Flow release asks for it with
// -PflowRelease. A repository-wide `gradlew publish` from the Java SDK workflows finds
// no repository here and publishes nothing.
if (project.hasProperty("flowRelease")) {
    publishing {
        repositories {
            maven {
                val releasesRepoUrl = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                val snapshotsRepoUrl = uri("https://central.sonatype.com/repository/maven-snapshots/")
                url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

                credentials {
                    username = project.properties["ossrhUsername"].toString()
                    password = project.properties["ossrhPassword"].toString()
                }
            }
        }
    }
}

signing {
    setRequired({
        !version.toString().endsWith("SNAPSHOT")
    })

    val signingKey: String? by project
    val signingPassword: String? by project

    useInMemoryPgpKeys(signingKey, signingPassword)

    sign(publishing.publications["mavenJava"])
}
