import com.google.protobuf.gradle.*

val buildProtoDir = File("${buildDir}", "__proto__")

plugins {
    id("java-library")
    id("com.google.protobuf") version "0.8.19"
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
    api("com.google.protobuf:protobuf-java:4.33.0")
    api("io.grpc:grpc-stub:1.51.0")
    api("io.grpc:grpc-protobuf:1.51.0")
    api("javax.annotation:javax.annotation-api:1.3.1")
    api(project(":yt:yt_proto:yt:core"))
    api(project(":yt:yt:flow:library:cpp:common:proto"))

    protobuf(files(File(buildProtoDir, "yt")))
}

protobuf {
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:4.33.0"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.45.0"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
            }
        }
    }
}

val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt/flow/library/cpp/companion/proto/companion_service.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
// Flow ships on its own flow/X.Y.Z tag at its own version. The Java SDK modules it depends
// on keep -Pversion, so the POM points at a released ytsaurus-client.
version = project.properties["flowVersion"]

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "flow-proto-companion"
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
                name.set("YTsaurus Flow companion proto library")
                description.set("gRPC contract between the YTsaurus Flow worker and its companions")
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
