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
    api(project(":yt:yt_proto:yt:core"))

    protobuf(files(File(buildProtoDir, "yt")))
}

protobuf {
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:4.33.0"
    }

}

val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt/flow/library/cpp/common/proto/admin_service.proto")
        include("yt/yt/flow/library/cpp/common/proto/message.proto")
        include("yt/yt/flow/library/cpp/common/proto/timer.proto")
        include("yt/yt/flow/library/cpp/common/proto/visit.proto")
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
            artifactId = "flow-proto-common"
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
                name.set("YTsaurus Flow common proto library")
                description.set("Core YTsaurus Flow protobuf messages")
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
