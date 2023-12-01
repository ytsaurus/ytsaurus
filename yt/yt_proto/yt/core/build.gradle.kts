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

dependencies {
    api("com.google.protobuf:protobuf-java:3.22.5")

    protobuf(files(File(buildProtoDir, "yt")))
}


val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt_proto/yt/core/crypto/proto/crypto.proto")
        include("yt/yt_proto/yt/core/misc/proto/bloom_filter.proto")
        include("yt/yt_proto/yt/core/misc/proto/error.proto")
        include("yt/yt_proto/yt/core/misc/proto/guid.proto")
        include("yt/yt_proto/yt/core/misc/proto/protobuf_helpers.proto")
        include("yt/yt_proto/yt/core/tracing/proto/span.proto")
        include("yt/yt_proto/yt/core/tracing/proto/tracing_ext.proto")
        include("yt/yt_proto/yt/core/bus/proto/bus.proto")
        include("yt/yt_proto/yt/core/rpc/proto/rpc.proto")
        include("yt/yt_proto/yt/core/yson/proto/protobuf_interop.proto")
        include("yt/yt_proto/yt/core/ytree/proto/attributes.proto")
        include("yt/yt_proto/yt/core/ytree/proto/request_complexity_limits.proto")
        include("yt/yt_proto/yt/core/ytree/proto/ypath.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
version = project.properties["version"]

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "ytsaurus-proto-core"
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
                name.set("YTsaurus proto core library")
                description.set("YTsaurus proto core library")
                url.set("https://github.com/ytsaurus/ytsaurus")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("nadya73")
                        name.set("Nadezhda Savchenko")
                        email.set("nadya73@ytsaurus.tech")
                        organization.set("YTsaurus")
                        organizationUrl.set("https://ytsaurus.tech")
                    }
                    developer {
                        id.set("ermolovd")
                        name.set("Dmitry Ermolov")
                        email.set("ermolovd@ytsaurus.tech")
                        organization.set("YTsaurus")
                        organizationUrl.set("https://ytsaurus.tech")

                    }
                    developer {
                        id.set("tinarsky")
                        name.set("Aleksei Tinarskii")
                        email.set("tinarsky@ytsaurus.tech")
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

    repositories {
        maven {
            val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

            credentials {
                username = project.properties["ossrhUsername"].toString()
                password = project.properties["ossrhPassword"].toString()
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
