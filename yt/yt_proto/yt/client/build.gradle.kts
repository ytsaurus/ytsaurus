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
    api(project(":yt:yt_proto:yt:core"))

    protobuf(files(File(buildProtoDir, "yt")))
}


val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto")
        include("yt/yt_proto/yt/client/api/rpc_proxy/proto/discovery_service.proto")
        include("yt/yt_proto/yt/client/cell_master/proto/cell_directory.proto")
        include("yt/yt_proto/yt/client/chaos_client/proto/replication_card.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/data_statistics.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/read_limit.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/confirm_chunk_replica_info.proto")
        include("yt/yt_proto/yt/client/discovery_client/proto/discovery_client_service.proto")
        include("yt/yt_proto/yt/client/hive/proto/timestamp_map.proto")
        include("yt/yt_proto/yt/client/hive/proto/cluster_directory.proto")
        include("yt/yt_proto/yt/client/node_tracker_client/proto/node.proto")
        include("yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.proto")
        include("yt/yt_proto/yt/client/transaction_client/proto/timestamp_service.proto")
        include("yt/yt_proto/yt/client/query_client/proto/query_statistics.proto")
        include("yt/yt_proto/yt/client/misc/proto/workload.proto")
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
            artifactId = "ytsaurus-proto-client"
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
                name.set("YTsaurus client proto library")
                description.set("YTsaurus client proto library")
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
