plugins {
    `java-library`
    `maven-publish`
    `signing`
}

group = "tech.ytsaurus"
version = "1.0.1"

repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies{
    api("io.dropwizard.metrics:metrics-core:3.1.2")
    api(project(":yt:java:annotations"))
    api(project(":yt:java:skiff"))
    api(project(":yt:java:type-info"))
    api(project(":yt:java:ytsaurus-client-core"))
    api(project(":yt:yt_proto:yt:client"))
    api("com.google.protobuf:protobuf-java:3.21.12")
    api("org.slf4j:slf4j-api:1.7.7")
    api("io.netty:netty-buffer:4.1.42.Final")
    api("io.netty:netty-codec:4.1.42.Final")
    api("io.netty:netty-common:4.1.42.Final")
    api("io.netty:netty-handler:4.1.42.Final")
    api("io.netty:netty-transport:4.1.42.Final")
    api("io.netty:netty-transport-native-epoll:4.1.42.Final:linux-x86_64")
    api("org.lz4:lz4-java:1.6.0")
    testImplementation("javax.persistence:persistence-api:1.0")
    testImplementation("com.google.protobuf:protobuf-java:3.21.12")
    testImplementation("junit:junit:4.13")
    testImplementation("org.apache.logging.log4j:log4j-core:2.13.1")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.1")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "ytsaurus-client"
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
                name.set("YTsaurus Java SDK")
                description.set("YTsaurus client")
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
    sign(publishing.publications["mavenJava"])
}
