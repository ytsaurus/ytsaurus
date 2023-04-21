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
    api(project(":yt:java:annotations"))
    api("com.google.code.findbugs:jsr305:3.0.2")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "yson"
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
                name.set("YTsaurus yson format library")
                description.set("YTsaurus yson format library")
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
