publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "annotations"
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
                name.set("Class annotations for YTsaurus libraries")
                description.set("Class annotations for YTsaurus libraries")
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
            // TODO: change URLs to point to your repos, e.g. http://my.org/repo
            val releasesRepoUrl = uri(layout.buildDirectory.dir("repos/releases"))
            val snapshotsRepoUrl = uri(layout.buildDirectory.dir("repos/snapshots"))
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}
