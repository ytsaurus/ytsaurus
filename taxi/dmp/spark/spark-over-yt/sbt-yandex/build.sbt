lazy val commonSettings: Seq[Setting[_]] = Seq(
  version in ThisBuild := "0.0.3-1-SNAPSHOT",
  organization in ThisBuild := "ru.yandex"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    sbtPlugin := true,
    name := "sbt-yandex",
    resolvers += Resolver.url("sbt-plugins", url("https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"))(
      Patterns()
        .withIvyPatterns(Vector("[organisation]/[module]/scala_2.12/sbt_1.0/[revision]/[type]s/[artifact].[ext]"))
        .withIsMavenCompatible(false)
    ),
    libraryDependencies += "com.typesafe.sbt" % "sbt-native-packager" % "1.4.1",
    libraryDependencies += "org.vafer" % "jdeb" % "1.3" artifacts (Artifact("jdeb", "jar", "jar")),
    publishTo := {
      val nexus = "http://artifactory.yandex.net/artifactory/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "yandex_spark_snapshots")
      else
        Some("releases" at nexus + "yandex_spark_releases")
    },
    publishMavenStyle := false,
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
  )
