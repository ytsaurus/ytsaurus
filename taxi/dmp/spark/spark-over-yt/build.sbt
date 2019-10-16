val circeVersion = "0.11.1"

lazy val `data-source` = (project in file("data-source"))
  .settings(
    resolvers += "Arcadia" at "http://artifactory.yandex.net/artifactory/yandex_media_releases",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.12.9",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion),
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
    libraryDependencies ++= Seq(
      "ru.yandex" % "iceberg-inside-yt" % "5702526" excludeAll (
        ExclusionRule(organization = "com.fasterxml.jackson.core")
        ),
      "org.apache.spark" %% "spark-core" % "2.4.3" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.3" % Provided,
      "org.scalactic" %% "scalactic" % "3.0.8",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    ),
    assemblyMergeStrategy in assembly := {
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x if x endsWith "Log4j2Plugins.dat" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("javax.annotation.**" -> "shaded.javax.annotation.@1")
        .inLibrary("com.google.code.findbugs" % "annotations" % "2.0.3"),
      ShadeRule.zap("META-INF.org.apache.logging.log4j.core.config.plugins.Log4j2Plugins.dat")
        .inLibrary("org.apache.logging.log4j" % "log4j-core" % "2.11.0")
    ),
    test in assembly := {}
  )

lazy val benchmark = (project in file("benchmark"))
  .dependsOn(`data-source`)
  .settings(
    resolvers += "Arcadia" at "http://artifactory.yandex.net/artifactory/yandex_media_releases",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.12.9",
    libraryDependencies ++= Seq(
      "ru.yandex" % "iceberg-inside-yt" % "5515244" excludeAll (
        ExclusionRule(organization = "com.fasterxml.jackson.core")
        ),
      "org.apache.spark" %% "spark-core" % "2.4.3" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.3" % Provided,
      "org.scalactic" %% "scalactic" % "3.0.8",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    )
  )


lazy val root = (project in file("."))
  .aggregate(`data-source`)
