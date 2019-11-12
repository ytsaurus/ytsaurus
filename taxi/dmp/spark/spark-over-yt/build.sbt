import Dependencies._

lazy val `data-source` = (project in file("data-source"))
  .dependsOn(`yt-utils`)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided),
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

lazy val `spark-launcher` = (project in file("spark-launcher"))
  .dependsOn(`yt-utils`)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= logging
  )

lazy val benchmark = (project in file("benchmark"))
  .dependsOn(`data-source`)
  .settings(
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg
  )

lazy val `test-job` = (project in file("test-job"))
  .dependsOn(`data-source`)
  .settings(
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg.map(_ % Provided),
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= scaldingArgs,
    excludeDependencies += ExclusionRule(organization = "org.slf4j"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    mainClass in assembly := Some("ru.yandex.spark.test.Test")
  )

lazy val `yt-utils` = (project in file("yt-utils"))
  .settings(
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided)
  )

lazy val root = (project in file("."))
  .aggregate(`data-source`)
