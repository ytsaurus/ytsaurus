import Dependencies._

lazy val `data-source` = (project in file("data-source"))
  .dependsOn(`yt-utils`)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg,
    test in assembly := {}
  )

lazy val `discovery-service` = (project in file("discovery-service"))
  .dependsOn(`yt-utils`)
  .settings(
    libraryDependencies ++= yandexIceberg,
  )

lazy val `spark-launcher` = (project in file("spark-launcher"))
  .dependsOn(`discovery-service`)
  .settings(
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

lazy val `yt-utils` = (project in file("yt-utils"))
  .settings(
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging
  )

lazy val `spark-supervisor` = (project in file("spark-supervisor"))

lazy val root = (project in file("."))
  .aggregate(`data-source`)
