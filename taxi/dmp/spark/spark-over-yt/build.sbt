import Dependencies._
import SparkPackagePlugin.autoImport._
import YtPublishPlugin.autoImport._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}

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
    libraryDependencies ++= logging,
    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
    publishYtArtifacts := Seq(assembly.value)
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

lazy val `client` = (project in file("client"))
  .enablePlugins(SparkPackagePlugin)
  .settings(
    maintainer := "Alexandra Belousova <sashbel@yandex-team.ru>",
    packageSummary := "Spark over YT Client Debian Package",
    packageDescription := "Client spark libraries and Spark over YT binaries",
    sparkName := s"spark-2.4.4-${version.value}",
    linuxPackageMappings ++= {
      val sparkDist = packageSpark.value
      Seq(
        createPackageMapping(sparkDist, s"/opt/${sparkName.value}"),
        LinuxPackageMapping(Map(
          (resourceDirectory in Compile).value / "spark-profile.sh" -> "/etc/profile.d/spark-profile.sh"
        ))
      )
    },
    linuxPackageSymlinks ++= Seq(
      LinuxSymlink("/usr/local/bin/spark-shell", s"/opt/${sparkName.value}/bin/spark-shell"),
      LinuxSymlink("/usr/local/bin/find-spark-home", s"/opt/${sparkName.value}/bin/find-spark-home"),
      LinuxSymlink("/usr/local/bin/spark-class", s"/opt/${sparkName.value}/bin/spark-class"),
      LinuxSymlink("/usr/local/bin/spark-submit", s"/opt/${sparkName.value}/bin/spark-submit"),
      LinuxSymlink("/usr/local/bin/spark-shell-yt", s"/opt/${sparkName.value}/bin/spark-shell-yt"),
      LinuxSymlink("/usr/local/bin/spark-submit-yt", s"/opt/${sparkName.value}/bin/spark-submit-yt"),
      LinuxSymlink("/usr/local/bin/spark-launch-yt", s"/opt/${sparkName.value}/bin/spark-launch-yt")
    ),
    sparkAdditionalJars := Seq(
      (assembly in `data-source`).value
    ),
    sparkLauncherName := (name in `spark-launcher`).value,
    publishYtArtifacts += packageSparkTgz.value,
    publishYtArtifacts ++= (publishYtArtifacts in `spark-launcher`).value
  )

lazy val root = (project in file("."))
  .aggregate(`data-source`)
