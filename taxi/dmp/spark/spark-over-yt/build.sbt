import Dependencies._
import SparkPackagePlugin.autoImport._
import ru.yandex.sbt.YtPublishPlugin.autoImport._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import TarArchiverPlugin.autoImport._
import ru.yandex.sbt.DebianPackagePlugin
import ru.yandex.sbt.DebianPackagePlugin.autoImport._

lazy val `data-source` = (project in file("data-source"))
  .dependsOn(`yt-utils`, `file-system`, `test-utils` % Test)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided),
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

lazy val `file-system` = (project in file("file-system"))
  .dependsOn(`yt-utils`)
  .settings(
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    libraryDependencies ++= circe,
    libraryDependencies ++= logging.map(_ % Provided)
  )

lazy val `test-utils` = (project in file("test-utils"))
  .dependsOn(`yt-utils`, `file-system`)
  .settings(
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.14.1",
      "org.scalactic" %% "scalactic" % scalatestVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion
    ),
    libraryDependencies ++= spark
  )

lazy val `client` = (project in file("client"))
  .enablePlugins(SparkPackagePlugin, DebianPackagePlugin)
  .settings(
    sparkAdditionalJars := Seq(
      (assembly in `data-source`).value
    ),
    sparkLauncherName := (name in `spark-launcher`).value
  )
  .settings(
    debPackageVersion := {
      val debBuildNumber = Option(System.getProperty("build")).getOrElse("")
      val beta = if ((version in ThisBuild).value.contains("SNAPSHOT")) "~beta1" else ""
      s"$sparkVersion-${(version in ThisBuild).value.takeWhile(_ != '-')}$beta+yandex$debBuildNumber"
    },
    version := debPackageVersion.value,
    packageSummary := "Spark over YT Client Debian Package",
    packageDescription := "Client spark libraries and Spark over YT binaries",
    linuxPackageMappings ++= {
      val sparkDist = sparkPackage.value
      Seq(
        createPackageMapping(sparkDist, s"/opt/${sparkName.value}"),
        LinuxPackageMapping(Map(
          sparkGenerateProfile.value -> "/etc/profile.d/spark-profile.sh",
          (resourceDirectory in Compile).value / "log4j.local.properties" -> s"/opt/${sparkName.value}/conf/log4j.properties"
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
    )
  )
  .settings(
    tarArchiveMapping += sparkPackage.value -> sparkName.value,
    tarArchivePath := Some(target.value / s"${sparkName.value}.tgz")
  )
  .settings(
    publishYtArtifacts += tarArchiveBuild.value,
    publishYtArtifacts ++= (publishYtArtifacts in `spark-launcher`).value
  )

lazy val root = (project in file("."))
  .aggregate(`data-source`)
