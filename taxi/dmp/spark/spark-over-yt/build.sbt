import Dependencies._
import SparkPackagePlugin.autoImport._
import YtPublishPlugin.autoImport._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import TarArchiverPlugin.autoImport._
import DebianPackagePlugin.autoImport._
import ZipPlugin.autoImport._
import PythonPlugin.autoImport._

val clientVersion = "0.5.11"

lazy val `yt-wrapper` = (project in file("yt-wrapper"))
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= sttp,
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= testDeps
  )

lazy val `proxy-test` = (project in file("proxy-test"))
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= logging,
    mainClass in assembly := Some("ru.yandex.spark.yt.arrow.test.ArrowTest"),
    assemblyJarName in assembly := s"proxy-test.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
  )

lazy val `spark-launcher` = (project in file("spark-launcher"))
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= logging,
    libraryDependencies ++= scalatra,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= scalatraTestDeps,
    assemblyJarName in assembly := s"spark-yt-launcher.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
  )

lazy val commonDependencies = yandexIceberg ++ spark ++ arrow ++ circe ++ logging.map(_ % Provided)

lazy val `data-source` = (project in file("data-source"))
  .enablePlugins(PythonPlugin)
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`, `file-system`, `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    version := clientVersion,
    Defaults.itSettings,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= commonDependencies,
    libraryDependencies += "ru.yandex" %% "spark-yt-common-utils" % "0.0.1",
    assemblyJarName in assembly := "spark-yt-data-source.jar",
    zipPath := Some(target.value / "spyt.zip"),
    zipMapping += sourceDirectory.value / "main" / "python" / "spyt" -> "",
    zipIgnore := { file: File =>
      file.getName.contains("__pycache__") || file.getName.endsWith(".pyc")
    },
    publishYtArtifacts ++= {
      val subdir = if (isSnapshot.value) "snapshots" else "releases"
      val publishDir = s"//sys/spark/spyt/$subdir/${version.value}"
      Seq(
        YtPublishFile(assembly.value, publishDir, proxy = None),
        YtPublishFile(zip.value, publishDir, proxy = None)
      )
    },
    test in assembly := {}
  )

lazy val `common-utils` = (project in file("common-utils"))
  .dependsOn(`data-source` % Provided)
  .settings(
    version := "0.0.2-SNAPSHOT",
    libraryDependencies ++= commonDependencies
  )

lazy val `file-system` = (project in file("file-system"))
  .dependsOn(`yt-wrapper`, `yt-wrapper` % "test->test")
  .settings(
    libraryDependencies ++= commonDependencies
  )
  .settings(
    assemblyMergeStrategy in assembly := {
      case x if x endsWith "ahc-default.properties" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly ++= Seq(
      ShadeRule.rename(
        "ru.yandex.spark.yt.fs.YtFileSystem" -> "ru.yandex.spark.yt.fs.YtFileSystem",
        "ru.yandex.misc.log.**" -> "ru.yandex.misc.log.@1",
        "ru.yandex.**" -> "shadedyandex.ru.yandex.@1",
        "org.asynchttpclient.**" -> "shadedyandex.org.asynchttpclient.@1"
      ).inAll
    ),
    test in assembly := {}
  )

lazy val `client` = (project in file("client"))
  .enablePlugins(SparkPackagePlugin, DebianPackagePlugin, PythonPlugin)
  .settings(
    sparkAdditionalJars := Seq(
      (assembly in `file-system`).value
    ),
    sparkAdditionalPython := Seq(
      (sourceDirectory in `data-source`).value / "main" / "python"
    )
  )
  .settings(
    debPackagePrefixPath := "/usr/lib/yandex",
    debPackagePublishRepo := "yandex-taxi-common",
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
      val sparkBasePath = s"${debPackagePrefixPath.value}/spark"
      Seq(
        createPackageMapping(sparkDist, sparkBasePath),
        LinuxPackageMapping(Map(
          (resourceDirectory in Compile).value / "log4j.local.properties" -> s"$sparkBasePath/conf/log4j.properties"
        ))
      )
    },
    linuxPackageSymlinks ++= {
      val sparkBasePath = s"${debPackagePrefixPath.value}/spark"
      Seq(
        LinuxSymlink("/usr/local/bin/spark-shell", s"$sparkBasePath/bin/spark-shell"),
        LinuxSymlink("/usr/local/bin/find-spark-home", s"$sparkBasePath/bin/find-spark-home"),
        LinuxSymlink("/usr/local/bin/spark-class", s"$sparkBasePath/bin/spark-class"),
        LinuxSymlink("/usr/local/bin/spark-submit", s"$sparkBasePath/bin/spark-submit"),
        LinuxSymlink("/usr/local/bin/spark-shell-yt", s"$sparkBasePath/bin/spark-shell-yt"),
        LinuxSymlink("/usr/local/bin/spark-submit-yt", s"$sparkBasePath/bin/spark-submit-yt"),
        LinuxSymlink("/usr/local/bin/spark-launch-yt", s"$sparkBasePath/bin/spark-launch-yt")
      )
    }
  )
  .settings(
    tarArchiveMapping += sparkPackage.value -> "spark",
    tarArchivePath := Some(target.value / s"spark.tgz")
  )
  .settings(
    sparkYtProxies := publishYtProxies.value,
    publishYtArtifacts += YtPublishFile(tarArchiveBuild.value, sparkYtBinBasePath.value, None),
    publishYtArtifacts += YtPublishFile((assembly in `spark-launcher`).value, sparkYtBinBasePath.value, None),
    publishYtArtifacts ++= sparkYtConfigs.value
  )
  .settings(
    pythonSetupName := "setup-yandex.py",
    pythonBuildDir := sparkHome.value / "python"
  )


lazy val `common-logging` = (project in file("common-logging"))
  .settings(
    libraryDependencies ++= logging
  )

// benchmark and test ----

//lazy val benchmark = (project in file("benchmark"))
//  .settings(
//    unmanagedJars in Compile ++= {
//      val spark = file("/Users/sashbel/Documents/repos/spark/dist/jars")
//      val dataSource = baseDirectory.value.getParentFile / "data-source"/ "target" / "scala-2.12" / "spark-yt-data-source.jar"
//      dataSource +: (spark.listFiles().toSeq)
//    },
//    libraryDependencies ++= sttp
//  )

lazy val `test-job` = (project in file("test-job"))
  .settings(
    libraryDependencies += "ru.yandex" %% "spark-yt-data-source" % "0.3.0" % Provided,
    libraryDependencies ++= spark,
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= scaldingArgs,
    excludeDependencies += ExclusionRule(organization = "org.slf4j"),
    mainClass in assembly := Some("ru.yandex.spark.test.Test"),
    publishYtArtifacts += YtPublishFile(assembly.value, "//home/sashbel", None),
    publishYtArtifacts += YtPublishFile(sourceDirectory.value / "main" / "python" / "test_conf.py", "//home/sashbel", None)
  )

// -----

lazy val root = (project in file("."))
  .aggregate(`data-source`)
