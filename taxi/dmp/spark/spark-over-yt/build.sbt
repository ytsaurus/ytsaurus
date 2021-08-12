import Dependencies._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import sbtrelease.ReleasePlugin.autoImport.releaseProcess
import spyt.DebianPackagePlugin.autoImport._
import spyt.PythonPlugin.autoImport._
import spyt.SparkPackagePlugin.autoImport._
import spyt.SpytPlugin.autoImport._
import spyt.TarArchiverPlugin.autoImport._
import spyt.YtPublishPlugin.autoImport._
import spyt.ZipPlugin.autoImport._
import spyt._

lazy val `yt-wrapper` = (project in file("yt-wrapper"))
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= sttp,
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= testDeps
  )

lazy val `spark-launcher` = (project in file("spark-launcher"))
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`, `yt-wrapper` % "test->test")
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= logging,
    libraryDependencies ++= scalatra,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= scalatraTestDeps,
    assembly / assemblyJarName := s"spark-yt-launcher.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true)
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`yt-wrapper`)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= py4j,
    libraryDependencies ++= commonDependencies,
    assembly / assemblyJarName := s"spark-yt-submit.jar"
  )

lazy val `submit-client` = (project in file("submit-client"))
  .dependsOn(`spark-submit`, `file-system`)
  .settings(
    libraryDependencies ++= sparkFork,
    libraryDependencies ++= logging
  )

lazy val commonDependencies = yandexIceberg ++ spark ++ circe ++ logging.map(_ % Provided)

lazy val `data-source` = (project in file("data-source"))
  .enablePlugins(PythonPlugin)
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`, `file-system`, `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    version := (ThisBuild / spytClientVersion).value,
    Defaults.itSettings,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= commonDependencies,
    assembly / assemblyJarName := "spark-yt-data-source.jar",
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
    assembly / test := {}
  )

lazy val `file-system` = (project in file("file-system"))
  .dependsOn(`yt-wrapper`, `yt-wrapper` % "test->test")
  .settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies += "net.logstash.log4j" % "jsonevent-layout" % "1.7"
  )
  .settings(
    assembly / assemblyMergeStrategy := {
      case x if x endsWith "ahc-default.properties" => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyShadeRules ++= Seq(
      ShadeRule.rename(
        "ru.yandex.spark.yt.fs.YtFileSystem" -> "ru.yandex.spark.yt.fs.YtFileSystem",
        "ru.yandex.spark.yt.fs.eventlog.YtEventLogFileSystem" -> "ru.yandex.spark.yt.fs.eventlog.YtEventLogFileSystem",
        "ru.yandex.misc.log.**" -> "ru.yandex.misc.log.@1",
        "ru.yandex.**" -> "shadedyandex.ru.yandex.@1",
        "org.asynchttpclient.**" -> "shadedyandex.org.asynchttpclient.@1"
      ).inAll
    ),
    assembly / test := {}
  )

lazy val `client` = (project in file("client"))
  .enablePlugins(SparkPackagePlugin, DebianPackagePlugin, PythonPlugin)
  .settings(
    sparkAdditionalJars := Seq(
      (`file-system` / assembly).value,
      (`spark-submit` / assembly).value
    ),
    sparkAdditionalPython := Seq(
      (`data-source` / sourceDirectory).value / "main" / "python"
    )
  )
  .settings(
    debPackagePrefixPath := "/usr/lib/yandex",
    debPackagePublishRepo := "yandex-taxi-common",
    debPackageVersion := {
      val debBuildNumber = Option(System.getProperty("build")).getOrElse("")
      val beta = if ((ThisBuild / version).value.contains("SNAPSHOT")) s"~beta1-${sys.env("USER")}" else ""
      s"$sparkVersion-${(ThisBuild / version).value.takeWhile(_ != '-')}$beta+yandex$debBuildNumber"
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
          (Compile / resourceDirectory).value / "log4j.local.properties" -> s"$sparkBasePath/conf/log4j.properties"
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
    publishYtArtifacts += YtPublishFile(tarArchiveBuild.value, sparkYtBinBasePath.value, None),
    publishYtArtifacts += YtPublishFile((`spark-launcher` / assembly).value, sparkYtBinBasePath.value, None),
    publishYtArtifacts ++= sparkYtConfigs.value
  )
  .settings(
    pythonSetupName := "setup-yandex.py",
    pythonBuildDir := sparkHome.value / "python"
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


//lazy val `test-job` = (project in file("test-job"))
//  .dependsOn(`data-source` % Provided)
//  .settings(
//    libraryDependencies ++= spark,
//    libraryDependencies ++= logging.map(_ % Provided),
//    libraryDependencies ++= scaldingArgs,
//    excludeDependencies += ExclusionRule(organization = "org.slf4j"),
//    assembly / mainClass := Some("ru.yandex.spark.test.Test"),
//    publishYtArtifacts += YtPublishFile(assembly.value, "//home/sashbel", None),
//    publishYtArtifacts += YtPublishFile(sourceDirectory.value / "main" / "python" / "test_conf.py", "//home/sashbel", None)
//  )
// -----

lazy val root = (project in file("."))
  .enablePlugins(SpytPlugin)
  .aggregate(
    `yt-wrapper`,
    `spark-launcher`,
    `file-system`,
    `data-source`,
    `client`
  )
  .settings(
    spytPublishCluster := (client / publishYt).value,
    spytPublishClient := Def.sequential(
      `data-source` / publishYt,
      `data-source` / pythonBuildAndUpload
    ).value,
    spytPublishAll := Def.sequential(
      spytPublishCluster,
      `client` / pythonBuildAndUpload,
      spytPublishClient
    ).value,
    releaseProcess := spytReleaseProcess.value
  )


