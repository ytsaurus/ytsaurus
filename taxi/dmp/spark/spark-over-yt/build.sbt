import CommonPlugin.autoImport._
import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.releaseProcess
import spyt.DebianPackagePlugin.autoImport._
import spyt.PythonPlugin.autoImport._
import spyt.SparkPackagePlugin.autoImport._
import spyt.SparkPaths._
import spyt.SpytPlugin.autoImport._
import spyt.TarArchiverPlugin.autoImport._
import spyt.YtPublishPlugin.autoImport._
import spyt.ZipPlugin.autoImport._

lazy val `yt-wrapper` = (project in file("yt-wrapper"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= sttp,
    libraryDependencies ++= yandexIceberg,
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= testDeps,
    buildInfoKeys := Seq[BuildInfoKey](version, BuildInfoKey.constant(("ytClientVersion", yandexIcebergVersion))),
    buildInfoPackage := "ru.yandex.spark.yt"
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
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true),
    assembly / test := {}
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`yt-wrapper` % Provided)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= py4j,
    libraryDependencies ++= yandexIceberg.map(_ % Provided) ++ spark ++ circe.map(_ % Provided) ++ logging.map(_ % Provided),
    assembly / assemblyJarName := s"spark-yt-submit.jar",
    assembly / assemblyShadeRules ++= clusterShadeRules
  )

lazy val `submit-client` = (project in file("submit-client"))
  .dependsOn(`spark-submit`, `file-system`)
  .settings(
    libraryDependencies ++= sparkFork,
    libraryDependencies ++= yandexIceberg ++ circe ++ logging
  )

lazy val commonDependencies = yandexIceberg ++ spark ++ circe ++ logging.map(_ % Provided)

def cutSnapshot(ver: String): String = {
  val r = "^(.*)-SNAPSHOT$".r
  ver match {
    case r(v) => v
    case v => v
  }
}

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
      val publishDir = s"$sparkYtClientPath/$subdir/${cutSnapshot(version.value)}"
      val link = if (!isSnapshot.value) {
        Seq(YtPublishLink(publishDir, s"$sparkYtLegacyClientPath/$subdir", None, version.value, isSnapshot.value))
      } else Nil

      link ++ Seq(
        YtPublishFile(assembly.value, publishDir, proxy = None, isSnapshot.value),
        YtPublishFile(zip.value, publishDir, proxy = None, isSnapshot.value)
      )
    },
    assembly / assemblyShadeRules ++= clientShadeRules,
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
    assembly / assemblyShadeRules ++= clusterShadeRules,
    assembly / test := {}
  )

lazy val `client` = (project in file("client"))
  .enablePlugins(SparkPackagePlugin, PythonPlugin)
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
    tarArchiveMapping += sparkPackage.value -> "spark",
    tarArchivePath := Some(target.value / s"spark.tgz")
  )
  .settings(
    publishYtArtifacts ++= {
      val basePath = sparkYtBinBasePath.value
      val legacyBasePath = s"$sparkYtLegacyBinPath/${sparkYtSubdir.value}"
      val versionValue = version.value
      val link = if (sparkReleaseLinks.value) {
        Seq(YtPublishLink(basePath, legacyBasePath, None, versionValue, sparkIsSnapshot.value))
      } else Nil

      link ++ Seq(
        YtPublishFile(tarArchiveBuild.value, sparkYtBinBasePath.value, None, sparkIsSnapshot.value),
        YtPublishFile((`spark-launcher` / assembly).value, sparkYtBinBasePath.value, None, sparkIsSnapshot.value),
      ) ++ sparkYtConfigs.value
    }
  )
  .settings(
    pythonSetupName := "setup-yandex.py",
    pythonBuildDir := sparkHome.value / "python"
  )
  .settings(
    debPackageVersion := {
      val debBuildNumber = Option(System.getProperty("build")).getOrElse("")
      val beta = if ((ThisBuild / version).value.contains("SNAPSHOT")) s"~beta1-${sys.env("USER")}" else ""
      s"$sparkVersion-${(ThisBuild / version).value.takeWhile(_ != '-')}$beta+yandex$debBuildNumber"
    },
    version := debPackageVersion.value
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


