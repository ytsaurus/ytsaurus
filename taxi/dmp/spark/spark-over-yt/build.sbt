import CommonPlugin.autoImport._
import Dependencies._
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

lazy val `spark-fork` = (project in file("spark-fork"))
  .enablePlugins(SparkPackagePlugin, PythonPlugin)
  .settings(
    sparkAdditionalJars := Seq(
      (`file-system` / assembly).value
    ),
    sparkAdditionalBin := Seq(
      baseDirectory.value / "driver-op-discovery.sh",
    ),
    sparkAdditionalPython := Nil
  )
  .settings(
    tarArchiveMapping += sparkPackage.value -> "spark",
    tarArchivePath := Some(target.value / s"spark.tgz")
  )
  .settings(
    pythonSetupName := "setup-yandex.py",
    pythonBuildDir := sparkHome.value / "python"
  )
  .settings(
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytSparkVersion).value
      val basePath = versionPath(sparkYtSparkForkPath, versionValue)
      val isSnapshotValue = isSnapshotVersion(versionValue)

      Seq(
        YtPublishFile(tarArchiveBuild.value, basePath, None, isSnapshotValue)
      )
    }
  )

lazy val `cluster` = (project in file("spark-cluster"))
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
  .settings(
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytClusterVersion).value
      val sparkVersionValue = (ThisBuild / spytSparkVersion).value
      val isSnapshotValue = isSnapshotVersion(versionValue)

      val basePath = versionPath(sparkYtClusterPath, versionValue)
      val legacyBasePath = versionBasePath(sparkYtLegacyBinPath, versionValue)
      val sparkPath = versionPath(sparkYtSparkForkPath, sparkVersionValue)

      val sparkLink = Seq(YtPublishLink(s"$sparkPath/spark.tgz", basePath, None, "spark.tgz", isSnapshotValue))
      val legacyLink = if (!isSnapshotValue) {
        Seq(YtPublishLink(basePath, legacyBasePath, None, versionValue, isSnapshotValue))
      } else Nil
      val clusterConfigArtifacts = spyt.ClusterConfig.artifacts(streams.value.log, versionValue,
        (Compile / resourceDirectory).value)

      sparkLink ++ legacyLink ++ Seq(
        YtPublishFile(assembly.value, basePath, None, isSnapshotValue),
      ) ++ clusterConfigArtifacts
    }
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`yt-wrapper` % Provided)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= py4j,
    libraryDependencies ++= yandexIceberg.map(_ % Provided) ++ (ThisBuild / spytSparkForkDependency).value ++
      circe.map(_ % Provided) ++ logging.map(_ % Provided),
    assembly / assemblyJarName := s"spark-yt-submit.jar",
    assembly / assemblyShadeRules ++= clusterShadeRules
  )

lazy val `submit-client` = (project in file("submit-client"))
  .dependsOn(`spark-submit`, `file-system`)
  .settings(
    libraryDependencies ++= spark,
    libraryDependencies ++= yandexIceberg ++ circe ++ logging
  )

lazy val `data-source` = (project in file("data-source"))
  .enablePlugins(PythonPlugin)
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`, `file-system`, `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    version := (ThisBuild / spytClientVersion).value,
    Defaults.itSettings,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= commonDependencies.value,
    assembly / assemblyJarName := "spark-yt-data-source.jar",
    zipPath := Some(target.value / "spyt.zip"),
    zipMapping += sourceDirectory.value / "main" / "python" / "spyt" -> "",
    zipIgnore := { file: File =>
      file.getName.contains("__pycache__") || file.getName.endsWith(".pyc")
    },
    publishYtArtifacts ++= {
      val subdir = if (isSnapshot.value) "snapshots" else "releases"
      val publishDir = s"$sparkYtClientPath/$subdir/${version.value}"
      val link = if (!isSnapshot.value) {
        Seq(YtPublishLink(publishDir, s"$sparkYtLegacyClientPath/$subdir", None, version.value, isSnapshot.value))
      } else Nil

      link ++ Seq(
        YtPublishFile(assembly.value, publishDir, proxy = None, isSnapshot.value),
        YtPublishFile(zip.value, publishDir, proxy = None, isSnapshot.value)
      )
    },
    assembly / assemblyShadeRules ++= clientShadeRules,
    assembly / test := {},
    pythonDeps := {
      val binBasePath = sourceDirectory.value / "main" / "bin"
      ("jars" -> (`spark-submit` / assembly).value) +: binBasePath.listFiles().map(f => "bin" -> f)
    }
  )

lazy val `file-system` = (project in file("file-system"))
  .enablePlugins(CommonPlugin)
  .dependsOn(`yt-wrapper`, `yt-wrapper` % "test->test")
  .settings(
    libraryDependencies ++= commonDependencies.value,
    libraryDependencies += "net.logstash.log4j" % "jsonevent-layout" % "1.7"
  )
  .settings(
    assembly / assemblyMergeStrategy := {
      case x if x endsWith "ahc-default.properties" => MergeStrategy.first
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x if x endsWith "Log4j2Plugins.dat" => MergeStrategy.last
      case x if x endsWith "git.properties" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_kqueue_x86_64.jnilib" => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyShadeRules ++= clusterShadeRules,
    assembly / test := {}
  )

lazy val `e2e-checker` = (project in file("e2e-checker"))
  .dependsOn(`data-source` % Provided)
  .settings(
    libraryDependencies ++= commonDependencies.value.map(d => if (d.configurations.isEmpty) d % Provided else d),
    libraryDependencies ++= scaldingArgs
  )

lazy val `e2e-test` = (project in file("e2e-test"))
  .enablePlugins(E2ETestPlugin, YtPublishPlugin, BuildInfoPlugin)
  .dependsOn(`yt-wrapper`, `file-system`, `data-source`, `spark-submit`, `e2e-checker`,
    `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    libraryDependencies ++= commonDependencies.value,
    publishYtArtifacts ++= {
      val checker = YtPublishFile((`e2e-checker` / assembly).value, sparkYtE2ETestPath,
        proxy = None, isSnapshot = false, Some("check.jar"))
      val pythonScripts: Seq[YtPublishArtifact] = (sourceDirectory.value / "test" / "python")
        .listFiles()
        .map { script =>
          YtPublishFile(script, s"$sparkYtE2ETestPath/${script.getName.dropRight(3)}",
            proxy = None, isSnapshot = false, Some("job.py"))
        }
      checker +: pythonScripts
    },
    buildInfoKeys := Seq[BuildInfoKey](ThisBuild / spytClientVersion),
    buildInfoPackage := "ru.yandex.spark.yt.e2e"
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
    `cluster`,
    `file-system`,
    `data-source`,
    `spark-submit`
  )
  .settings(
    spytPublishCluster := (cluster / publishYt).value,
    spytPublishClient := Def.sequential(
      `data-source` / publishYt,
      `data-source` / pythonBuildAndUpload
    ).value,
    spytPublishSparkFork := Def.sequential(
      `spark-fork` / publishYt,
      `spark-fork` / pythonBuildAndUpload
    ).value,
    spytMvnInstallSparkFork := (`spark-fork` / sparkMvnInstall).value,
    spytMvnDeploySparkFork := (`spark-fork` / sparkMvnDeploy).value
  )


