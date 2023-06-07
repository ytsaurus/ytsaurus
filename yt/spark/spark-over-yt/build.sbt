import CommonPlugin.autoImport._
import Dependencies._
import sbtassembly.AssemblyKeys.assembly
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
    libraryDependencies ++= metrics,
    libraryDependencies ++= circe,
    libraryDependencies ++= sttp,
    libraryDependencies ++= ytsaurusClient,
    libraryDependencies ++= logging.map(_ % Provided),
    libraryDependencies ++= testDeps,
    buildInfoKeys := Seq[BuildInfoKey](version, BuildInfoKey.constant(("ytClientVersion", ytsaurusClientVersion))),
    buildInfoPackage := "tech.ytsaurus.spyt"
  )

lazy val `spark-fork` = (project in file("spark-fork"))
  .enablePlugins(SparkPackagePlugin, PythonPlugin)
  .settings(
    sparkAdditionalJars := Seq(
      (`file-system` / assembly).value
    ),
    sparkAdditionalBin := Seq(
      baseDirectory.value / "driver-op-discovery.sh",
      baseDirectory.value / "job-id-discovery.sh",
    ),
    sparkAdditionalPython := Nil
  )
  .settings(
    tarArchiveMapping += sparkPackage.value -> "spark",
    tarArchivePath := Some(target.value / s"spark.tgz"),
    tarSparkArchiveBuild := {
      val tarFile = tarArchiveBuild.value // Forces build
      makeLinkToBuildDirectory(tarFile,  baseDirectory.value.getParentFile, tarFile.getName)
    }
  )
  .settings(
    pythonSetupName := "setup-ytsaurus.py",
    pythonBuildDir := sparkHome.value / "python"
  )
  .settings(
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytSparkVersion).value
      val basePath = versionPath(sparkYtSparkForkPath, versionValue)
      val isSnapshotValue = isSnapshotVersion(versionValue)
      val isTtlLimited = isSnapshotValue && limitTtlEnabled

      Seq(
        YtPublishFile(tarArchiveBuild.value, basePath, None, isTtlLimited = isTtlLimited)
      )
    }
  )

lazy val `cluster` = (project in file("spark-cluster"))
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper`, `file-system`, `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= logging,
    libraryDependencies ++= scalatra,
    libraryDependencies ++= itTestDeps,
    libraryDependencies ++= scalatraTestDeps,
    libraryDependencies ++= spark,
    assembly / assemblyJarName := s"spark-yt-launcher.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true),
    assembly / test := {}
  )
  .settings(
    clusterSpytBuild := {
      val rootDirectory = baseDirectory.value.getParentFile
      val assemblyFile = assembly.value // Forces build
      makeLinkToBuildDirectory(assemblyFile, rootDirectory, assemblyFile.getName)
      val versionValue = (ThisBuild / spytClusterVersion).value
      val baseConfigDir = (Compile / resourceDirectory).value
      val sidecarConfigsFiles = spyt.ClusterConfig.sidecarConfigs(baseConfigDir)
      val launchConfigYson = spyt.ClusterConfig.launchConfig(versionValue, sidecarConfigsFiles)
      val globalConfigYsons = spyt.ClusterConfig.globalConfig(streams.value.log, versionValue, baseConfigDir)
      copySidecarConfigsToBuildDirectory(rootDirectory, sidecarConfigsFiles)
      dumpYsonToConfBuildDirectory(launchConfigYson, rootDirectory, "spark-launch-conf")
      globalConfigYsons.foreach {
        case (proxy, config) => dumpYsonToConfBuildDirectory(config, rootDirectory, s"global-$proxy")
      }
    },
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytClusterVersion).value
      val sparkVersionValue = (ThisBuild / spytSparkVersion).value
      val isSnapshotValue = isSnapshotVersion(versionValue)
      val isTtlLimited = isSnapshotValue && limitTtlEnabled

      val basePath = versionPath(sparkYtClusterPath, versionValue)
      val sparkPath = versionPath(sparkYtSparkForkPath, sparkVersionValue)

      val sparkLink = Seq(YtPublishLink(s"$sparkPath/spark.tgz", basePath, None, "spark.tgz", isTtlLimited))

      val clusterConfigArtifacts = spyt.ClusterConfig.artifacts(streams.value.log, versionValue,
        (Compile / resourceDirectory).value)

      sparkLink ++ Seq(
        YtPublishFile(assembly.value, basePath, None, isTtlLimited = isTtlLimited),
      ) ++ clusterConfigArtifacts
    }
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`yt-wrapper` % Provided)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= py4j,
    libraryDependencies ++= ytsaurusClient.map(_ % Provided) ++ (ThisBuild / spytSparkForkDependency).value ++
      circe.map(_ % Provided) ++ logging.map(_ % Provided),
    assembly / assemblyJarName := s"spark-yt-submit.jar",
    assembly / assemblyShadeRules ++= clusterShadeRules,
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
  )

lazy val `submit-client` = (project in file("submit-client"))
  .dependsOn(`spark-submit`, `file-system`)
  .settings(
    libraryDependencies ++= spark,
    libraryDependencies ++= ytsaurusClient ++ circe ++ logging
  )

lazy val `data-source` = (project in file("data-source"))
  .enablePlugins(PythonPlugin)
  .configs(IntegrationTest)
  .dependsOn(`yt-wrapper` % "compile->compile;test->test", `file-system` % "compile->compile;test->test")
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
    clientSpytBuild := {
      val assemblyFile = assembly.value // Forces build
      makeLinkToBuildDirectory(assemblyFile, baseDirectory.value.getParentFile, assemblyFile.getName)
      val zipFile = zip.value // Forces build
      makeLinkToBuildDirectory(zipFile, baseDirectory.value.getParentFile, zipFile.getName)
    },
    publishYtArtifacts ++= {
      val subdir = if (isSnapshot.value) "snapshots" else "releases"
      val publishDir = s"$sparkYtClientPath/$subdir/${version.value}"
      val isTtlLimited = isSnapshot.value && limitTtlEnabled

      Seq(
        YtPublishFile(assembly.value, publishDir, proxy = None, isTtlLimited = isTtlLimited),
        YtPublishFile(zip.value, publishDir, proxy = None, isTtlLimited = isTtlLimited)
      )
    },
    assembly / assemblyShadeRules ++= clientShadeRules,
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    pythonDeps := {
      val binBasePath = sourceDirectory.value / "main" / "bin"
      ("jars" -> (`spark-submit` / assembly).value) +: binBasePath.listFiles().map(f => "bin" -> f)
    }
  )

lazy val `file-system` = (project in file("file-system"))
  .enablePlugins(CommonPlugin)
  .dependsOn(`yt-wrapper` % "compile->compile;test->test")
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
    assembly / test := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
  )

lazy val `e2e-checker` = (project in file("e2e-checker"))
  .dependsOn(`data-source` % Provided)
  .settings(
    libraryDependencies ++= commonDependencies.value.map(d => if (d.configurations.isEmpty) d % Provided else d),
    libraryDependencies ++= scaldingArgs,
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
  )

lazy val `e2e-test` = (project in file("e2e-test"))
  .enablePlugins(E2ETestPlugin, YtPublishPlugin, SparkPackagePlugin, BuildInfoPlugin)
  .dependsOn(`yt-wrapper`, `file-system`, `data-source`, `spark-submit`, `e2e-checker`, `spark-fork`,
    `yt-wrapper` % "test->test", `file-system` % "test->test")
  .settings(
    sparkAdditionalJars := Nil,
    sparkAdditionalBin := Nil,
    sparkAdditionalPython := Nil,
    libraryDependencies ++= commonDependencies.value,
    publishYtArtifacts ++= {
      val tempFolder = YtPublishDirectory(e2eTestUDirPath, proxy = None,
        isTtlLimited = true, forcedTTL = Some(e2eDirTTL))
      val checker = YtPublishFile((`e2e-checker` / assembly).value, e2eTestUDirPath,
        proxy = None, Some("check.jar"))
      val pythonScripts: Seq[YtPublishArtifact] = (sourceDirectory.value / "test" / "python" / "scripts")
        .listFiles()
        .map { script =>
          YtPublishFile(script, s"$e2eTestUDirPath/scripts",
            proxy = None)
        }
      tempFolder +: checker +: pythonScripts
    },
    Test / javaOptions ++= Seq(s"-De2eTestHomePath=$sparkYtE2ETestPath"),
    Test / javaOptions ++= Seq(s"-De2eTestUDirPath=$e2eTestUDirPath"),
    Test / javaOptions ++= Seq(s"-Dproxies=$onlyYtProxy"),
    Test / javaOptions ++= Seq(s"-DdiscoveryPath=$discoveryPath"),
    Test / javaOptions ++= Seq(s"-DclientVersion=${e2eClientVersion.value}"),
    Test / javaOptions ++= Seq("-cp", ".tox/py37/lib/python3.7/site-packages/pyspark/jars:.tox/py37/lib/python3.7/site-packages/spyt/jars")
  )

lazy val maintenance = (project in file("maintenance"))
  .dependsOn(`data-source`)
  .settings(
    libraryDependencies ++= ytsaurusClient ++ sparkRuntime ++ circe ++ logging
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
//    assembly / mainClass := Some("tech.ytsaurus.spark.test.Test"),
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
    prepareBuildDirectory := {
      streams.value.log.info(s"Preparing build directory in ${baseDirectory.value}")
      deleteBuildDirectory(baseDirectory.value)
    },
    spytPublishCluster := {
      if (publishYtEnabled) {
        (cluster / publishYt).value
      } else {
        streams.value.log.info("Publishing cluster files to YT is skipped because of disabled publishYt")
        (cluster / clusterSpytBuild).value
      }
    },
    spytPublishClient := Def.taskDyn {
      val task1 = if (publishYtEnabled) {
        `data-source` / publishYt
      } else {
        streams.value.log.info("Publishing client files to YT is skipped because of disabled publishYt")
        `data-source` / clientSpytBuild
      }
      val task2 = Def.task {
        if (publishRepoEnabled) {
          (`data-source` / pythonBuildAndUpload).value
        } else {
          streams.value.log.info("Publishing spyt client to pypi is skipped because of disabled publishRepo")
          val pythonDist = (`data-source` / pythonBuild).value
          makeLinkToBuildDirectory(pythonDist, baseDirectory.value, "ytsaurus-spyt")
        }
      }
      Def.sequential(task1, task2)
    }.value,
    spytPublishSparkFork := Def.taskDyn {
      val task1 = if (publishYtEnabled) {
        `spark-fork` / publishYt
      } else {
        streams.value.log.info("Publishing spark fork to YT is skipped because of disabled publishYt")
        `spark-fork` / tarSparkArchiveBuild
      }
      val task2 = Def.task {
        if (publishRepoEnabled) {
          (`spark-fork` / pythonBuildAndUpload).value
        } else {
          streams.value.log.info("Publishing pyspark fork to pypi is skipped because of disabled publishRepo")
          val pythonDist = (`spark-fork` / pythonBuild).value
          makeLinkToBuildDirectory(pythonDist, baseDirectory.value, "ytsaurus-pyspark")
        }
      }
      Def.sequential(task1, task2)
    }.value,
    spytMvnInstallSparkFork := (`spark-fork` / sparkMvnInstall).value,
    spytMvnDeploySparkFork := (`spark-fork` / sparkMvnDeploy).value,
    spytPublishLibraries := Def.sequential(
      `data-source` / publish,
      `spark-submit` / publish,
      `submit-client` / publish,
      `file-system` / publish,
      `yt-wrapper` / publish
    ).value
  )


