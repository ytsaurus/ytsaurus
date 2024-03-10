import Dependencies._
import com.jsuereth.sbtpgp.PgpKeys.publishSigned
import sbt.io.Path.relativeTo
import spyt.PythonPlugin.autoImport._
import spyt.SparkPackagePlugin.autoImport._
import spyt.SparkPaths._
import spyt.SpytPlugin.autoImport._
import spyt.TarArchiverPlugin.autoImport._
import spyt.YtPublishPlugin.autoImport._

lazy val `spark-patch` = (project in file("spark-patch"))
  .settings(
    libraryDependencies ++= spark ++ livy,
    Compile / packageBin / packageOptions +=
      Package.ManifestAttributes(new java.util.jar.Attributes.Name("PreMain-Class") -> "tech.ytsaurus.spyt.patch.SparkPatchAgent")
  )

lazy val `yt-wrapper` = (project in file("yt-wrapper"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= sttp,
    libraryDependencies ++= ytsaurusClient,
    libraryDependencies ++= logging,
    libraryDependencies ++= testDeps,
    libraryDependencies ++= spark,
    buildInfoKeys := Seq[BuildInfoKey](version, BuildInfoKey.constant(("ytClientVersion", ytsaurusClientVersion))),
    buildInfoPackage := "tech.ytsaurus.spyt"
  )

lazy val `file-system` = (project in file("file-system"))
  .enablePlugins(CommonPlugin)
  .dependsOn(`yt-wrapper` % "compile->compile;test->test;provided->provided")

lazy val `data-source` = (project in file("data-source"))
  .configs(IntegrationTest)
  .dependsOn(`file-system` % "compile->compile;test->test;provided->provided", `spark-patch` % Provided)
  .enablePlugins(JavaAgent)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= itTestDeps,
    resolvedJavaAgents := Seq(JavaAgent.ResolvedAgent(
      JavaAgent.AgentModule("spark-patch", null, JavaAgent.AgentScope(test = true, dist = false), ""),
      (`spark-patch` / Compile / packageBin).value
    ))
  )

lazy val `spark-fork` = (project in file("spark-fork"))
  .enablePlugins(SparkPackagePlugin, PythonPlugin)
  .settings(
    sparkAdditionalJars := Nil,
    sparkAdditionalBin := Nil,
    sparkAdditionalPython := Nil
  )
  .settings(
    tarArchiveMapping += sparkPackage.value -> "spark",
    tarArchivePath := Some(target.value / s"spark.tgz"),
    tarSparkArchiveBuild := {
      val tarFile = tarArchiveBuild.value // Forces build
      makeLinkToBuildDirectory(tarFile, baseDirectory.value.getParentFile, tarFile.getName)
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

lazy val `resource-manager` = (project in file("resource-manager"))
  .settings(
    libraryDependencies ++= spark ++ ytsaurusClient ++ sparkTest
  )

lazy val `cluster` = (project in file("spark-cluster"))
  .configs(IntegrationTest)
  .dependsOn(`data-source` % "compile->compile;test->test;provided->provided")
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= scalatra,
    libraryDependencies ++= scalatraTestDeps,
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`cluster` % "compile->compile;test->test;provided->provided")
  .settings(
    libraryDependencies ++= scaldingArgs,
  )

lazy val `spyt-package` = (project in file("spyt-package"))
  .enablePlugins(JavaAppPackaging, PythonPlugin)
  .dependsOn(`spark-submit` % "compile->compile;test->test;provided->provided", `resource-manager`, `spark-patch`)
  .settings(

    // These dependencies are already provided by spark distributive
    excludeDependencies ++= Seq(
      "commons-lang" % "commons-lang",
      "org.apache.commons" % "commons-lang3",
      "org.typelevel" %% "cats-kernel",
      "org.lz4" % "lz4-java",
      "com.chuusai" %% "shapeless",
      "io.dropwizard.metrics" % "metrics-core",
      "com.google.protobuf" % "protobuf-java",
      "org.slf4j" % "slf4j-api",
      "org.scala-lang.modules" %% "scala-parser-combinators",
      "org.scala-lang.modules" %% "scala-xml",
    ),
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "org.apache.httpcomponents")
    ),

    Compile / discoveredMainClasses := Seq(),
    Universal / packageName := "spyt-package",
    Universal / mappings ++= {
      val dir = sourceDirectory.value / "main" / "spark-extra"
      (dir ** AllPassFilter --- dir) pair relativeTo(dir)
    },
    Universal / mappings ++= {
      val dir = sourceDirectory.value / "main" / "python" / "spyt"
      val pythonFilter = new SimpleFileFilter(file =>
        !(file.getName.contains("__pycache__") || file.getName.endsWith(".pyc"))
      )
      (dir ** pythonFilter --- dir) pair relativeTo(dir.getParentFile.getParentFile)
    },
    Universal / mappings := {
      val oldMappings = (Universal / mappings).value
      val scalaLibs = scalaInstance.value.libraryJars.toSet
      oldMappings.filterNot(lib => scalaLibs.contains(lib._1)).map { case (file, targetPath) =>
        file -> (if (targetPath.startsWith("lib/")) s"jars/${targetPath.substring(4)}" else targetPath)
      }
    },

    setupSpytEnvScript := sourceDirectory.value / "main" / "bash" / "setup-spyt-env.sh",

    pythonDeps := {
      val packagePaths = (Universal / mappings).value.flatMap {
        case (file, path) if !file.isDirectory =>
          val target = path.substring(0, path.lastIndexOf("/")) match {
            case dir@("jars"|"bin"|"conf") => s"spyt/$dir"
            case dir@("python/spyt") => "spyt"
            case x => x
          }
          Some(target -> file)
        case _ => None
      }
      val binBasePath = sourceDirectory.value / "main" / "bin"
      binBasePath.listFiles().map(f => "bin" -> f) ++ packagePaths
    },
    pythonAppends := {
      if (isSnapshot.value) { Seq("spyt/conf/spark-defaults.conf" -> (
        s"spark.ytsaurus.config.releases.path                   //home/spark/conf/snapshots\n" +
          s"spark.ytsaurus.spyt.releases.path                     //home/spark/spyt/snapshots\n" +
          s"spark.ytsaurus.spyt.version                           ${version.value}\n"
        ), "spyt/conf/log4j.properties" -> "log4j.rootLogger=INFO, console\n") } else {
        Seq.empty
      }
    }
  )
  .settings(
    spytArtifacts := {
      val rootDirectory = baseDirectory.value.getParentFile
      val files = Seq((Universal / packageBin).value, setupSpytEnvScript.value)
      makeLinksToBuildDirectory(files, rootDirectory)
      val versionValue = (ThisBuild / spytVersion).value
      val baseConfigDir = (Compile / resourceDirectory).value
      val logger = streams.value.log
      if (configGenerationEnabled) {
        val sidecarConfigsFiles = if (innerSidecarConfigEnabled) {
          spyt.ClusterConfig.innerSidecarConfigs(baseConfigDir)
        } else {
          spyt.ClusterConfig.sidecarConfigs(baseConfigDir)
        }
        copySidecarConfigsToBuildDirectory(rootDirectory, sidecarConfigsFiles)
        val launchConfigYson = spyt.ClusterConfig.launchConfig(versionValue, sidecarConfigsFiles)
        dumpYsonToConfBuildDirectory(launchConfigYson, rootDirectory, "spark-launch-conf")
        val globalConfigYsons = spyt.ClusterConfig.globalConfig(logger, versionValue, baseConfigDir)
        globalConfigYsons.foreach {
          case (proxy, config) => dumpYsonToConfBuildDirectory(config, rootDirectory, s"global-$proxy")
        }
      } else {
        copySidecarConfigsToBuildDirectory(rootDirectory,
          spyt.ClusterConfig.innerSidecarConfigs(baseConfigDir), "inner-sidecar-config")
        copySidecarConfigsToBuildDirectory(rootDirectory,
          spyt.ClusterConfig.sidecarConfigs(baseConfigDir), "sidecar-config")
      }
    },
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytVersion).value
      val sparkVersionValue = (ThisBuild / spytSparkVersion).value
      val isSnapshotValue = isSnapshotVersion(versionValue)
      val isTtlLimited = isSnapshotValue && limitTtlEnabled

      val basePath = versionPath(spytPath, versionValue)
      val sparkPath = versionPath(sparkYtSparkForkPath, sparkVersionValue)

      val sparkLink = Seq(YtPublishLink(s"$sparkPath/spark.tgz", basePath, None, "spark.tgz", isTtlLimited))

      val clusterConfigArtifacts = spyt.ClusterConfig.artifacts(streams.value.log, versionValue,
        (Compile / resourceDirectory).value)

      sparkLink ++ Seq(
        YtPublishFile((Universal / packageBin).value, basePath, None, isTtlLimited = isTtlLimited),
        YtPublishFile(setupSpytEnvScript.value, basePath, None, isTtlLimited = isTtlLimited, isExecutable = true)
      ) ++ clusterConfigArtifacts
    }
  )


lazy val root = (project in file("."))
  .enablePlugins(SpytPlugin)
  .aggregate(
    `yt-wrapper`,
    `file-system`,
    `data-source`,
    `cluster`,
    `resource-manager`,
    `spark-submit`,
    `spark-patch`,
    `spyt-package`
  )
  .settings(
    prepareBuildDirectory := {
      streams.value.log.info(s"Preparing build directory in ${baseDirectory.value}")
      deleteBuildDirectory(baseDirectory.value)
    },
    spytPublish := Def.taskDyn {
      val task1 = if (publishYtEnabled) {
        `spyt-package` / publishYt
      } else {
        streams.value.log.info("Publishing SPYT files to YT is skipped because of disabled publishYt")
        `spyt-package` / spytArtifacts
      }
      val task2 = Def.task {
        if (publishRepoEnabled) {
          (`spyt-package` / pythonBuildAndUpload).value
        } else {
          streams.value.log.info("Publishing spyt client to pypi is skipped because of disabled publishRepo")
          val pythonDist = (`spyt-package` / pythonBuild).value
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
    spytPublishLibraries := {
      if (publishMavenCentralEnabled) {
        Def.sequential(
          `data-source` / publishSigned,
          `spark-submit` / publishSigned,
          `file-system` / publishSigned,
          `yt-wrapper` / publishSigned
        ).value
      } else {
        streams.value.log.info("Publishing spyt libraries to maven is skipped because of disabled publishRepo")
      }
    }
  )
