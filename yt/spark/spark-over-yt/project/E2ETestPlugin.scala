import sbt.Keys.{TaskStreams, streams, test}
import sbt._
import sbt.plugins.JvmPlugin
import sbtbuildinfo.BuildInfoPlugin
import spyt.SparkPackagePlugin.autoImport.sparkAddCustomFiles
import spyt.SparkPaths.sparkYtE2ETestPath
import spyt.SpytPlugin.autoImport.{spytClientPythonVersion, spytClientVersion, spytClusterVersion, spytPublishClusterSnapshot}
import spyt.{SparkPackagePlugin, YtPublishPlugin}

import java.time.Duration
import java.util.UUID
import scala.sys.process.{Process, ProcessLogger}

object E2ETestPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin && SparkPackagePlugin && BuildInfoPlugin

  object autoImport {
    lazy val e2eDirTTL = Duration.ofHours(2).toMillis
    lazy val e2eTestUDirPath = s"$sparkYtE2ETestPath/${UUID.randomUUID()}"

    lazy val e2eConfigShow = taskKey[Unit]("Show e2e configuration")
    lazy val e2ePreparation = taskKey[Unit]("Prepare e2e environment for running tests")
    lazy val e2eTest = taskKey[Unit]("Run all e2e tests")
    lazy val e2eScalaTest = taskKey[Unit]("Run scala e2e tests")
    lazy val e2ePythonTest = taskKey[Unit]("Run python e2e tests")
    lazy val e2eScalaTestImpl = taskKey[Unit]("Run scala e2e tests process")
    lazy val e2ePythonTestImpl = taskKey[Unit]("Run python e2e tests process")
    lazy val e2eFullCircleTest = taskKey[Unit]("Build e2e environment and run all tests")
    lazy val e2eTestImpl = taskKey[Unit]("")
    lazy val e2ePrepareEnv = taskKey[Unit]("")
    lazy val e2eLaunchCluster = taskKey[Unit]("")
    lazy val e2eDiscoveryCluster = taskKey[Unit]("")

    lazy val e2eClientVersion: TaskKey[String] = taskKey[String]("Client version for e2e tests, " +
      "default is current client version")
    lazy val e2ePythonClientVersion: SettingKey[String] = ThisBuild / spytClientPythonVersion
  }

  import YtPublishPlugin.autoImport._
  import autoImport._

  private def runProcess(command: String, log: Logger, env: (String, String)*): Unit = {
    log.info(s"Running command: $command")
    log.info(s"Process environment: $env")
    val process = Process(
      command,
      new File("."),
      env : _*
    ).run(ProcessLogger(log.info(_)))
    val exitCode = process.exitValue()
    if (exitCode != 0) throw new IllegalStateException(s"Process running is failed (exit code = $exitCode)")
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    e2eFullCircleTest := Def.sequential(e2ePrepareEnv, e2eLaunchCluster, e2eDiscoveryCluster, e2eTest).value,
    e2eConfigShow := {
      val log = streams.value.log
      log.info(s"===== E2E CONFIGURATION =====")
      log.info(s"Input home: $sparkYtE2ETestPath")
      log.info(s"Temp user location: $e2eTestUDirPath")
      log.info(s"Proxy: $onlyYtProxy")
      log.info(s"Client version: ${e2eClientVersion.value}")
      log.info(s"Python client version: ${e2ePythonClientVersion.value}")
      log.info(s"=====")
    },
    e2ePreparation := Def.sequential(e2eConfigShow, publishYt, sparkAddCustomFiles).value,
    e2eScalaTest := Def.sequential(e2ePreparation, e2eScalaTestImpl).value,
    e2ePythonTest := Def.sequential(e2ePreparation, e2ePythonTestImpl).value,
    e2eTestImpl := Def.taskDyn {
      val scalaTestSuccess = e2eScalaTestImpl.result.value.toEither.isRight
      Def.task {
        val pythonTestSuccess = e2ePythonTestImpl.result.value.toEither.isRight
        if (!scalaTestSuccess || !pythonTestSuccess) {
          throw new IllegalStateException("Tests are failed")
        }
      }
    }.value,
    e2eTest := Def.sequential(e2ePreparation, e2eTestImpl).value,
    e2eScalaTestImpl := (Test / test).value,
    e2eClientVersion := {
      Option(System.getProperty("clientVersion")).getOrElse((ThisBuild / spytClientVersion).value)
    },
    e2ePrepareEnv := {
      runProcess(
        "tox -e ALL --notest",
        streams.value.log,
        "PYTHON_CLIENT_VERSION" -> e2ePythonClientVersion.value
      )
    },
    e2eLaunchCluster := {
      val sparkLaunchYtCommand = Seq(
        ".tox/py37/bin/spark-launch-yt",
        "--proxy", onlyYtProxy,
        "--abort-existing",
        "--discovery-path", discoveryPath,
        "--worker-cores", "12",
        "--worker-num", "2",
        "--worker-memory", "48G",
        "--tmpfs-limit", "24G",
        "--spark-cluster-version", (ThisBuild / spytClusterVersion).value,
        "--enable-advanced-event-log",
      )
      runProcess(
        sparkLaunchYtCommand.mkString(" "),
        streams.value.log
      )
    },
    e2eDiscoveryCluster := {
      val log = streams.value.log
      log.info("===== CLUSTER =====")
      val sparkDiscoveryYtCommand = Seq(
        ".tox/py37/bin/spark-discovery-yt",
        "--proxy", onlyYtProxy,
        "--discovery-path", discoveryPath
      )
      runProcess(
        sparkDiscoveryYtCommand.mkString(" "),
        log
      )
      log.info("=====")
    },
    e2ePythonTestImpl := {
      runProcess(
        "tox test",
        streams.value.log,
        "SPARK_HOME" -> ".tox/py37/lib/python3.7/site-packages/pyspark",
        "DISCOVERY_PATH" -> discoveryPath,
        "E2E_TEST_HOME_PATH" -> sparkYtE2ETestPath,
        "E2E_TEST_UDIR_PATH" -> e2eTestUDirPath,
        "PROXIES" -> onlyYtProxy,
        "CLIENT_VERSION" -> e2eClientVersion.value,
        "PYTHON_CLIENT_VERSION" -> e2ePythonClientVersion.value
      )
    }
  )

}
