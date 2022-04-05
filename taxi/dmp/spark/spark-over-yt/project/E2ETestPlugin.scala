import sbt.Keys.{TaskStreams, streams, test}
import sbt._
import sbt.plugins.JvmPlugin
import sbtbuildinfo.BuildInfoPlugin
import spyt.SparkPaths.sparkYtE2ETestPath
import spyt.SpytPlugin.autoImport.spytClientVersion
import spyt.YtPublishPlugin

import java.time.Duration
import java.util.UUID
import scala.sys.process.Process

object E2ETestPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin && BuildInfoPlugin

  object autoImport {
    lazy val e2eDirTTL = Duration.ofMinutes(30).toMillis
    lazy val e2eTestUDirPath = s"$sparkYtE2ETestPath/${UUID.randomUUID()}"

    lazy val e2eTest = taskKey[Unit]("Run all e2e tests")
    lazy val e2eScalaTest = taskKey[Unit]("Run scala e2e tests")
    lazy val e2ePythonTest = taskKey[Unit]("Run python e2e tests")
    lazy val e2eScalaTestImpl = taskKey[Unit]("Run scala e2e tests process")
    lazy val e2ePythonTestImpl = taskKey[Unit]("Run python e2e tests process")

    def clientVersionKey: SettingKey[String] = ThisBuild / spytClientVersion
  }

  import YtPublishPlugin.autoImport._
  import autoImport._


  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    e2eScalaTest := Def.sequential(publishYt, e2eScalaTestImpl).value,
    e2ePythonTest := Def.sequential(publishYt, e2ePythonTestImpl).value,
    e2eTest := Def.sequential(publishYt, e2eScalaTestImpl, e2ePythonTestImpl).value,
    e2eScalaTestImpl := (Test / test).value,
    e2ePythonTestImpl := {
      val command = "pytest --show-progress --durations=0 e2e-test/src/test/python/pytest"
      val workingDirectory = new File(".")
      val spytPathToSource = workingDirectory.toPath.resolve("data-source/src/main/python").toAbsolutePath
      val s: TaskStreams = streams.value
      val exitCode = Process(command,
        // optional
        workingDirectory,
        // optional system variables
        "PYTHONPATH" -> spytPathToSource.toString,
        "e2eTestHomePath" -> sparkYtE2ETestPath,
        "e2eTestUDirPath" -> e2eTestUDirPath,
        "proxies" -> onlyYtProxy,
        "clientVersion" -> clientVersionKey.value) ! s.log
      if (exitCode != 0) throw new IllegalStateException(s"Exit code is $exitCode")
    }
  )

}
