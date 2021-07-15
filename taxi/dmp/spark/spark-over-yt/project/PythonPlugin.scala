package spyt

import sbt.Keys._
import sbt._

import scala.sys.process._

object PythonPlugin extends AutoPlugin {
  override def trigger = NoTrigger

  override def requires = empty

  object autoImport {
    val pythonCommand = settingKey[String]("")
    val pythonSetupName = settingKey[String]("")
    val pythonBuildDir = settingKey[File]("")

    val pythonClean = taskKey[Unit]("")
    val pythonWheel = taskKey[Unit]("")
    val pythonUpload = taskKey[Unit]("")
    val pythonBuildAndUpload = taskKey[Unit]("")
  }

  import autoImport._

  private def runCommand(command: String, cwd: File): Unit = {
    val res = Process(command, cwd) !

    if (res != 0) {
      throw new IllegalStateException(s"Fail in command: $command")
    }
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    pythonCommand := "python3",
    pythonSetupName := "setup.py",
    pythonBuildDir := sourceDirectory.value / "main" / "python",

    pythonClean := {
      val dirs = Seq("dist", "build").map(pythonBuildDir.value / _)
      dirs.foreach(FileUtils.deleteDirectory(_, recursive = true))
    },
    pythonWheel := {
      val command = s"${pythonCommand.value} ${pythonSetupName.value} sdist bdist_wheel"
      runCommand(command, pythonBuildDir.value)
    },
    pythonUpload := {
      val log = streams.value.log
      val command = s"${pythonCommand.value} -m twine upload -r yandex --verbose dist/*"
      if (sys.env.get("RELEASE_TEST").exists(_.toBoolean)) {
        log.info(s"RELEASE_TEST: run $command")
      } else {
        runCommand(command, pythonBuildDir.value)
      }
    },
    pythonBuildAndUpload := Def.sequential(
      pythonClean,
      pythonWheel,
      pythonUpload
    ).value
  )
}
