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
    val pythonWheel = taskKey[File]("")
    val pythonUpload = taskKey[Unit]("")
    val pythonBuild = taskKey[File]("")
    val pythonBuildAndUpload = taskKey[Unit]("")

    val pythonDeps = taskKey[Seq[(String, File)]]("")
    val pythonAppends = taskKey[Seq[(String, String)]]("")
  }

  import autoImport._

  private def runCommand(command: String, cwd: File): Unit = {
    val res = Process(command, cwd) !

    if (res != 0) {
      throw new IllegalStateException(s"Fail in command: $command")
    }
  }

  private def createDeps(deps: File, files: Seq[(String, File)]): Unit = {
    if (files.nonEmpty) deps.mkdirs()
    files.foreach { case (relativePath, file) =>
      val targetDir = new File(deps, relativePath)
      targetDir.mkdirs()
      val target = new File(targetDir, file.getName)
      if (file.isDirectory) {
        IO.copyDirectory(file, target)
      } else if (file.isFile) {
        IO.copyFile(file, target)
      }
    }
  }

  private def preProcessDeps(deps: File, appends: Seq[(String, String)]): Unit = {
    appends.foreach { case (relativePath, append) =>
      IO.append(new File(deps, relativePath), append)
    }
  }

  private def pypiPassword: Option[String] = Option(System.getProperty("pypi.password"))

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    pythonCommand := "python3",
    pythonSetupName := "setup.py",
    pythonBuildDir := target.value / "python",

    pythonClean := {
      val dirs = Seq("dist", "build", "deps").map(pythonBuildDir.value / _)
      dirs.foreach(FileUtils.deleteDirectory(_, recursive = true))
    },
    pythonWheel := {
      val pythonSources = sourceDirectory.value / "main" / "python"
      if (pythonSources != null && pythonSources.listFiles() != null) {
        createDeps(pythonBuildDir.value, pythonSources.listFiles().map(f => "" -> f))
      }
      val deps = pythonBuildDir.value / "deps"
      createDeps(deps, pythonDeps.value)
      preProcessDeps(deps, pythonAppends.value)
      val command = s"${pythonCommand.value} ${pythonSetupName.value} sdist bdist_wheel"
      runCommand(command, pythonBuildDir.value)
      pythonBuildDir.value / "dist"
    },
    pythonUpload := {
      val log = streams.value.log
      val dPassword = pypiPassword.map(x => s"-p $x").getOrElse("")
      val command = s"${pythonCommand.value} -m twine upload --non-interactive --repository-url https://upload.pypi.org/legacy/ -u __token__ $dPassword --verbose dist/*"
      if (sys.env.get("RELEASE_TEST").exists(_.toBoolean)) {
        log.info(s"RELEASE_TEST: run $command")
      } else {
        runCommand(command, pythonBuildDir.value)
      }
    },
    pythonBuild := Def.sequential(
      pythonClean,
      pythonWheel
    ).value,
    pythonBuildAndUpload := Def.sequential(
      pythonBuild,
      pythonUpload
    ).value,
    pythonDeps := Nil,
    pythonAppends := Nil
  )
}
