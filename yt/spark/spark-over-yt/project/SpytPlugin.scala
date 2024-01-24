package spyt

import sbt.Keys._
import sbt.PluginTrigger.NoTrigger
import sbt._
import sbtrelease.Versions
import spyt.ReleaseUtils.{runProcess, updatePythonVersion}
import spyt.SparkPackagePlugin.autoImport._
import spyt.SpytRelease._
import spyt.SpytSnapshot._

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import scala.reflect.io.Directory

object SpytPlugin extends AutoPlugin {
  override def trigger = NoTrigger

  override def requires = super.requires

  object autoImport {
    val spytVersion = settingKey[String]("Spyt client version")
    val spytPythonVersion = settingKey[String]("ytsaurus-spyt version")

    val spytSparkVersion = settingKey[String]("ytsaurus-spark version")
    val spytSparkPythonVersion = settingKey[String]("ytsaurus-spark version")

    val spytVersions = settingKey[Versions]("")
    val sparkVersions = settingKey[Versions]("")

    val pypiRegistry = settingKey[String]("PyPi registry to use")

    val spytPublishSnapshot = taskKey[Unit]("Publish spyt with snapshot version")
    val spytPublishSparkForkSnapshot = taskKey[Unit]("Publish spyt and spark fork with snapshot version")

    val spytPublishRelease = taskKey[Unit]("Publish spyt with release version")
    val spytPublishSparkForkRelease = taskKey[Unit]("Publish spyt and spark fork with release version")

    val prepareBuildDirectory = taskKey[Unit]("")
    val spytPublish = taskKey[Unit]("Publish spyt")
    val spytPublishSparkFork = taskKey[Unit]("Publish spyt and spark fork")

    val spytPublishLibraries = taskKey[Unit]("Publish spyt libraries")

    val spytMvnInstallSparkFork = taskKey[Unit]("Install spark fork to local mvn")
    val spytMvnDeploySparkFork = taskKey[Unit]("Deploy spark fork")

    val spytUpdatePythonVersion = taskKey[Unit]("Update versions in data-source/version.py")

    val spytArtifacts = taskKey[Unit]("Build Spyt artifacts")

    val setupSpytEnvScript = taskKey[File]("Script for preparing SPYT environment inside YT job")

    val spytVersionFile = settingKey[File]("Spyt client version")
    val spytVersionPyFile = settingKey[File]("Spyt client version")
    val spytSparkVersionFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkVersionPyFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkPomFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkDependencyFile = settingKey[File]("ytsaurus-spark version")
    val spytBuildDirectory = settingKey[File]("Build directory")

    def publishRepoEnabled: Boolean = Option(System.getProperty("publishRepo")).exists(_.toBoolean)
    def publishMavenCentralEnabled: Boolean = Option(System.getProperty("publishMavenCentral")).exists(_.toBoolean)
    def publishYtEnabled: Boolean = Option(System.getProperty("publishYt")).forall(_.toBoolean)
    def customSpytVersion: Option[String] = Option(System.getProperty("customSpytVersion"))
    def customSparkForkVersion: Option[String] = Option(System.getProperty("customSparkForkVersion"))
    def gpgPassphrase: Option[String] = Option(System.getProperty("gpg.passphrase"))

    private def getBuildDirectory(rootDirectory: File): File = {
      rootDirectory / "build_output"
    }

    def deleteBuildDirectory(rootDirectory: File): Unit = {
      val directory = new Directory(getBuildDirectory(rootDirectory))
      directory.deleteRecursively()
    }

    def makeLinkToBuildDirectory(target: File, rootDirectory: File, linkName: String): File = {
      val linksDirectory = getBuildDirectory(rootDirectory)
      linksDirectory.mkdirs()
      val linkFile = linksDirectory / linkName
      if (linkFile.exists()) linkFile.delete()
      val linkFile2 = Files.createSymbolicLink(linkFile.toPath, target.toPath)
      linkFile2.toFile
    }

    def makeLinksToBuildDirectory(files: Seq[File], rootDirectory: File): Unit = {
      files.foreach(file => makeLinkToBuildDirectory(file, rootDirectory, file.getName))
    }

    def dumpYsonToConfBuildDirectory(config: YsonableConfig, rootDirectory: File, fileName: String): File = {
      val confDirectory = getBuildDirectory(rootDirectory) / "conf"
      confDirectory.mkdirs()
      val confFile = confDirectory / fileName
      val confFile2 = Files.writeString(confFile.toPath, config.toYson).toFile
      confFile2
    }

    def dumpVersionsToBuildDirectory(versions: Map[String, String], rootDirectory: File, fileName: String): File = {
      val versionsDirectory = getBuildDirectory(rootDirectory)
      versionsDirectory.mkdirs()
      val confFile = versionsDirectory / fileName
      val serializedPairs = versions.toSeq.map {
        case (key, value) => s""""$key":"$value""""
      }
      val json = serializedPairs.mkString("{", ",", "}")
      val confFile2 = Files.writeString(confFile.toPath, json).toFile
      confFile2
    }

    def copySidecarConfigsToBuildDirectory(rootDirectory: File, files: Seq[File],
                                           dirName: String = "sidecar_configs"): Unit = {
      val sidecarConfDirectory = getBuildDirectory(rootDirectory) / "conf" / dirName
      sidecarConfDirectory.mkdirs()
      files.foreach {
        file =>
          val targetFile = sidecarConfDirectory / file.getName
          Files.copy(file.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    }
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
    spytVersionFile := baseDirectory.value / "version.sbt",
    spytSparkVersionFile := baseDirectory.value / "spark_version.sbt",
    spytSparkPomFile := baseDirectory.value.getParentFile / "spark" / "pom.xml",
    spytSparkDependencyFile := baseDirectory.value / "project" / "SparkForkVersion.scala",

    pypiRegistry := "https://pypi.org/simple",

    spytVersionPyFile := baseDirectory.value / "spyt-package" / "src" / "main" / "python" / "spyt" / "version.py",
    spytSparkVersionPyFile := (ThisBuild / sparkVersionPyFile).value,

    spytUpdatePythonVersion := {
      updatePythonVersion(
        (ThisBuild / spytPythonVersion).value,
        (ThisBuild / spytVersion).value,
        spytVersionPyFile.value,
        (ThisBuild / spytSparkPythonVersion).value,
        spytSparkVersionPyFile.value
      )
    },

    spytPublishSnapshot := {
      runProcess(state.value, spytSnapshotProcess)
    },

    spytPublishSparkForkSnapshot := {
      runProcess(state.value, sparkForkSnapshotProcess)
    },

    spytPublishRelease := {
      runProcess(state.value, spytReleaseProcess)
    },

    spytPublishSparkForkRelease := {
      runProcess(state.value, sparkForkReleaseProcess)
    }
  )
}
