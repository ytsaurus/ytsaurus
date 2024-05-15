package spyt

import sbt.Keys._
import sbt.PluginTrigger.NoTrigger
import sbt._
import sbtrelease.Versions
import spyt.ReleaseUtils.{runProcess, updatePythonVersion}
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

    val sparkCompileVersion = settingKey[String]("Spark compile version")

    val spytVersions = settingKey[Versions]("")

    val pypiRegistry = settingKey[String]("PyPi registry to use")

    val spytPublishSnapshot = taskKey[Unit]("Publish spyt with snapshot version")

    val spytPublishRelease = taskKey[Unit]("Publish spyt with release version")

    val prepareBuildDirectory = taskKey[Unit]("")
    val spytPublish = taskKey[Unit]("Publish spyt")

    val spytPublishLibraries = taskKey[Unit]("Publish spyt libraries")

    val spytUpdatePythonVersion = taskKey[Unit]("Update versions in data-source/version.py")

    val spytArtifacts = taskKey[Unit]("Build Spyt artifacts")

    val setupSpytEnvScript = taskKey[File]("Script for preparing SPYT environment inside YT job")

    val spytVersionFile = settingKey[File]("Spyt client version")
    val spytVersionPyFile = settingKey[File]("Spyt client version")
    val spytBuildDirectory = settingKey[File]("Build directory")

    def publishRepoEnabled: Boolean = Option(System.getProperty("publishRepo")).exists(_.toBoolean)
    def publishMavenCentralEnabled: Boolean = Option(System.getProperty("publishMavenCentral")).exists(_.toBoolean)
    def publishYtEnabled: Boolean = Option(System.getProperty("publishYt")).forall(_.toBoolean)
    def customSpytVersion: Option[String] = Option(System.getProperty("customSpytVersion"))
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

    pypiRegistry := "https://pypi.org/simple",

    spytVersionPyFile := baseDirectory.value / "spyt-package" / "src" / "main" / "python" / "spyt" / "version.py",

    spytUpdatePythonVersion := {
      updatePythonVersion(
        (ThisBuild / spytPythonVersion).value,
        (ThisBuild / spytVersion).value,
        spytVersionPyFile.value,
        sparkCompileVersion.value
      )
    },

    spytPublishSnapshot := {
      runProcess(state.value, spytSnapshotProcess)
    },

    spytPublishRelease := {
      runProcess(state.value, spytReleaseProcess)
    }
  )
}
