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
    val spytClusterVersion = settingKey[String]("Spyt cluster version")

    val spytClientVersion = settingKey[String]("Spyt client version")
    val spytClientPythonVersion = settingKey[String]("ytsaurus-spyt version")

    val spytSparkVersion = settingKey[String]("ytsaurus-spark version")
    val spytSparkPythonVersion = settingKey[String]("ytsaurus-spark version")

    val clientVersions = settingKey[Versions]("")
    val clusterVersions = settingKey[Versions]("")
    val sparkVersions = settingKey[Versions]("")

    val pypiRegistry = settingKey[String]("PyPi registry to use")

    val spytPublishClusterSnapshot = taskKey[Unit]("Publish spyt cluster with snapshot version")
    val spytPublishClientSnapshot = taskKey[Unit]("Publish spyt client with snapshot version")
    val spytPublishSparkForkSnapshot = taskKey[Unit]("Publish spyt client & cluster with snapshot version")

    val spytPublishClusterRelease = taskKey[Unit]("Publish spyt cluster with snapshot version")
    val spytPublishClientRelease = taskKey[Unit]("Publish spyt client with snapshot version")
    val spytPublishSparkForkRelease = taskKey[Unit]("Publish spyt client & cluster with snapshot version")

    val prepareBuildDirectory = taskKey[Unit]("")
    val spytPublishCluster = taskKey[Unit]("Publish spyt cluster")
    val spytPublishClient = taskKey[Unit]("Publish spyt client")
    val spytPublishSparkFork = taskKey[Unit]("Publish spyt client & cluster")

    val spytPublishLibraries = taskKey[Unit]("Publish spyt libraries")

    val spytMvnInstallSparkFork = taskKey[Unit]("Publish spyt client & cluster")
    val spytMvnDeploySparkFork = taskKey[Unit]("Publish spyt client & cluster")

    val spytUpdatePythonVersion = taskKey[Unit]("Update versions in data-source/version.py")

    val clientSpytBuild = taskKey[Unit]("Build Spyt .zip archive")
    val clusterSpytBuild = taskKey[Unit]("Build Spyt binary")

    val spytClusterVersionFile = settingKey[File]("Spyt cluster version")
    val spytClientVersionFile = settingKey[File]("Spyt client version")
    val spytClientVersionPyFile = settingKey[File]("Spyt client version")
    val spytSparkVersionFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkVersionPyFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkPomFile = settingKey[File]("ytsaurus-spark version")
    val spytSparkDependencyFile = settingKey[File]("ytsaurus-spark version")
    val spytBuildDirectory = settingKey[File]("Build directory")

    val spytSparkForkDependency = settingKey[Seq[ModuleID]]("")

    val releaseClusterCommitMessage = taskKey[String]("")
    val releaseNextClusterCommitMessage = taskKey[String]("")
    val releaseClientCommitMessage = taskKey[String]("")
    val releaseNextClientCommitMessage = taskKey[String]("")
    val releaseSparkForkCommitMessage = taskKey[String]("")

    def publishRepoEnabled: Boolean = Option(System.getProperty("publishRepo")).exists(_.toBoolean)
    def publishMavenCentralEnabled: Boolean = Option(System.getProperty("publishMavenCentral")).exists(_.toBoolean)
    def publishYtEnabled: Boolean = Option(System.getProperty("publishYt")).forall(_.toBoolean)
    def customClusterVersion: Option[String] = Option(System.getProperty("customClusterVersion"))
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

  sealed abstract class ReleaseComponent(val name: String)

  object ReleaseComponent {
    case object Cluster extends ReleaseComponent("cluster")

    case object Client extends ReleaseComponent("client")

    case object All extends ReleaseComponent("all")

    def fromString(name: String): Option[ReleaseComponent] = Seq(Cluster, Client, All).find(_.name == name)
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
    spytClusterVersionFile := baseDirectory.value / "cluster_version.sbt",
    spytClientVersionFile := baseDirectory.value / "client_version.sbt",
    spytSparkVersionFile := baseDirectory.value / "spark_version.sbt",
    spytSparkPomFile := baseDirectory.value.getParentFile / "spark" / "pom.xml",
    spytSparkDependencyFile := baseDirectory.value / "project" / "SparkForkVersion.scala",

    pypiRegistry := "https://pypi.org/simple",

    spytClientVersionPyFile := baseDirectory.value / "data-source" / "src" / "main" / "python" / "spyt" / "version.py",
    spytSparkVersionPyFile := (ThisBuild / sparkVersionPyFile).value,

    spytUpdatePythonVersion := {
      updatePythonVersion(
        (ThisBuild / spytClientPythonVersion).value,
        (ThisBuild / spytClientVersion).value,
        spytClientVersionPyFile.value,
        (ThisBuild / spytSparkPythonVersion).value,
        spytSparkVersionPyFile.value
      )
    },

    spytPublishClientSnapshot := {
      runProcess(state.value, clientSnapshotProcess)
    },

    spytPublishClusterSnapshot := {
      runProcess(state.value, clusterSnapshotProcess)
    },

    spytPublishSparkForkSnapshot := {
      runProcess(state.value, sparkForkSnapshotProcess)
    },

    releaseClusterCommitMessage := s"Release cluster and client ${(ThisBuild / spytClusterVersion).value}",
    releaseNextClusterCommitMessage := s"Start cluster and client ${(ThisBuild / spytClusterVersion).value}",
    releaseClientCommitMessage := s"Release client ${(ThisBuild / spytClientVersion).value}",
    releaseNextClientCommitMessage := s"Start client ${(ThisBuild / spytClientVersion).value}",
    releaseSparkForkCommitMessage := s"Release spark fork ${(ThisBuild / spytSparkVersion).value}",

    spytPublishClientRelease := {
      runProcess(state.value, clientReleaseProcess)
    },

    spytPublishClusterRelease := {
      runProcess(state.value, clusterReleaseProcess)
    },

    spytPublishSparkForkRelease := {
      runProcess(state.value, sparkForkReleaseProcess)
    }
  )
}
