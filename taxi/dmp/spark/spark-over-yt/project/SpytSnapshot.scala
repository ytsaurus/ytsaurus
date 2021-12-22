package spyt

import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.versions
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, releaseStepTask}
import sbtrelease.Utilities.stateW
import sbtrelease.Versions
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

import scala.sys.process.Process

object SpytSnapshot {
  lazy val clientSnapshotProcess: Seq[ReleaseStep] = Seq(
    clientSnapshotVersions,
    setSnapshotClientVersion,
    releaseStepTask(spytUpdatePythonVersion),
    releaseStepTask(spytPublishClient),
    logClientVersion
  )

  lazy val clusterSnapshotProcess: Seq[ReleaseStep] = Seq(
    clusterSnapshotVersions,
    setClusterSnapshotVersion,
    clientSnapshotVersions,
    setSnapshotClientVersion,
    releaseStepTask(spytUpdatePythonVersion),
    releaseStepTask(spytPublishCluster),
    releaseStepTask(spytPublishClient),
    logClusterVersion,
    logClientVersion
  )

  lazy val sparkForkSnapshotProcess: Seq[ReleaseStep] = Seq(
    sparkForkSnapshotVersions,
    setSparkForkSnapshotVersion,
    clusterSnapshotVersions,
    setClusterSnapshotVersion,
    clientSnapshotVersions,
    setSnapshotClientVersion,
    ReleaseStep(releaseStepTask(spytUpdatePythonVersion))
  ) ++ sparkInstallProcess ++ Seq(
    ReleaseStep(releaseStepTask(spytPublishSparkFork)),
    ReleaseStep(releaseStepTask(spytPublishCluster)),
    ReleaseStep(releaseStepTask(spytPublishClient)),
    logSparkForkVersion,
    logClusterVersion,
    logClientVersion
  )

  case class SnapshotVersion(main: String,
                             fork: Option[String],
                             ticket: String,
                             dev: Int,
                             hash: String) {
    def plusFork: String = fork.map(f => s"+$f").getOrElse("")

    def minusFork: String = fork.map(f => s"-fork-$f").getOrElse("")

    def toScalaString: String = {
      s"$main$minusFork-$ticket-$dev-$hash-SNAPSHOT"
    }

    def toPythonString: String = {
      if (fork.nonEmpty) {
        s"$main${plusFork}b1.dev$dev"
      } else {
        s"${main}b1.dev$dev+$ticket.$hash"
      }
    }

    def inc: SnapshotVersion = {
      val hash = gitHash().getOrElse("")
      val ticket = gitBranch().flatMap(ticketFromBranch).getOrElse("")
      copy(dev = dev + 1, hash = hash, ticket = ticket)
    }

    def updateDev(other: Option[SnapshotVersion]): SnapshotVersion = {
      other.map(o => copy(dev = dev.max(o.dev))).getOrElse(this)
    }

    private def ticketFromBranch(branchName: String): Option[String] = {
      val p = "^(\\w+)[ -_](\\d+)[ -_].*$".r

      branchName match {
        case p(q, n) => Some(q.toLowerCase + n)
        case _ => None
      }
    }

    private def gitBranch(submodule: String = ""): Option[String] = {
      val cmd = "git rev-parse --abbrev-ref HEAD"
      val real = if (submodule.isEmpty) cmd else s"git submodule foreach $cmd -- $submodule"
      try Process(real).lineStream.headOption catch {
        case _: Throwable => None
      }
    }

    private def gitHash(submodule: String = ""): Option[String] = {
      val loc = if (submodule.isEmpty) "HEAD" else s"HEAD:$submodule"
      try Process(s"git rev-parse --short $loc").lineStream.headOption catch {
        case _: Throwable => None
      }
    }
  }

  object SnapshotVersion {
    private val snapshotVersionRegex = "^([0-9.]*)(-fork-)*([0-9.]*?)(-[a-z0-9]*?)?(-[0-9]*?)?(-[a-f0-9]{8})?-SNAPSHOT$".r
    private val releaseVersionRegex = "^([0-9.]*)(-fork-)*(\\d+)\\.(\\d+)\\.(\\d+)$".r
    private val pythonVersionRegex = "^([0-9.]*)[+]*([0-9.]*)([ab](\\d+))?(\\.dev(\\d+))?([.+].*)?$".r

    def parse(str: String): SnapshotVersion = {
      str match {
        case snapshotVersionRegex(main, _, fork, _, dev, _) =>
          val parseDev = Option(dev).map(_.drop(1)).map(_.toInt).getOrElse(0)
          SnapshotVersion(main, Option(fork).filter(_.nonEmpty), "", parseDev, "")
        case releaseVersionRegex(main, _, major, minor, bugfix) =>
          SnapshotVersion(main, Some(s"$major.$minor.${bugfix.toInt + 1}"), "", 0, "")
        case _ =>
          println(s"Unable to parse version $str")
          throw new IllegalArgumentException(s"Unable to parse version $str")
      }
    }

    def latestPublishedPython(log: Logger, pythonRegistry: String, packageName: String): Option[SnapshotVersion] = {
      PypiUtils.latestVersion(log, pythonRegistry, packageName).map {
        case pythonVersionRegex(main, fork, _, _, _, dev, _) =>
          SnapshotVersion(main, Option(fork).filter(_.nonEmpty), "", dev = Option(dev).map(_.toInt).getOrElse(0), "")
      }
    }
  }

  private lazy val clientSnapshotVersions: ReleaseStep = { st: State => snapshotVersions(st, spytClientVersion, "yandex-spyt") }
  private lazy val setSnapshotClientVersion: ReleaseStep = {
    setVersion(Seq(
      spytClientVersion -> { v: Versions => v._1 },
      spytClientPythonVersion -> { v: Versions => v._2 }
    ), spytClientVersionFile)
  }

  private lazy val sparkForkSnapshotVersions: ReleaseStep = { st: State => snapshotVersions(st, spytSparkVersion, "yandex-pyspark") }
  private lazy val setSparkForkSnapshotVersion: ReleaseStep = {
    setVersion(Seq(
      spytSparkVersion -> { v: Versions => v._1 },
      spytSparkPythonVersion -> { v: Versions => v._2 }
    ), spytSparkVersionFile)
  }
  private lazy val sparkInstallProcess: Seq[ReleaseStep] = Seq(
    setSparkForkSnapshotVersionMvn,
    ReleaseStep(releaseStepTask(spytMvnInstallSparkFork)),
    unsetSparkForkSnapshotVersionMvn,
    updateSparkForkDependency
  ).filter(_ => sparkInstallEnabled)

  private def sparkInstallEnabled: Boolean = {
    Option(System.getProperty("installSpark")).forall(_.toBoolean)
  }

  private lazy val clusterSnapshotVersions: ReleaseStep = { st: State => snapshotVersion(st, spytClusterVersion) }
  private lazy val setClusterSnapshotVersion: ReleaseStep = {
    setVersion(Seq(
      spytClusterVersion -> { v: Versions => v._1 }
    ), spytClusterVersionFile)
  }

  private def snapshotVersions(st: State,
                               versionSetting: SettingKey[String],
                               pythonPackage: String): State = {
    val curVer = SnapshotVersion.parse(st.extract.get(versionSetting))
    val latestPythonVer = SnapshotVersion.latestPublishedPython(st.log, st.extract.get(pypiRegistry), pythonPackage)
    val newVer = curVer.updateDev(latestPythonVer).inc
    st.log.info(s"Current version: ${curVer.toScalaString}")
    st.log.info(s"New scala version: ${newVer.toScalaString}")
    st.log.info(s"New python version: ${newVer.toPythonString}")

    st.put(versions, (newVer.toScalaString, newVer.toPythonString))
  }

  private def snapshotVersion(st: State, versionSetting: SettingKey[String]): State = {
    val curVer = SnapshotVersion.parse(st.extract.get(versionSetting))
    val newVer = curVer.inc
    st.log.info(s"Current version: ${curVer.toScalaString}")
    st.log.info(s"New scala version: ${newVer.toScalaString}")

    st.put(versions, (newVer.toScalaString, ""))
  }
}
