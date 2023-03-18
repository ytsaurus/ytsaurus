package spyt

import com.typesafe.sbt.packager.Compat.ProcessLogger
import sbt._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, releaseStepTask}
import sbtrelease.Utilities.stateW
import sbtrelease.Versions
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

import scala.sys.process.Process
import scala.util.Random
import scala.util.control.NonFatal

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
    def mainWithSuffix(suffix: String): String = {
      if (main.nonEmpty) {
        s"$main$suffix"
      } else {
        ""
      }
    }

    def mainPlusFork: String = {
      mainWithSuffix("+") + fork.getOrElse("")
    }

    def mainMinusFork: String = {
      mainWithSuffix("-fork-") + fork.getOrElse("")
    }

    def toScalaString: String = {
      s"$mainMinusFork-$ticket-$dev-$hash-SNAPSHOT"
    }

    def toPythonString: String = {
      if (main.nonEmpty) {
        s"${mainPlusFork}b1.dev$dev.$hash"
      } else {
        s"${fork.get}b1.dev$dev+$ticket.$hash"
      }
    }

    def inc: SnapshotVersion = getVcsInfo()
        .map(info => copy(dev = dev + 1, hash = info.hash, ticket = info.ticketName.getOrElse("")))
        .get

    def updateDev(other: Option[SnapshotVersion]): SnapshotVersion = {
      other.map(o => copy(dev = dev.max(o.dev))).getOrElse(this)
    }

    case class VcsInfo(hash: String, branch: String, isGit: Boolean) {
      def ticketName: Option[String] = {
        val p = "^(\\w+)[ -_](\\d+).*$".r

        branch match {
          case p(q, n) => Some(q.toLowerCase + n)
          case "develop" => Some("develop")
          case "trunk" => Some("trunk")
          case "teamcity" => Some("teamcity")
          case _ => None
        }
      }
    }

    private def getTeamcityBuildNumber: String = {
      sys.env.getOrElse("TC_BUILD", Random.alphanumeric.take(7).mkString)
    }

    private def getVcsInfo(submodule: String = ""): Option[VcsInfo] = {
      if (isTeamCity) {
        Some(VcsInfo(getTeamcityBuildNumber, "teamcity", isGit = false))
      } else {
        try {
          val catchStderr: ProcessLogger = new ProcessLogger {
            override def out(s: => String): Unit = ()
            override def err(s: => String): Unit = ()
            override def buffer[T](f: => T): T = f
          }
          val out = Process("arc info").lineStream(catchStderr).toList
          val m = out.map(_.split(':').toList)
            .map(as => (as(0), as(1).trim))
            .toMap
          for {
            hash <- m.get("hash")
            branch <- m.get("branch")
          } yield VcsInfo(hash.take(7), branch, isGit = false)
        } catch {
          // arc not found or it is not arc branch
          // let's try git
          case NonFatal(_) =>
            for {
              branch <- gitBranch(submodule)
              hash <- gitHash(submodule)
            } yield VcsInfo(hash, branch, isGit = true)
        }
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
    private val snapshotVersionRegex = "^([0-9.]*)(-fork-)*([0-9.]*?)(-[a-z0-9]*?)?(-[0-9]*?)?(-[a-f0-9]*?)?-SNAPSHOT$".r
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
        case pythonVersionRegex(main, fork, _, _, _, dev, suffix) =>
            SnapshotVersion(main, Option(fork).filter(_.nonEmpty), "", dev = Option(dev).map(_.toInt).getOrElse(0), suffix)      }
    }
  }

  private lazy val clientSnapshotVersions: ReleaseStep = { st: State =>
    snapshotVersions(clientVersions, st, spytClientVersion, "ytsaurus-spyt")
  }
  private lazy val setSnapshotClientVersion: ReleaseStep = {
    setVersion(clientVersions, Seq(
      spytClientVersion -> { v: Versions => v._1 },
      spytClientPythonVersion -> { v: Versions => v._2 }
    ), spytClientVersionFile)
  }

  private lazy val sparkForkSnapshotVersions: ReleaseStep = { st: State =>
    snapshotVersions(sparkVersions, st, spytSparkVersion, "ytsaurus-pyspark")
  }
  private lazy val setSparkForkSnapshotVersion: ReleaseStep = {
    setVersion(sparkVersions, Seq(
      spytSparkVersion -> { v: Versions => v._1 },
      spytSparkPythonVersion -> { v: Versions => v._2 }
    ), spytSparkVersionFile)
  }
  private lazy val sparkInstallProcess: Seq[ReleaseStep] = Seq(
    setSparkForkSnapshotVersionMvn,
    ReleaseStep(releaseStepTask(spytMvnDeploySparkFork)),
    unsetSparkForkSnapshotVersionMvn,
    updateSparkForkDependency
  ).filter(_ => sparkInstallEnabled)

  private def sparkInstallEnabled: Boolean = {
    Option(System.getProperty("installSpark")).forall(_.toBoolean)
  }

  private lazy val clusterSnapshotVersions: ReleaseStep = { st: State =>
    snapshotVersion(clusterVersions, st, spytClusterVersion)
  }
  private lazy val setClusterSnapshotVersion: ReleaseStep = {
    setVersion(clusterVersions, Seq(
      spytClusterVersion -> { v: Versions => v._1 }
    ), spytClusterVersionFile)
  }

  private def snapshotVersions(versions: SettingKey[Versions],
                               st: State,
                               versionSetting: SettingKey[String],
                               pythonPackage: String): State = {
    val curVer = SnapshotVersion.parse(st.extract.get(versionSetting))
    val latestPythonVer = SnapshotVersion.latestPublishedPython(st.log, st.extract.get(pypiRegistry), pythonPackage)
    val newVer = curVer.updateDev(latestPythonVer).inc
    st.log.info(s"Current version: ${curVer.toScalaString}")
    st.log.info(s"New scala version: ${newVer.toScalaString}")
    st.log.info(s"New python version: ${newVer.toPythonString}")

    st.put(versions.key, (newVer.toScalaString, newVer.toPythonString))
  }

  private def snapshotVersion(versions: SettingKey[Versions],
                              st: State,
                              versionSetting: SettingKey[String]): State = {
    val curVer = SnapshotVersion.parse(st.extract.get(versionSetting))
    val newVer = curVer.inc
    st.log.info(s"Current version: ${curVer.toScalaString}")
    st.log.info(s"New scala version: ${newVer.toScalaString}")

    st.put(versions.key, (newVer.toScalaString, ""))
  }
}
