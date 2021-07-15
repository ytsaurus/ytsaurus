package sbtrelease

import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.versions
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.Utilities.stateW
import spyt.SpytPlugin.autoImport.{spytSparkVersionFile, _}

import java.io.File

object SpytRelease {
  def releaseVersions(st: State, versionSetting: SettingKey[String]): State = {
    val extracted = Project.extract(st)

    val currentV = extracted.get(versionSetting)

    val releaseFunc = extracted.runTask(releaseVersion, st)._2
    val releaseV = releaseFunc(currentV)
    st.log.info(s"Release version: $releaseV")

    val nextFunc = extracted.runTask(releaseNextVersion, st)._2
    val nextV = nextFunc(releaseV)
    st.log.info(s"Next version: $nextV")

    st.put(versions, (releaseV, nextV))
  }

  def releaseMinorVersions(st: State, versionSetting: SettingKey[String]): State = {
    val extracted = Project.extract(st)

    val currentV = extracted.get(versionSetting)

    val releaseV = Version(currentV).map(_.bump(Version.Bump.Minor).withoutQualifier.string)
      .getOrElse(versionFormatError(currentV))
    st.log.info(s"Release version: $releaseV")

    val nextFunc = extracted.runTask(releaseNextVersion, st)._2
    val nextV = nextFunc(releaseV)
    st.log.info(s"Cluster next version: $nextV")

    st.put(versions, (releaseV, nextV))
  }

  def setVersion(spytVersions: Seq[(SettingKey[String], Versions => String)],
                 fileSetting: SettingKey[File]): ReleaseStep = { st: State =>
    val vs = st.get(versions).getOrElse(sys.error("No versions are set! Was this release part executed before inquireVersions?"))
    val selected = spytVersions.map(v => v._1 -> v._2.apply(vs))

    st.log.info(s"Setting ${selected.map{case (k, v) => s"${k.key} to $v"}.mkString(", ")}")

    val file = st.extract.get(fileSetting)
    writeVersion(selected, file)

    reapply(selected.map{ case (k, v) => ThisBuild / k := v }, st)
  }

  def writeVersion(versions: Seq[(SettingKey[String], String)],
                   file: File): Unit = {
    val versionStr =
      s"""import spyt.SpytPlugin.autoImport._
         |
         |${versions.map{ case (k, v) => s"""ThisBuild / ${k.key} := "$v""""}.mkString("\n")}""".stripMargin
    IO.writeLines(file, Seq(versionStr))
  }

  private def vcs(st: State): Vcs = {
    st.extract.get(releaseVcs)
      .getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

  private def commitVersion(st: State,
                    commitMessage: TaskKey[String],
                    files: Seq[SettingKey[File]]) = {
    val log = st.log
    val addFiles = files.map(f => st.extract.get(f).getCanonicalFile)
    val base = vcs(st).baseDir.getCanonicalFile
    val sign = st.extract.get(releaseVcsSign)
    val signOff = st.extract.get(releaseVcsSignOff)
    val relativePaths = addFiles.map(f => IO.relativize(base, f)
      .getOrElse("Version file [%s] is outside of this VCS repository with base directory [%s]!" format(f, base)))

    relativePaths.foreach(p => vcs(st).add(p) !! log)
    val status = vcs(st).status.!!.trim

    val newState = if (status.nonEmpty) {
      val (state, msg) = st.extract.runTask(commitMessage, st)
      vcs(state).commit(msg, sign, signOff) ! log
      state
    } else {
      // nothing to commit. this happens if the version.sbt file hasn't changed.
      st
    }
    newState
  }

  lazy val setYtProxies: ReleaseStep = { st: State =>
    System.setProperty("proxies", Seq("hume", "hahn", "arnold").mkString(","))
    st
  }

  lazy val setRebuildSpark: ReleaseStep = { st: State =>
    System.setProperty("rebuildSpark", "true")
    st
  }

  def getReleaseVersion(vs: Versions): String = vs._1
  def getReleaseSparkVersion(vs: Versions): String = s"3.0.1+${vs._1}" //TODO 3.0.1 -> sparkVersion
  def getNextVersion(vs: Versions): String = vs._2
  def getNextPythonVersion(vs: Versions): String = vs._2.replace("-SNAPSHOT", "b1")

  lazy val clusterReleaseVersions: ReleaseStep = { st: State => releaseVersions(st, spytClusterVersion) }
  lazy val setReleaseClusterVersion: ReleaseStep = setVersion(Seq(spytClusterVersion -> getReleaseVersion), spytClusterVersionFile)
  lazy val setNextClusterVersion: ReleaseStep = setVersion(Seq(spytClusterVersion -> getNextVersion), spytClusterVersionFile)
  lazy val commitReleaseClusterVersion = { st: State => commitVersion(st, releaseClusterCommitMessage, Seq(spytClusterVersionFile)) }
  lazy val commitNextClusterVersion = { st: State => commitVersion(st, releaseNextClusterCommitMessage, Seq(spytClusterVersionFile)) }


  lazy val clientReleaseVersions: ReleaseStep = { st: State => releaseVersions(st, spytClientVersion) }
  lazy val setReleaseClientVersion: ReleaseStep = {
    setVersion(Seq(
      spytClientVersion -> getReleaseVersion,
      spytClientPythonVersion -> getReleaseVersion
    ), spytClientVersionFile)
  }
  lazy val setNextClientVersion: ReleaseStep = {
    setVersion(Seq(
      spytClientVersion -> getNextVersion,
      spytClientPythonVersion -> getNextPythonVersion
    ), spytClientVersionFile)
  }
  lazy val commitReleaseClientVersion = { st: State =>
    commitVersion(st, releaseClientCommitMessage, Seq(spytClientVersionFile, spytClientVersionPyFile))
  }
  lazy val commitNextClientVersion = { st: State =>
    commitVersion(st, releaseNextClientCommitMessage, Seq(spytClientVersionFile, spytClientVersionPyFile))
  }

  lazy val allReleaseVersions: ReleaseStep = { st: State => releaseMinorVersions(st, spytClusterVersion) }
  lazy val setReleaseSparkVersion: ReleaseStep = setVersion(Seq(spytSparkPythonVersion -> getReleaseSparkVersion), spytSparkVersionFile)
  lazy val setNextSparkVersion: ReleaseStep = setVersion(Seq(spytSparkPythonVersion -> getReleaseSparkVersion), spytSparkVersionFile)
  lazy val commitReleaseAllVersion = { st: State =>
    commitVersion(st, releaseAllCommitMessage, Seq(
      spytClientVersionFile,
      spytClientVersionPyFile,
      spytClusterVersionFile,
      spytSparkVersionFile
//      spytSparkVersionPyFile
    ))
  }
  lazy val commitNextAllVersion = { st: State =>
    commitVersion(st, releaseNextAllCommitMessage,Seq(
      spytClientVersionFile,
      spytClientVersionPyFile,
      spytClusterVersionFile
    ))
  }

}
