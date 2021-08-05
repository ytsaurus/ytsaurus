package spyt

import sbt.{IO, Project, SettingKey, State, TaskKey, ThisBuild}
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.versions
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.Utilities.stateW
import sbtrelease._
import spyt.SpytPlugin.autoImport.{spytSparkVersionFile, _}

import java.io.File

object SpytRelease {
  val isTeamCity: Boolean = sys.env.get("IS_TEAMCITY").contains("1")

  def releaseVersions(st: State, versionSetting: SettingKey[String]): State = {
    val extracted = Project.extract(st)
    st.log.info(s"Teamcity build: $isTeamCity")

    val releaseV = if (isTeamCity) {
      sys.env("BUILD_NUMBER")
    } else {
      val currentV = extracted.get(versionSetting)
      val releaseFunc = extracted.runTask(releaseVersion, st)._2
      releaseFunc(currentV)
    }
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

  def maybeSetVersion(spytVersions: Seq[(SettingKey[String], Versions => String)],
                      fileSetting: SettingKey[File]): ReleaseStep = {
    if (isTeamCity) {
      identity[State](_)
    } else {
      setVersion(spytVersions, fileSetting)
    }
  }

  def setVersion(spytVersions: Seq[(SettingKey[String], Versions => String)],
                 fileSetting: SettingKey[File]): ReleaseStep = { st: State =>
    val vs = st.get(versions).getOrElse(sys.error("No versions are set! Was this release part executed before inquireVersions?"))
    val selected = spytVersions.map(v => v._1 -> v._2.apply(vs))

    st.log.info(s"Setting ${selected.map { case (k, v) => s"${k.key} to $v" }.mkString(", ")}")

    val file = st.extract.get(fileSetting)
    writeVersion(selected, file)

    reapply(selected.map { case (k, v) => ThisBuild / k := v }, st)
  }

  def writeVersion(versions: Seq[(SettingKey[String], String)],
                   file: File): Unit = {
    val versionStr =
      s"""import spyt.SpytPlugin.autoImport._
         |
         |${versions.map { case (k, v) => s"""ThisBuild / ${k.key} := "$v"""" }.mkString("\n")}""".stripMargin
    IO.writeLines(file, Seq(versionStr))
  }

  private def vcs(st: State): Vcs = {
    st.extract.get(releaseVcs)
      .getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

  private def commit(st: State,
                     commitMessage: TaskKey[String],
                     files: Seq[SettingKey[File]]): State = {
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

  private def maybeCommit(st: State,
                          commitMessage: TaskKey[String],
                          files: Seq[SettingKey[File]]): State = {
    if (isTeamCity) {
      st
    } else {
      commit(st, commitMessage, files)
    }
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

  lazy val maybePushChanges: ReleaseStep = if (isTeamCity) identity[State](_) else pushChanges

  lazy val clusterReleaseVersions: ReleaseStep = { st: State => releaseVersions(st, spytClusterVersion) }
  lazy val setReleaseClusterVersion: ReleaseStep = setVersion(Seq(spytClusterVersion -> getReleaseVersion), spytClusterVersionFile)
  lazy val maybeSetNextClusterVersion: ReleaseStep = maybeSetVersion(Seq(spytClusterVersion -> getNextVersion), spytClusterVersionFile)
  lazy val maybeCommitReleaseClusterVersion = { st: State => maybeCommit(st, releaseClusterCommitMessage, Seq(spytClusterVersionFile)) }
  lazy val maybeCommitNextClusterVersion = { st: State => maybeCommit(st, releaseNextClusterCommitMessage, Seq(spytClusterVersionFile)) }


  lazy val clientReleaseVersions: ReleaseStep = { st: State => releaseVersions(st, spytClientVersion) }
  lazy val setReleaseClientVersion: ReleaseStep = {
    setVersion(Seq(
      spytClientVersion -> getReleaseVersion,
      spytClientPythonVersion -> getReleaseVersion
    ), spytClientVersionFile)
  }
  lazy val maybeSetNextClientVersion: ReleaseStep = {
    maybeSetVersion(Seq(
      spytClientVersion -> getNextVersion,
      spytClientPythonVersion -> getNextPythonVersion
    ), spytClientVersionFile)
  }
  lazy val maybeCommitReleaseClientVersion = { st: State =>
    maybeCommit(st, releaseClientCommitMessage, Seq(spytClientVersionFile, spytClientVersionPyFile))
  }
  lazy val maybeCommitNextClientVersion = { st: State =>
    maybeCommit(st, releaseNextClientCommitMessage, Seq(spytClientVersionFile, spytClientVersionPyFile))
  }

  lazy val allReleaseVersions: ReleaseStep = { st: State => releaseMinorVersions(st, spytClusterVersion) }
  lazy val setReleaseSparkVersion: ReleaseStep = setVersion(Seq(spytSparkPythonVersion -> getReleaseSparkVersion), spytSparkVersionFile)
  lazy val setNextSparkVersion: ReleaseStep = setVersion(Seq(spytSparkPythonVersion -> getReleaseSparkVersion), spytSparkVersionFile)
  lazy val commitReleaseAllVersion = { st: State =>
    maybeCommit(st, releaseAllCommitMessage, Seq(
      spytClientVersionFile,
      spytClientVersionPyFile,
      spytClusterVersionFile,
      spytSparkVersionFile
      //      spytSparkVersionPyFile
    ))
  }
  lazy val commitNextAllVersion = { st: State =>
    maybeCommit(st, releaseNextAllCommitMessage, Seq(
      spytClientVersionFile,
      spytClientVersionPyFile,
      spytClusterVersionFile
    ))
  }

}
