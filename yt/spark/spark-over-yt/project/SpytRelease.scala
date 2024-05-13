package spyt

import sbt.{IO, Project, SettingKey, State}
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease._
import spyt.ReleaseUtils._
import spyt.SpytPlugin.autoImport._

object SpytRelease {

  lazy val spytReleaseProcess: Seq[ReleaseStep] = testProcess ++ Seq(
    ReleaseStep(releaseStepTask(prepareBuildDirectory)),
    minorReleaseVersions,
    setReleaseSpytVersion,
  ) ++ setCustomVersions ++ Seq(
    ReleaseStep(releaseStepTask(spytUpdatePythonVersion)),
    ReleaseStep(releaseStepTask(spytPublish)),
    ReleaseStep(releaseStepTask(spytPublishLibraries)),
    dumpVersions,
    setNextSpytVersion,
    ReleaseStep(releaseStepTask(spytUpdatePythonVersion)),
    logSpytVersion
  )

  private lazy val testProcess: Seq[ReleaseStep] = {
    val skipTests = Option(System.getProperty("skipTests")).forall(_.toBoolean)
    if (skipTests) {
      Nil
    } else {
      Seq(
        checkSnapshotDependencies,
        runClean,
        runTest
      )
    }
  }

  private def releaseMinorVersions(versions: SettingKey[Versions],
                                   st: State,
                                   versionSetting: SettingKey[String]): State = {
    val extracted = Project.extract(st)

    val currentV = extracted.get(versionSetting)

    val releaseV = Version(currentV).map(_.bump(Version.Bump.Minor).withoutQualifier.string)
      .getOrElse(versionFormatError(currentV))
    st.log.info(s"Release version: $releaseV")

    val nextFunc = extracted.runTask(releaseNextVersion, st)._2
    val nextV = nextFunc(releaseV)
    st.log.info(s"Next version: $nextV")

    st.put(versions.key, (releaseV, nextV))
  }

  private def getReleaseVersion(vs: Versions): String = vs._1

  private def getReleasePythonVersion(vs: Versions): String = vs._1

  private def getNextVersion(vs: Versions): String = vs._2

  private def getNextPythonVersion(vs: Versions): String = vs._2.replace("-SNAPSHOT", "b0")

  private lazy val maybePushChanges: ReleaseStep = pushChanges

  private lazy val setReleaseSpytVersion: ReleaseStep = {
    setVersion(
      spytVersions,
      Seq(spytVersion -> getReleaseVersion, spytPythonVersion -> getReleaseVersion),
      spytVersionFile
    )
  }
  private lazy val setNextSpytVersion: ReleaseStep = {
    maybeSetVersion(
      spytVersions,
      Seq(spytVersion -> getNextVersion, spytPythonVersion -> getNextPythonVersion),
      spytVersionFile
    )
  }

  private lazy val minorReleaseVersions: ReleaseStep = { st: State =>
    releaseMinorVersions(spytVersions, st, spytVersion)
  }

  private lazy val setCustomVersions: Seq[ReleaseStep] = Seq(setCustomSpytVersions)

  private lazy val setCustomSpytVersions: ReleaseStep = {
    customSpytVersion.map { v =>
      setVersionForced(Seq(spytVersion -> v, spytPythonVersion -> v), spytVersionFile)
    }.getOrElse(ReleaseStep(identity))
  }
}
