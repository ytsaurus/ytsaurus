package ru.yandex.sbt

import sbt.Keys._
import sbt._

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = empty

  object autoImport {
    val publishYtBaseDir = settingKey[String]("Yt publish base path")
    val publishYt = taskKey[Unit]("Publish to yt directory")
    val publishYtArtifacts = taskKey[Seq[File]]("Yt publish artifacts")
    val publishYtConfig = taskKey[Seq[File]]("Yt publish artifacts")
    val publishYtDir = taskKey[String]("Yt publish path")
    val publishYtConfigDir = taskKey[String]("Yt publish path")
  }

  import autoImport._

  private def publish(src: File, dst: String, log: Logger): Unit = {
    import scala.sys.process._
    import scala.language.postfixOps

    log.info(s"Upload ${src.getAbsolutePath} to YT cluster ${sys.env("YT_PROXY")} $dst/${src.getName}")
    s"cat ${src.getAbsolutePath}" #| s"yt write-file $dst/${src.getName}" !
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtArtifacts := Seq(),
    publishYtConfig := Seq(),
    publishYtDir := {
      if (isSnapshot.value || version.value.contains("beta")) {
        s"${publishYtBaseDir.value}/snapshots/${version.value}"
      } else {
        s"${publishYtBaseDir.value}/releases/${version.value}"
      }
    },
    publishYtConfigDir := {
      if (isSnapshot.value || version.value.contains("beta")) {
        s"${publishYtBaseDir.value}/snapshots"
      } else {
        s"${publishYtBaseDir.value}/releases"
      }
    },
    publishYt := {
      import scala.sys.process._
      import scala.language.postfixOps

      val log = streams.value.log
      val publishDir = publishYtDir.value
      val publishConfDir = publishYtConfigDir.value

      log.info(s"Create map_node $publishDir")
      s"yt create -i map_node $publishDir" !

      publishYtArtifacts.value.foreach(publish(_, publishDir, log))
      publishYtConfig.value.foreach(publish(_, publishConfDir, log))
    }
  )
}
