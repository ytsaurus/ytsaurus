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
    val publishYtDir = taskKey[String]("Yt publish path")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtArtifacts := Seq(),
    publishYtDir := {
      if (isSnapshot.value || version.value.contains("beta")) {
        s"${publishYtBaseDir.value}/snapshots/${version.value}"
      } else {
        s"${publishYtBaseDir.value}/releases/${version.value}"
      }
    },
    publishYt := {
      import scala.sys.process._
      import scala.language.postfixOps

      val log = streams.value.log
      val publishDir = publishYtDir.value

      log.info(s"Create map_node $publishDir")
      s"yt create -i map_node $publishDir" !

      publishYtArtifacts.value.foreach { art =>
        log.info(s"Upload ${art.getAbsolutePath} to YT cluster ${sys.env("YT_PROXY")} $publishDir/${art.getName}")
        s"cat ${art.getAbsolutePath}" #| s"yt write-file $publishDir/${art.getName}" !
      }
    }
  )
}
