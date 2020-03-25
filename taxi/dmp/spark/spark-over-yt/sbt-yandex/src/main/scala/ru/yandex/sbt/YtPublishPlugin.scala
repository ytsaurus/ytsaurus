package ru.yandex.sbt

import sbt.Keys._
import sbt._
import scala.language.postfixOps
import scala.sys.process._

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = empty

  object autoImport {

    sealed trait YtPublishArtifact {
      def proxy: Option[String]

      def remoteDir: String

      def publish(proxy: String, log: Logger): Unit
    }

    case class YtPublishFile(localFile: File,
                             remoteDir: String,
                             proxy: Option[String],
                             remoteName: Option[String] = None) extends YtPublishArtifact {
      private def dstName: String = remoteName.getOrElse(localFile.getName)

      override def publish(proxy: String, log: Logger): Unit = {
        val src = localFile.getAbsolutePath
        val dst = s"$remoteDir/$dstName"

        log.info(s"Upload $src to YT cluster $proxy $dst..")
        s"cat $src" #| s"yt --proxy $proxy write-file $dst" !

        log.info(s"Finished upload $src to YT cluster $proxy $dst")
      }
    }

    case class YtPublishDocument(yson: String,
                                 remoteDir: String,
                                 proxy: Option[String],
                                 remoteName: String) extends YtPublishArtifact {
      override def publish(proxy: String, log: sbt.Logger): Unit = {
        val dst = s"$remoteDir/$remoteName"
        val exists = (s"yt --proxy $proxy exists $dst" !!).trim.toBoolean
        if (!exists) {
          log.info(s"Create document $dst at YT cluster $proxy")
          s"yt --proxy $proxy create document $dst" !
        }
        log.info(s"Upload document $yson to YT cluster $proxy $dst..")
        s"yt --proxy $proxy set $dst $yson" !

        log.info(s"Finished upload document to YT cluster $proxy $dst..")
      }
    }

    val publishYt = taskKey[Unit]("Publish to yt directory")
    val publishYtArtifacts = taskKey[Seq[YtPublishArtifact]]("Yt publish artifacts")
    val publishYtProxies = settingKey[Seq[String]]("Yt publish proxies")
  }

  import autoImport._

  private def createDir(dir: String, proxy: String, log: Logger): Unit = {
    val exists = (s"yt --proxy $proxy exists $dir" !!).trim.toBoolean
    if (!exists) {
      log.info(s"Create map_node $dir at YT cluster $proxy")
      s"yt --proxy $proxy create -i -r map_node $dir" !
    }
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtArtifacts := Seq(),
    publishYtProxies := {
      System.getProperty("proxies") match {
        case null => Seq(sys.env.get("YT_PROXY")).flatten
        case str => str.split(",")
      }
    },
    publishYt := {
      val log = streams.value.log
      for {
        proxy <- publishYtProxies.value.par
        artifact <- publishYtArtifacts.value.par
      } {
        if (artifact.proxy.forall(_ == proxy)) {
          createDir(artifact.remoteDir, proxy, log)
          artifact.publish(proxy, log)
        }
      }
    }
  )
}
