import java.time.Duration

import _root_.io.netty.channel.nio.NioEventLoopGroup
import ru.yandex.yt.ytclient.bus.{BusConnector, DefaultBusConnector}
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType}
import ru.yandex.yt.ytclient.rpc.RpcCredentials
import sbt.Keys._
import sbt._

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = empty

  object autoImport {

    sealed trait YtPublishArtifact {
      def proxy: Option[String]

      def remoteDir: String

      def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit
    }

    case class YtPublishFile(localFile: File,
                             remoteDir: String,
                             proxy: Option[String],
                             remoteName: Option[String] = None) extends YtPublishArtifact {
      private def dstName: String = remoteName.getOrElse(localFile.getName)

      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit = {
        val src = localFile.getAbsolutePath
        val dst = s"$remoteDir/$dstName"

        log.info(s"Upload $src to YT cluster $proxyName $dst..")
        s"cat $src" #| s"yt --proxy $proxyName write-file $dst" !

        log.info(s"Finished upload $src to YT cluster $proxyName $dst")
      }
    }

    case class YtPublishDocument(yson: YsonableConfig,
                                 remoteDir: String,
                                 proxy: Option[String],
                                 remoteName: String) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit = {
        val dst = s"$remoteDir/$remoteName"
        val exists = yt.existsNode(dst).join().booleanValue()
        if (!exists) {
          log.info(s"Create document $dst at YT cluster $proxyName")
          yt.createNode(new CreateNode(dst, ObjectType.Document))
        }
        val ysonForPublish = yson.resolveSymlinks(yt)
        log.info(s"Upload document $ysonForPublish to YT cluster $proxyName $dst..")
        yt.setNode(dst, ysonForPublish.toYTree)

        log.info(s"Finished upload document to YT cluster $proxyName $dst..")
      }
    }

    def ytProxies: Seq[String] = {
      Option(System.getProperty("proxies")).map(_.split(",").toSeq).getOrElse(Nil)
    }

    val publishYt = taskKey[Unit]("Publish to yt directory")
    val publishYtArtifacts = taskKey[Seq[YtPublishArtifact]]("Yt publish artifacts")
    val publishYtCredentials = settingKey[RpcCredentials]("Yt publish credentials")
  }

  import autoImport._

  private def createDir(dir: String, proxy: String, log: Logger)(implicit yt: YtClient): Unit = {
    val exists = yt.existsNode(dir).join().booleanValue()
    if (!exists) {
      log.info(s"Create map_node $dir at YT cluster $proxy")
      val request = new CreateNode(dir, ObjectType.MapNode)
        .setIgnoreExisting(true)
        .setRecursive(true)
      yt.createNode(request)
    }
  }

  private def createYtClient(proxy: String, credentials: RpcCredentials): (YtClient, BusConnector) = {
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(Duration.ofMinutes(5))
      .setWriteTimeout(Duration.ofMinutes(5))

    new YtClient(connector, proxy, credentials) -> connector
  }

  private def readDefaultToken: String = {
    val f = file(sys.env("HOME")) / ".yt" / "token"
    val src = Source.fromFile(f)
    try {
      src.mkString.trim
    } finally {
      src.close()
    }
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtCredentials := new RpcCredentials(
      sys.env.getOrElse("YT_USER", sys.env("USER")),
      sys.env.getOrElse("YT_TOKEN", readDefaultToken)
    ),
    publishYtArtifacts := Nil,
    publishYt := {
      val log = streams.value.log

        val creds = publishYtCredentials.value
        ytProxies.par.foreach { proxy =>
          val (ytClient, connector) = createYtClient(proxy, creds)
          implicit val yt: YtClient = ytClient
          try {
            publishYtArtifacts.value.par.foreach { artifact =>
              if (artifact.proxy.forall(_ == proxy)) {
                if (sys.env.get("RELEASE_TEST").exists(_.toBoolean)) {
                  log.info(s"RELEASE_TEST: Publish $artifact to $proxy")
                } else {
                  createDir(artifact.remoteDir, proxy, log)
                  artifact.publish(proxy, log)
                }

              }
            }
          } finally {
            yt.close()
            connector.close()
          }
        }
    }
  )
}
