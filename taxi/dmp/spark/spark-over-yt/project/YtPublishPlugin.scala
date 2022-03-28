package spyt

import io.netty.channel.nio.NioEventLoopGroup
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.yt.ytclient.bus.{BusConnector, DefaultBusConnector}
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request._
import ru.yandex.yt.ytclient.rpc.{RpcCredentials, RpcOptions}
import sbt.Keys._
import sbt._

import java.io.{BufferedInputStream, FileInputStream}
import java.time.Duration
import scala.annotation.tailrec
import scala.io.Source

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = empty

  object autoImport {

    sealed trait YtPublishArtifact {
      def proxy: Option[String]

      def remoteDir: String

      def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit

      def isSnapshot: Boolean
    }

    case class YtPublishLink(originalPath: String,
                             remoteDir: String,
                             proxy: Option[String],
                             linkName: String,
                             override val isSnapshot: Boolean) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit = {
        val link = s"$remoteDir/$linkName"
        log.info(s"Link $originalPath to $link..")
        yt.linkNode(new LinkNode(originalPath, link).setIgnoreExisting(true)).join()
      }
    }

    case class YtPublishFile(localFile: File,
                             remoteDir: String,
                             proxy: Option[String],
                             override val isSnapshot: Boolean,
                             remoteName: Option[String] = None) extends YtPublishArtifact {
      private def dstName: String = remoteName.getOrElse(localFile.getName)

      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit = {
        val src = localFile
        val dst = s"$remoteDir/$dstName"

        log.info(s"Upload $src to YT cluster $proxyName $dst..")
        val transaction = yt.startTransaction(new StartTransaction(TransactionType.Master)
          .setTransactionTimeout(Duration.ofMinutes(10))).join()
        try {
          if (yt.existsNode(dst).join()) yt.removeNode(dst).join()
          yt.createNode(dst, ObjectType.File).join()

          val buffer = new Array[Byte](32 * 1024 * 1024)
          val writer = yt.writeFile(new WriteFile(dst).setTimeout(Duration.ofMinutes(10))).join()
          @tailrec
          def write(len: Int): Unit = {
            writer.readyEvent().join()
            if (!writer.write(buffer, 0, len)) {
              write(len)
            }
          }

          try {
            val is = new BufferedInputStream(new FileInputStream(src))
            var writtenBytes = 0
            try {
              Stream.continually {
                val res = is.read(buffer)
                if (res > 0) write(res)
                writtenBytes += res
                res
              }.dropWhile(_ > 0)
            } finally {
              is.close()
            }
          } finally {
            writer.close().join()
          }

          transaction.commit().join()
        } catch {
          case e: Throwable =>
            transaction.abort().join()
            throw e
        }

        log.info(s"Finished upload $src to YT cluster $proxyName $dst")
      }
    }

    case class YtPublishDocument(yson: YsonableConfig,
                                 remoteDir: String,
                                 proxy: Option[String],
                                 remoteName: String,
                                 override val isSnapshot: Boolean) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YtClient): Unit = {
        val dst = s"$remoteDir/$remoteName"
        val exists = yt.existsNode(dst).join().booleanValue()
        if (!exists) {
          log.info(s"Create document $dst at YT cluster $proxyName")
          yt.createNode(new CreateNode(dst, ObjectType.Document)).join()
        }
        val ysonForPublish = yson.resolveSymlinks(yt)
        log.info(s"Upload document $ysonForPublish to YT cluster $proxyName $dst..")
        yt.setNode(dst, ysonForPublish.toYTree).join()

        log.info(s"Finished upload document to YT cluster $proxyName $dst..")
      }
    }

    def ytProxies: Seq[String] = {
      val envProxy = Option(System.getenv("YT_PROXY"))
      val propProxy = Option(System.getProperty("proxies"))
      val proxy = propProxy.orElse(envProxy)
      proxy match {
        case Some(value) => value.split(",").toSeq
        case None => Nil
      }
    }

    val publishYt = taskKey[Unit]("Publish to yt directory")
    val publishYtArtifacts = taskKey[Seq[YtPublishArtifact]]("Yt publish artifacts")
    val publishYtCredentials = settingKey[RpcCredentials]("Yt publish credentials")
  }

  import autoImport._

  private def createDir(dir: String, proxy: String, log: Logger,
                        ttlMillis: Option[Long])(implicit yt: YtClient): Unit = {
    val exists = yt.existsNode(dir).join().booleanValue()
    if (!exists) {
      log.info(s"Create map_node $dir at YT cluster $proxy")
      val request = new CreateNode(dir, ObjectType.MapNode)
        .setIgnoreExisting(true)
        .setRecursive(true)
      ttlMillis.foreach { ttl =>
        request.setAttributes(java.util.Map.of("expiration_timeout", YTree.integerNode(ttl)));
      }
      yt.createNode(request).join()
    } else {
      ttlMillis.foreach { ttl =>
        log.info(s"Updating expiration timeout for map_node $dir")
        val request = new SetNode(YPath.simple(dir).attribute("expiration_timeout"), YTree.integerNode(ttl))
        yt.setNode(request).join()
      }
    }
  }

  private def createYtClient(proxy: String, credentials: RpcCredentials): (YtClient, BusConnector) = {
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(Duration.ofMinutes(5))
      .setWriteTimeout(Duration.ofMinutes(5))

    val options = new RpcOptions()
    options.setGlobalTimeout(Duration.ofMinutes(10))
    options.setStreamingReadTimeout(Duration.ofMinutes(10))
    options.setStreamingWriteTimeout(Duration.ofMinutes(10))

    if (proxy == "local") {
      val proxyHost = sys.env.getOrElse("YT_LOCAL_HOST", "localhost")
      val credentials = new RpcCredentials("root", "")
      new YtClient(connector, s"$proxyHost:8000", credentials, options) -> connector
    } else {
      new YtClient(connector, proxy, credentials, options) -> connector
    }
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

  private def publishArtifact(artifact: YtPublishArtifact, proxy: String, log: Logger)
                             (implicit yt: YtClient): Unit = {
    if (artifact.proxy.forall(_ == proxy)) {
      if (sys.env.get("RELEASE_TEST").exists(_.toBoolean)) {
        log.info(s"RELEASE_TEST: Publish $artifact to $proxy")
      } else {
        val ttlMillis = if (artifact.isSnapshot) Some(Duration.ofDays(7).toMillis) else None
        createDir(artifact.remoteDir, proxy, log, ttlMillis)
        artifact.publish(proxy, log)
      }
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
      val artifacts = publishYtArtifacts.value
      val (links, files) = artifacts.partition {
        case _: YtPublishLink => true
        case _ => false
      }
      if (ytProxies.isEmpty) {
        log.warn("No yt proxies provided. " +
          "Use `proxies` property or `YT_PROXY` environment variable")
      }
      ytProxies.par.foreach { proxy =>
        val (ytClient, connector) = createYtClient(proxy, creds)
        implicit val yt: YtClient = ytClient
        try {
          // publish links strictly after files
          files.par.foreach(publishArtifact(_, proxy, log))
          links.par.foreach(publishArtifact(_, proxy, log))
        } finally {
          yt.close()
          connector.close()
        }
      }
    }
  )
}
