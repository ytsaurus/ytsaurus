package ru.yandex.spark.launcher

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets

import com.google.common.net.HostAndPort
import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode}
import ru.yandex.spark.discovery.DiscoveryService
import ru.yandex.spark.launcher.ByopLauncher.ByopConfig
import ru.yandex.spark.launcher.Service.BasicService
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._


trait ByopLauncher {
  self: VanillaLauncher =>

  private val log = Logger.getLogger(getClass)

  private def waitByopStart(config: ByopConfig, process: Thread, timeout: Duration): Unit = {
    val address = HostAndPort.fromParts("localhost", config.rpcPort)
    val monitoringAddress = HostAndPort.fromParts("localhost", config.monitoringPort)
    DiscoveryService.waitFor(DiscoveryService.isAlive(address, 0) || !process.isAlive, timeout)
    DiscoveryService.waitFor(DiscoveryService.isAlive(monitoringAddress, 0) || !process.isAlive, timeout)
  }

  def startByop(config: ByopConfig,
                ytConf: YtClientConfiguration,
                timeout: Duration): BasicService = {
    log.info(s"Start RPC proxy with config: $config")

    val ytRpc = YtWrapper.createRpcClient(ytConf)

    try {
      val binaryAbsolutePath = path(config.binaryPath)
      val configTemplatePath = path(config.configPath)
      val configFile = createFromTemplate(new File(configTemplatePath)) { content =>
        val replacedAliases = replaceHome(content)
          .replaceAll("\\$SPARK_YT_BYOP_PORT", config.rpcPort.toString)
          .replaceAll("\\$SPARK_YT_BYOP_MONITORING_PORT", config.monitoringPort.toString)
          .replaceAll("\\$YT_OPERATION_ALIAS", config.operationAlias)
          .replaceAll("\\$YT_JOB_COOKIE", config.ytJobCookie)

        val is = new ByteArrayInputStream(replacedAliases.getBytes(StandardCharsets.UTF_8))
        try {
          val ysonConfig = YTreeTextSerializer.deserialize(is).asInstanceOf[YTreeMapNode]
          val remoteClusterConnection = YtWrapper.attribute("//sys", "cluster_connection")(ytRpc.yt)
          ByopLauncher.update(ysonConfig, remoteClusterConnection, "cluster_connection")
          YTreeTextSerializer.serialize(ysonConfig)
        } finally {
          is.close()
        }
      }

      val thread = new Thread(() => {
        val process = Process(
          s"$binaryAbsolutePath --config ${configFile.getAbsolutePath}",
          cwd = None,
          "YT_ALLOC_CONFIG" -> "{profiling_backtrace_depth=10;enable_eager_memory_release=%true;bugs=%false}"
        ).run()
        try {
          val exitCode = process.exitValue()

          log.info(s"Rpc proxy exit code is $exitCode")
        } catch {
          case e: Throwable =>
            process.destroy()
            throw e
        }
      })
      thread.setDaemon(true)
      thread.start()
      waitByopStart(config, thread, timeout)
      if (thread.isAlive) {
        log.info(s"Rpc proxy started on port ${config.rpcPort}, monitoring port ${config.monitoringPort}")
      }
      BasicService("RPC Proxy", config.rpcPort, thread)
    } finally {
      log.info("Close yt rpc")
      ytRpc.close()
    }
  }
}

object ByopLauncher {
  private[launcher] def update(node: YTreeMapNode, patch: YTreeNode, key: String): Unit = {
    val emptyMapNode = new YTreeBuilder().beginMap().endMap().build()
    val updateNode = node.get(key).getOrElse(emptyMapNode)
    node.put(key, update(updateNode, patch))
  }

  private[launcher] def update(node: YTreeNode, patch: YTreeNode): YTreeNode = {
    import scala.collection.JavaConverters._

    @tailrec
    def inner(patches: Seq[(YTreeNode, YTreeNode)]): Unit = {
      patches match {
        case (n: YTreeMapNode, p: YTreeMapNode) :: tail =>
          val newPatches = p.asMap().asScala.flatMap {
            case (key, value: YTreeMapNode) if n.containsKey(key) => Some((n.getOrThrow(key), value))
            case (key, value) =>
              n.put(key, value)
              None
          }
          inner(tail ++ newPatches)
        case Nil =>
      }
    }

    inner(Seq(node -> patch))
    node
  }

  case class ByopConfig(binaryPath: String,
                        configPath: String,
                        rpcPort: Int,
                        monitoringPort: Int,
                        operationAlias: String,
                        ytJobCookie: String)

  object ByopConfig {
    private val baseName = "byop"
    private val envBaseName = "SPARK_YT_BYOP"

    private def envName(name: String): String = s"${envBaseName}_${name.toUpperCase().replace("-", "_")}"

    private def arg(name: String)(implicit args: Args): String = {
      args.optional(s"$baseName-$name").getOrElse(sys.env(envName(name)))
    }

    def optionArg(name: String)(implicit args: Args): Option[String] = {
      args.optional(s"$baseName-$name").orElse(sys.env.get(envName(name)))
    }

    def create(sparkConf: Map[String, String], args: Array[String]): Option[ByopConfig] = {
      create(sparkConf, Args(args))
    }

    def byopEnabled(sparkConf: Map[String, String]): Boolean = {
      sparkConf.get("spark.hadoop.yt.byop.enabled").exists(_.toBoolean)
    }

    def byopPort(sparkConf: Map[String, String], args: Array[String]): Option[Int] = {
      byopPort(sparkConf, Args(args))
    }

    def byopPort(sparkConf: Map[String, String], args: Args): Option[Int] = {
      if (byopEnabled(sparkConf)) Some(arg("port")(args).toInt) else None
    }

    def create(sparkConf: Map[String, String], args: Args): Option[ByopConfig] = {
      if (byopEnabled(sparkConf)) {
        implicit val a = args
        Some(ByopConfig(
          binaryPath = arg("binary-path"),
          configPath = arg("config-path"),
          rpcPort = arg("port").toInt,
          monitoringPort = optionArg("monitoring-port").map(_.toInt).getOrElse(27001),
          operationAlias = args.optional("operation-alias").getOrElse(sys.env("YT_OPERATION_ALIAS")),
          ytJobCookie = args.optional("job-cookie").getOrElse(sys.env("YT_JOB_COOKIE"))
        ))
      } else None
    }
  }

}
