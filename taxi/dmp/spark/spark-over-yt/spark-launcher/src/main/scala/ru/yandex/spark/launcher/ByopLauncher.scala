package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode}
import ru.yandex.spark.launcher.ByopLauncher.ByopConfig
import ru.yandex.spark.launcher.Service.BasicService
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{ByopConfiguration, YtClientConfiguration}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try


trait ByopLauncher extends SidecarLauncher {
  self: VanillaLauncher =>

  private def prepareBinaryFile(path: Path): Path = {
    ByopLauncher.prepareBinary(path)
  }

  private def prepareConfigFile(templateContent: String,
                                config: SidecarConfig): String = {
    config match {
      case c: ByopConfig =>
        val replacedAliases = templateContent
          .replaceAll("\\$SPARK_YT_BYOP_PORT", c.port.toString)
          .replaceAll("\\$SPARK_YT_BYOP_MONITORING_PORT", c.monitoringPort.toString)
          .replaceAll("\\$TVM_ENABLED", c.tvm_enabled.toString)
          .replaceAll("\\$TVM_CLIENT_ID", c.tvm_client_id.toString)
          .replaceAll("\\$TVM_CLIENT_SECRET", c.tvm_client_secret)
          .replaceAll("\\$TVM_ENABLE_USER_TICKET_CHECKING", c.tvm_enable_user_ticket_checking.toString)
          .replaceAll("\\$TVM_ENABLE_SERVICE_TICKET_FETCHING", c.tvm_client_enable_service_ticket_fetching.toString)
          .replaceAll("\\$TVM_HOST", c.tvm_host)
          .replaceAll("\\$TVM_PORT", c.tvm_port.toString)

        val is = new ByteArrayInputStream(replacedAliases.getBytes(StandardCharsets.UTF_8))
        val ysonConfigTry = Try(YTreeTextSerializer.deserialize(is).asInstanceOf[YTreeMapNode])
        is.close()
        val ysonConfig = ysonConfigTry.get

        val ytRpc = YtWrapper.createRpcClient("byop", config.ytConf.copy(byop = ByopConfiguration.DISABLED))
        val remoteClusterConnection = Try(YtWrapper.attribute("//sys", "cluster_connection")(ytRpc.yt))
        ytRpc.close()

        ByopLauncher.update(ysonConfig, remoteClusterConnection.get, "cluster_connection")
        YTreeTextSerializer.serialize(ysonConfig)
    }
  }

  private val serviceEnv: Map[String, String] = Map(
    "YT_ALLOC_CONFIG" -> "{profiling_backtrace_depth=10;enable_eager_memory_release=%true;bugs=%false}"
  )

  def startByop(config: ByopConfig): BasicService = {
    startService(config, prepareConfigFile, prepareBinaryFile, serviceEnv = serviceEnv)
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

  private[launcher] def prepareBinary(path: Path): Path = {
    val binaryName = "ytserver-proxy"
    if (path.getFileName.toString == binaryName) {
      path
    } else {
      val newPath = Paths.get(path.getParent.toString, binaryName)
      Files.move(path, newPath)
      newPath
    }
  }

  case class ByopConfig(binaryPath: String,
                        configPaths: Seq[String],
                        port: Int,
                        monitoringPort: Int,
                        operationAlias: String,
                        ytJobCookie: String,
                        ytConf: YtClientConfiguration,
                        timeout: Duration,
                        tvm_enabled: Boolean,
                        tvm_host: String,
                        tvm_port: Int,
                        tvm_client_id: Int,
                        tvm_client_secret: String,
                        tvm_enable_user_ticket_checking: Boolean,
                        tvm_client_enable_service_ticket_fetching: Boolean) extends SidecarConfig {
    override def host: String = "localhost"

    override def serviceName: String = "BYOP"
  }

  object ByopConfig extends SidecarConfigUtils {

    override protected def argBaseName: String = "byop"

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
        val tvmEnabled = optionArg("tvm-enabled").exists(_.toBoolean)
        Some(ByopConfig(
          binaryPath = arg("binary-path"),
          configPaths = arg("config-path").split(",").map(_.trim),
          port = arg("port").toInt,
          monitoringPort = optionArg("monitoring-port").map(_.toInt).getOrElse(27001),
          operationAlias = args.optional("operation-alias").getOrElse(sys.env("YT_OPERATION_ALIAS")),
          ytJobCookie = args.optional("job-cookie").getOrElse(sys.env("YT_JOB_COOKIE")),
          ytConf = ytConf,
          timeout = timeout,
          tvm_enabled = tvmEnabled,
          tvm_host = optionArg("tvm-host").getOrElse("localhost"),
          tvm_port = optionArg("tvm-port").map(_.toInt).getOrElse(13000),
          tvm_client_id = sys.env.get("YT_SECURE_VAULT_SPARK_TVM_ID").map(_.toInt).getOrElse(0),
          tvm_client_secret = sys.env.getOrElse("YT_SECURE_VAULT_SPARK_TVM_SECRET", ""),
          tvm_enable_user_ticket_checking = optionArg("tvm-enable-user-ticket-checking")
            .map(_.toBoolean).getOrElse(tvmEnabled),
          tvm_client_enable_service_ticket_fetching = optionArg("tvm-enable-service-ticket-checking")
            .map(_.toBoolean).getOrElse(tvmEnabled),
        ))
      } else None
    }
  }
}
