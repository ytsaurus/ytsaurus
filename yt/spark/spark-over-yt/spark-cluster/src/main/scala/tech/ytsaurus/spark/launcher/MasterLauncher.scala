package tech.ytsaurus.spark.launcher

import com.codahale.metrics.MetricRegistry
import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.launcher.TcpProxyService.updateTcpAddress
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.discovery.{Address, SparkConfYsonable}
import tech.ytsaurus.spark.launcher.rest.MasterWrapperLauncher
import tech.ytsaurus.spark.metrics.AdditionalMetrics

import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App
  with VanillaLauncher
  with SparkLauncher
  with MasterWrapperLauncher
  with SolomonLauncher {

  private val log = LoggerFactory.getLogger(getClass)
  val masterArgs = MasterLauncherArgs(args)
  import masterArgs._

  val autoscalerConf: Option[AutoScaler.Conf] = AutoScaler.Conf(sparkSystemProperties)
  val additionalMetrics: MetricRegistry = new MetricRegistry
  AdditionalMetrics.register(additionalMetrics, "master")

  withDiscovery(ytConfig, discoveryPath) { case (discoveryService, yt) =>
    val tcpRouter = TcpProxyService.register("HOST", "WEBUI", "REST", "WRAPPER")(yt)
    val reverseProxyUrl = tcpRouter.map(x => "http://" + x.getExternalAddress("WEBUI").toString)
    withService(startMaster(reverseProxyUrl)) { master =>
      withService(startMasterWrapper(args, master)) { masterWrapper =>
        withOptionalService(startSolomonAgent(args, "master", master.masterAddress.webUiHostAndPort.port)) {
          solomonAgent =>

            master.waitAndThrowIfNotAlive(5 minutes)
            masterWrapper.waitAndThrowIfNotAlive(5 minutes)

            val masterAddress = tcpRouter.map { router =>
              Address(
                router.getExternalAddress("HOST"),
                router.getExternalAddress("WEBUI"),
                router.getExternalAddress("REST")
              )
            }.getOrElse(master.masterAddress)
            val masterWrapperAddress =
              tcpRouter.map(_.getExternalAddress("WRAPPER")).getOrElse(masterWrapper.address)
            log.info("Register master")
            discoveryService.registerMaster(
              operationId,
              masterAddress,
              clusterVersion,
              masterWrapperAddress,
              SparkConfYsonable(sparkSystemProperties)
            )
            log.info("Master registered")
            tcpRouter.foreach { router =>
              updateTcpAddress(master.masterAddress.hostAndPort.toString, router.getPort("HOST"))(yt)
              updateTcpAddress(master.masterAddress.webUiHostAndPort.toString, router.getPort("WEBUI"))(yt)
              updateTcpAddress(master.masterAddress.restHostAndPort.toString, router.getPort("REST"))(yt)
              updateTcpAddress(masterWrapper.address.toString, router.getPort("WRAPPER"))(yt)
              log.info("Tcp proxy port addresses updated")
            }

            autoscalerConf foreach { conf =>
              AutoScaler.start(AutoScaler.build(conf, discoveryService, yt), conf, additionalMetrics)
            }
            if (solomonAgent.nonEmpty) {
              AdditionalMetricsSender(sparkSystemProperties, "master", additionalMetrics).start()
            }

            checkPeriodically(master.isAlive(3) && solomonAgent.forall(_.isAlive(3)))
            log.error("Master is not alive")
        }
      }
    }
  }
}

case class MasterLauncherArgs(ytConfig: YtClientConfiguration,
                              discoveryPath: String,
                              operationId: String,
                              clusterVersion: String)

object MasterLauncherArgs {
  def apply(args: Args): MasterLauncherArgs = MasterLauncherArgs(
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("operation-id").getOrElse(sys.env("YT_OPERATION_ID")),
    args.optional("cluster-version").getOrElse(sys.env("SPARK_CLUSTER_VERSION"))
  )

  def apply(args: Array[String]): MasterLauncherArgs = MasterLauncherArgs(Args(args))
}
