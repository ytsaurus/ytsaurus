package tech.ytsaurus.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.launcher.TcpProxyService.updateTcpAddress
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.Utils.{parseDuration, ytHostnameOrIpAddress}
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.discovery.DiscoveryService

import scala.concurrent.duration._
import scala.language.postfixOps

object LivyLauncher extends App with VanillaLauncher with SparkLauncher {
  private val log = LoggerFactory.getLogger(getClass)
  private val livyArgs = LivyLauncherArgs(args)

  import livyArgs._

  withDiscovery(ytConfig, discoveryPath) { case (discoveryService, yt) =>
    val tcpRouter = TcpProxyService.register("LIVY")(yt)
    val masterAddress = waitForMaster(waitMasterTimeout, discoveryService)

    log.info(s"Starting livy server for master $masterAddress")
    val address = HostAndPort(ytHostnameOrIpAddress, port)
    val externalAddress = tcpRouter.map(_.getExternalAddress("LIVY")).getOrElse(address)
    log.info(f"Server will started on address $externalAddress")
    prepareLivyConf(address, masterAddress, maxSessions)
    prepareLivyClientConf(driverCores, driverMemory)

    withService(startLivyServer(address)) { livyServer =>
      discoveryService.registerLivy(externalAddress)

      def isAlive: Boolean = {
        val isMasterAlive = DiscoveryService.isAlive(masterAddress.webUiHostAndPort, 3)
        val isLivyAlive = livyServer.isAlive(3)

        isMasterAlive && isLivyAlive
      }
      tcpRouter.foreach { router =>
        updateTcpAddress(livyServer.address.toString, router.getPort("LIVY"))(yt)
        log.info("Tcp proxy port addresses updated")
      }
      checkPeriodically(isAlive)
    }
  }
}

case class LivyLauncherArgs(port: Int, ytConfig: YtClientConfiguration,
                            driverCores: Int, driverMemory: String, maxSessions: Int,
                            discoveryPath: String, waitMasterTimeout: Duration)

object LivyLauncherArgs {
  def apply(args: Args): LivyLauncherArgs = LivyLauncherArgs(
    args.optional("port").orElse(sys.env.get("SPARK_YT_LIVY_PORT")).map(_.toInt).getOrElse(27105),
    YtClientConfiguration(args.optional),
    args.required("driver-cores").toInt,
    args.required("driver-memory"),
    args.required("max-sessions").toInt,
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): LivyLauncherArgs = LivyLauncherArgs(Args(args))
}
