package tech.ytsaurus.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
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

  withDiscovery(ytConfig, discoveryPath) { case (discoveryService, _) =>
    val masterAddress = waitForMaster(waitMasterTimeout, discoveryService)

    log.info(s"Starting livy server for master $masterAddress")
    val address = HostAndPort(ytHostnameOrIpAddress, port)
    log.info(f"Server will started on address $address")
    prepareLivyConf(address, masterAddress, maxSessions)
    prepareLivyClientConf()

    withService(startLivyServer(address)) { livyServer =>
      discoveryService.registerLivy(address)

      def isAlive: Boolean = {
        val isMasterAlive = DiscoveryService.isAlive(masterAddress.webUiHostAndPort, 3)
        val isLivyAlive = livyServer.isAlive(3)

        isMasterAlive && isLivyAlive
      }
      checkPeriodically(isAlive)
    }
  }
}

case class LivyLauncherArgs(port: Int, ytConfig: YtClientConfiguration, maxSessions: Int,
                            discoveryPath: String, waitMasterTimeout: Duration)

object LivyLauncherArgs {
  def apply(args: Args): LivyLauncherArgs = LivyLauncherArgs(
    args.optional("port").orElse(sys.env.get("SPARK_YT_LIVY_PORT")).map(_.toInt).getOrElse(27105),
    YtClientConfiguration(args.optional),
    args.optional("max-sessions").orElse(sys.env.get("SPARK_YT_LIVY_SESSIONS")).map(_.toInt).getOrElse(2),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): LivyLauncherArgs = LivyLauncherArgs(Args(args))
}
