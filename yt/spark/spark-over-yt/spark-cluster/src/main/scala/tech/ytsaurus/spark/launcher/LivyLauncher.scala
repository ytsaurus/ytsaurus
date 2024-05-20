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
import scala.util.Random

object LivyLauncher extends App with VanillaLauncher with SparkLauncher {
  private val log = LoggerFactory.getLogger(getClass)
  private val livyArgs = LivyLauncherArgs(args)

  import livyArgs._

  prepareLivyLog4jConfig()

  withOptionalYtClient(ytConfigO) { ytO =>
    withCompoundDiscovery(groupId.map(getFullGroupId), masterGroupId.map(getFullGroupId), baseDiscoveryPath, ytO) { discoveryService =>
      log.info("Used discovery service: " + discoveryService.toString)
      val (masterAddress, pingAddress) = fixedMasterAddress.map(a => (a, None)).getOrElse {
        val address = waitForMaster(waitMasterTimeout, discoveryService)
        (s"spark://${address.hostAndPort.toString}", Some(address.webUiHostAndPort))
      }
      log.info(s"Starting livy server for master $masterAddress")
      val tcpRouter = ytO.flatMap(yt => TcpProxyService.register("LIVY")(yt))
      val address = HostAndPort(ytHostnameOrIpAddress, port)
      val externalAddress = tcpRouter.map(_.getExternalAddress("LIVY")).getOrElse(address)
      log.info(f"Server will started on address $externalAddress")
      prepareLivyConf(address, masterAddress, maxSessions)
      prepareLivyClientConf(driverCores, driverMemory)

      withService(startLivyServer(address)) { livyServer =>
        discoveryService.registerLivy(externalAddress, clusterVersion)

        def isAlive: Boolean = {
          val isMasterAlive = pingAddress.forall(a => DiscoveryService.isAlive(a, 3))
          val isLivyAlive = livyServer.isAlive(3)

          val res = isMasterAlive && isLivyAlive
          if (res) {
            discoveryService.updateLivy(externalAddress, clusterVersion)
          }
          res
        }

        tcpRouter.foreach { router =>
          updateTcpAddress(livyServer.address.toString, router.getPort("LIVY"))(ytO.get)
          log.info("Tcp proxy port addresses updated")
        }
        checkPeriodically(isAlive)
      }
    }
  }
}

case class LivyLauncherArgs(port: Int, ytConfigO: Option[YtClientConfiguration],
                            driverCores: Int, driverMemory: String, maxSessions: Int,
                            groupId: Option[String], masterGroupId: Option[String],
                            baseDiscoveryPath: Option[String], waitMasterTimeout: Duration,
                            fixedMasterAddress: Option[String])

object LivyLauncherArgs {
  def apply(args: Args): LivyLauncherArgs = LivyLauncherArgs(
    args.optional("port").orElse(sys.env.get("SPARK_YT_LIVY_PORT")).map(_.toInt)
      .getOrElse(27105 + Random.nextInt(20)), // Random port in range 27105...27124
    YtClientConfiguration.optionalApply(args.optional),
    args.required("driver-cores").toInt,
    args.required("driver-memory"),
    args.required("max-sessions").toInt,
    args.optional("discovery-group-id").orElse(sys.env.get("SPARK_DISCOVERY_GROUP_ID")),
    args.optional("master-discovery-group-id").orElse(sys.env.get("SPARK_MASTER_DISCOVERY_GROUP_ID")),
    args.optional("base-discovery-path").orElse(sys.env.get("SPARK_BASE_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes),
    args.optional("master-address"),
  )

  def apply(args: Array[String]): LivyLauncherArgs = LivyLauncherArgs(Args(args))
}
