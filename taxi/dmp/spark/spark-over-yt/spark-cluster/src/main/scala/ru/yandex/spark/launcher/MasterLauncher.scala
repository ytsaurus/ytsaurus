package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.rest.MasterWrapperLauncher
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.{OperationSet, SparkConfYsonable}

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

  withDiscovery(ytConfig, discoveryPath) { case (discoveryService, _) =>
    withService(startMaster) { master =>
      withService(startMasterWrapper(args, master)) { masterWrapper =>
        withService(startSolomonAgent(args, "master", master.masterAddress.webUiHostAndPort.getPort)) { solomonAgent =>
          master.waitAndThrowIfNotAlive(5 minutes)
          masterWrapper.waitAndThrowIfNotAlive(5 minutes)

          log.info("Register master")
          discoveryService.registerMaster(
            operationId,
            master.masterAddress,
            clusterVersion,
            masterWrapper.address,
            SparkConfYsonable(sparkSystemProperties)
          )
          log.info("Master registered")

          checkPeriodically(master.isAlive(3) && solomonAgent.isAlive(3))
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
