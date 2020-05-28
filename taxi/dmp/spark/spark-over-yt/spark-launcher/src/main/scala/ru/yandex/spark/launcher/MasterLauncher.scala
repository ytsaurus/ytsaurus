package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.SparkConfYsonable
import ru.yandex.spark.launcher.rest.MasterWrapperLauncher
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App with VanillaLauncher with SparkLauncher with MasterWrapperLauncher {
  private val log = Logger.getLogger(getClass)
  val masterArgs = MasterLauncherArgs(args)

  run(masterArgs.ytConfig, masterArgs.discoveryPath) { discoveryService =>
    log.info("Start master")
    val (address, thread) = startMaster()
    withThread(thread) { _ =>
      log.info("Start byop discovery service")
      val (masterWrapperEndpoint, masterWrapperThread) = startMasterWrapper(args, address)
      withThread(masterWrapperThread) { _ =>
        if (!discoveryService.waitAlive(address.hostAndPort, 5 minutes)) {
          throw new RuntimeException("Master is not started")
        }
        log.info(s"Master started at port ${address.port}")

        log.info("Register master")
        discoveryService.register(
          masterArgs.operationId,
          address,
          masterArgs.clusterVersion,
          masterWrapperEndpoint,
          SparkConfYsonable(sparkSystemProperties)
        )
        log.info("Master registered")

        checkPeriodically(thread.isAlive)
        log.error("Master is not alive")
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
