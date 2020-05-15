package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.SparkClusterConf
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object MasterLauncher extends App with VanillaLauncher with SparkLauncher {
  private val log = Logger.getLogger(getClass)
  val masterArgs = MasterLauncherArgs(args)

  run(masterArgs.ytConfig, masterArgs.discoveryPath) { discoveryService =>
    log.info("Start master")
    val (address, thread) = startMaster()
    Try {
      val masterAlive = discoveryService.waitAlive(address.hostAndPort, 5 minutes)
      if (!masterAlive) {
        throw new RuntimeException("Master is not started")
      }
      log.info(s"Master started at port ${address.port}")

      log.info("Register master")
      discoveryService.register(masterArgs.operationId, address, masterArgs.clusterVersion,
        SparkClusterConf(sparkSystemProperties))
      log.info("Master registered")

      checkPeriodically(thread.isAlive)
      log.error("Master is not alive")
    }
    thread.interrupt()
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
