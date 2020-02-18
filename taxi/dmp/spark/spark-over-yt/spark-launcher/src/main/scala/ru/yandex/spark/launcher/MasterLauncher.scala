package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App with SparkLauncher {
  private val log = Logger.getLogger(getClass)
  val masterArgs = MasterLauncherArgs(args)

  run(masterArgs.ytConfig, masterArgs.discoveryPath) { discoveryService =>
    log.info("Start master")
    val masterAddress = startMaster(masterArgs.port, masterArgs.webUiPort, masterArgs.opts)
    val masterAlive = discoveryService.waitAlive(masterAddress.hostAndPort, 5 minutes)
    if (!masterAlive) {
      throw new RuntimeException("Master is not started")
    }
    log.info(s"Master started at port ${masterAddress.port}")

    log.info("Register master")
    discoveryService.register(masterArgs.operationId, masterAddress)
    log.info("Master registered")

    checkPeriodically(sparkThreadIsAlive)
    log.info("Master thread is not alive!")
  }
}

case class MasterLauncherArgs(port: Int,
                              webUiPort: Int,
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String,
                              operationId: String,
                              opts: Option[String])

object MasterLauncherArgs {
  def apply(args: Args): MasterLauncherArgs = MasterLauncherArgs(
    args.required("port").toInt,
    args.required("web-ui-port").toInt,
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.required("operation-id"),
    args.optional("opts").map(_.drop(1).dropRight(1))
  )

  def apply(args: Array[String]): MasterLauncherArgs = MasterLauncherArgs(Args(args))
}
