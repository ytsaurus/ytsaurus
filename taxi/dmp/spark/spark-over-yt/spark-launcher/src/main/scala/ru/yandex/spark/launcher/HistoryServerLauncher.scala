package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.Utils.parseDuration
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object HistoryServerLauncher extends App with VanillaLauncher with SparkLauncher {
  val log = LoggerFactory.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)
  import launcherArgs._

  prepareProfiler()

  withDiscovery(ytConfig, discoveryPath) { discoveryService =>
    log.info("Waiting for master http address")
    discoveryService.waitAddress(waitMasterTimeout)

    withService(startHistoryServer(logPath, memory, discoveryService)) { historyServer =>
      discoveryService.registerSHS(historyServer.address)
      checkPeriodically(historyServer.isAlive(3))
      log.error("Spark History Server is not alive")
    }
  }
}

case class HistoryServerLauncherArgs(logPath: String,
                                     memory: String,
                                     ytConfig: YtClientConfiguration,
                                     discoveryPath: String,
                                     waitMasterTimeout: Duration)

object HistoryServerLauncherArgs {
  def apply(args: Args): HistoryServerLauncherArgs = HistoryServerLauncherArgs(
    args.required("log-path"),
    args.optional("memory").getOrElse("16G"),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes)
  )

  def apply(args: Array[String]): HistoryServerLauncherArgs = HistoryServerLauncherArgs(Args(args))
}
