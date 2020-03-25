package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.yt.utils.YtClientConfiguration

object HistoryServerLauncher extends App with SparkLauncher {
  val log = Logger.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)

  run(launcherArgs.ytConfig, launcherArgs.discoveryPath) { discoveryService =>
    val address = startHistoryServer(launcherArgs.logPath)
    discoveryService.registerSHS(address)
    checkPeriodically(sparkThreadIsAlive)
    log.warn("Spark History Server is not alive")
  }
}

case class HistoryServerLauncherArgs(logPath: String,
                                     ytConfig: YtClientConfiguration,
                                     discoveryPath: String)

object HistoryServerLauncherArgs {
  def apply(args: Args): HistoryServerLauncherArgs = HistoryServerLauncherArgs(
    args.required("log-path"),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH"))
  )

  def apply(args: Array[String]): HistoryServerLauncherArgs = HistoryServerLauncherArgs(Args(args))
}
