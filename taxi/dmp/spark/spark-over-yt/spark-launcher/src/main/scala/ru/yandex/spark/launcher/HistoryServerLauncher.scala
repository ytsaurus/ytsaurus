package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.yt.utils.YtClientConfiguration

object HistoryServerLauncher extends App with SparkLauncher {
  val log = Logger.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)

  run(launcherArgs.ytConfig, launcherArgs.discoveryPath){discoveryService =>
    val address = startHistoryServer(launcherArgs.port, launcherArgs.logPath, launcherArgs.opts)
    discoveryService.registerSHS(launcherArgs.id, address)
    checkPeriodically(sparkThreadIsAlive)
    log.warn("Spark History Server is not alive")
  }
}

case class HistoryServerLauncherArgs(id: String,
                                     port: Int,
                                     logPath: String,
                                     ytConfig: YtClientConfiguration,
                                     discoveryPath: String,
                                     opts: Option[String])

object HistoryServerLauncherArgs {
  def apply(args: Args): HistoryServerLauncherArgs = HistoryServerLauncherArgs(
    args.required("id"),
    args.required("port").toInt,
    args.required("log-path"),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.optional("opts").map(_.drop(1).dropRight(1))
  )

  def apply(args: Array[String]): HistoryServerLauncherArgs = HistoryServerLauncherArgs(Args(args))
}
