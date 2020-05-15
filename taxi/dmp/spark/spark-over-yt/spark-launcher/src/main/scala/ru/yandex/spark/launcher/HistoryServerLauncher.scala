package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.util.Try

object HistoryServerLauncher extends App with VanillaLauncher with SparkLauncher {
  val log = Logger.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)

  run(launcherArgs.ytConfig, launcherArgs.discoveryPath) { discoveryService =>
    val (address, thread) = startHistoryServer(launcherArgs.logPath)
    Try {
      discoveryService.registerSHS(address)
      checkPeriodically(thread.isAlive)
      log.error("Spark History Server is not alive")
    }
    thread.interrupt()
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
