package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.{metaSchema, schema}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.tables.TableSchema

object HistoryServerLauncher extends App with VanillaLauncher with SparkLauncher {
  val log = LoggerFactory.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)
  import launcherArgs._

  withDiscovery(ytConfig, discoveryPath) { case (discoveryService, yt) =>
    if (logPath.startsWith("ytEventLog")) {
      createIfNotExists(logPath, schema, yt)
      createIfNotExists(s"${logPath}_meta", metaSchema, yt)
    }
    withService(startHistoryServer(logPath)) { historyServer =>
      discoveryService.registerSHS(historyServer.address)
      checkPeriodically(historyServer.isAlive(3))
      log.error("Spark History Server is not alive")
    }
  }

  private def createIfNotExists(path: String, schema: TableSchema, yt: CompoundClient) = {
    if (!YtWrapper.exists(path)(yt)) {
      YtWrapper.createDynTable(path, schema)(yt)
    }
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
