package ru.yandex.spark.yt.maintenance

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.logger.YtDynTableLogger
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.table.YtTableSettings

import scala.concurrent.duration._
import scala.language.postfixOps

object CreateYtLogTable extends App {
  val cluster = "hume"
  implicit val yt = YtWrapper.createRpcClient("maintenance", YtClientConfiguration.default(cluster)).yt

  val tableSettings = new YtTableSettings {
    override def ytSchema: YTreeNode = YtDynTableLogger.logTableSchema.toYTree

    override def optionsAny: Map[String, Any] = Map("dynamic" -> true)
  }

  YtWrapper.createTable("//home/spark/logs/log_table", tableSettings)
  YtWrapper.mountTableSync("//home/spark/logs/log_table", 10 seconds)
}
