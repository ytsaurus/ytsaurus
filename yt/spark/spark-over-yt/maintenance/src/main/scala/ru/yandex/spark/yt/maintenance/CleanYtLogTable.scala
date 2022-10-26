package ru.yandex.spark.yt.maintenance

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.yandex.spark.yt.logger.YtDynTableLogger
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.collection.JavaConverters._

object CleanYtLogTable extends App {
  val cluster = "hume"
  val logName = "pushdown"
  val startDttm = "2022-03-26T00:00:00"
  val endDttm = "2022-03-29T00:00:00"
  val tablePath = "//home/spark/logs/log_table"

  implicit val yt = YtWrapper.createRpcClient("maintenance", YtClientConfiguration.default(cluster)).yt

  def levelGreaterThanOrEqual(level: Level) = udf((str: String) => Level.toLevel(str).isGreaterOrEqual(level))

  val keys = Seq(
    "logger_name",
    "dttm",
    "discovery_path",
    "spyt_version",
    "app_id",
    "app_name",
    "uid"
  )
  val rows = YtWrapper
    .selectRows(tablePath, Some(s"""logger_name = "$logName" and dttm >= "$startDttm" and dttm <= "$endDttm""""))
    .map { row => keys.map(k => k -> row.getString(k)).toMap }


  println(s"Count: ${rows.length}")
  YtWrapper.deleteRows(tablePath, YtDynTableLogger.logTableSchema, rows.map(_.mapValues(_.asInstanceOf[Any]).asJava))
}
