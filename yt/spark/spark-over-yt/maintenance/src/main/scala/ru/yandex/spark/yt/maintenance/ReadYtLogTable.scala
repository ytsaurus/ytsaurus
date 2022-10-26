package ru.yandex.spark.yt.maintenance

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.collection.JavaConverters._
import scala.language.postfixOps

object ReadYtLogTable extends App {
  val cluster = "hume"
  val logName = "pushdown"
  val startDttm = "2022-03-29T00:00:00"
  val endDttm = "2022-03-30T00:00:00"
  val tablePath = "//home/spark/logs/log_table"

  val spark = SparkSession.builder().master("local[1]").getOrCreate()

  import spark.implicits._

  implicit val yt = YtWrapper.createRpcClient("maintenance", YtClientConfiguration.default(cluster)).yt

  def levelGreaterThanOrEqual(level: Level) = udf((str: String) => Level.toLevel(str).isGreaterOrEqual(level))


  val rows = YtWrapper
    .selectRows(tablePath, Some(s"""logger_name = "$logName" and dttm >= "$startDttm" and dttm <= "$endDttm""""))
    .map { row =>
      (
        row.getString("dttm"),
        row.getString("app_name"),
        row.getString("level"),
        row.getString("spark_component"),
        row.getString("msg"),
        row.getMap("info").asMap().asScala.toMap.mapValues(_.stringValue()),
        toOption(row.getIntO("stage_id")),
        toOption(row.getIntO("partition_id")),
        row.getString("app_id"),
        row.getString("discovery_path"),
        row.getString("spyt_version")
      )
    }
    .toDF("dttm", "app_name", "level", "spark_component", "msg", "info",
      "stage_id", "partition_id", "app_id", "discovery_path", "spyt_version")
    .cache()

  println(s"Count: ${rows.count()}")
  println(s"Distinct apps: ${rows.select('discovery_path, 'app_id).distinct().count()}")
  rows
    .filter(levelGreaterThanOrEqual(Level.DEBUG)('level))
//    .filter('partition_id === 0)
    .filter('app_id === "app-20220329162640-0008")
    .sort("dttm")
    .show(1000, truncate = false)

}
