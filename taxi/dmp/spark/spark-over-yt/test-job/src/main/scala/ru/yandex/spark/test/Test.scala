package ru.yandex.spark.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.proxy.YtClient

object Test extends SparkApp {
  override def sparkConf = super.sparkConf
    .set("spark.test", "test_app")

  import spark.implicits._

  val user = spark.read.yt("//sys/spark/examples/example_1")

  user
    .withColumn("first_letter", substring('uuid, 0, 1))
    .groupBy("first_letter").count().sort("count").coalesce(1)
    .write.mode(SaveMode.Overwrite).sortedBy("count").optimizeFor(OptimizeMode.Scan)
    .yt("//home/sashbel/data/test")
}
