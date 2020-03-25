package ru.yandex.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.yandex.spark.yt._
import ru.yandex.yt.ytclient.proxy.YtClient
import org.apache.spark.sql.functions._

object Test extends SparkApp {
  override def sparkConf = super.sparkConf.set("spark.test", "test_app")

  override def remoteConfigPath = "//sys/spark/conf/snapshots/spark-launch-conf"

  override def run(args: Array[String])(implicit spark: SparkSession, yt: YtClient): Unit = {
    import spark.implicits._

    val user = spark.read.yt("//sys/spark/examples/example_1")

    user
      .withColumn("first_letter", substring('uuid, 0, 1))
      .groupBy("first_letter").count().sort("count").coalesce(1)
      .write.mode(SaveMode.Overwrite).sortedBy("count").optimizeFor(OptimizeMode.Scan)
      .yt("//home/sashbel/data/test")
  }
}
