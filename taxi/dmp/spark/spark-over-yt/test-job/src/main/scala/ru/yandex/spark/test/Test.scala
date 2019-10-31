package ru.yandex.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import ru.yandex.spark.yt._


object Test extends SparkApp {
  override def run(spark: SparkSession): Unit = {
    val user = spark.read.yt("/home/sashbel/data/user")

    user
      .groupBy("application_platform").count().sort("count").coalesce(1)
      .write.mode(SaveMode.Overwrite).sortedBy("count").optimizeFor(OptimizeMode.Scan)
      .yt("/home/sashbel/data/test")
  }
}
