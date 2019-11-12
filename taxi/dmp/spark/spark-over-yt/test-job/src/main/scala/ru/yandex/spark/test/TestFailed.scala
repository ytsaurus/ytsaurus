package ru.yandex.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.yandex.spark.yt._

object TestFailed extends SparkApp {
  override def run(spark: SparkSession): Unit = {
    val user = spark.read.yt("/home/sashbel/data/user")

    throw new RuntimeException("Badumtss")
  }
}
