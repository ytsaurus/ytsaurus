package ru.yandex.spark.test

import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt._
import ru.yandex.yt.ytclient.proxy.YtClient

object TestFailed extends SparkApp {
  override def run(args: Array[String])(implicit spark: SparkSession, yt: YtClient): Unit = {
    val user = spark.read.yt("//sys/spark/examples/example_1")

    throw new RuntimeException("Badumtss")
  }
}
