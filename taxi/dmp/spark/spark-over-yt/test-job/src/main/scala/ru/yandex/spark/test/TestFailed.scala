package ru.yandex.spark.test

import ru.yandex.spark.yt._

object TestFailed extends SparkApp {
  val user = spark.read.yt("//sys/spark/examples/example_1")

  throw new RuntimeException("Badumtss")
}
