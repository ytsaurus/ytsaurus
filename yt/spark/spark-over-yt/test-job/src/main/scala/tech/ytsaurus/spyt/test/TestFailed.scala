package tech.ytsaurus.spyt.test

import tech.ytsaurus.spyt._

object TestFailed extends SparkApp {
  val user = spark.read.yt("//sys/spark/examples/example_1")

  throw new RuntimeException("Badumtss")
}
