package ru.yandex.spark.yt.logger

import org.apache.spark.sql.SparkSession

sealed abstract class SparkComponent(val name: String)

object SparkComponent {
  case object Driver extends SparkComponent("driver")

  case object Executor extends SparkComponent("executor")

  def get(): SparkComponent = {
    if (SparkSession.getDefaultSession.nonEmpty) Driver else Executor
  }

  def fromName(name: String): SparkComponent = {
    Seq(Driver, Executor).find(_.name == name).get
  }
}
