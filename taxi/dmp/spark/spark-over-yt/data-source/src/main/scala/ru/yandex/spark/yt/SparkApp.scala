package ru.yandex.spark.yt

import org.apache.spark.SparkConf

trait SparkApp extends App {
  protected val spark = SessionUtils.buildSparkSession(sparkConf)

  def sparkConf: SparkConf = SessionUtils.prepareSparkConf()

}
