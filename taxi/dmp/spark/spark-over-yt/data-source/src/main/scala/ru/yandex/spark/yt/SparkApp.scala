package ru.yandex.spark.yt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.YtClientProvider
import ru.yandex.yt.ytclient.proxy.CompoundClient

trait SparkApp extends App {
  protected val spark = SessionUtils.buildSparkSession(sparkConf)

  def sparkConf: SparkConf = SessionUtils.prepareSparkConf()

}
