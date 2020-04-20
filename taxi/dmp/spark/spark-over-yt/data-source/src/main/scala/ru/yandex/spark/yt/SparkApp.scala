package ru.yandex.spark.yt

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.YtClientProvider
import ru.yandex.yt.ytclient.proxy.YtClient

trait SparkApp extends App {
  private val log = Logger.getLogger(getClass)

  def run(args: Array[String])(implicit spark: SparkSession, yt: YtClient): Unit

  def sparkConf: SparkConf = SessionUtils.prepareSparkConf()

  override def main(args: Array[String]): Unit = {
    try {
      val spark = SessionUtils.buildSparkSession(sparkConf)
      try {
        run(args)(spark, yt)
      } finally {
        log.info("Stopping SparkSession")
        spark.stop()
      }
    } finally {
      log.info("Closing YtClientProvider")
      YtClientProvider.close()
    }
  }
}
