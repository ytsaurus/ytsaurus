package ru.yandex.spark.yt

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends App {
  private val log = Logger.getLogger(getClass)

  def run(args: Array[String], spark: SparkSession): Unit

  def sparkConf: SparkConf = new SparkConf()

  override def main(args: Array[String]): Unit = {
    try {
      val spark = createSparkSession(sparkConf)
      try {
        run(args, spark)
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
