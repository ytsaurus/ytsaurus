package ru.yandex.spark.yt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends App {
  def run(spark: SparkSession): Unit

  def sparkConf: SparkConf = new SparkConf()

  override def main(args: Array[String]): Unit = {
    try {
      val spark = createSparkSession(sparkConf)
      run(spark)
    } finally {
      YtClientProvider.close()
    }
  }
}
