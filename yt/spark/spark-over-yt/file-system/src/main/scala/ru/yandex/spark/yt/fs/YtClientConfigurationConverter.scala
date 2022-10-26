package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

object YtClientConfigurationConverter {
  def ytClientConfiguration(spark: SparkSession): YtClientConfiguration = {
    ytClientConfiguration(spark.sparkContext.hadoopConfiguration)
  }

  def ytClientConfiguration(conf: Configuration, proxy: Option[String] = None): YtClientConfiguration = {
    YtClientConfiguration(conf.getYtConf(_), proxy)
  }

  def ytClientConfiguration(conf: SparkConf): YtClientConfiguration = {
    ytClientConfiguration(hadoopConf(conf.getAll))
  }

  def ytClientConfiguration(sqlConf: SQLConf): YtClientConfiguration = {
    ytClientConfiguration(hadoopConf(sqlConf.getAllConfs.toArray))
  }

  private def hadoopConf(conf: Array[(String, String)]): Configuration = {
    val hadoopConf = new Configuration()
    for ((key, value) <- conf if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
    hadoopConf
  }
}
