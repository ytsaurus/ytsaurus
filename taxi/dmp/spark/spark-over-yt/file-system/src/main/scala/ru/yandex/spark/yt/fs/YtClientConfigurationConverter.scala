package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

object YtClientConfigurationConverter {
  def ytClientConfiguration(spark: SparkSession): YtClientConfiguration = {
    ytClientConfiguration(spark.sparkContext.hadoopConfiguration)
  }

  def ytClientConfiguration(conf: Configuration): YtClientConfiguration = {
    YtClientConfiguration(conf.getYtConf(_))
  }

  def ytClientConfiguration(conf: SparkConf): YtClientConfiguration = {
    ytClientConfiguration(SparkHadoopUtil.get.newConfiguration(conf))
  }
}
