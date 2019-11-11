package ru.yandex.spark.yt

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.utils.{YtClientConfiguration, YtRpcClient, YtUtils}
import ru.yandex.yt.ytclient.proxy.YtClient


object YtClientProvider {
  private val _client: ThreadLocal[Option[YtRpcClient]] = new ThreadLocal[Option[YtRpcClient]]
  private val _conf: ThreadLocal[YtClientConfiguration] = new ThreadLocal[YtClientConfiguration]

  private def cachedYtClient: Option[YtClient] = cachedClient.map(_.yt)

  private def cachedClient: Option[YtRpcClient] = Option(_client.get()).flatten

  def ytClient(conf: YtClientConfiguration): YtClient = cachedYtClient.getOrElse {
    _conf.set(conf)
    val client = YtUtils.createRpcClient(conf)
    _client.set(Some(client))
    _client.get.get.yt
  }

  def ytClient: YtClient = cachedYtClient.getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def httpClient: Yt = {
    YtUtils.createHttpClient(_conf.get())
  }

  def close(): Unit = {
    cachedClient.foreach(_.close())
  }
}

object YtClientConfigurationConverter {

  import ru.yandex.spark.yt.conf._

  def apply(spark: SparkSession): YtClientConfiguration = {
    apply(spark.sparkContext.hadoopConfiguration)
  }

  def apply(conf: Configuration): YtClientConfiguration = {
    YtClientConfiguration(conf.getYtConf(_))
  }

  def apply(conf: SparkConf): YtClientConfiguration = {
    apply(SparkHadoopUtil.get.newConfiguration(conf))
  }
}
