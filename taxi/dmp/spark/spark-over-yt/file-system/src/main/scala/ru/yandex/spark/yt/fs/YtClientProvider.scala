package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.utils.{YtClientConfiguration, YtClientUtils, YtRpcClient}
import ru.yandex.yt.ytclient.proxy.YtClient

import ru.yandex.spark.yt.fs.conf._

object YtClientProvider {
  private val _client: ThreadLocal[Option[YtRpcClient]] = new ThreadLocal[Option[YtRpcClient]]
  private val _conf: ThreadLocal[YtClientConfiguration] = new ThreadLocal[YtClientConfiguration]

  private def cachedYtClient: Option[YtClient] = cachedClient.map(_.yt)

  private def cachedClient: Option[YtRpcClient] = Option(_client.get()).flatten

  def ytClient(conf: YtClientConfiguration): YtClient = cachedYtClient.getOrElse {
    _conf.set(conf)
    val client = YtClientUtils.createRpcClient(conf)
    _client.set(Some(client))
    _client.get.get.yt
  }

  def ytClient: YtClient = cachedYtClient
    .orElse(SparkSession.getDefaultSession.map(spark => ytClient(YtClientConfigurationConverter(spark))))
    .getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def httpClient: Yt = {
    YtClientUtils.createHttpClient(_conf.get())
  }

  def close(): Unit = {
    cachedClient.foreach(_.close())
  }
}

object YtClientConfigurationConverter {

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
