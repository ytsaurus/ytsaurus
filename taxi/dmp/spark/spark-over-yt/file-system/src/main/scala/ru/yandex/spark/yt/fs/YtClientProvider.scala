package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.utils.{YtClientConfiguration, YtClientUtils, YtRpcClient}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.collection.mutable

object YtClientProvider {
  private val client: ThreadLocal[Option[YtRpcClient]] = new ThreadLocal[Option[YtRpcClient]]
  private val conf: ThreadLocal[YtClientConfiguration] = new ThreadLocal[YtClientConfiguration]
  private val fsClient = mutable.HashMap.empty[String, YtRpcClient]

  private def cachedYtClient: Option[YtClient] = cachedClient.map(_.yt)

  private def cachedClient: Option[YtRpcClient] = Option(client.get()).flatten

  def ytClient(conf: YtClientConfiguration): YtClient = cachedYtClient.getOrElse {
    this.conf.set(conf)
    val client = YtClientUtils.createRpcClient(conf)
    this.client.set(Some(client))
    this.client.get.get.yt
  }

  def ytClient(conf: YtClientConfiguration, fs: YtFileSystem): YtClient = fsClient
    .getOrElseUpdate(fs.id, YtClientUtils.createRpcClient(conf))
    .yt

  def ytClient: YtClient = cachedYtClient
    .orElse(SparkSession.getDefaultSession.map(spark => ytClient(YtClientConfigurationConverter(spark))))
    .getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def httpClient: Yt = {
    YtClientUtils.createHttpClient(conf.get())
  }

  def close(): Unit = {
    cachedClient.foreach(_.close())
  }

  def close(fs: YtFileSystem): Unit = {
    fsClient.get(fs.id).foreach(_.close())
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
