package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.utils.{YtClientConfiguration, YtClientUtils, YtRpcClient}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.collection.mutable

object YtClientProvider {
  private val log = Logger.getLogger(getClass)

  private val client = new ThreadLocal[Option[YtRpcClient]]
  private val conf = new ThreadLocal[YtClientConfiguration]
  private val fsClient = mutable.HashMap.empty[String, YtRpcClient]

  private def cachedYtClient: Option[YtClient] = cachedClient.map(_.yt)

  private def cachedClient: Option[YtRpcClient] = Option(client.get()).flatten

  def ytClient(conf: YtClientConfiguration): YtClient = cachedYtClient.getOrElse {
    this.conf.set(conf)
    val client = YtClientUtils.createRpcClient(conf)
    this.client.set(Some(client))
    this.client.get.get.yt
  }

  def ytClient(conf: YtClientConfiguration, id: String): YtClient = fsClient
    .getOrElseUpdate(id, {
      log.info(s"Create YT Client for id $id")
      YtClientUtils.createRpcClient(conf)
    })
    .yt

  def ytClient: YtClient = cachedYtClient
    .orElse(SparkSession.getDefaultSession.map(spark => ytClient(YtClientConfigurationConverter(spark))))
    .getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def httpClient: Yt = {
    YtClientUtils.createHttpClient(conf.get())
  }

  def close(): Unit = {
    log.info(s"Close all YT Clients")
    cachedClient.foreach(_.close())
    fsClient.foreach(_._2.close())
  }

  def close(id: String): Unit = {
    log.info(s"Close YT Client for id $id")
    fsClient.get(id).foreach(_.close())
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
