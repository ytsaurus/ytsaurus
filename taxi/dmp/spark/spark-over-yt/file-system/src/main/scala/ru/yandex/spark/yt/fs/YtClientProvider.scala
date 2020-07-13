package ru.yandex.spark.yt.fs

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtRpcClient}
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
    val client = YtWrapper.createRpcClient(conf)
    this.client.set(Some(client))
    this.client.get.get.yt
  }

  def ytClient(conf: YtClientConfiguration, id: String): YtClient = fsClient
    .getOrElseUpdate(id, {
      log.info(s"Create YT Client for id $id")
      YtWrapper.createRpcClient(conf)
    })
    .yt

  def ytClient: YtClient = cachedYtClient
    .orElse(SparkSession.getDefaultSession.map(spark => ytClient(ytClientConfiguration(spark))))
    .getOrElse(throw new IllegalStateException("YtClient is not initialized"))

  def httpClient: Yt = {
    YtWrapper.createHttpClient(conf.get())
  }

  def close(): Unit = {
    log.info(s"Close all YT Clients")
    cachedClient.foreach(_.close())
    fsClient.foreach(_._2.close())
    fsClient.clear()
    this.client.set(None)
  }

  def close(id: String): Unit = {
    log.info(s"Close YT Client for id $id")
    fsClient.get(id).foreach(_.close())
    fsClient.remove(id)
  }
}
