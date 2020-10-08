package ru.yandex.spark.yt.fs

import java.util.concurrent.atomic.AtomicReference

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

  private val conf = new AtomicReference[YtClientConfiguration]
  private val client = mutable.HashMap.empty[String, YtRpcClient]

  private def threadId: String = Thread.currentThread().getId.toString

  private def sparkDefaultConf: YtClientConfiguration = SparkSession.getDefaultSession
    .map(ytClientConfiguration)
    .getOrElse(throw new IllegalStateException("Spark is not initialized"))

  def ytClient(conf: YtClientConfiguration, id: String): YtClient = ytRpcClient(conf, id).yt

  // for java
  def ytClient(conf: YtClientConfiguration): YtClient = ytRpcClient(conf, threadId).yt

  def ytClient: YtClient = client.getOrElseUpdate(threadId, ytRpcClient(sparkDefaultConf)).yt

  def ytRpcClient(conf: YtClientConfiguration, id: String = threadId): YtRpcClient = client.getOrElseUpdate(threadId, {
    this.conf.set(conf)
    log.info(s"Create YtClient for id $id")
    YtWrapper.createRpcClient(conf)
  })

  def httpClient: Yt = {
    YtWrapper.createHttpClient(conf.get())
  }

  def close(): Unit = {
    log.info(s"Close all YT Clients")
    client.foreach(_._2.close())
    client.clear()
  }

  def close(id: String): Unit = {
    log.info(s"Close YT Client for id $id")
    client.get(id).foreach(_.close())
    client.remove(id)
  }
}
