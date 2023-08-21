package tech.ytsaurus.spyt.wrapper.client

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap

object YtClientProvider {
  private val log = LoggerFactory.getLogger(getClass)

  private val conf = new AtomicReference[YtClientConfiguration]
  private val client = TrieMap.empty[String, YtRpcClient]

  private def threadId: String = Thread.currentThread().getId.toString

  def ytClient(conf: YtClientConfiguration, id: String): CompoundClient = ytRpcClient(conf, id).yt

  // for java
  def ytClient(conf: YtClientConfiguration): CompoundClient = ytRpcClient(conf, threadId).yt

  def cachedClient(id: String): YtRpcClient = client(id)

  def ytRpcClient(conf: YtClientConfiguration, id: String = threadId): YtRpcClient = client.getOrElseUpdate(id, {
    this.conf.set(conf)
    log.info(s"Create YtClient for id $id")
    YtWrapper.createRpcClient(id, conf)
  })

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
