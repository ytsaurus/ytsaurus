package ru.yandex.spark.yt.wrapper.client

import org.slf4j.LoggerFactory
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.CompoundClient

case class YtRpcClient(id: String, yt: CompoundClient, connector: DefaultBusConnector) extends AutoCloseable {
  private val log = LoggerFactory.getLogger(getClass)

  def close(): Unit = {
    log.info(s"Close yt client $id")
    yt.close()
    connector.close()
  }
}
