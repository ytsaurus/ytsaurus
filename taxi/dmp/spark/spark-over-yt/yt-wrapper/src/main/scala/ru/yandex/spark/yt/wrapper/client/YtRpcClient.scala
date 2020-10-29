package ru.yandex.spark.yt.wrapper.client

import org.apache.log4j.Logger
import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.YtClient

case class YtRpcClient(id: String, yt: YtClient, connector: DefaultBusConnector) extends AutoCloseable {
  private val log = Logger.getLogger(getClass)

  def close(): Unit = {
    log.info(s"Close yt client $id")
    yt.close()
    connector.close()
  }
}
