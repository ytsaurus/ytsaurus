package ru.yandex.spark.yt.wrapper.client

import ru.yandex.yt.ytclient.bus.DefaultBusConnector
import ru.yandex.yt.ytclient.proxy.YtClient

case class YtRpcClient(yt: YtClient, connector: DefaultBusConnector) extends AutoCloseable {
  def close(): Unit = {
    yt.close()
    connector.close()
  }
}
