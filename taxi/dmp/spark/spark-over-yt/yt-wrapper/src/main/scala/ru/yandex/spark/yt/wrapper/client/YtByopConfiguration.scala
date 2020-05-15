package ru.yandex.spark.yt.wrapper.client

import ru.yandex.yt.ytclient.proxy.internal.HostPort

case class YtByopConfiguration(host: String, port: Int) {
  def address: HostPort = HostPort.parse(s"$host:$port")
}
