package ru.yandex.spark.yt.wrapper.discovery

import ru.yandex.spark.HostAndPort

case class Address(host: String, port: Int, webUiPort: Option[Int], restPort: Option[Int]) {
  def hostAndPort: HostAndPort = HostAndPort(host, port)

  def webUiHostAndPort: HostAndPort = HostAndPort(host, webUiPort.get)

  def restHostAndPort: HostAndPort = HostAndPort(host, restPort.get)
}

object Address {
  def apply(hostAndPort: HostAndPort, webUiHostAndPort: HostAndPort, restHostAndPort: HostAndPort): Address = {
    Address(hostAndPort.host, hostAndPort.port, Some(webUiHostAndPort.port), Some(restHostAndPort.port))
  }
}