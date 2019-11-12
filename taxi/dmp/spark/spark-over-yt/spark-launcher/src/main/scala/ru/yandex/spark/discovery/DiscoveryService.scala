package ru.yandex.spark.discovery

import com.google.common.net.HostAndPort

import scala.concurrent.duration.Duration

trait DiscoveryService extends AutoCloseable {
  def register(id: String, operationId: String, address: Address): Unit

  def getAddress(id: String): Option[Address]

  def waitAddress(id: String, timeout: Duration): Option[Address]

  def removeAddress(id: String): Unit

  def checkPeriodically(hostPort: HostAndPort): Unit
}

case class Address(host: String, port: Int, webUiPort: Int, restPort: Int) {
  def hostAndPort: HostAndPort = HostAndPort.fromParts(host, port)

  def webUiHostAndPort: HostAndPort = HostAndPort.fromParts(host, webUiPort)

  def restHostAndPort: HostAndPort = HostAndPort.fromParts(host, restPort)
}

object Address {
  def apply(hostAndPort: HostAndPort, webUiHostAndPort: HostAndPort, restHostAndPort: HostAndPort): Address = {
    Address(hostAndPort.getHost, hostAndPort.getPort, webUiHostAndPort.getPort, restHostAndPort.getPort)
  }
}
