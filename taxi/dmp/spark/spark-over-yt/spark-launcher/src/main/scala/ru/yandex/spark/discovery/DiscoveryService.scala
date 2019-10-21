package ru.yandex.spark.discovery

import com.google.common.net.HostAndPort

import scala.concurrent.duration.Duration

trait DiscoveryService extends AutoCloseable {
  def register(id: String, operationId: String, host: String, port: Int, webUiPort: Int): Unit

  def getAddress(id: String): Option[HostAndPort]

  def waitAddress(id: String, timeout: Duration): Option[HostAndPort]

  def removeAddress(id: String): Unit

  def checkPeriodically(hostPort: HostAndPort): Unit
}
