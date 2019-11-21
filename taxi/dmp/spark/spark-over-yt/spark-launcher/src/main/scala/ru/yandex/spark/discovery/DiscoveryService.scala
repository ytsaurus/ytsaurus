package ru.yandex.spark.discovery

import java.io.IOException
import java.net.{InetSocketAddress, Socket}

import com.google.common.net.HostAndPort

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait DiscoveryService extends AutoCloseable {
  def register(id: String, operationId: String, address: Address): Unit

  def getAddress(id: String): Option[Address]

  def waitAddress(id: String, timeout: Duration): Option[Address]

  def removeAddress(id: String): Unit
}

object DiscoveryService {
  @tailrec
  final def checkPeriodically(p: => Boolean): Unit = {
    if (p) {
      Thread.sleep((10 seconds).toMillis)
      checkPeriodically(p)
    }
  }

  def isAlive(hostPort: HostAndPort): Boolean = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(hostPort.getHost, hostPort.getPort), (5 seconds).toMillis.toInt)
      true
    } catch {
      case _: IOException => false
    } finally {
      socket.close()
    }
  }
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
