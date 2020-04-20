package ru.yandex.spark.discovery

import java.io.IOException
import java.net.{InetSocketAddress, Socket}

import com.google.common.net.HostAndPort
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait DiscoveryService extends AutoCloseable {
  def register(operationId: String, address: Address, clusterVersion: String): Unit

  def registerSHS(address: Address): Unit

  def discoverAddress(): Option[Address]

  def waitAddress(timeout: Duration): Option[Address]

  def waitAlive(hostPort: HostAndPort, timeout: Duration): Boolean

  def removeAddress(): Unit
}

object DiscoveryService {
  private val log = Logger.getLogger(getClass)

  @tailrec
  final def waitFor[T](f: => Option[T], timeout: Long, retryCount: Int = 2): Option[T] = {
    val start = System.currentTimeMillis()

    f match {
      case r@Some(_) => r
      case _ =>
        log.info("Sleep 5 seconds before next retry")
        Thread.sleep(5000)
        log.info(s"Retry ($retryCount)")
        if (timeout > 0) {
          waitFor(f, timeout - (System.currentTimeMillis() - start), retryCount + 1)
        } else {
          None
        }
    }
  }

  def waitFor(f: => Boolean, timeout: Duration): Boolean = {
    waitFor(Option(true).filter(_ => f), timeout.toMillis).getOrElse(false)
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

case class Address(host: String, port: Int, webUiPort: Option[Int], restPort: Option[Int]) {
  def hostAndPort: HostAndPort = HostAndPort.fromParts(host, port)

  def webUiHostAndPort: HostAndPort = HostAndPort.fromParts(host, webUiPort.get)

  def restHostAndPort: HostAndPort = HostAndPort.fromParts(host, restPort.get)
}

object Address {
  def apply(hostAndPort: HostAndPort, webUiHostAndPort: HostAndPort, restHostAndPort: HostAndPort): Address = {
    Address(hostAndPort.getHost, hostAndPort.getPort, Some(webUiHostAndPort.getPort), Some(restHostAndPort.getPort))
  }
}
