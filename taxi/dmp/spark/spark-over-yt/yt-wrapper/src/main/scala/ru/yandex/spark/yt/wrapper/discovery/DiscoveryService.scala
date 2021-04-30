package ru.yandex.spark.yt.wrapper.discovery

import java.net.{InetSocketAddress, Socket}

import com.google.common.net.HostAndPort
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait DiscoveryService {
  def register(operationId: String,
               address: Address,
               clusterVersion: String,
               masterWrapperEndpoint: HostAndPort,
               clusterConf: SparkConfYsonable): Unit

  def registerSHS(address: HostAndPort): Unit

  def discoverAddress(): Option[Address]

  def masterWrapperEndpoint(): Option[HostAndPort]

  def waitAddress(timeout: Duration): Option[Address]

  def waitAlive(hostPort: HostAndPort, timeout: Duration): Boolean
}

object DiscoveryService {
  private val log = LoggerFactory.getLogger(getClass)

  @tailrec
  final def waitFor[T](f: => Option[T], timeout: Long, retryCount: Int = 2): Option[T] = {
    val start = System.currentTimeMillis()

    f match {
      case r@Some(_) => r
      case _ =>
        log.info("Sleep 5 seconds before next retry")
        Thread.sleep((5 seconds).toMillis)
        log.info(s"Retry ($retryCount)")
        if (timeout > 0) {
          waitFor(f, timeout - (System.currentTimeMillis() - start), retryCount + 1)
        } else {
          None
        }
    }
  }

  def waitFor[T](f: => Option[T], timeout: Duration): Option[T] = {
    waitFor(f, timeout.toMillis)
  }

  def waitFor(f: => Boolean, timeout: Duration): Boolean = {
    waitFor(Option(true).filter(_ => f), timeout.toMillis).getOrElse(false)
  }

  @tailrec
  def isAlive(hostPort: HostAndPort, retry: Int): Boolean = {
    val socket = new Socket()
    val res = Try(
      socket.connect(new InetSocketAddress(hostPort.getHostText, hostPort.getPort), (5 seconds).toMillis.toInt)
    )
    socket.close()
    res match {
      case Success(_) => true
      case Failure(_) => if (retry > 0) {
        Thread.sleep((5 seconds).toMillis)
        isAlive(hostPort, retry - 1)
      } else false
    }
  }
}
