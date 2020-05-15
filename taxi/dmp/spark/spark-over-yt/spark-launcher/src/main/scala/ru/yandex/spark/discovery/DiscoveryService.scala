package ru.yandex.spark.discovery

import java.net.{InetSocketAddress, Socket}

import com.google.common.net.HostAndPort
import org.apache.log4j.Logger
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.ytree.YTreeNode

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait DiscoveryService extends AutoCloseable {
  def register(operationId: String, address: Address, clusterVersion: String, clusterConf: SparkClusterConf): Unit

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
      socket.connect(new InetSocketAddress(hostPort.getHost, hostPort.getPort), (5 seconds).toMillis.toInt)
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

case class SparkClusterConf(spark_conf: Map[String, String]) {
  def toYson: YTreeNode = {
    this.getClass.getDeclaredFields.foldLeft(new YTreeBuilder().beginMap()){case (builder, f) =>
      f.setAccessible(true)
      toYson(f.get(this), builder.key(f.getName))
    }.endMap().build()
  }

  def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
    value match {
      case m: Map[String, String] => m.foldLeft(builder.beginMap()){case (b, (k, v)) => b.key(k).value(v)}.endMap()
      case s: Seq[String] => s.foldLeft(builder.beginList()){case (b, v) => b.value(v)}.endList()
      case _ => builder.value(value)
    }
  }
}
