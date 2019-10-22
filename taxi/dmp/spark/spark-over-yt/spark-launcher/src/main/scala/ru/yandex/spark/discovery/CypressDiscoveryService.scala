package ru.yandex.spark.discovery

import java.io.IOException
import java.net.{InetSocketAddress, Socket}

import com.google.common.net.HostAndPort
import org.apache.log4j.Logger
import org.joda.time.{Duration => JDuration}
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.rpc.TransactionManager
import ru.yandex.spark.yt.utils.{YtClientConfiguration, YtUtils}
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType, RemoveNode, TransactionalOptions}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class CypressDiscoveryService(config: YtClientConfiguration,
                              discoveryPath: String) extends DiscoveryService {
  private val log = Logger.getLogger(getClass)
  private val client = YtUtils.createRpcClient(config)
  private val yt = client.yt

  private def addressPath(id: String): String = s"$discoveryPath/$id/address"

  private def webUiPath(id: String): String = s"$discoveryPath/$id/webui"

  private def operationPath(id: String): String = s"$discoveryPath/$id/operation"

  override def register(id: String, operationId: String, address: Address): Unit = {
    getAddress(id) match {
      case Some(address) if isAlive(address.hostAndPort) =>
        throw new IllegalStateException(s"Spark instance with id $id already exists")
      case Some(_) =>
        log.info(s"Spark instance with id $id registered, but is not alive, rewriting id")
        removeAddress(id)
      case _ => log.info(s"Spark instance with id $id doesn't exist, registering new one")
    }

    val tm = new TransactionManager(yt)
    val transaction = tm.start(JDuration.standardMinutes(1)).join()
    try {
      createNode(s"${addressPath(id)}/${address.hostAndPort}", transaction)
      createNode(s"${webUiPath(id)}/${address.webUiHostAndPort}", transaction)
      createNode(s"${operationPath(id)}/$operationId", transaction)
    } catch {
      case e: Throwable =>
        yt.abortTransaction(transaction, true)
        throw e
    }
    yt.commitTransaction(transaction, true)
  }

  private def createNode(path: String, transaction: GUID): Unit = {
    val request = new CreateNode(path, ObjectType.MapNode)
      .setRecursive(true)
      .setTransactionalOptions(new TransactionalOptions(transaction))
    yt.createNode(request).join()
  }

  private def cypressHostAndPort(path: String): HostAndPort = {
    HostAndPort.fromString(yt.getNode(path).join().asMap().keys().first())
  }

  override def getAddress(id: String): Option[Address] = Try {
    val hostAndPort = cypressHostAndPort(addressPath(id))
    val webUiHostAndPort = cypressHostAndPort(webUiPath(id))
    Address(hostAndPort, webUiHostAndPort)
  }.toOption

  override def waitAddress(id: String, timeout: Duration): Option[Address] = {
    waitAddress(id, timeout.toMillis)
  }

  @tailrec
  private def waitAddress(id: String, timeout: Long, retryCount: Int = 2): Option[Address] = {
    val start = System.currentTimeMillis()
    val maybeAddress = getAddress(id)

    maybeAddress match {
      case Some(address) if isAlive(address.hostAndPort) => maybeAddress
      case _ =>
        log.info("Sleep 200 milliseconds before next retry")
        Thread.sleep(200)
        log.info(s"Retry ($retryCount)")
        if (timeout > 0) {
          waitAddress(id, timeout - (System.currentTimeMillis() - start), retryCount + 1)
        } else {
          None
        }
    }
  }

  @tailrec
  final def waitAlive(hostPort: HostAndPort, timeout: Long, retryCount: Int = 2): Boolean = {
    val start = System.currentTimeMillis()
    if (timeout < 0) {
      false
    } else {
      if (!isAlive(hostPort)) {
        Thread.sleep((10 seconds).toMillis)
        waitAlive(hostPort, timeout - (System.currentTimeMillis() - start), retryCount + 1)
      } else {
        true
      }
    }

  }

  override def removeAddress(id: String): Unit = {
    val request = new RemoveNode(s"$discoveryPath/$id").setRecursive(true)
    yt.removeNode(request).join()
  }

  private def isAlive(hostPort: HostAndPort): Boolean = {
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

  @tailrec
  override final def checkPeriodically(hostPort: HostAndPort): Unit = {
    if (isAlive(hostPort)) {
      Thread.sleep((10 seconds).toMillis)
      checkPeriodically(hostPort)
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
