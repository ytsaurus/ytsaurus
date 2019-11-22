package ru.yandex.spark.discovery

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

  private def restPath(id: String): String = s"$discoveryPath/$id/rest"

  private def operationPath(id: String): String = s"$discoveryPath/$id/operation"

  private def shsPath(id: String): String = s"$discoveryPath/$id/shs"

  override def register(id: String, operationId: String, address: Address): Unit = {
    getAddress(id) match {
      case Some(address) if DiscoveryService.isAlive(address.hostAndPort) && getOperation(id).exists(_ != operationId) =>
        throw new IllegalStateException(s"Spark instance with id $id already exists")
      case Some(_) =>
        log.info(s"Spark instance with id $id registered, but is not alive, rewriting id")
        removeAddress(id)
      case _ =>
        log.info(s"Spark instance with id $id doesn't exist, registering new one")
    }

    val tm = new TransactionManager(yt)
    val transaction = tm.start(JDuration.standardMinutes(1)).join()
    try {
      createNode(s"${addressPath(id)}/${address.hostAndPort}", transaction)
      createNode(s"${webUiPath(id)}/${address.webUiHostAndPort}", transaction)
      createNode(s"${restPath(id)}/${address.restHostAndPort}", transaction)
      createNode(s"${operationPath(id)}/$operationId", transaction)
    } catch {
      case e: Throwable =>
        yt.abortTransaction(transaction, true)
        throw e
    }
    yt.commitTransaction(transaction, true)
  }


  override def registerSHS(id: String, address: Address): Unit = {
    val tm = new TransactionManager(yt)
    val transaction = tm.start(JDuration.standardMinutes(1)).join()
    createNode(s"${shsPath(id)}/${address.hostAndPort}", transaction)
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
    val restHostAndPort = cypressHostAndPort(restPath(id))
    Address(hostAndPort, webUiHostAndPort, restHostAndPort)
  }.toOption

  private def getOperation(id: String): Option[String] = Try {
    yt.getNode(operationPath(id)).join().asMap().keys().first()
  }.toOption

  override def waitAddress(id: String, timeout: Duration): Option[Address] = {
    DiscoveryService.waitFor(getAddress(id).filter(a => DiscoveryService.isAlive(a.hostAndPort)), timeout.toMillis)
  }

  override def waitAlive(hostPort: HostAndPort, timeout: Duration): Boolean = {
    waitAlive(hostPort, timeout.toMillis)
  }

  @tailrec
  private def waitAlive(hostPort: HostAndPort, timeout: Long, retryCount: Int = 2): Boolean = {
    val start = System.currentTimeMillis()
    if (timeout < 0) {
      false
    } else {
      if (!DiscoveryService.isAlive(hostPort)) {
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

  override def close(): Unit = {
    client.close()
  }
}
