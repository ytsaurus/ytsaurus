package ru.yandex.spark.discovery

import com.google.common.net.HostAndPort
import org.apache.log4j.Logger
import org.joda.time.{Duration => JDuration}
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.rpc.TransactionManager
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType, RemoveNode, TransactionalOptions}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class CypressDiscoveryService(config: YtClientConfiguration,
                              discoveryPath: String) extends DiscoveryService {
  private val log = Logger.getLogger(getClass)
  private val client = YtWrapper.createRpcClient(config)
  private val yt = client.yt

  private def addressPath: String = s"$discoveryPath/spark_address"

  private def webUiPath: String = s"$discoveryPath/webui"

  private def restPath: String = s"$discoveryPath/rest"

  private def operationPath: String = s"$discoveryPath/operation"

  private def shsPath: String = s"$discoveryPath/shs"

  private def clusterVersionPath: String = s"$discoveryPath/version"

  override def register(operationId: String, address: Address, clusterVersion: String): Unit = {
    discoverAddress() match {
      case Some(address) if DiscoveryService.isAlive(address.hostAndPort) && operation.exists(_ != operationId) =>
        throw new IllegalStateException(s"Spark instance with path $discoveryPath already exists")
      case Some(_) =>
        log.info(s"Spark instance with path $discoveryPath registered, but is not alive, rewriting id")
        removeAddress()
      case _ =>
        log.info(s"Spark instance with path $discoveryPath doesn't exist, registering new one")
    }

    val tm = new TransactionManager(yt)
    val transaction = tm.start(JDuration.standardMinutes(1)).join()
    try {
      createNode(s"$addressPath/${address.hostAndPort}", transaction)
      createNode(s"$webUiPath/${address.webUiHostAndPort}", transaction)
      createNode(s"$restPath/${address.restHostAndPort}", transaction)
      createNode(s"$operationPath/$operationId", transaction)
      createNode(s"$clusterVersionPath/$clusterVersion", transaction)
    } catch {
      case e: Throwable =>
        yt.abortTransaction(transaction, true)
        throw e
    }
    yt.commitTransaction(transaction, true)
  }


  override def registerSHS(address: Address): Unit = {
    val tm = new TransactionManager(yt)
    val transaction = tm.start(JDuration.standardMinutes(1)).join()
    removeNode(shsPath, Some(transaction))
    createNode(s"$shsPath/${address.hostAndPort}", transaction)
    yt.commitTransaction(transaction, true)
  }

  private def createNode(path: String, transaction: GUID): Unit = {
    val request = new CreateNode(path, ObjectType.MapNode)
      .setRecursive(true)
      .setTransactionalOptions(new TransactionalOptions(transaction))
    yt.createNode(request).join()
  }

  private def removeNode(path: String, transaction: Option[GUID] = None): Unit = {
    if (yt.existsNode(path).join()) {
      log.info(s"Removing $path")
      val request = new RemoveNode(path).setRecursive(true)
      transaction.foreach(t => request.setTransactionalOptions(new TransactionalOptions(t)))
      yt.removeNode(request).join()
      log.info(s"Removed $path")
    }
  }

  private def cypressHostAndPort(path: String): HostAndPort = {
    HostAndPort.fromString(yt.getNode(path).join().asMap().keys().first())
  }

  override def discoverAddress(): Option[Address] = Try {
    val hostAndPort = cypressHostAndPort(addressPath)
    val webUiHostAndPort = cypressHostAndPort(webUiPath)
    val restHostAndPort = cypressHostAndPort(restPath)
    Address(hostAndPort, webUiHostAndPort, restHostAndPort)
  }.toOption

  private def operation: Option[String] = Try {
    yt.getNode(operationPath).join().asMap().keys().first()
  }.toOption

  override def waitAddress(timeout: Duration): Option[Address] = {
    DiscoveryService.waitFor(discoverAddress().filter(a => DiscoveryService.isAlive(a.hostAndPort)), timeout.toMillis)
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

  override def removeAddress(): Unit = {
    removeNode(addressPath)
    removeNode(webUiPath)
    removeNode(restPath)
    removeNode(operationPath)
    removeNode(clusterVersionPath)
  }

  override def close(): Unit = {
    client.close()
  }
}
