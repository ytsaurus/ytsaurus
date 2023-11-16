package tech.ytsaurus.spark.launcher

import org.slf4j.{Logger, LoggerFactory}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.CreateNode
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.discovery.Address
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


object TcpProxyService {
  case class TcpRouter(externalAddress: String, mapping: Map[String, Int]) {
    def getExternalAddress(internalName: String): HostAndPort = HostAndPort(externalAddress, getPort(internalName))

    def getPort(internalName: String): Int = mapping(internalName)
  }

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val EXPIRATION_TIMEOUT: Long = (10 minutes).toMillis

  private val DEFAULT_ROUTES: YPath = YPath.simple("//sys/tcp_proxies/routes/default")

  private val isEnabled: Boolean = {
    sys.env.get("SPARK_YT_TCP_PROXY_ENABLED").exists(_.toBoolean)
  }

  private val startPort: Int = {
    sys.env.get("SPARK_YT_TCP_PROXY_RANGE_START").map(_.toInt).getOrElse(30000)
  }

  private val endPort: Int = {
    startPort + sys.env.get("SPARK_YT_TCP_PROXY_RANGE_SIZE").map(_.toInt).getOrElse(1000)
  }

  private def proxyAddress(implicit yt: CompoundClient): Option[String] = {
    if (isEnabled) {
      try {
        val externalAddresses = YtWrapper.attribute(DEFAULT_ROUTES, "external_addresses", None).asList()
        if (externalAddresses.size() >= 1) {
          Some(externalAddresses.get(0).stringValue())
        } else {
          None
        }
      } catch {
        case e: Exception =>
          log.warn(f"Error while get external addresses request", e)
          None
      }
    } else {
      None
    }
  }

  private def portYPath(port: Int): YPath = DEFAULT_ROUTES.child(port.toString)

  private def isPortBusy(port: Int)(implicit yt: CompoundClient): Boolean = YtWrapper.exists(portYPath(port))

  private def buildEndpointsNode(address: String): YTreeNode = YTree.listBuilder().value(address).endList().build()

  private def createPortNode(address: String, port: Int)(implicit yt: CompoundClient): Unit = {
    val attributes = java.util.Map.of(
      "expiration_timeout", YTree.longNode(EXPIRATION_TIMEOUT),
      "endpoints", buildEndpointsNode(address)
    )
    val createRequest = CreateNode.builder()
      .setPath(portYPath(port)).setType(CypressNodeType.MAP).setAttributes(attributes)
      .build()
    yt.createNode(createRequest).join()
  }

  private def tryTakePort(address: String, port: Int)(implicit yt: CompoundClient): Boolean = {
    if (!isPortBusy(port)) {
      try {
        createPortNode(address, port)
        true
      } catch {
        case e: Exception =>
          log.warn(f"Error while creating port $port map node request", e)
          false
      }
    } else {
      false
    }
  }

  @tailrec
  private def takeFreePortIterative(address: String, current: Int)(implicit yt: CompoundClient): Int = {
    if (current >= endPort) {
      throw new IllegalStateException("No free ports found")
    }
    if (tryTakePort(address, current)) {
      current
    } else {
      log.debug(f"Port $current is busy")
      takeFreePortIterative(address, current + 1)
    }
  }

  private def takeFreePort(address: String)(implicit yt: CompoundClient): Int = {
    log.debug(f"Search free port for address $address")
    takeFreePortIterative(address, startPort)
  }

  private def takeFreePorts(addresses: Seq[String])(implicit yt: CompoundClient): Map[String, Int] = {
    addresses.map(x => x -> takeFreePort(x)).toMap
  }

  private def pingPortNode(address: String, port: Int)(implicit yt: CompoundClient): Unit = {
    try {
      if (!isPortBusy(port)) log.warn(f"Reserved port $port for address $address is free now")
    } catch {
      case e: Exception => log.warn(f"Error while ping port $port map node", e)
    }
  }

  def register(addresses: String*)(implicit yt: CompoundClient): Option[TcpRouter] = {
    if (isEnabled) {
      val externalAddress = proxyAddress.get
      val addressesWithPorts = takeFreePorts(addresses)
      log.info(f"External address: $externalAddress. Ports for given addresses $addressesWithPorts")
      val tcpRouter = TcpRouter(externalAddress, addressesWithPorts)
      val thread = new Thread(() => {
        while (true) {
          Thread.sleep((30 seconds).toMillis)
          log.debug(f"Ping proxy port nodes")
          tcpRouter.mapping.foreach { case (address, port) => pingPortNode(address, port) }
        }
      })
      thread.setDaemon(true)
      thread.start()
      log.info("TcpProxyService started")
      Some(tcpRouter)
    } else {
      None
    }
  }

  def register(address: Address)(implicit yt: CompoundClient): Option[TcpRouter] = {
    register(address.hostAndPort.toString, address.webUiHostAndPort.toString, address.restHostAndPort.toString)
  }

  def updateTcpAddress(address: String, port: Int)(implicit yt: CompoundClient): Unit = {
    log.info(f"Update address $address request for port $port")
    try {
      YtWrapper.setAttribute(portYPath(port).toString, "endpoints", buildEndpointsNode(address))
    } catch {
      case e: Exception => log.warn(f"Error while updating port $port map node to address $address", e)
    }
  }
}
