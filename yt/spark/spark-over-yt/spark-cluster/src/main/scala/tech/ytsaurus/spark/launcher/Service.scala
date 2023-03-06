package tech.ytsaurus.spark.launcher

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.Utils.ytHostnameOrIpAddress
import tech.ytsaurus.spyt.wrapper.discovery.{Address, DiscoveryService}
import tech.ytsaurus.spyt.HostAndPort

import scala.concurrent.duration.Duration

sealed trait Service {
  private val log = LoggerFactory.getLogger(getClass)

  def name: String

  def thread: Thread

  def isAlive(retry: Int = 0): Boolean = thread.isAlive

  def waitAlive(timeout: Duration): Boolean = {
    DiscoveryService.waitFor(isAlive(), timeout, name)
    thread.isAlive
  }

  protected def successMessage: String = s"$name started"

  def waitAndThrowIfNotAlive(timeout: Duration): Unit = {
    if (!waitAlive(timeout)) {
      throw new RuntimeException(s"$name didn't start in $timeout, failing job")
    }
    log.info(successMessage)
  }

  def stop(): Unit = {
    log.info(s"Stop $name")
    try {
      thread.interrupt()
      log.info(s"Successfully stopped $name")
    } catch {
      case e: Throwable =>
        log.error(s"Exception while stopping $name")
        log.error(e.getMessage)
    }
  }
}

sealed trait ServiceWithAddress extends Service {
  private val log = LoggerFactory.getLogger(getClass)

  def address: HostAndPort

  def isAddressAvailable(retry: Int = 0): Boolean = DiscoveryService.isAlive(address, retry)

  override def isAlive(retry: Int = 0): Boolean = {
    val isAlive = isAddressAvailable(retry) && thread.isAlive
    if (!isAlive) log.error(s"$name is not alive")
    isAlive
  }

  override def waitAlive(timeout: Duration): Boolean = {
    DiscoveryService.waitFor(isAddressAvailable() || !thread.isAlive, timeout,
      s"$name on port ${address.port}")
    thread.isAlive
  }

  override protected def successMessage: String = s"$name started at port ${address.port}"
}

object Service {
  case class BasicService(name: String, address: HostAndPort, thread: Thread) extends ServiceWithAddress

  case class LocalService(name: String, thread: Thread) extends Service

  object BasicService {
    def apply(name: String, port: Int, thread: Thread): BasicService = {
      BasicService(name, HostAndPort(ytHostnameOrIpAddress, port), thread)
    }
  }

  case class MasterService(name: String, masterAddress: Address, thread: Thread) extends ServiceWithAddress {
    override def address: HostAndPort = masterAddress.webUiHostAndPort
  }
}
