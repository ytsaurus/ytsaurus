package ru.yandex.spark.launcher

import com.google.common.net.HostAndPort
import org.slf4j.LoggerFactory
import ru.yandex.spark.discovery.{Address, DiscoveryService}

import scala.concurrent.duration.Duration

sealed trait Service {
  private val log = LoggerFactory.getLogger(getClass)

  def name: String

  def thread: Thread

  def isAlive(retry: Int = 0): Boolean = thread.isAlive

  def waitAlive(timeout: Duration): Boolean = {
    DiscoveryService.waitFor(isAlive(), timeout)
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
    thread.interrupt()
  }
}

sealed trait ServiceWithAddress extends Service {
  def address: HostAndPort

  def isAddressAvailable(retry: Int = 0): Boolean = DiscoveryService.isAlive(address, retry)

  override def isAlive(retry: Int = 0): Boolean = isAddressAvailable(retry) && thread.isAlive

  override def waitAlive(timeout: Duration): Boolean = {
    DiscoveryService.waitFor(isAddressAvailable() || !thread.isAlive, timeout)
    thread.isAlive
  }

  override protected def successMessage: String = s"$name started at port ${address.getPort}"
}

object Service {
  case class BasicService(name: String, address: HostAndPort, thread: Thread) extends ServiceWithAddress

  object BasicService {
    def apply(name: String, port: Int, thread: Thread): BasicService = {
      BasicService(name, HostAndPort.fromParts(Utils.ytHostnameOrIpAddress, port), thread)
    }
  }

  case class MasterService(name: String, masterAddress: Address, thread: Thread) extends ServiceWithAddress {
    override def address: HostAndPort = masterAddress.hostAndPort
  }

  case class WorkerService(name: String, thread: Thread) extends Service
}
