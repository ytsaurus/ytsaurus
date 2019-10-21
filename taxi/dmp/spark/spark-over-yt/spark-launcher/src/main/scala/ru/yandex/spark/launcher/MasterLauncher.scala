package ru.yandex.spark.launcher

import java.net.InetAddress

import com.google.common.net.HostAndPort
import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.CypressDiscoveryService
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App {
  private val log = Logger.getLogger(getClass)
  val masterArgs = MasterLauncherArgs(args)
  val discoveryService = new CypressDiscoveryService(masterArgs.ytConfig, masterArgs.discoveryPath)
  val host = InetAddress.getLocalHost.getHostName

  try {
    log.info("Start master")
    val boundPorts = SparkLauncher.startMaster(Ports(masterArgs.port, masterArgs.webUiPort))
    val hostAndPort = HostAndPort.fromParts(host, boundPorts.port)
    discoveryService.waitAlive(hostAndPort, (5 minutes).toMillis)
    log.info(s"Master started at port ${boundPorts.port}")

    log.info("Register master")
    discoveryService.register(masterArgs.id, masterArgs.operationId, host, boundPorts.port, boundPorts.webUiPort)
    log.info("Master registered")

    try {
      discoveryService.checkPeriodically(hostAndPort)
    } finally {
      discoveryService.removeAddress(masterArgs.id)
    }
  } finally {
    discoveryService.close()
  }
}

case class MasterLauncherArgs(id: String,
                              port: Int,
                              webUiPort: Int,
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String,
                              operationId: String)

object MasterLauncherArgs {
  def apply(args: Args): MasterLauncherArgs = MasterLauncherArgs(
    args.required("id"),
    args.optional("port").map(_.toInt).getOrElse(7077),
    args.optional("web-ui-port").map(_.toInt).getOrElse(8080),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.required("operation-id")
  )

  def apply(args: Array[String]): MasterLauncherArgs = MasterLauncherArgs(Args(args))
}
