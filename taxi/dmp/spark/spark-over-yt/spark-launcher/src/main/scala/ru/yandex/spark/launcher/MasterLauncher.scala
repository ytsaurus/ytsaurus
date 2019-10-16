package ru.yandex.spark.launcher

import java.net.InetAddress

import com.google.common.net.HostAndPort
import com.twitter.scalding.Args
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.model.CypressDiscoveryService
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App {
  private val log = Logger.getLogger(getClass)
  val masterLauncherArgs = MasterLauncherArgs(args)
  val discoveryService = new CypressDiscoveryService(masterLauncherArgs.ytConfig, masterLauncherArgs.discoveryPath)
  val hostAndPort = HostAndPort.fromParts(InetAddress.getLocalHost.getHostName, masterLauncherArgs.port)

  try {
    log.info("Start master")
    SparkLauncher.startMaster(masterLauncherArgs.port)
    discoveryService.waitAlive(hostAndPort, (5 minutes).toMillis)
    log.info(s"Master started at port ${masterLauncherArgs.port}")

    log.info("Register master")
    discoveryService.register(masterLauncherArgs.id, masterLauncherArgs.operationId, hostAndPort)
    log.info("Master registered")

    try {
      discoveryService.checkPeriodically(hostAndPort)
    } finally {
      discoveryService.removeAddress(masterLauncherArgs.id)
    }
  } finally {
    discoveryService.close()
  }
}

case class MasterLauncherArgs(id: String,
                              port: Int,
                              ytConfig: YtClientConfiguration,
                              discoveryPath: String,
                              operationId: String)

object MasterLauncherArgs {
  def apply(args: Args): MasterLauncherArgs = MasterLauncherArgs(
    args.required("id"),
    args.optional("port").map(_.toInt).getOrElse(7077),
    YtClientConfiguration(args.optional),
    args.optional("discovery-path").getOrElse(sys.env("SPARK_DISCOVERY_PATH")),
    args.required("operation-id")
  )

  def apply(args: Array[String]): MasterLauncherArgs = MasterLauncherArgs(Args(args))
}
