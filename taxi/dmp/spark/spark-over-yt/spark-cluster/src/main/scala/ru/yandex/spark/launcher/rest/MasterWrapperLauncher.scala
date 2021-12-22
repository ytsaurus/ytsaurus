package ru.yandex.spark.launcher.rest

import com.google.common.net.HostAndPort
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.ByopLauncher.ByopConfig
import ru.yandex.spark.launcher.Service.{BasicService, MasterService}
import ru.yandex.spark.launcher.VanillaLauncher

trait MasterWrapperLauncher {
  self: VanillaLauncher =>

  private val log = LoggerFactory.getLogger(getClass)

  private def startMasterWrapper(masterEndpoint: HostAndPort, byopPort: Option[Int])
                                (port: Int): (Thread, Int) = {
    val thread = MasterWrapperServer.start(port, masterEndpoint, byopPort).joinThread()
    (thread, port)
  }

  def startMasterWrapper(args: Array[String], master: MasterService): BasicService = {
    log.info("Start master wrapper")

    val startPort = sparkSystemProperties.get("spark.master.port").map(_.toInt).getOrElse(27001)
    val maxRetries = sparkSystemProperties.get("spark.port.maxRetries").map(_.toInt).getOrElse(200)

    val byopPort = ByopConfig.byopPort(sparkSystemProperties, args)

    val (thread, port) = Utils.startServiceOnPort(
      startPort, startMasterWrapper(master.masterAddress.restHostAndPort, byopPort), maxRetries
    )

    BasicService("MasterWrapper", port, thread)
  }

}
