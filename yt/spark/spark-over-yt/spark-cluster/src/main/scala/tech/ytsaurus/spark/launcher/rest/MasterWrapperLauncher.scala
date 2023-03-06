package tech.ytsaurus.spark.launcher.rest

import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.launcher.ByopLauncher.ByopConfig
import tech.ytsaurus.spark.launcher.Service.{BasicService, MasterService}
import tech.ytsaurus.spark.launcher.VanillaLauncher
import tech.ytsaurus.spyt.HostAndPort

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
