package ru.yandex.spark.launcher.rest

import java.net.InetAddress

import com.google.common.net.HostAndPort
import ru.yandex.spark.discovery.Address
import ru.yandex.spark.launcher.RpcProxyLauncher.RpcProxyConfig
import ru.yandex.spark.launcher.VanillaLauncher

trait MasterWrapperLauncher {
  self: VanillaLauncher =>

  private def startMasterWrapper(masterEndpoint: HostAndPort, byopPort: Option[Int])
                                (port: Int): (Thread, Int) = {
    val thread = MasterWrapperServer.start(port, masterEndpoint, byopPort).joinThread()
    (thread, port)
  }

  def startMasterWrapper(args: Array[String], masterAddress: Address): (HostAndPort, Thread) = {
    val startPort = sparkSystemProperties.get("spark.master.port").map(_.toInt).getOrElse(27001)
    val maxRetries = sparkSystemProperties.get("spark.port.maxRetries").map(_.toInt).getOrElse(200)

    val byopPort = RpcProxyConfig.byopPort(sparkSystemProperties, args)

    val (thread, port) = Utils.startServiceOnPort(
      startPort, startMasterWrapper(masterAddress.restHostAndPort, byopPort), maxRetries
    )
    val hostAndPort = HostAndPort.fromParts(InetAddress.getLocalHost.getHostName, port)

    (hostAndPort, thread)
  }

}
