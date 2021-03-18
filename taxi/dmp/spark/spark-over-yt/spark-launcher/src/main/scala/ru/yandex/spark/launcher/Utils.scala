package ru.yandex.spark.launcher

import java.net.InetAddress

object Utils {
  def ytNetworkProjectEnabled: Boolean = sys.env.contains("YT_NETWORK_PROJECT_ID")

  def ytHostIp: String = sys.env("YT_IP_ADDRESS_DEFAULT")

  def ytHostnameOrIpAddress: String = {
    if (ytNetworkProjectEnabled) ytHostIp else InetAddress.getLocalHost.getHostName
  }
}
