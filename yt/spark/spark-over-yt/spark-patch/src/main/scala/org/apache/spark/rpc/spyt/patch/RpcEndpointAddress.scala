package org.apache.spark.rpc.spyt.patch

import org.apache.spark.SparkException
import tech.ytsaurus.spyt.patch.annotations.OriginClass
import tech.ytsaurus.spyt.Utils.{addBracketsIfIpV6Host, removeBracketsIfIpV6Host}


/**
 * Patches:
 * 1. Support for ipV6 addresses in host
 */
@OriginClass("org.apache.spark.rpc.RpcEndpointAddress")
case class RpcEndpointAddress(rpcAddress: RpcAddress, name: String) {

  require(name != null, "RpcEndpoint name must be provided.")

  def this(host: String, port: Int, name: String) = {
    this(RpcAddress(host, port), name)
  }

  override val toString = if (rpcAddress != null) {
    s"spark://$name@${addBracketsIfIpV6Host(rpcAddress.host)}:${rpcAddress.port}"
  } else {
    s"spark-client://$name"
  }
}

@OriginClass("org.apache.spark.rpc.RpcEndpointAddress$")
private[spark] object RpcEndpointAddress {

  def apply(host: String, port: Int, name: String): RpcEndpointAddress = {
    new RpcEndpointAddress(host, port, name)
  }

  def apply(sparkUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = removeBracketsIfIpV6Host(uri.getHost)
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new SparkException("Invalid Spark URL: " + sparkUrl)
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid Spark URL: " + sparkUrl, e)
    }
  }
}
