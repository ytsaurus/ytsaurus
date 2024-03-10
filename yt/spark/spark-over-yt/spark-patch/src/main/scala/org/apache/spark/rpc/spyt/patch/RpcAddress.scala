package org.apache.spark.rpc.spyt.patch

import tech.ytsaurus.spyt.patch.annotations.OriginClass

import tech.ytsaurus.spyt.Utils

/**
 * Patches:
 * 1. Support for ipV6 addresses in hostPort
 *
 */
@OriginClass("org.apache.spark.rpc.RpcAddress")
private[spark] case class RpcAddress(host: String, port: Int) {

  def hostPort: String = {
    val newHost = Utils.addBracketsIfIpV6Host(host)
    s"$newHost:$port"
  }

  /** Returns a string in the form of "spark://host:port". */
  def toSparkURL: String = "spark://" + hostPort

  override def toString: String = hostPort
}