package org.apache.spark.rpc.spyt.patch

import org.apache.spark.{SecurityManager, SparkConf}
import tech.ytsaurus.spyt.patch.annotations.OriginClass

/**
 * Patches:
 * 1. bindAddress is var and is set to null in constructor. Main reason: we need to bind RPC endpoint to wildcard
 *    network interface for Kubernetes deployments with host network.
 */
@OriginClass("org.apache.spark.rpc.RpcEnvConfig")
private[spark] case class RpcEnvConfig(
  conf: SparkConf,
  name: String,
  var bindAddress: String,
  advertiseAddress: String,
  port: Int,
  securityManager: SecurityManager,
  numUsableCores: Int,
  clientMode: Boolean) {
  this.bindAddress = null
}
