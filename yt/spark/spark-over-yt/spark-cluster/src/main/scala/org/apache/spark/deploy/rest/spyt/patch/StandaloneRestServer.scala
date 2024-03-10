package org.apache.spark.deploy.rest.spyt.patch

import org.apache.spark.SparkConf
import org.apache.spark.deploy.rest._
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.patch.annotations.OriginClass

/**
 * Patches:
 * 1. Set `host` parameter to null in superclass constructor. Main reason: we need to bind RPC endpoint to wildcard
 *    network interface for Kubernetes deployments with host network.
 */
@OriginClass("org.apache.spark.deploy.rest.StandaloneRestServer")
private[deploy] class StandaloneRestServer(host: String,
                                           requestedPort: Int,
                                           masterConf: SparkConf,
                                           masterEndpoint: RpcEndpointRef,
                                           masterUrl: String)
  extends RestSubmissionServer(null, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet =
    new YtStatusRequestServlet(masterEndpoint, masterConf)

  protected val masterStateRequestServlet =
    new StandaloneMasterStateRequestServlet(masterEndpoint, masterConf)
  protected val appIdRequestServlet =
    new StandaloneAppIdRequestServlet(masterEndpoint, masterConf)
  protected val appStatusRequestServlet =
    new StandaloneAppStatusRequestServlet(masterEndpoint, masterConf)

  protected override lazy val contextToServlet: Map[String, RestServlet] = Map(
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/kill/*" -> killRequestServlet,
    s"$baseContext/status/*" -> statusRequestServlet,
    s"$baseContext/master/*" -> masterStateRequestServlet,
    s"$baseContext/getAppId/*" -> appIdRequestServlet,
    s"$baseContext/getAppStatus/*" -> appStatusRequestServlet,
    "/*" -> new ErrorServlet // default handler
  )
}