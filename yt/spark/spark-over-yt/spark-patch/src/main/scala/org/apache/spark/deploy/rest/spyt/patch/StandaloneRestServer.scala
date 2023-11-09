package org.apache.spark.deploy.rest.spyt.patch

import org.apache.spark.SparkConf
import org.apache.spark.deploy.rest.{RestSubmissionServer, StandaloneAppIdRequestServlet, StandaloneAppStatusRequestServlet, StandaloneKillRequestServlet, StandaloneMasterStateRequestServlet, StandaloneStatusRequestServlet, StandaloneSubmitRequestServlet}
import org.apache.spark.rpc.RpcEndpointRef

/**
 * Patches:
 * 1. Set `host` parameter to null in superclass constructor. Main reason: we need to bind RPC endpoint to wildcard
 *    network interface for Kubernetes deployments with host network.
 */
private[deploy] class StandaloneRestServer(
                                            host: String,
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
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
  protected override val masterStateRequestServlet =
    new StandaloneMasterStateRequestServlet(masterEndpoint, masterConf)
  protected override val appIdRequestServlet =
    new StandaloneAppIdRequestServlet(masterEndpoint, masterConf)
  protected override val appStatusRequestServlet =
    new StandaloneAppStatusRequestServlet(masterEndpoint, masterConf)
}