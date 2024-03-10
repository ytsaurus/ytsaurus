package org.apache.spark.deploy.rest

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.deploy.DeployMessages
import org.apache.spark.deploy.master.WorkerInfo
import org.apache.spark.rpc.RpcEndpointRef

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * A servlet for handling master state requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class MasterStateRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
                                request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    val responseMessage =
      try {
        handleMasterState(response)
      } catch {
        // The client failed to provide a valid JSON, so this is not our fault
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, response)
  }

  protected def handleMasterState(responseServlet: HttpServletResponse): MasterStateResponse
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class MasterStateResponse extends SubmitRestProtocolResponse {
  var workers: Array[WorkerInfo] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(workers, "workers")
  }
}

private[rest] class StandaloneMasterStateRequestServlet(
                                                         masterEndpoint: RpcEndpointRef,
                                                         conf: SparkConf)
  extends MasterStateRequestServlet {
  override protected def handleMasterState(
                                            responseServlet: HttpServletResponse
                                          ): MasterStateResponse = {
    val response = masterEndpoint.askSync[DeployMessages.MasterStateResponse](
      DeployMessages.RequestMasterState)
    val masterStateResponse = new MasterStateResponse
    masterStateResponse.workers = response.workers
    masterStateResponse.serverSparkVersion = sparkVersion
    masterStateResponse
  }
}