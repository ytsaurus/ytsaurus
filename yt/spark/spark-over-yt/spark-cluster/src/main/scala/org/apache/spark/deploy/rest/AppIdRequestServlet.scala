package org.apache.spark.deploy.rest

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}


/**
 * A servlet for handling get application id requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class AppIdRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
                                request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleGetAppId(_, response)).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleGetAppId(submissionId: String,
                               responseServlet: HttpServletResponse): AppIdRestResponse
}


/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppIdRestResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var appId: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}


private[rest] class StandaloneAppIdRequestServlet(
                                                   masterEndpoint: RpcEndpointRef,
                                                   conf: SparkConf)
  extends AppIdRequestServlet {
  override protected def handleGetAppId(
                                         submissionId: String,
                                         responseServlet: HttpServletResponse
                                       ): AppIdRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.AppIdResponse](
      DeployMessages.RequestAppId(submissionId))
    val appIdResponse = new AppIdRestResponse
    appIdResponse.submissionId = submissionId
    appIdResponse.success = true
    appIdResponse.appId = response.appId.orNull
    appIdResponse.serverSparkVersion = sparkVersion
    appIdResponse
  }
}
