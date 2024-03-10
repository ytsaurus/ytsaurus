package org.apache.spark.deploy.rest

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages

import java.util.Date
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * A servlet for handling get application id requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class AppStatusRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
                                request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    val responseMessage = parseSubmissionId(request.getPathInfo) match {
      case Some(appId) =>
        handleGetAppStatus(appId, response)
      case None =>
        handleGetAllAppStatuses(response)
    }
    sendResponse(responseMessage, response)
  }

  protected def handleGetAppStatus(appId: String,
                                   responseServlet: HttpServletResponse): AppStatusRestResponse

  protected def handleGetAllAppStatuses(response: HttpServletResponse): AppStatusesRestResponse

}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppStatusRestResponse extends SubmitRestProtocolResponse {
  var appId: String = null
  var appState: String = null
  var appSubmittedAt: Date = null
  var appStartedAt: Long = -1L

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(appId, "appId")
    assertFieldIsSet(appState, "appState")
    assertFieldIsSet(success, "success")
    assertFieldIsSet(appSubmittedAt, "appStartedAt")
  }
}

private[spark] class AppStatusesRestResponse extends SubmitRestProtocolResponse {
  var statuses: Seq[AppStatusRestResponse] = Seq()

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(statuses, "statuses")
  }
}

private[rest] class StandaloneAppStatusRequestServlet(
                                                       masterEndpoint: RpcEndpointRef,
                                                       conf: SparkConf)
  extends AppStatusRequestServlet {
  override protected def handleGetAppStatus(
                                             appId: String,
                                             responseServlet: HttpServletResponse
                                           ): AppStatusRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.ApplicationStatusResponse](
      DeployMessages.RequestApplicationStatus(appId))
    val appStatusResponse = new AppStatusRestResponse
    appStatusResponse.appId = appId
    appStatusResponse.success = response.found
    appStatusResponse.appState = response.info.map(_.state).orNull
    appStatusResponse.appSubmittedAt = response.info.map(_.submitDate).orNull
    appStatusResponse.appStartedAt = response.info.map(_.startTime).getOrElse(-1L)
    appStatusResponse.serverSparkVersion = sparkVersion
    appStatusResponse
  }

  override protected def handleGetAllAppStatuses(response: HttpServletResponse):
  AppStatusesRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.ApplicationStatusesResponse](
      DeployMessages.RequestApplicationStatuses)
    val statusesRestResponse = new AppStatusesRestResponse
    statusesRestResponse.statuses = response.statuses.map { info =>
      val status = new AppStatusRestResponse
      status.appId = info.id
      status.appState = info.state
      status.appSubmittedAt = info.submitDate
      status.appStartedAt = info.startTime
      status.serverSparkVersion = sparkVersion
      status.success = true
      status
    }
    statusesRestResponse.success = true
    statusesRestResponse.serverSparkVersion = sparkVersion
    statusesRestResponse
  }
}
