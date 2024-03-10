package org.apache.spark.deploy.rest

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

private[rest] class YtStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends StandaloneStatusRequestServlet(masterEndpoint, conf) {

  protected override def doGet(
                                request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId match {
      case Some(value) =>
        log.debug("Status request for submission ID " + value)
        handleStatus(value)
      case None =>
        log.debug("No submission ID in status request.")
        handleStatuses
    }
    sendResponse(responseMessage, response)
  }

  protected def handleStatuses: SubmissionStatusesResponse = {
    val response = masterEndpoint.askSync[DeployMessages.DriverStatusesResponse](
      DeployMessages.RequestDriverStatuses)
    val resp = new SubmissionStatusesResponse
    resp.serverSparkVersion = sparkVersion
    resp.success = response.exception.isEmpty
    resp.message = response.exception.map(s"Exception from the cluster:\n"
      + formatException(_)).orNull
    resp.statuses = response.statuses.map(r => {
      val d = new SubmissionsStatus
      d.driverId = r.id
      d.status = r.state
      d.startedAt = r.startTimeMs
      d
    })
    resp
  }
}

private[spark] class SubmissionsStatus {
  var driverId: String = null
  var status: String = null
  var startedAt: Long = -1L
}

private[spark] class SubmissionStatusesResponse extends SubmitRestProtocolResponse {
  var statuses: Seq[SubmissionsStatus] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
    assertFieldIsSet(statuses, "statuses")
  }
}

