package tech.ytsaurus.spark.launcher.rest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spark.launcher.rest.AppStatusesRestClient.{AppState, AppStatus, SubmissionStatus}
import tech.ytsaurus.spark.launcher.rest.AppStatusesRestClient.AppState.WAITING
import tech.ytsaurus.spyt.HostAndPort

import scala.util.Success

class AppStatusesRestClientTest extends AnyFlatSpec with Matchers {
  val appStatusMsg = """{
    "action" : "AppStatusesRestResponse",
    "serverSparkVersion" : "3.0.1",
    "statuses" : [ {
    "action" : "AppStatusRestResponse",
    "appId" : "app-20220405015804-0000",
    "appStartedAt" : 1649113084242,
    "appState" : "WAITING",
    "appSubmittedAt" : 1649113084242,
    "serverSparkVersion" : "3.0.1",
    "success" : true
  } ],
    "success" : true
  }"""

  it should "correctly parse app status response" in {
    val client = AppStatusesRestClient.create(HostAndPort.fromString("localhost:8080"))
    client.decodeAppStatuses(appStatusMsg) should be (Success(Seq(
      AppStatus("app-20220405015804-0000", WAITING, 1649113084242L)
    )))
  }

  val submissionStatusMsg = """{
      "action" : "SubmissionStatusesResponse",
      "serverSparkVersion" : "3.0.1",
      "statuses" : [ {
      "driverId" : "driver-20220406232616-0000",
      "status" : "RUNNING",
      "startedAt" : 1649276776826
  }, {
      "driverId" : "driver-20220406232826-0001",
      "status" : "SUBMITTED",
      "startedAt" : 1649276906209
  } ],
      "success" : true
  }"""

  it should "correctly parse submission status response" in {
      val client = AppStatusesRestClient.create(HostAndPort.fromString("localhost:8080"))
      client.decodeSubmissionStatuses(submissionStatusMsg) should be (Success(Seq(
            SubmissionStatus("driver-20220406232616-0000", "RUNNING", 1649276776826L),
            SubmissionStatus("driver-20220406232826-0001", "SUBMITTED", 1649276906209L)
      )))
  }
}
