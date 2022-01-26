package ru.yandex.spark.yt.e2e

import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import ru.yandex.spark.e2e.check.{CheckApp, CheckResult}
import ru.yandex.spark.yt.submit.SubmissionClient
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.concurrent.duration._
import scala.language.postfixOps

class SubmitTest extends FlatSpec with Matchers with HumeYtClient {
  private val log = LoggerFactory.getLogger(getClass)

  import SubmitTest._

  val jobs = Seq(
    E2ETestCase("link_eda_user_appsession_request_id", Seq("appsession_id")),
    E2ETestCase("link_eda_user_appsession_request_id_python2", Seq("appsession_id"))
      .withConf("spark.pyspark.python" , "python2.7"),
    E2ETestCase("fct_extreme_user_order_act", Seq("phone_pd_id"))
      .withConf("spark.sql.mapKeyDedupPolicy", "LAST_WIN"),
    E2ETestCase("yt_cdm_agg_ca_adjust_event_sfo", Seq("moscow_dt", "brand", "platform")),
    E2ETestCase("cdm_callcenter_fct_operator_state_hist_yt", Seq("agent_id", "utc_valid_from_dttm")),
    E2ETestCase("summary_fct_user_rating", Seq("user_uid", "brand", "utc_order_dttm", "taximeter_order_id")),
    E2ETestCase("DmCommutationCheckNewbieCheck", Seq("check_id", "check_name", "check_root_id", "msk_updated_dttm"))
  )

  jobs.foreach { testCase =>
    testCase.name should "work correctly" in {
      YtWrapper.removeIfExists(testCase.outputPath)

      log.info(s"Start job ${testCase.name}")
      runJob(testCase)
      log.info(s"Finished job ${testCase.name}")

      log.info(s"Check job ${testCase.name}")
      val res = runCheck(testCase)(yt)
      log.info(s"Finished check ${testCase.name}")
      res shouldEqual CheckResult.Ok
    }
  }
}

object SubmitTest {
  private val basePath = "//home/spark/e2e"

  private val submitClient = new SubmissionClient("hume", s"$basePath/cluster",
    BuildInfo.spytClientVersion, DefaultRpcCredentials.user, DefaultRpcCredentials.token)

  def runJob(testCase: E2ETestCase): Unit = {
    val launcher = testCase.conf
      .foldLeft(submitClient.newLauncher()) { case (launcher, (key, value)) =>
        launcher.setConf(key, value)
      }
      .setAppName(testCase.name)
      .setAppResource(testCase.jobPath)

    val id = submitClient.submit(launcher).get

    var status = submitClient.getStatus(id)
    while (!status.isFinal) {
      status = submitClient.getStatus(id)
      Thread.sleep((5 seconds).toMillis)
    }

    if (!status.isSuccess) {
      throw new IllegalStateException(s"Job ${testCase.name} failed, $status")
    }
  }

  def runCheck(testCase: E2ETestCase)(implicit yt: CompoundClient): CheckResult = {
    val launcher = submitClient.newLauncher()
      .setAppName(s"${testCase.name}_check")
      .setAppResource(s"yt:/$basePath/check.jar")
      .setMainClass(CheckApp.getClass.getCanonicalName.dropRight(1))
      .addAppArgs(
        "--actual",
        testCase.outputPath,
        "--expected",
        testCase.expectedPath,
        "--result",
        testCase.checkResultPath,
        "--keys",
        testCase.keyColumns.mkString(","),
        "--uniqueKeys",
        testCase.uniqueKeys.toString
      )

    val id = submitClient.submit(launcher).get

    var status = submitClient.getStatus(id)
    while (!status.isFinal) {
      status = submitClient.getStatus(id)
      Thread.sleep((5 seconds).toMillis)
    }

    if (!status.isSuccess) {
      throw new IllegalStateException(s"Job ${testCase.name}_check failed, $status")
    }

    CheckResult.readWithoutDetails(testCase.checkResultPath)
  }
}
