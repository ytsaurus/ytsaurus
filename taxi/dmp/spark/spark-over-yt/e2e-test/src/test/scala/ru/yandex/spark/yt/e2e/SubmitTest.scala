package ru.yandex.spark.yt.e2e

import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import ru.yandex.spark.e2e.check.{CheckApp, CheckResult}
import ru.yandex.spark.yt.submit.SubmissionClient
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class SubmitTest extends FlatSpec with Matchers with E2EYtClient {
  private val log = LoggerFactory.getLogger(getClass)

  import SubmitTest._

  val jobs = Seq(
    E2ETestCase("link_eda_user_appsession_request_id", 80 seconds, Seq("appsession_id")),
    E2ETestCase("link_eda_user_appsession_request_id_python2", 70 seconds, Seq("appsession_id"),
      customInputPath = Some(s"${SubmitTest.basePath}/link_eda_user_appsession_request_id/input"))
      .withConf("spark.pyspark.python" , "python2.7"),
    E2ETestCase("fct_extreme_user_order_act", 100 seconds, Seq("phone_pd_id"))
      .withConf("spark.sql.mapKeyDedupPolicy", "LAST_WIN"),
    E2ETestCase("yt_cdm_agg_ca_adjust_event_sfo", 70 seconds, Seq("moscow_dt", "brand", "platform")),
    E2ETestCase("cdm_callcenter_fct_operator_state_hist_yt", 180 seconds, Seq("agent_id", "utc_valid_from_dttm")),
    E2ETestCase("summary_fct_user_rating", 50 seconds, Seq("user_uid", "brand", "utc_order_dttm", "taximeter_order_id")),
    E2ETestCase("DmCommutationCheckNewbieCheck", 35 seconds, Seq("check_id", "check_name", "check_root_id", "msk_updated_dttm"))
  )

  jobs.foreach { testCase =>
    testCase.name should "work correctly" in {
      YtWrapper.removeIfExists(testCase.outputPath)

      val lowerBound = testCase.executionTime / executionTimeSpread
      val upperBound = testCase.executionTime * executionTimeSpread

      log.info(s"Start job ${testCase.name}")
      val executionTime = measure(runJob(testCase), upperBound)
      log.info(s"Finished job ${testCase.name}")
      log.info(s"Used ${executionTime.toSeconds} seconds")

      log.info(s"Check job ${testCase.name}")
      val res = runCheck(testCase)(yt)
      log.info(s"Finished check ${testCase.name}")
      res shouldEqual CheckResult.Ok

      executionTime should be > lowerBound
    }
  }

  private def measure(block: => Unit, limit: Duration): Duration = {
    val t0 = System.nanoTime()
    Await.ready(Future{ block }, limit)
    val t1 = System.nanoTime()
    Duration.fromNanos(t1 - t0)
  }
}

object SubmitTest {
  private val executionTimeSpread = 1.5
  val basePath: String = System.getProperty("e2eTestHomePath")
  val userDirPath: String = System.getProperty("e2eTestUDirPath")

  private val submitClient = new SubmissionClient(E2EYtClient.ytProxy, s"$basePath/cluster",
    BuildInfo.spytClientVersion, DefaultRpcCredentials.user, DefaultRpcCredentials.token)

  def runJob(testCase: E2ETestCase): Unit = {
    val launcher = testCase.conf
      .foldLeft(submitClient.newLauncher()) { case (launcher, (key, value)) =>
        launcher.setConf(key, value)
      }
      .setAppName(testCase.name)
      .setAppResource(testCase.jobPath)
      .addAppArgs(
        testCase.inputPath,
        testCase.outputPath
      )

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
      .setAppResource(s"yt:/$userDirPath/check.jar")
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
      .setConf("spark.sql.schema.forcingNullableIfNoMetadata.enabled", "true")

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
