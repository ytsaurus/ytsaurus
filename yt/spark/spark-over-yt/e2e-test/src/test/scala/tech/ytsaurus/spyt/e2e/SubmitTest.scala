package tech.ytsaurus.spyt.e2e

import org.apache.spark.deploy.rest.DriverState
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.e2e.check.CheckResult
import tech.ytsaurus.spyt.submit.SubmissionClient
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.DefaultRpcCredentials
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spark.e2e.check.{CheckApp, CheckResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class SubmitTest extends FlatSpec with Matchers with E2EYtClient {
  private val log = LoggerFactory.getLogger(getClass)

  import SubmitTest._

  val jobs = Seq(
    E2ETestCase("read_yt_table", 30 seconds, Seq("id"))
      .withConf("spark.hadoop.fs.defaultFS", "yt:///"),
    E2ETestCase("read_csv", 30 seconds, Seq("id"))
      .withConf("spark.hadoop.fs.defaultFS", "yt:///"),
    E2ETestCase("link_eda_user_appsession_request_id", 80 seconds, Seq("appsession_id")),
    E2ETestCase("fct_extreme_user_order_act", 100 seconds, Seq("phone_pd_id"))
      .withConf("spark.sql.mapKeyDedupPolicy", "LAST_WIN"),
    E2ETestCase("yt_cdm_agg_ca_adjust_event_sfo", 70 seconds, Seq("moscow_dt", "brand", "platform")),
    E2ETestCase("cdm_callcenter_fct_operator_state_hist_yt", 115 seconds, Seq("agent_id", "utc_valid_from_dttm")),
    E2ETestCase("summary_fct_user_rating", 50 seconds, Seq("user_uid", "brand", "utc_order_dttm", "taximeter_order_id")),
    E2ETestCase("DmCommutationCheckNewbieCheck", 35 seconds, Seq("check_id", "check_name", "check_root_id", "msk_updated_dttm"))
  )

  // takes no lock
  private val jobNoOutput = E2ETestCase("link_eda_user_appsession_request_id_no_output", 80 seconds, Seq("appsession_id"),
    customInputPath = Some(s"${SubmitTest.basePath}/link_eda_user_appsession_request_id/input"))

  private val jobWithFail = E2ETestCase("link_eda_user_appsession_request_id_fail", 40 seconds, Seq("appsession_id"),
    customInputPath = Some(s"${SubmitTest.basePath}/link_eda_user_appsession_request_id/input"))

  it should "get active drivers" in {
    val testCase = jobNoOutput
    measure({
      submitClient.getActiveDrivers should contain theSameElementsAs Seq()

      val job1Id = submitJob(testCase)
      log.info(s"Started job ${testCase.name} (id: $job1Id)")
      submitClient.getActiveDrivers should contain theSameElementsAs Seq(job1Id)

      val job2Id = submitJob(testCase)
      log.info(s"Started job ${testCase.name} (id: $job2Id)")
      submitClient.getActiveDrivers should contain theSameElementsAs Seq(job1Id, job2Id)

      while (submitClient.getActiveDrivers.nonEmpty) {
        Thread.sleep((5 seconds).toMillis)
      }
      log.info(s"Jobs finished")

      val status1 = submitClient.getStatus(job1Id)
      val status2 = submitClient.getStatus(job2Id)
      if (!status1.isSuccess || !status2.isSuccess) {
        throw new IllegalStateException(s"Tasks failed (1: $status1, 2: $status2)")
      }
    }, testCase.executionTime * executionTimeSpread * 2)
  }

  it should "kill driver" in {
    measure({
      val testCase = jobNoOutput
      val jobId = submitJob(testCase)
      log.info(s"Started job ${testCase.name} (id: $jobId)")

      submitClient.kill(jobId) shouldBe true
      log.info(s"Job kill requested")
      Thread.sleep((2 seconds).toMillis)

      val status = submitClient.getStatus(jobId)
      status shouldBe DriverState.KILLED
    }, 30 seconds)
  }

  it should "process failed job" in {
    measure({
      val testCase = jobWithFail
      val jobId = submitJob(testCase)
      log.info(s"Started job ${testCase.name} (id: $jobId)")

      val status = waitFinalStatus(jobId)
      status shouldBe DriverState.FAILED
    }, 30 seconds)
  }

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
}

object SubmitTest {
  private val executionTimeSpread = 1.5
  val basePath: String = System.getProperty("e2eTestHomePath")
  val userDirPath: String = System.getProperty("e2eTestUDirPath")
  val discoveryPath: String = System.getProperty("discoveryPath")

  private val submitClient = new SubmissionClient(E2EYtClient.ytProxy, discoveryPath,
    System.getProperty("clientVersion"), DefaultRpcCredentials.user, DefaultRpcCredentials.token)

  def submitJob(testCase: E2ETestCase): String = {
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

    submitClient.submit(launcher).get
  }

  def waitFinalStatus(id: String): DriverState = {
    var status = submitClient.getStatus(id)
    while (!status.isFinal) {
      status = submitClient.getStatus(id)
      Thread.sleep((5 seconds).toMillis)
    }
    status
  }

  def runJob(testCase: E2ETestCase): Unit = {
    val id = submitJob(testCase)
    val status = waitFinalStatus(id)
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

    val status = waitFinalStatus(id)
    if (!status.isSuccess) {
      throw new IllegalStateException(s"Job ${testCase.name}_check failed, $status")
    }

    CheckResult.readWithoutDetails(testCase.checkResultPath)
  }

  private def measure(block: => Unit, limit: Duration): Duration = {
    val t0 = System.nanoTime()
    Await.ready(Future{ block }, limit)
    val t1 = System.nanoTime()
    Duration.fromNanos(t1 - t0)
  }
}
