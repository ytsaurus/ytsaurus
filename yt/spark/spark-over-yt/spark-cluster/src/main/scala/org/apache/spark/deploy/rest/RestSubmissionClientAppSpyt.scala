package org.apache.spark.deploy.rest

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkException}
import tech.ytsaurus.spyt.Utils
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import java.io.File
import java.net.URI
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

@Subclass
@OriginClass("org.apache.spark.deploy.rest.RestSubmissionClientApp")
class RestSubmissionClientAppSpyt extends RestSubmissionClientApp with Logging {

  override def run(appResource: String,
                   mainClass: String,
                   appArgs: Array[String],
                   conf: SparkConf,
                   env: Map[String, String]): SubmitRestProtocolResponse = {
    // Almost exact copy of super.run(...) method, with spark.rest.master property instead of spark.master
    val master = conf.getOption("spark.rest.master").getOrElse {
      throw new IllegalArgumentException("'spark.rest.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClientSpyt(master)
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    client.createSubmission(submitRequest)
  }

  @tailrec
  private def getSubmissionStatus(submissionId: String,
                                  client: RestSubmissionClient,
                                  retry: Int,
                                  retryInterval: Duration,
                                  rnd: Random = new Random): SubmissionStatusResponse = {
    val response = Try(client.requestSubmissionStatus(submissionId)
      .asInstanceOf[SubmissionStatusResponse])
    response match {
      case Success(value) => value
      case Failure(exception) if retry > 0 =>
        log.error(s"Exception while getting submission status: ${exception.getMessage}")
        val sleepInterval = if (retryInterval > 1.second) {
          1000 + rnd.nextInt(retryInterval.toMillis.toInt - 1000)
        } else rnd.nextInt(retryInterval.toMillis.toInt)
        Thread.sleep(sleepInterval)
        getSubmissionStatus(submissionId, client, retry - 1, retryInterval, rnd)
      case Failure(exception) => throw exception
    }
  }

  def awaitAppTermination(submissionId: String,
                          conf: SparkConf,
                          checkStatusInterval: Duration): Unit = {
    import org.apache.spark.deploy.master.DriverState._

    val master = conf.getOption("spark.rest.master").getOrElse {
      throw new IllegalArgumentException("'spark.rest.master' must be set.")
    }
    val client = new RestSubmissionClient(master)
    val runningStates = Set(RUNNING.toString, SUBMITTED.toString)
    val finalStatus = Stream.continually {
      Thread.sleep(checkStatusInterval.toMillis)
      val response = getSubmissionStatus(submissionId, client, retry = 3, checkStatusInterval)
      logInfo(s"Driver report for $submissionId (state: ${response.driverState})")
      response
    }.find(response => !runningStates.contains(response.driverState)).get
    logInfo(s"Driver $submissionId finished with status ${finalStatus.driverState}")
    finalStatus.driverState match {
      case s if s == FINISHED.toString => // success
      case s if s == FAILED.toString =>
        throw new SparkException(s"Driver $submissionId failed")
      case _ =>
        throw new SparkException(s"Driver $submissionId failed with unexpected error")
    }
  }

  def shutdownYtClient(sparkConf: SparkConf): Unit = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
    val fs = FileSystem.get(new URI("yt:///"), hadoopConf)
    fs.close()
  }

  private def writeToFile(file: File, message: String): Unit = {
    val tmpFile = new File(file.getParentFile, s"${file.getName}_tmp")
    FileUtils.writeStringToFile(tmpFile, message)
    FileUtils.moveFile(tmpFile, file)
  }


  override def start(args: Array[String], conf: SparkConf): Unit = {
    val submissionIdFile = conf.getOption("spark.rest.client.submissionIdFile").map(new File(_))
    val submissionErrorFile = conf.getOption("spark.rest.client.submissionErrorFile")
      .map(new File(_))
    try {
      // Here starts an almost exact copy of super.start(...) method
      if (args.length < 2) {
        sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
        sys.exit(1)
      }
      val appResource = args(0)
      val mainClass = args(1)
      val appArgs = args.slice(2, args.length)
      val env = RestSubmissionClient.filterSystemEnvironment(sys.env) ++ Utils.filterSparkConf(conf)

      val submissionId = try {
        val response = run(appResource, mainClass, appArgs, conf, env)
        response match {
          case r: CreateSubmissionResponse => r.submissionId
          case _ => throw new IllegalStateException("Job is not submitted")
        }
      } finally {
        shutdownYtClient(conf)
      }

      submissionIdFile.foreach(writeToFile(_, submissionId))

      if (conf.getOption("spark.rest.client.awaitTermination.enabled").forall(_.toBoolean)) {
        val checkStatusInterval = conf.getOption("spark.rest.client.statusInterval")
          .map(_.toInt.seconds).getOrElse(5.seconds)
        awaitAppTermination(submissionId, conf, checkStatusInterval)
      }
    } catch {
      case e: Throwable =>
        submissionErrorFile.foreach(writeToFile(_, e.getMessage))
        throw e
    }
  }
}
