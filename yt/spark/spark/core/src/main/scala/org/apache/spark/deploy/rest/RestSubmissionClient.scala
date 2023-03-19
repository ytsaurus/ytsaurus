/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.rest

import java.io.{DataOutputStream, File, FileNotFoundException}
import java.net.{ConnectException, HttpURLConnection, SocketException, URI, URL}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException
import javax.servlet.http.HttpServletResponse

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Random, Success, Try}
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf, SparkException}
import org.apache.spark.deploy.{SparkApplication, SparkHadoopUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A client that submits applications to a [[RestSubmissionServer]].
 *
 * In protocol version v1, the REST URL takes the form http://[host:port]/v1/submissions/[action],
 * where [action] can be one of create, kill, or status. Each type of request is represented in
 * an HTTP message sent to the following prefixes:
 *   (1) submit - POST to /submissions/create
 *   (2) kill - POST /submissions/kill/[submissionId]
 *   (3) status - GET /submissions/status/[submissionId]
 *
 * In the case of (1), parameters are posted in the HTTP body in the form of JSON fields.
 * Otherwise, the URL fully specifies the intended action of the client.
 *
 * Since the protocol is expected to be stable across Spark versions, existing fields cannot be
 * added or removed, though new optional fields can be added. In the rare event that forward or
 * backward compatibility is broken, Spark must introduce a new protocol version (e.g. v2).
 *
 * The client and the server must communicate using the same version of the protocol. If there
 * is a mismatch, the server will respond with the highest protocol version it supports. A future
 * implementation of this client can use that information to retry using the version specified
 * by the server.
 */
private[spark] class RestSubmissionClient(master: String) extends Logging {
  import RestSubmissionClient._

  private val masters: Array[String] = if (master.startsWith("spark://")) {
    Utils.parseStandaloneMasterUrls(master)
  } else {
    Array(master)
  }

  // Set of masters that lost contact with us, used to keep track of
  // whether there are masters still alive for us to communicate with
  private val lostMasters = new mutable.HashSet[String]

  object HandledResponse extends Enumeration {
    type HandledResponse = Value
    val HFailure, HUnexpectedResponse, HSuccess = Value
  }
  import HandledResponse._

  private def requestProcess(urlGetter: String => URL,
                             responseGetter: URL => SubmitRestProtocolResponse,
                             handler: SubmitRestProtocolResponse => HandledResponse):
  SubmitRestProtocolResponse = {
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = urlGetter(m)
      try {
        response = responseGetter(url)
        handler(response) match {
          case HSuccess =>
            handled = true
          case HUnexpectedResponse =>
            handleUnexpectedRestResponse(response)
          case HFailure =>
            logWarning(s"Failure response from $url")
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }
  /**
   * Submit an application specified by the parameters in the provided request.
   *
   * If the submission was successful, poll the status of the submission and report
   * it to the user. Otherwise, report the error message provided by the server.
   */
  def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to launch an application in $master.")
    requestProcess(
      getSubmitUrl,
      postJson(_, request.toJson),
      {
        case s: CreateSubmissionResponse =>
          if (s.success) {
            reportSubmissionStatus(s)
            handleRestResponse(s)
            HSuccess
          } else {
            HFailure
          }
        case _ => HUnexpectedResponse
      }
    )
  }

  /** Request that the server kill the specified submission. */
  def killSubmission(submissionId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to kill submission $submissionId in $master.")
    requestProcess(
      getKillUrl(_, submissionId),
      post,
      {
        case k: KillSubmissionResponse =>
          if (!Utils.responseFromBackup(k.message)) {
            handleRestResponse(k)
            HSuccess
          } else {
            HFailure
          }
        case _ => HUnexpectedResponse
      }
    )
  }

  def requestAppId(submissionId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the app id of submission $submissionId in $master.")
    requestProcess(
      getAppIdUrl(_, submissionId),
      get,
      {
        case r: AppIdRestResponse =>
          if (r.success) HSuccess else HFailure
        case _ => HUnexpectedResponse
      }
    )
  }

  def requestAppStatus(appId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the status of application $appId in $master.")
    requestProcess(
      getAppStatusUrl(_, appId),
      get,
      {
        case r: AppStatusRestResponse =>
          if (r.success) HSuccess else HFailure
        case _ => HUnexpectedResponse
      }
    )
  }


  def requestAppStatuses: SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the status of applications in $master.")
    requestProcess(getAppStatusUrl(_, ""), get, {
      case r: AppStatusesRestResponse =>
        if (r.success) HSuccess else HFailure
      case _ =>
        HUnexpectedResponse
    }
    )
  }

  def logAppId(submissionId: String): Unit = {
    val response = requestAppId(submissionId)
    val shs = response match {
      case s: AppIdRestResponse if s.success && s.appId != null =>
        // TODO SPYT return history server
        val shsUrl = Option.empty[String]
        shsUrl.map(baseUrl => s"http://$baseUrl/history/${s.appId}/jobs/")
          .getOrElse("SHS url not found")
      case _ =>
        "App id not found"
    }
    logInfo( s"Job on history server: $shs")
  }

  /** Request the status of a submission from the server. */
  def requestSubmissionStatus(
                               submissionId: String,
                               quiet: Boolean = false): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the status of submission $submissionId in $master.")
    requestProcess(
      getStatusUrl(_, submissionId),
      get,
      {
        case s: SubmissionStatusResponse =>
          if (s.success) {
            if (!quiet) {
              logAppId(submissionId)
              handleRestResponse(s)
            }
            HSuccess
          } else {
            HFailure
          }
        case _ => HUnexpectedResponse
      }
    )
  }

  def requestSubmissionStatuses(): SubmitRestProtocolResponse = {
    logDebug(s"Submitting a request for the statuses of submissions in $master.")
    requestProcess(getStatusUrl(_, ""), get, {
      case s: SubmissionStatusesResponse =>
        if (s.success) HSuccess else HFailure
      case _ =>
        HUnexpectedResponse
    })
  }

  /** Construct a message that captures the specified parameters for submitting an application. */
  def constructSubmitRequest(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      sparkProperties: Map[String, String],
      environmentVariables: Map[String, String]): CreateSubmissionRequest = {
    val message = new CreateSubmissionRequest
    message.clientSparkVersion = sparkVersion
    message.appResource = appResource
    message.mainClass = mainClass
    message.appArgs = appArgs
    message.sparkProperties = sparkProperties
    message.environmentVariables = environmentVariables
    message.validate()
    message
  }

  /** Send a GET request to the specified URL. */
  private def get(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending GET request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    readResponse(conn)
  }

  /** Send a POST request to the specified URL. */
  private def post(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    readResponse(conn)
  }

  /** Send a POST request with the given JSON as the body to the specified URL. */
  private def postJson(url: URL, json: String): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url:\n$json")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(json.getBytes(StandardCharsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    readResponse(conn)
  }

  /**
   * Read the response from the server and return it as a validated [[SubmitRestProtocolResponse]].
   * If the response represents an error, report the embedded message to the user.
   * Exposed for testing.
   */
  private[rest] def readResponse(connection: HttpURLConnection): SubmitRestProtocolResponse = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val responseFuture = Future {
      val responseCode = connection.getResponseCode

      if (responseCode != HttpServletResponse.SC_OK) {
        val errString = Some(Source.fromInputStream(connection.getErrorStream())
          .getLines().mkString("\n"))
        if (responseCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR &&
          !connection.getContentType().contains("application/json")) {
          throw new SubmitRestProtocolException(s"Server responded with exception:\n${errString}")
        }
        logError(s"Server responded with error:\n${errString}")
        val error = new ErrorResponse
        if (responseCode == RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION) {
          error.highestProtocolVersion = RestSubmissionServer.PROTOCOL_VERSION
        }
        error.message = errString.get
        error
      } else {
        val dataStream = connection.getInputStream

        // If the server threw an exception while writing a response, it will not have a body
        if (dataStream == null) {
          throw new SubmitRestProtocolException("Server returned empty body")
        }
        val responseJson = Source.fromInputStream(dataStream).mkString
        logDebug(s"Response from the server:\n$responseJson")
        val response = SubmitRestProtocolMessage.fromJson(responseJson)
        response.validate()
        response match {
          // If the response is an error, log the message
          case error: ErrorResponse =>
            logError(s"Server responded with error:\n${error.message}")
            error
          // Otherwise, simply return the response
          case response: SubmitRestProtocolResponse => response
          case unexpected =>
            throw new SubmitRestProtocolException(
              s"Message received from server was not a response:\n${unexpected.toJson}")
        }
      }
    }

    // scalastyle:off awaitresult
    try { Await.result(responseFuture, 10.seconds) } catch {
      // scalastyle:on awaitresult
      case unreachable @ (_: FileNotFoundException | _: SocketException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
      case malformed @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        throw new SubmitRestProtocolException("Malformed response received from server", malformed)
      case timeout: TimeoutException =>
        throw new SubmitRestConnectionException("No response from server", timeout)
      case NonFatal(t) =>
        throw new SparkException("Exception while waiting for response", t)
    }
  }

  /** Return the REST URL for creating a new submission. */
  private def getSubmitUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/create")
  }

  /** Return the REST URL for killing an existing submission. */
  private def getKillUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/kill/$submissionId")
  }

  /** Return the REST URL for getting app id an existing submission. */
  private def getAppIdUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/getAppId/$submissionId")
  }

  /** Return the REST URL for getting app status. */
  private def getAppStatusUrl(master: String, appId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/getAppStatus/$appId")
  }

  /** Return the REST URL for requesting the status of an existing submission. */
  private def getStatusUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/status/$submissionId")
  }

  /** Return the base URL for communicating with the server, including the protocol version. */
  private def getBaseUrl(master: String): String = {
    var masterUrl = master
    supportedMasterPrefixes.foreach { prefix =>
      if (master.startsWith(prefix)) {
        masterUrl = master.stripPrefix(prefix)
      }
    }
    masterUrl = masterUrl.stripSuffix("/")
    s"http://$masterUrl/$PROTOCOL_VERSION/submissions"
  }

  /** Throw an exception if this is not standalone mode. */
  private def validateMaster(master: String): Unit = {
    val valid = supportedMasterPrefixes.exists { prefix => master.startsWith(prefix) }
    if (!valid) {
      throw new IllegalArgumentException(
        "This REST client only supports master URLs that start with " +
          "one of the following: " + supportedMasterPrefixes.mkString(","))
    }
  }

  /** Report the status of a newly created submission. */
  private def reportSubmissionStatus(
      submitResponse: CreateSubmissionResponse): Unit = {
    if (submitResponse.success) {
      val submissionId = submitResponse.submissionId
      if (submissionId != null) {
        logInfo(s"Submission successfully created as $submissionId. Polling submission state...")
        pollSubmissionStatus(submissionId)
      } else {
        // should never happen
        logError("Application successfully submitted, but submission ID was not provided!")
      }
    } else {
      val failMessage = Option(submitResponse.message).map { ": " + _ }.getOrElse("")
      logError(s"Application submission failed$failMessage")
    }
  }

  /**
   * Poll the status of the specified submission and log it.
   * This retries up to a fixed number of times before giving up.
   */
  private def pollSubmissionStatus(submissionId: String): Unit = {
    (1 to REPORT_DRIVER_STATUS_MAX_TRIES).foreach { _ =>
      val response = requestSubmissionStatus(submissionId, quiet = true)
      val statusResponse = response match {
        case s: SubmissionStatusResponse => s
        case _ => return // unexpected type, let upstream caller handle it
      }
      if (statusResponse.success) {
        val driverState = Option(statusResponse.driverState)
        val workerId = Option(statusResponse.workerId)
        val workerHostPort = Option(statusResponse.workerHostPort)
        val exception = Option(statusResponse.message)
        // Log driver state, if present
        driverState match {
          case Some(state) => logInfo(s"State of driver $submissionId is now $state.")
          case _ => logError(s"State of driver $submissionId was not found!")
        }
        // Log worker node, if present
        (workerId, workerHostPort) match {
          case (Some(id), Some(hp)) => logInfo(s"Driver is running on worker $id at $hp.")
          case _ =>
        }
        // Log exception stack trace, if present
        exception.foreach { e => logError(e) }
        return
      }
      Thread.sleep(REPORT_DRIVER_STATUS_INTERVAL)
    }
    logError(s"Error: Master did not recognize driver $submissionId.")
  }

  /** Log the response sent by the server in the REST application submission protocol. */
  private def handleRestResponse(response: SubmitRestProtocolResponse): Unit = {
    logInfo(s"Server responded with ${response.messageType}:\n${response.toJson}")
  }

  /** Log an appropriate error if the response sent by the server is not of the expected type. */
  private def handleUnexpectedRestResponse(unexpected: SubmitRestProtocolResponse): Unit = {
    logError(s"Error: Server responded with message of unexpected type ${unexpected.messageType}.")
  }

  /**
   * When a connection exception is caught, return true if all masters are lost.
   * Note that the heuristic used here does not take into account that masters
   * can recover during the lifetime of this client. This assumption should be
   * harmless because this client currently does not support retrying submission
   * on failure yet (SPARK-6443).
   */
  private def handleConnectionException(masterUrl: String): Boolean = {
    if (!lostMasters.contains(masterUrl)) {
      logWarning(s"Unable to connect to server ${masterUrl}.")
      lostMasters += masterUrl
    }
    lostMasters.size >= masters.length
  }
}

private[spark] object RestSubmissionClient extends Logging {

  val supportedMasterPrefixes = Seq("spark://", "mesos://")

  // SPARK_HOME and SPARK_CONF_DIR are filtered out because they are usually wrong
  // on the remote machine (SPARK-12345) (SPARK-25934)
  private val EXCLUDED_SPARK_ENV_VARS = Set("SPARK_ENV_LOADED", "SPARK_HOME", "SPARK_CONF_DIR")
  private val REPORT_DRIVER_STATUS_INTERVAL = 1000
  private val REPORT_DRIVER_STATUS_MAX_TRIES = 10
  val PROTOCOL_VERSION = "v1"

  /**
   * Filter non-spark environment variables from any environment.
   */
  private[rest] def filterSystemEnvironment(env: Map[String, String]): Map[String, String] = {
    env.filterKeys { k =>
      (k.startsWith("SPARK_") && !EXCLUDED_SPARK_ENV_VARS.contains(k)) || k.startsWith("MESOS_")
    }.toMap
  }

  private[spark] def supportsRestClient(master: String): Boolean = {
    supportedMasterPrefixes.exists(master.startsWith)
  }

  private[rest] def filterSparkConf(conf: SparkConf): Map[String, String] = {
    conf.getAll.collect {
      case (key, value) if key.startsWith("spark.yt") || key.startsWith("spark.hadoop.yt") =>
        key.toUpperCase().replace(".", "_") -> value
    }.toMap
  }
}

private[spark] class RestSubmissionClientApp extends SparkApplication with Logging {

  /** Submits a request to run the application and return the response. Visible for testing. */
  def run(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      conf: SparkConf,
      env: Map[String, String] = Map()): SubmitRestProtocolResponse = {
    val master = conf.getOption("spark.rest.master").getOrElse {
      throw new IllegalArgumentException("'spark.rest.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClient(master)
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
      if (args.length < 2) {
        sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
        sys.exit(1)
      }
      val appResource = args(0)
      val mainClass = args(1)
      val appArgs = args.slice(2, args.length)
      val env = RestSubmissionClient.filterSystemEnvironment(sys.env) ++
        RestSubmissionClient.filterSparkConf(conf)

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
