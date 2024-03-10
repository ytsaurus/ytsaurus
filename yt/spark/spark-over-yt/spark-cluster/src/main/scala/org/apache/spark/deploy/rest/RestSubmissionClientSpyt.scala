package org.apache.spark.deploy.rest

import org.apache.spark.util.Utils

import java.lang.invoke.MethodType
import java.net.URL

class RestSubmissionClientSpyt(master: String) extends RestSubmissionClient(master) {

  object HandledResponse extends Enumeration {
    type HandledResponse = Value
    val HFailure, HUnexpectedResponse, HSuccess = Value
  }
  import HandledResponse._

  private val baseClass = this.getClass.getSuperclass

  // Stubs for methods declared private in base class
  private def invokeBaseMethod[T](args: AnyRef*): T = {
    val (methodName, methodType): (String, MethodType) =
      StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).walk { frames =>
        val callerFrame = frames.skip(1).findFirst().get()
        (callerFrame.getMethodName, callerFrame.getMethodType)
      }

    val argClasses = methodType.parameterArray()

    val method = baseClass.getDeclaredMethod(methodName, argClasses: _*)
    method.setAccessible(true)
    val result = method.invoke(this, args: _*).asInstanceOf[T]
    method.setAccessible(false)
    result
  }
  
  private def validateMaster(master: String): Unit = invokeBaseMethod(master)
  private def handleUnexpectedRestResponse(unexpected: SubmitRestProtocolResponse): Unit = invokeBaseMethod(unexpected)
  private def handleConnectionException(masterUrl: String): Boolean = invokeBaseMethod(masterUrl)
  private def reportSubmissionStatus(submitResponse: CreateSubmissionResponse): Unit = invokeBaseMethod(submitResponse)
  private def handleRestResponse(response: SubmitRestProtocolResponse): Unit = invokeBaseMethod(response)
  private def postJson(url: URL, json: String): SubmitRestProtocolResponse = invokeBaseMethod(url, json)
  private def getStatusUrl(master: String, submissionId: String): URL = invokeBaseMethod(master, submissionId)
  private def getKillUrl(master: String, submissionId: String): URL = invokeBaseMethod(master, submissionId)
  private def getSubmitUrl(master: String): URL = invokeBaseMethod(master)
  private def get(url: URL): SubmitRestProtocolResponse = invokeBaseMethod(url)
  private def post(url: URL): SubmitRestProtocolResponse = invokeBaseMethod(url)
  private def getBaseUrl(master: String): String = invokeBaseMethod(master)

  private val masters: Array[String] = {
    val field = baseClass.getDeclaredField("masters")
    field.setAccessible(true)
    val value = field.get(this).asInstanceOf[Array[String]]
    field.setAccessible(false)
    value
  }

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

  override def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse = {
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

  override def killSubmission(submissionId: String): SubmitRestProtocolResponse = {
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

  override def requestSubmissionStatus(submissionId: String, quiet: Boolean): SubmitRestProtocolResponse = {
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
}
