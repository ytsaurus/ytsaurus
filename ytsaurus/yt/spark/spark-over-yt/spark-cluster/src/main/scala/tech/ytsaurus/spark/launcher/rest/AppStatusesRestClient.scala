package tech.ytsaurus.spark.launcher.rest

import io.circe.{Decoder, parser}
import io.circe.generic.semiauto.deriveDecoder
import org.slf4j.{Logger, LoggerFactory}
import AppStatusesRestClient.{AppStatus, SubmissionStatus}
import sttp.client.{HttpError, HttpURLConnectionBackend, Identity, NothingT, SttpBackend, UriContext, basicRequest}
import tech.ytsaurus.spyt.HostAndPort

import scala.util.{Failure, Try}


trait AppStatusesRestClient {
  def getAppStatuses: Try[Seq[AppStatus]]
  def getSubmissionStatuses: Try[Seq[SubmissionStatus]]

  private[rest] def decodeAppStatuses(json: String): Try[Seq[AppStatus]]
  private[rest] def decodeSubmissionStatuses(json: String): Try[Seq[SubmissionStatus]]
}

object AppStatusesRestClient {
  val log: Logger = LoggerFactory.getLogger(AppStatusesRestClient.getClass)

  case class AppStatus(id: String, state: AppState, startedAt: Long) {
    def runtime(currentTime: Long = System.currentTimeMillis()): Long = currentTime - startedAt
  }

  case class SubmissionStatus(driverId: String, status: String, startedAt: Long) {
    def runtime(currentTime: Long = System.currentTimeMillis()): Long = currentTime - startedAt
  }

  case class AppStatusesRestResponse(action: String, serverSparkVersion: String, statuses: Seq[AppStatusRestResponse],
                                     success: Boolean)

  case class SubmissionStatusesRestResponse(action: String, serverSparkVersion: String,
                                            statuses: Seq[SubmissionStatus], success: Boolean)

  case class AppStatusRestResponse(action: String, appId: String, appStartedAt: Long, appState: String,
                                   appSubmittedAt: Long, serverSparkVersion: String, success: Boolean) {
    def toAppStatus: AppStatus = AppStatus(appId, AppState(appState), appStartedAt)
  }

  def create(rest: HostAndPort): AppStatusesRestClient = {
    implicit val submStatusDecoder: Decoder[SubmissionStatus] = deriveDecoder
    implicit val submStatusesDecoder: Decoder[SubmissionStatusesRestResponse] = deriveDecoder
    implicit val statusDecoder: Decoder[AppStatusRestResponse] = deriveDecoder
    implicit val workerDecoder: Decoder[AppStatusesRestResponse] = deriveDecoder
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()

    log.info(s"Creating AppStatusesRestClient for master $rest")
    new AppStatusesRestClient {
      override def getAppStatuses: Try[Seq[AppStatus]] = {
        val uri = uri"http://${rest}/v1/submissions/getAppStatus"
        log.debug(s"querying $uri")
        basicRequest
          .get(uri)
          .send()
          .body
          .fold(
            error => Failure(HttpError(error)),
            body => decodeAppStatuses(body)
          )
      }

      override def getSubmissionStatuses: Try[Seq[SubmissionStatus]] = {
        val uri = uri"http://${rest}/v1/submissions/status/"
        log.debug(s"querying $uri")
        basicRequest
          .get(uri)
          .send()
          .body
          .fold(
            error => Failure(HttpError(error)),
            body => decodeSubmissionStatuses(body)
        )
      }

      override def decodeAppStatuses(msg: String): Try[Seq[AppStatus]] =
        for {
          json <- parser.parse(msg).toTry
          resp <- json.as[AppStatusesRestResponse].toTry
        } yield
          if (resp.success) resp.statuses.filter(_.success).map(_.toAppStatus)
          else Seq()

      override def decodeSubmissionStatuses(msg: String): Try[Seq[SubmissionStatus]] =
        for {
          json <- parser.parse(msg).toTry
          resp <- json.as[SubmissionStatusesRestResponse].toTry
        } yield
          if (resp.success) resp.statuses
          else Seq()
    }
  }

  sealed trait AppState {
    def isFinished: Boolean
  }

  object AppState {
    case object WAITING extends AppState {
      override def isFinished: Boolean = false
    }

    case object RUNNING extends AppState {
      override def isFinished: Boolean = false
    }

    case object FINISHED extends AppState {
      override def isFinished: Boolean = true
    }

    case object FAILED extends AppState {
      override def isFinished: Boolean = true
    }

    case object KILLED extends AppState {
      override def isFinished: Boolean = true
    }

    case object UNKNOWN extends AppState {
      override def isFinished: Boolean = false
    }

    def apply(st: String): AppState = st match {
      case "WAITING" => AppState.WAITING
      case "RUNNING" => AppState.RUNNING
      case "FINISHED" => AppState.FINISHED
      case "FAILED" => AppState.FAILED
      case "KILLED" => AppState.KILLED
      case "UNKNOWN" => AppState.UNKNOWN
      case _ => throw new IllegalArgumentException(s"Unknown app state: $st")
    }
  }
}
