package tech.ytsaurus.spark.launcher

import io.circe.{Decoder, Error}
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import org.slf4j.LoggerFactory
import AutoScaler.SparkState
import SparkStateService.{AppStats, MasterStats, WorkerInfo, WorkerStats}
import tech.ytsaurus.spark.launcher.rest.AppStatusesRestClient.AppState
import sttp.client.{HttpError, HttpURLConnectionBackend, Identity, NothingT, SttpBackend, UriContext, basicRequest}
import sttp.model.Uri
import tech.ytsaurus.spark.launcher.rest.AppStatusesRestClient
import tech.ytsaurus.spyt.HostAndPort

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationLong}
import scala.util.{Failure, Try}

trait SparkStateService {
  def query: Try[SparkState]
  def activeWorkers: Try[Seq[WorkerInfo]]
  def workerStats(workers: Seq[WorkerInfo]): Seq[Try[WorkerStats]]
  def idleWorkers(workers: Seq[WorkerInfo]): Seq[WorkerInfo]
  def masterStats: Try[MasterStats]
  def appStats: Try[Seq[AppStats]]

  private[launcher] def parseWorkerMetrics(resp: String, workerInfo: WorkerInfo): Try[WorkerStats]
  private[launcher] def parseMasterMetrics(resp: String): Try[MasterStats]
  private[launcher] def parseAppMetrics(resp: String): Try[Seq[AppStats]]
}

object SparkStateService {
  private val sparkStateQueryThreads = 8

  case class WorkerStats(coresTotal: Long, coresUsed: Long, executors: Long, memoryFreeMb: Long, memoryUsedMb: Long) {
    def coresFree: Long = coresTotal - coresUsed
  }

  case class MasterStats(aliveWorkers: Long, workers: Long, apps: Long, waitingApps: Long)

  case class AppStats(name: String, cores: Long, runtime: Duration)

  case class AppStatusesStats(runningApps: Long, waitingApps: Long, maxWaitingTimeMs: Long)

  case class WorkerInfo(id: String, host: String, port: Int, cores: Int, memory: Int, webUiAddress: String,
                        alive: Boolean, resources: Map[String, ResourceInfo]) {
    def isDriverOp: Boolean = resources.contains("driverop")
    def metricsUrl: Uri = uri"$webUiAddress/metrics/prometheus"
    def ytJobId: Option[String] = resources.get("jobid").flatMap(_.addresses.headOption)
  }

  case class ResourceInfo(name: String, addresses: Seq[String])

  private val log = LoggerFactory.getLogger(getClass)

  def sparkStateService(webUi: HostAndPort, rest: HostAndPort): SparkStateService =
    new SparkStateService {
      private val restClient: AppStatusesRestClient = AppStatusesRestClient.create(rest)
      implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()

      val es: ExecutorService = Executors.newFixedThreadPool(sparkStateQueryThreads)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es, ex =>
        log.error("Failed spark state query", ex)
      )

      override def query: Try[SparkState] = {
        activeWorkers.flatMap(ws => {
          log.info(s"active workers: $ws")
          val allWorkerStats = workerStats(ws).foldLeft(Try(Seq[WorkerStats]())) {
            case (ac, r) => for {
              as <- ac
              a <- r
            } yield as :+ a
          }
          for {
            stats <- allWorkerStats
            master <- masterStats
            apps <- queryAppStatuses
          } yield SparkState(ws.size, stats.count(_.coresUsed > 0), master.waitingApps,
            stats.map(_.coresTotal).sum, stats.map(_.coresUsed).sum,
            apps.maxWaitingTimeMs
          )
        })
      }

      def queryAppStatuses: Try[AppStatusesStats] = {
        for {
          appStatuses <- restClient.getAppStatuses
          submissionStatuses <- restClient.getSubmissionStatuses
        } yield {
            log.debug(s"app statuses: $appStatuses, submission statuses: $submissionStatuses")
            val activeApps = appStatuses.filter(!_.state.isFinished)
            val maxAppWaitingTime = activeApps.filter(_.state == AppState.WAITING)
                .map(_.runtime())
                .foldLeft(0L)(_ max _)
            val maxSubmissionWaitingTIme = submissionStatuses.map(_.runtime())
                .foldLeft(0L)(_ max _)
            log.debug(s"maxAppWaitingTime: $maxAppWaitingTime, maxSubmissionWaitingTime: $maxSubmissionWaitingTIme")
            AppStatusesStats(
                runningApps = activeApps.count(_.state == AppState.RUNNING),
                waitingApps = activeApps.count(_.state == AppState.WAITING),
                maxWaitingTimeMs = maxAppWaitingTime max maxSubmissionWaitingTIme
            )
        }
      }

      override def masterStats: Try[MasterStats] = {
        val uri = uri"http://${webUi.asString}/metrics/master/prometheus"
        log.debug(s"querying $uri")
        basicRequest
          .get(uri)
          .send()
          .body
          .fold(
            error => Failure(HttpError(error)),
            body => parseMasterMetrics(body)
          )
      }

      override def appStats: Try[Seq[AppStats]] = {
        val uri = uri"http://${webUi.asString}/metrics/applications/prometheus"
        log.debug(s"querying $uri")
        basicRequest
          .get(uri)
          .send()
          .body
          .fold(
            error => Failure(HttpError(error)),
            body => parseAppMetrics(body)
          )
      }

      override def activeWorkers: Try[Seq[WorkerInfo]] = {
        val uri = uri"http://${rest.asString}/v1/submissions/master"
        log.debug(s"querying $uri")
        basicRequest
          .get(uri)
          .send()
          .body
          .fold(
            error => Failure(HttpError(error)),
            body => parseWorkersList(body).map(_.filter(!_.isDriverOp).filter(_.alive)).toTry
          )
      }

      def parseWorkersList(body: String): Either[Error, Seq[WorkerInfo]] = {
        implicit val resourceDecoder: Decoder[ResourceInfo] = deriveDecoder
        implicit val workerDecoder: Decoder[WorkerInfo] = deriveDecoder
        log.debug(s"parseWorkerList:\n$body")
        for {
          cursor <- parse(body).map(_.hcursor)
          workers <- cursor.downField("workers").as[Seq[WorkerInfo]]
        } yield workers
      }

      override def workerStats(workers: Seq[WorkerInfo]): Seq[Try[WorkerStats]] =
        workers.map {
          wi =>
            log.debug(s"querying ${wi.metricsUrl}")
            basicRequest
              .get(wi.metricsUrl)
              .send()
              .body
              .fold(
                error => Failure(HttpError(error)),
                body => parseWorkerMetrics(body, wi)
              )
        }

      override def idleWorkers(workers: Seq[WorkerInfo]): Seq[WorkerInfo] = {
        val stats = workerStats(workers)
        workers.zip(stats)
          .filter(_._2.isSuccess) // let's ignore workers with failed stats request
          .filter(_._2.get.coresUsed == 0)
          .map(_._1)
      }


      def parseMetrics(prefix: String, resp: String, ignoreNotMatching: Boolean = false): Try[Map[String, Long]] = {
        log.debug(s"parseMetrics: $prefix: $resp")
        val matcher = (s"metrics_${prefix}_(.*?)_Value\\{.*?} (\\d+)").r
        Try {
          resp.split('\n').flatMap {
            case matcher(name, value) =>
              Option((name, value.toLong))
            case unknown =>
              if (ignoreNotMatching) log.debug(s"Unable to parse metrics string: $unknown")
              else log.warn(s"Unable to parse metrics string: $unknown")
              None
          }.toMap
        }
      }

      override def parseWorkerMetrics(resp: String, wi: WorkerInfo): Try[WorkerStats] =
        parseMetrics("worker", resp).map(metrics =>
          WorkerStats(wi.cores, metrics("coresUsed"), metrics("executors"), metrics("memFree_MB"),
            metrics("memUsed_MB"))
        )

      override def parseMasterMetrics(resp: String): Try[MasterStats] =
        parseMetrics("master", resp).map(metrics =>
          MasterStats(metrics("aliveWorkers"), metrics("workers"), metrics("apps"), metrics("waitingApps"))
        )

      override def parseAppMetrics(resp: String): Try[Seq[AppStats]] = {
        val matcher = "^(.*?)_(cores|runtime_ms)$".r
        def split(s: String): Option[(String, String)] =
          s match {
            case matcher(appName, metric) => Some((appName, metric))
            case _ => None
          }

        parseMetrics("application", resp, ignoreNotMatching = true)
          .map(metrics =>
            metrics.flatMap(kv => split(kv._1).map { case (appName, metric) =>
              (appName, metric, kv._2)
            }).groupBy(_._1)
              .toSeq
              .map { case (appName, vs) =>
                val m = vs.map{ case (_, metric, value) => (metric, value) }.toMap
                AppStats(appName, m.getOrElse("cores", 0L), m.getOrElse("runtime_ms", 0L).millis)
              }
          )
      }

    }
}
