package ru.yandex.spark.launcher

import com.google.common.net.HostAndPort
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.{Decoder, Error}
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.AutoScaler.SparkState
import ru.yandex.spark.launcher.SparkStateService.{MasterStats, WorkerInfo, WorkerStats}
import sttp.client.{HttpError, HttpURLConnectionBackend, Identity, NothingT, SttpBackend, UriContext, basicRequest}
import sttp.model.Uri

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

trait SparkStateService {
  def query: Try[SparkState]
  def activeWorkers: Try[Seq[WorkerInfo]]
  def workerStats(workers: Seq[WorkerInfo]): Seq[Try[WorkerStats]]
  def masterStats: Try[MasterStats]

  private[launcher] def parseWorkerMetrics(resp: String, workerInfo: WorkerInfo): Try[WorkerStats]
  private[launcher] def parseMasterMetrics(resp: String): Try[MasterStats]
}

object SparkStateService {
  private val sparkStateQueryThreads = 8

  case class WorkerStats(coresTotal: Long, coresUsed: Long, executors: Long, memoryFreeMb: Long, memoryUsedMb: Long) {
    def coresFree: Long = coresTotal - coresUsed
  }

  case class MasterStats(aliveWorkers: Long, workers: Long, apps: Long, waitingApps: Long)

  case class WorkerInfo(id: String, host: String, port: Int, cores: Int, memory: Int, webUiAddress: String,
                        alive: Boolean, resources: Map[String, ResourceInfo]) {
    def isDriverOp: Boolean = resources.contains("driverop")
    def metricsUrl: Uri = uri"$webUiAddress/metrics/prometheus"
  }

  case class ResourceInfo(name: String, addresses: Seq[String])

  private val log = LoggerFactory.getLogger(getClass)

  def sparkStateService(webUi: HostAndPort, rest: HostAndPort): SparkStateService =
    new SparkStateService {
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
          } yield SparkState(ws.size, stats.count(_.coresUsed > 0), master.waitingApps)
        })
      }

      override def masterStats: Try[MasterStats] = {
        val uri = uri"http://$webUi/metrics/master/prometheus"
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

      override def activeWorkers: Try[Seq[WorkerInfo]] = {
        val uri = uri"http://$rest/v1/submissions/master"
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
            log.info(s"querying ${wi.metricsUrl}")
            basicRequest
              .get(wi.metricsUrl)
              .send()
              .body
              .fold(
                error => Failure(HttpError(error)),
                body => parseWorkerMetrics(body, wi)
              )
        }


      def parseMetrics(prefix: String, resp: String): Try[Map[String, Long]] = {
        log.debug(s"parseMetrics: $prefix: $resp")
        val matcher = (s"metrics_${prefix}_(.*?)_Value\\{.*?} (\\d+)").r
        Try {
          resp.split('\n').flatMap {
            case matcher(name, value) =>
              Option((name, value.toLong))
            case unknown =>
              log.warn(s"Unable to parse metrics string: $unknown")
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
    }
}
