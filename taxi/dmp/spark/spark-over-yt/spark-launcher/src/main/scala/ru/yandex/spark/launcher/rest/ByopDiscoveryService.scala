package ru.yandex.spark.launcher.rest

import com.google.common.net.HostAndPort
import io.circe._
import io.circe.generic.semiauto.deriveEncoder
import io.circe.parser._
import ru.yandex.spark.yt.wrapper.Utils.flatten
import ru.yandex.spark.yt.wrapper.cypress.{YsonWriter, YsonableProduct}
import sttp.client._

import scala.util.{Failure, Try}

class ByopDiscoveryService(masterEndpoint: String, byopPort: Int) {

  import ByopDiscoveryService._

  private val baseUri = s"http://$masterEndpoint/v1/submissions"

  def discoveryInfo: DiscoveryInfo = {
    DiscoveryInfo(proxiesList.get)
  }

  private def proxiesList: Try[Seq[String]] = {
    implicit val backend = HttpURLConnectionBackend()
    basicRequest
      .get(uri"$baseUri/master")
      .send()
      .body
      .fold[Try[Seq[String]]](
        error => Failure(HttpError(error)),
        body => parseWorkersList(body).toTry.map(workersToProxies(_, byopPort))
      )
  }
}

case class DiscoveryInfo(proxies: Seq[String] = Nil)

object DiscoveryInfo {
  implicit val encoder: Encoder[DiscoveryInfo] = deriveEncoder
  implicit val ysonWriter: YsonWriter[DiscoveryInfo] = YsonableProduct.ysonWriter[DiscoveryInfo]
}

case class WorkerInfo(host: String, alive: Boolean)

object ByopDiscoveryService {
  def appendPort(host: String, port: Int): String = {
    HostAndPort.fromParts(host, port).toString
  }

  def parseWorkerInfo(worker: Json): Either[Error, WorkerInfo] = {
    val workerCursor = worker.hcursor
    for {
      host <- workerCursor.downField("host").as[String]
      alive <- workerCursor.downField("alive").as[Boolean]
    } yield WorkerInfo(host, alive)
  }

  def parseWorkersList(body: String): Either[Error, Seq[WorkerInfo]] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      workers <- cursor.downField("workers").as[Array[Json]]
      workersHosts <- flatten(workers.map(parseWorkerInfo))
    } yield workersHosts
  }

  def workersToProxies(workers: Seq[WorkerInfo], byopPort: Int): Seq[String] = {
    workers.flatMap {
      case WorkerInfo(host, true) => Some(appendPort(host, byopPort))
      case _ => None
    }
  }
}
