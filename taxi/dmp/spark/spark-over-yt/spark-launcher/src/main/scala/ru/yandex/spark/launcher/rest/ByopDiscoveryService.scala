package ru.yandex.spark.launcher.rest

import com.google.common.net.HostAndPort
import io.circe._
import io.circe.generic.semiauto.deriveEncoder
import io.circe.parser._
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
        body => parseWorkersList(body).toTry.map(appendPort(_, byopPort))
      )
  }
}

case class DiscoveryInfo(proxies: Seq[String] = Nil)

object DiscoveryInfo {
  implicit val encoder: Encoder[DiscoveryInfo] = deriveEncoder
  implicit val ysonWriter: YsonWriter[DiscoveryInfo] = YsonableProduct.ysonWriter[DiscoveryInfo]
}

object ByopDiscoveryService {
  def appendPort(hosts: Seq[String], port: Int): Seq[String] = {
    hosts.map(appendPort(_, port))
  }

  def appendPort(host: String, port: Int): String = {
    HostAndPort.fromParts(host, port).toString
  }

  def parseWorkerHost(worker: Json): Either[Error, String] = {
    val workerCursor = worker.hcursor
    workerCursor.downField("host").as[String]
  }

  def parseWorkersList(body: String): Either[Error, Seq[String]] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      workers <- cursor.downField("workers").as[Array[Json]]
      workersHosts <- flatten(workers.map(parseWorkerHost))
    } yield workersHosts
  }

  def flatten[A, B](seq: Seq[Either[A, B]]): Either[A, Seq[B]] = {
    seq
      .find(_.isLeft)
      .map(e => Left(e.left.get))
      .getOrElse(Right(seq.map(_.right.get)))
  }
}
