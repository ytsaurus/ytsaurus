package org.apache.spark.deploy.rest

import com.google.common.net.HostAndPort
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.{Decoder, Error, Json}
import ru.yandex.spark.yt.wrapper.Utils.flatten
import sttp.client._
import sttp.model.Uri

import scala.util.{Failure, Try}

case class DriverInfo(startTime: Long, id: String)
case class WorkerInfo(id: String, host: String, port: Int, cores: Int, memory: Int, webUiAddress: String, alive: Boolean,
                      resources: Map[String, ResourceInfo]) {
  def isDriverOp: Boolean = resources.contains("driverop")
}
case class ResourceInfo(name: String, addresses: Seq[String])

object MasterClient {
  def activeDrivers(master: HostAndPort): Try[Seq[String]] = {
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()
    basicRequest
      .get(Uri.parse(s"http://$master/v1/submissions/status").toOption.get)
      .send()
      .body
      .fold[Try[Seq[String]]](
        error => Failure(HttpError(error)),
        body => parseDriversList(body).toTry
      )
  }

  def parseDriverIdInfo(driver: Json): Either[Error, String] = {
    val driverCursor = driver.hcursor
    for {
      id <- driverCursor.downField("driverId").as[String]
    } yield id
  }

  def parseDriversList(body: String): Either[Error, Seq[String]] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      rawDrivers <- cursor.downField("statuses").as[Array[Json]]
      drivers <- flatten(rawDrivers.map(parseDriverIdInfo))
    } yield drivers
  }

  def activeWorkers(master: HostAndPort): Try[Seq[WorkerInfo]] = {
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()
    basicRequest
      .get(Uri.parse(s"http://$master/v1/submissions/master").toOption.get)
      .send()
      .body
      .fold[Try[Seq[WorkerInfo]]](
        error => Failure(HttpError(error)),
        body => parseWorkersList(body).toTry
      )
  }

  def parseWorkersList(body: String): Either[Error, Seq[WorkerInfo]] = {
    implicit val resourceDecoder: Decoder[ResourceInfo] = deriveDecoder
    implicit val workerDecoder: Decoder[WorkerInfo] = deriveDecoder
    for {
      cursor <- parse(body).map(_.hcursor)
      workers <- cursor.downField("workers").as[Seq[WorkerInfo]]
    } yield workers
  }
}
