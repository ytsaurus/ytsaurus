package org.apache.spark.deploy.rest

import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.parse
import io.circe.{Decoder, Error, Json}
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.Utils.flatten
import sttp.client._
import sttp.model.Uri

import scala.util.{Failure, Try}

case class DriverInfo(driverId: String, status: String, startedAt: Long)
case class WorkerInfo(id: String, host: String, port: Int, cores: Int, memory: Int, webUiAddress: String, alive: Boolean,
                      resources: Map[String, ResourceInfo]) {
  def isDriverOp: Boolean = resources.contains("driverop")
}
case class ResourceInfo(name: String, addresses: Seq[String])

object MasterClient {
  def allDrivers(master: HostAndPort): Try[Seq[DriverInfo]] = {
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()
    basicRequest
      .get(Uri.parse(s"http://$master/v1/submissions/status").toOption.get)
      .send()
      .body
      .fold[Try[Seq[DriverInfo]]](
        error => Failure(HttpError(error)),
        body => parseDriversList(body).toTry
      )
  }

  def parseDriverIdInfo(driver: Json): Either[Error, DriverInfo] = {
    val driverCursor = driver.hcursor
    for {
      driverId <- driverCursor.downField("driverId").as[String]
      status <- driverCursor.downField("status").as[String]
      startedAt <- driverCursor.downField("startedAt").as[Long]
    } yield DriverInfo(driverId, status, startedAt)
  }

  def parseDriversList(body: String): Either[Error, Seq[DriverInfo]] = {
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
