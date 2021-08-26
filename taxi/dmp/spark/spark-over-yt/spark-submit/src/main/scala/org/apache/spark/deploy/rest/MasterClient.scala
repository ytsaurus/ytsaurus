package org.apache.spark.deploy.rest

import io.circe.parser.parse
import io.circe.{Error, Json}
import ru.yandex.spark.yt.wrapper.Utils.flatten
import sttp.client._

import scala.util.{Failure, Try}

case class DriverInfo(startTime: Long, id: String)

object MasterClient {
  def activeDrivers(master: String): Try[Seq[String]] = {
    implicit val backend = HttpURLConnectionBackend()
    basicRequest
      .get(uri"http://$master/v1/submissions/master")
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
      id <- driverCursor.downField("id").as[String]
    } yield id
  }

  def parseDriversList(body: String): Either[Error, Seq[String]] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      rawDrivers <- cursor.downField("drivers").as[Array[Json]]
      drivers <- flatten(rawDrivers.map(parseDriverIdInfo))
    } yield drivers
  }
}
