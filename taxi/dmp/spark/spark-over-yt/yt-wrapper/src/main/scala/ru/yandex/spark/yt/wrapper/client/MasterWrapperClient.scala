package ru.yandex.spark.yt.wrapper.client

import com.google.common.net.HostAndPort
import io.circe._
import io.circe.parser._
import sttp.client._

import scala.util.{Failure, Try}

class MasterWrapperClient(val endpoint: HostAndPort) {

  import MasterWrapperClient._

  implicit private val backend = HttpURLConnectionBackend()

  def byopEnabled: Try[Boolean] = {
    // выносим url в отдельную переменную, потому что uri"http://$endpoint"
    // сделает percent encoding и добавит '%' в ipv6 адрес. uri"$url" дает корректное поведение.
    val url = s"http://$endpoint"
    Try {
      basicRequest
        .get(uri"$url")
        .send()
    }.flatMap(_.body.fold[Try[Boolean]](
      error => Failure(HttpError(error)),
      body => parseByopEnabled(body).toTry
    ))
  }

  def discoverProxies: Try[Seq[String]] = {
    val url = s"http://$endpoint/api/v4/discover_proxies"
    Try {
      basicRequest
        .get(uri"$url")
        .send()
    }.flatMap(_.body.fold[Try[Seq[String]]](
      error => Failure(HttpError(error)),
      body => parseProxiesList(body).toTry
    ))
  }
}

object MasterWrapperClient {
  def parseByopEnabled(body: String): Either[Error, Boolean] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      res <- cursor.downField("byop_enabled").as[Boolean]
    } yield res
  }

  def parseProxiesList(body: String): Either[Error, Seq[String]] = {
    for {
      cursor <- parse(body).map(_.hcursor)
      res <- cursor.downField("proxies").as[Seq[String]]
    } yield res
  }
}
