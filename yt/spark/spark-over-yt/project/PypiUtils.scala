package spyt

import sbt.{Logger, URI}

import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.time.Duration

object PypiUtils {
  case class PythonVersion(major: Int, minor: Int, patch: Int, beta: Int, dev: Int)

  type PythonVerTuple = (PythonVersion, Option[PythonVersion])

  def listPypiPackageVersions(log: Logger, pythonRegistry: String, packageName: String): Seq[String] = {
    implicit val ord: Ordering[PythonVersion] = Ordering.by {
      v => (v.major, v.minor, v.patch, v.beta, v.dev)
    }

    def verTuple(ver: String): Option[PythonVerTuple] = {
      val splitVer = "^(\\d+)\\.(\\d+).(\\d+)(b(\\d+))?(\\.dev(\\d+))?\\+?(.*)$".r
      ver match {
        case splitVer(maj, min, patch, _, beta, _, dev, local) =>
          val p1 = PythonVersion(
            maj.toInt,
            min.toInt,
            patch.toInt,
            if (beta == null) 0 else beta.toInt,
            if (dev == null) 0 else dev.toInt
          )
          val p2 = if (local != null) verTuple(local).map(_._1) else None
          Some((p1, p2))
        case _ => None
      }
    }

    val extractVer = (s".*<a.*?>$packageName-(.*?)\\.tar\\.gz</a>.*$$").r

    httpQuery(log, s"$pythonRegistry/$packageName")
      .toList
      .flatMap(_.split("\n").toList)
      .flatMap {
        _ match {
          case extractVer(ver) => List((ver, verTuple(ver)))
          case _ => List()
        }
      }
      .sortBy(_._2)
      .reverse
      .map(_._1)
  }

  def latestVersion(log: Logger, pythonRegistry: String, packageName: String): Option[String] = {
    listPypiPackageVersions(log, pythonRegistry, packageName).headOption
  }

  private def httpQuery(log: Logger, url: String, timeoutSec: Int = 30): Option[String] = {
    val timeout = Duration.ofSeconds(timeoutSec)
    val cli = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(timeout).build()
    val req: HttpRequest = HttpRequest.newBuilder()
      .timeout(timeout)
      .uri(new URI(url))
      .build()
    log.info(s"Requesting $url")
    val res = cli.send(req, BodyHandlers.ofString())
    if (res.statusCode() != 200) {
      log.error(s"Invalid http response: ${res.statusCode()}: ${res.body()}")
      None
    } else {
      Some(res.body())
    }
  }
}
