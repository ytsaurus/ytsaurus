package ru.yandex.spark.launcher.rest

import io.circe.syntax._
import javax.servlet.http.HttpServletRequest
import org.scalatra.servlet.RichRequest
import org.scalatra.{ApiFormats, ScalatraServlet}

class MasterWrapperServlet(sparkMasterEndpoint: String,
                           sparkByopPort: Option[Int]) extends ScalatraServlet with ApiFormats {
  private val byopDiscoveryService = sparkByopPort.map(bp => new ByopDiscoveryService(sparkMasterEndpoint, bp))

  before() {
    contentType = formats("json")
  }

  get("/") {
    Map(
      "byop_enabled" -> sparkByopPort.nonEmpty
    ).asJson.spaces2
  }

  get("/api/v4/discover_proxies") {
    implicit def enrichRequest(implicit request: HttpServletRequest): RichRequest = RichRequest(request)

    val info = byopDiscoveryService.map(_.discoveryInfo).getOrElse(DiscoveryInfo())
    val headerFormat = YtHeaderFormat.fromHeader

    YtOutputFormat.fromHeaders(headerFormat).format(info)
  }
}
