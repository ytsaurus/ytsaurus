package tech.ytsaurus.spark.launcher.rest

import org.scalatra.servlet.RichRequest
import tech.ytsaurus.core.YtFormat
import tech.ytsaurus.ysontree.YTreeTextSerializer

sealed trait YtHeaderFormat

object YtHeaderFormat {
  val ysonTextFormat: String = YTreeTextSerializer.serialize(YtFormat.YSON_TEXT)

  case object Yson extends YtHeaderFormat

  case object Unknown extends YtHeaderFormat

  def fromHeader(implicit request: RichRequest): YtHeaderFormat = {
    request.headers.get("X-YT-Header-Format") match {
      case Some(format) if format == ysonTextFormat => Yson
      case _ => Unknown
    }
  }
}
