package tech.ytsaurus.spark.launcher.rest

import io.circe._
import io.circe.syntax._
import org.scalatra.servlet.RichRequest
import tech.ytsaurus.spyt.wrapper.cypress.YsonSyntax._
import tech.ytsaurus.spyt.wrapper.cypress.YsonWriter
import tech.ytsaurus.ysontree.YTreeTextSerializer

trait YtOutputFormat {
  def format[T: YsonWriter : Encoder](t: T): String
}

object YtOutputFormat {

  case object Yson extends YtOutputFormat {
    override def format[T: YsonWriter : Encoder](t: T): String = {
      YTreeTextSerializer.serialize(t.toYson)
    }
  }

  case object Json extends YtOutputFormat {
    override def format[T: YsonWriter : Encoder](t: T): String = {
      t.asJson.noSpaces
    }
  }

  def fromHeaders(headerFormat: YtHeaderFormat)
                 (implicit request: RichRequest): YtOutputFormat = {
    headerFormat match {
      case YtHeaderFormat.Yson =>
        request.headers.get("X-YT-Output-Format") match {
          case Some(format) if format == YtHeaderFormat.ysonTextFormat => YtOutputFormat.Yson
          case _ => YtOutputFormat.Json
        }
      case YtHeaderFormat.Unknown => YtOutputFormat.Json
    }
  }
}
