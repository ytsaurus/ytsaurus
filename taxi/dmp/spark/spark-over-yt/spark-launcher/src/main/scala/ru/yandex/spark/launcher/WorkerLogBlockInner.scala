package ru.yandex.spark.launcher

import io.circe.parser.decode
import io.circe.{Decoder, HCursor}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.launcher.WorkerLogBlock.formatter
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.Key._

import java.time.{LocalDate, LocalDateTime}

case class WorkerLogBlockInner(date: LocalDate,
                               dateTime: String,
                               loggerName: String,
                               level: Option[String],
                               sourceHost: Option[String],
                               file: Option[String],
                               lineNumber: Option[String],
                               thread: Option[String],
                               message: String,
                               exceptionClass: Option[String],
                               exceptionMessage: Option[String],
                               stack: Option[String])

object WorkerLogBlockInner {
  def parseDate(dt: String): LocalDate = {
    LocalDateTime.parse(dt, formatter).toLocalDate
  }

  private implicit val jsonDecoder: Decoder[WorkerLogBlockInner] = (c: HCursor) => {
    for {
      dt <- c.downField("@timestamp").as[String]
      loggerName <- c.downField("logger_name").as[String]
      level <- c.downField("level").as[String]
      thread <- c.downField("thread_name").as[String]
      message <- c.downField("message").as[String]
      excClass <- c.downField("exception").getOrElse[String]("exception_class")(null)
      excMessage <- c.downField("exception").getOrElse[String]("exception_message")(null)
      stack <- c.downField("exception").getOrElse[String]("stacktrace")(null)
      file <- c.getOrElse[String]("file")(null)
      lineNumber <- c.getOrElse[String]("line_number")(null)
      sourceHost <- c.getOrElse[String]("source_host")(null)
    } yield {
      new WorkerLogBlockInner(
        parseDate(dt),
        dt,
        loggerName,
        Some(level),
        Some(sourceHost),
        Option(file),
        Option(lineNumber),
        Some(thread),
        message,
        Option(excClass),
        Option(excMessage),
        Option(stack)
      )
    }
  }

  def fromJson(json: String, fileCreationTime: LocalDateTime): WorkerLogBlockInner = {
    val res = decode[WorkerLogBlockInner](json)(jsonDecoder)
    res match {
      case Left(_) => fromMessage(json, fileCreationTime)
      case Right(value) => value
    }
  }

  def fromMessage(message: String, fileCreationTime: LocalDateTime): WorkerLogBlockInner = {
    WorkerLogBlockInner(
      fileCreationTime.toLocalDate, fileCreationTime.format(formatter), "", None, None, None,
      None, None, message, None, None, None
    )
  }

  def apply(node: YTreeNode): WorkerLogBlockInner = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()
    val dt = mp.getOrThrow(DATE).stringValue()

    new WorkerLogBlockInner(
      parseDate(dt),
      dt,
      mp.getOrThrow(LOGGER_NAME).stringValue(),
      mp.getOptionString(LEVEL),
      mp.getOptionString(SOURCE_HOST),
      mp.getOptionString(FILE),
      mp.getOptionString(LINE_NUMBER),
      mp.getOptionString(THREAD),
      mp.getOrThrow(MESSAGE).stringValue(),
      mp.getOptionString(EXCEPTION_CLASS),
      mp.getOptionString(EXCEPTION_MESSAGE),
      mp.getOptionString(STACK)
    )
  }
}
