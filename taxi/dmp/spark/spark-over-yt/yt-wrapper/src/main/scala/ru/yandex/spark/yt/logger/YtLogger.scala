package ru.yandex.spark.yt.logger

import org.apache.log4j.Level
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.spark.yt.wrapper.YtWrapper.RichLogger

trait YtLogger {
  def trace(msg: => String, info: Map[String, String] = Map.empty): Unit = logYt(msg, info, Level.TRACE)

  def debug(msg: => String, info: Map[String, String] = Map.empty): Unit = logYt(msg, info, Level.DEBUG)

  def info(msg: => String, info: Map[String, String] = Map.empty): Unit = logYt(msg, info, Level.INFO)

  def warn(msg: => String, info: Map[String, String] = Map.empty): Unit = logYt(msg, info, Level.WARN)

  def error(msg: => String, info: Map[String, String] = Map.empty): Unit = logYt(msg, info, Level.ERROR)

  def logYt(msg: => String, info: Map[String, String] = Map.empty, level: Level): Unit
}

object YtLogger {
  @transient val noop: YtLogger = new YtLogger {
    private val log: Logger = LoggerFactory.getLogger(getClass)

    override def logYt(msg: => String, info: Map[String, String], level: Level): Unit = {
      log.debugLazy(s"Skip logging message to YT: $msg, $info")
    }
  }
}
