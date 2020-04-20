package ru.yandex.spark.yt.wrapper

import org.apache.log4j.Logger

trait LogLazy {
  implicit class RichLogger(log: Logger) {
    def debugLazy(message: => String): Unit = {
      if (log.isDebugEnabled) {
        log.debug(message)
      }
    }
  }
}
