package ru.yandex.spark.yt.wrapper

import org.slf4j.Logger

trait LogLazy {

  implicit class RichLogger(log: Logger) {
    def debugLazy(message: => String): Unit = {
      if (log.isDebugEnabled) {
        log.debug(message)
      }
    }

    def traceLazy(message: => String): Unit = {
      if (log.isTraceEnabled()) {
        log.trace(message)
      }
    }
  }
}
