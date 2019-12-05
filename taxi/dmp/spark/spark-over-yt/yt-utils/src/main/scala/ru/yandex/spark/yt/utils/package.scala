package ru.yandex.spark.yt

import org.apache.log4j.Logger

package object utils {
  implicit class RichLogger(log: Logger) {
    def debugLazy(message: => String): Unit = {
      if (log.isDebugEnabled) {
        log.debug(message)
      }
    }
  }
}
