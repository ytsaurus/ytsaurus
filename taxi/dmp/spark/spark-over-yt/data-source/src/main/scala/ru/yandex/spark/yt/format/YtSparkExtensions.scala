package ru.yandex.spark.yt.format

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSessionExtensions

class YtSparkExtensions extends (SparkSessionExtensions => Unit) {
  private val log = Logger.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
  }
}
