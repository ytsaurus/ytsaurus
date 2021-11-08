package ru.yandex.spark.yt.format

import org.apache.spark.sql.SparkSessionExtensions
import org.slf4j.LoggerFactory

class YtSparkExtensions extends (SparkSessionExtensions => Unit) {
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
//    extensions.injectPlannerStrategy(spark => new YtUInt64StrategyChecker(spark))
  }
}
