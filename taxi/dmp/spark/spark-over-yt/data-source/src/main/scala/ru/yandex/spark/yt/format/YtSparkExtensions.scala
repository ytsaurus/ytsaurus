package ru.yandex.spark.yt.format

import org.apache.spark.sql.SparkSessionExtensions
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format.optimizer.{YtSortedTableAggregationStrategy, YtSourceStrategy}

class YtSparkExtensions extends (SparkSessionExtensions => Unit) {
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(YtSortedTableAggregationStrategy)
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
  }
}
