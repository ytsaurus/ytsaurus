package tech.ytsaurus.spyt.format

import org.apache.spark.sql.SparkSessionExtensions
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.optimizer.YtSourceStrategy
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableStrategy

class YtSparkExtensions extends (SparkSessionExtensions => Unit) {
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(YtSortedTableStrategy(_))
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
  }
}
