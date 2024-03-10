package tech.ytsaurus.spyt.format

import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.optimizer.{YtSortedTableStrategy, YtSourceStrategy}

class YtSparkExtensions extends SparkSessionExtensionsProvider {
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(YtSortedTableStrategy(_))
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
  }
}
