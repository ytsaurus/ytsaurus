package tech.ytsaurus.spyt.format

import org.apache.spark.scheduler.UserDefinedSparkListener
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableMarkerRule

class ExtraOptimizationsSparkListener(spark: SparkSession) extends UserDefinedSparkListener {
  private val log = LoggerFactory.getLogger(getClass)

  override def onListenerStart(): Unit = {
    log.info("ExtraOptimizationsSparkListener started")
    spark.experimental.extraOptimizations ++= Seq(new YtSortedTableMarkerRule(spark))
  }
}
