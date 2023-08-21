package tech.ytsaurus.spyt.format

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, UserDefinedSparkListener}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class GlobalTransactionSparkListener(spark: SparkSession) extends UserDefinedSparkListener {
  private val log = LoggerFactory.getLogger(getClass)

  import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.GlobalTransaction
  import tech.ytsaurus.spyt.fs.conf._

  override def onListenerStart(): Unit = {
    log.info("GlobalTransactionSparkListener started")
    if (spark.ytConf(GlobalTransaction.Enabled)) {
      log.info("Global transaction enabled, creating transaction")
      val yt = YtClientProvider.ytClient(ytClientConfiguration(spark))
      val transaction = YtWrapper.createTransaction(None, spark.ytConf(GlobalTransaction.Timeout))(yt)
      log.info(s"Global transaction id is ${transaction.getId.toString}")
      spark.setYtConf(GlobalTransaction.Id, transaction.getId.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    spark.getYtConf(GlobalTransaction.Id).foreach { transaction =>
      val yt = YtClientProvider.ytClient(ytClientConfiguration(spark))
      YtWrapper.commitTransaction(transaction)(yt)
      log.info(s"Global transaction $transaction committed")
    }
  }
}
