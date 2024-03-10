package tech.ytsaurus.spyt.format

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.GlobalTransaction
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class GlobalTransactionSparkListener(conf: SparkConf) extends SparkListener {
  private val log = LoggerFactory.getLogger(getClass)



  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    log.info("GlobalTransactionSparkListener started")
    if (conf.ytConf(GlobalTransaction.Enabled)) {
      log.info("Global transaction enabled, creating transaction")
      val yt = YtClientProvider.ytClient(ytClientConfiguration(conf))
      val transaction = YtWrapper.createTransaction(None, conf.ytConf(GlobalTransaction.Timeout))(yt)
      log.info(s"Global transaction id is ${transaction.getId.toString}")
      conf.setYtConf(GlobalTransaction.Id, transaction.getId.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    conf.getYtConf(GlobalTransaction.Id).foreach { transaction =>
      val yt = YtClientProvider.ytClient(ytClientConfiguration(conf))
      YtWrapper.commitTransaction(transaction)(yt)
      log.info(s"Global transaction $transaction committed")
    }
  }
}

object GlobalTransactionUtils {
  def getGlobalTransactionId(spark: SparkSession): Option[String] = {
    if (!spark.sparkContext.getConf.ytConf(GlobalTransaction.Enabled)) {
      return None
    }

    // This loop is needed because setting global transaction id is performed in separate thread, so we need to poll
    // spark context configuration for this parameter
    (1 to 10).foreach {_ =>
      val optGlobalTransactionId = spark.sparkContext.getConf.getYtConf(GlobalTransaction.Id)
      if (optGlobalTransactionId.nonEmpty) {
        return optGlobalTransactionId
      }
      Thread.sleep(1000)
    }

    throw new IllegalStateException("Can't get global transaction id from YT")
  }
}