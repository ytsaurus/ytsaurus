package ru.yandex.spark.yt.format

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, UserDefinedSparkListener}
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider

class GlobalTransactionSparkListener(spark: SparkSession) extends UserDefinedSparkListener {

  import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.GlobalTransaction
  import ru.yandex.spark.yt.fs.conf._

  override def onListenerStart(): Unit = {
    if (spark.ytConf(GlobalTransaction.Enabled)) {
      val yt = YtClientProvider.ytClient(ytClientConfiguration(spark))
      val transaction = YtWrapper.createTransaction(None, spark.ytConf(GlobalTransaction.Timeout))(yt)
      spark.setYtConf(GlobalTransaction.Id, transaction.getId.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    spark.getYtConf(GlobalTransaction.Id).foreach { transaction =>
      val yt = YtClientProvider.ytClient(ytClientConfiguration(spark))
      YtWrapper.commitTransaction(transaction)(yt)
    }
  }
}
