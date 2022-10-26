package ru.yandex.spark.yt.logger

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.yt.ytclient.proxy.CompoundClient

case class YtDynTableLoggerConfig(ytConfig: YtClientConfiguration,
                                  logTable: String,
                                  appId: String,
                                  appName: String,
                                  discoveryPath: String,
                                  spytVersion: String,
                                  logLevels: Map[String, Level],
                                  mergeExecutors: Map[String, Boolean],
                                  maxPartitionId: Map[String, Int],
                                  taskContext: Option[TaskInfo] = None) {
  @transient lazy val yt: CompoundClient = YtWrapper.createRpcClient("yt logger", ytConfig).yt

  def forceTraceOnExecutor(name: String): Option[Level] = {
    val shouldForceTrace = mergeExecutors(name) && taskContext.exists(_.partitionId > maxPartitionId(name))
    if (shouldForceTrace) Some(Level.TRACE) else None
  }

}

object YtDynTableLoggerConfig {

  import SparkYtLogConfiguration._
  import ru.yandex.spark.yt.fs.conf._

  def fromSpark(spark: SparkSession): Option[YtDynTableLoggerConfig] = {
    if (!spark.ytConf(Enabled)) {
      None
    } else {
      Some(YtDynTableLoggerConfig(
        ytConfig = ytClientConfiguration(spark),
        logTable = spark.ytConf(Table),
        appId = spark.conf.get("spark.app.id"),
        appName = spark.conf.get("spark.app.name"),
        discoveryPath = spark.conf.get("spark.base.discovery.path"),
        spytVersion = spark.conf.get("spark.yt.version"),
        logLevels = spark.ytConf(LogLevel),
        mergeExecutors = spark.ytConf(MergeExecutors),
        maxPartitionId = spark.ytConf(MaxPartitionId)
      ))
    }
  }
}
