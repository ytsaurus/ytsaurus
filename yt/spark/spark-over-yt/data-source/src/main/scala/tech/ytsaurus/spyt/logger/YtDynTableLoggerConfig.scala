package tech.ytsaurus.spyt.logger

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.client.CompoundClient

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
  import tech.ytsaurus.spyt.fs.conf._

  def fromSpark(spark: SparkSession): Option[YtDynTableLoggerConfig] = {
    if (!spark.ytConf(Enabled)) {
      None
    } else {
      Some(YtDynTableLoggerConfig(
        ytConfig = ytClientConfiguration(spark),
        logTable = spark.ytConf(Table),
        appId = spark.conf.get("spark.app.id"),
        appName = spark.conf.get("spark.app.name"),
        discoveryPath = spark.conf.getOption("spark.base.discovery.path").getOrElse("-"),
        spytVersion = spark.conf.get("spark.yt.version"),
        logLevels = spark.ytConf(LogLevel),
        mergeExecutors = spark.ytConf(MergeExecutors),
        maxPartitionId = spark.ytConf(MaxPartitionId)
      ))
    }
  }
}
