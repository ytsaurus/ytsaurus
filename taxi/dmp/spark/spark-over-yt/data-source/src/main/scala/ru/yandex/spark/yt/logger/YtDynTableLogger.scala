package ru.yandex.spark.yt.logger

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MapType, StringType}
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.YsonEncoder
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.tables.TableSchema

import java.time.LocalDateTime
import java.util.UUID

class YtDynTableLogger(name: String,
                       config: YtDynTableLoggerConfig,
                       forceLevel: Option[Level] = None) extends YtLogger {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val sparkComponent = SparkComponent.get()

  override def logYt(msg: => String, info: Map[String, String] = Map.empty, level: Level): Unit = {
    try {
      val newLevel = forceLevel.getOrElse(level)
      if (newLevel.isGreaterOrEqual(config.logLevels(name))) {
        val row = Seq(
          name,
          LocalDateTime.now().toString,
          config.discoveryPath,
          config.spytVersion,
          config.appId,
          config.appName,
          UUID.randomUUID().toString,
          newLevel.toString,
          sparkComponent.name,
          config.taskContext.map(_.stageId.asInstanceOf[Integer]).orNull,
          config.taskContext.map(_.partitionId.asInstanceOf[Integer]).orNull,
          msg,
          YsonEncoder.encode(info, MapType(StringType, StringType), skipNulls = true)
        )
        YtWrapper.insertRows(config.logTable, YtDynTableLogger.logTableSchema, Seq(row))(config.yt)
      }
    } catch {
      case e: Throwable =>
        log.warn(s"Failed to log message to YT: ${e.getMessage}")
    }

  }
}

object YtDynTableLogger {
  val logTableSchema: TableSchema = TableSchema.builder()
    .addKey("logger_name", TiType.string())
    .addKey("dttm", TiType.string())
    .addKey("discovery_path", TiType.string())
    .addKey("spyt_version", TiType.string())
    .addKey("app_id", TiType.string())
    .addKey("app_name", TiType.string())
    .addKey("uid", TiType.string())
    .addValue("level", TiType.string())
    .addValue("spark_component", TiType.string())
    .addValue("stage_id", TiType.optional(TiType.int32()))
    .addValue("partition_id", TiType.optional(TiType.int32()))
    .addValue("msg", TiType.string())
    .addValue("info", TiType.optional(TiType.yson()))
    .build()

  def fromSpark(name: String, spark: SparkSession): YtLogger = fromConfig(name, YtDynTableLoggerConfig.fromSpark(spark))

  def fromConfig(name: String, config: Option[YtDynTableLoggerConfig]): YtLogger = {
    config.map(c =>
      new YtDynTableLogger(name, c, c.forceTraceOnExecutor(name))
    ).getOrElse(YtLogger.noop)
  }

  def pushdown(config: Option[YtDynTableLoggerConfig]): YtLogger = fromConfig("pushdown", config)

  def pushdown(spark: SparkSession): YtLogger = fromSpark("pushdown", spark)
}
