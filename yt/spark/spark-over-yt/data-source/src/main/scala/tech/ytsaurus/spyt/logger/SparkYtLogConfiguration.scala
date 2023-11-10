package tech.ytsaurus.spyt.logger

import org.apache.log4j.Level
import tech.ytsaurus.spyt.fs.conf._

object SparkYtLogConfiguration {
  import ConfigEntry.implicits._
  private val prefix = "log"

  case object Enabled extends ConfigEntry[Boolean](s"$prefix.enabled", Some(true))

  case object Table extends ConfigEntry[String](s"$prefix.table", Some("//home/spark/logs/log_table"))

  case object LogLevel extends MultiConfigEntry(prefix, "level", Some(Level.INFO),
    { (name: String, default: Option[Level]) => new ConfigEntry[Level](name, default) })

  case object MergeExecutors extends MultiConfigEntry(prefix, "mergeExecutors", Some(true),
    { (name: String, default: Option[Boolean]) => new ConfigEntry[Boolean](name, default) })

  case object MaxPartitionId extends MultiConfigEntry(prefix, "maxPartitionId", Some(10),
    { (name: String, default: Option[Int]) => new ConfigEntry[Int](name, default) })

}
