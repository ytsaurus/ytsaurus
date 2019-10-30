package ru.yandex.spark.yt.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

case class YtTableSettings(configuration: Configuration) {

  import YtTableSettings._

  def all: Map[String, String] = {
    val optionsKeys = configuration.ytConf(Options)
    optionsKeys.collect { case key if Options.available.contains(key) =>
      key.drop(prefix.length + 1) -> configuration.getYtConf(key).get
    }.toMap
  }

  def sortColumns: Seq[String] = configuration.ytConf(SortColumns)

  def schema: StructType = configuration.ytConf(Schema)
}

object YtTableSettings {
  private val prefix = "table_settings"

  case object SortColumns extends StringListConfigEntry(s"$prefix.sort_columns", Some(Nil))
  case object Schema extends StructTypeConfigEntry(s"$prefix.schema")
  case object OptimizeFor extends StringConfigEntry(s"$prefix.optimize_for")

  case object Options extends StringListConfigEntry(s"$prefix.options") {
    val available: Set[String] = Set(SortColumns, Schema, OptimizeFor).map(_.name)
  }

  def isTableSorted(configuration: Configuration): Boolean = {
    configuration.getYtConf(SortColumns).exists(_.nonEmpty)
  }

  def deserialize(configuration: Configuration): YtTableSettings = {
    YtTableSettings(configuration)
  }

  def serialize(options: Map[String, String], schema: StructType, configuration: Configuration): Unit = {
    options.foreach { case (key, value) =>
      if (Options.available.contains(key)) {
        configuration.setYtConf(key, value)
      }
    }
    configuration.setYtConf(Schema, schema)
    configuration.setYtConf(Options, options.keys.toSeq)
  }


}
