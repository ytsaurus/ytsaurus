package ru.yandex.spark.yt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.table.YtTableSettings

case class YtTableSparkSettings(configuration: Configuration) extends YtTableSettings {

  import YtTableSparkSettings._

  private def sortColumns: Seq[String] = configuration.ytConf(SortColumns)

  private def schema: StructType = configuration.ytConf(Schema)

  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortColumns)

  override def options: Map[String, String] = {
    val optionsKeys = configuration.ytConf(Options)
    optionsKeys.collect { case key if Options.ytOptions.contains(key) =>
      key.drop(prefix.length + 1) -> configuration.getYtConf(key).get
    }.toMap
  }
}

object YtTableSparkSettings {
  private val prefix = "table_settings"

  case object SortColumns extends StringListConfigEntry(s"$prefix.sort_columns", Some(Nil))

  case object Schema extends StructTypeConfigEntry(s"$prefix.schema")

  case object OptimizeFor extends StringConfigEntry(s"$prefix.optimize_for")

  case object Options extends StringListConfigEntry(s"$prefix.options") {
    val available: Set[String] = Set(SortColumns, Schema, OptimizeFor).map(_.name)

    val ytOptions: Set[String] = Set(OptimizeFor).map(_.name)
  }

  def isTableSorted(configuration: Configuration): Boolean = {
    configuration.getYtConf(SortColumns).exists(_.nonEmpty)
  }

  def deserialize(configuration: Configuration): YtTableSparkSettings = {
    YtTableSparkSettings(configuration)
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
