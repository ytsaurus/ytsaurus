package ru.yandex.spark.yt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.table.YtTableSettings

case class YtTableSparkSettings(configuration: Configuration) extends YtTableSettings {

  import YtTableSparkSettings._

  private def sortColumns: Seq[String] = configuration.ytConf(SortColumns)

  private def schema: StructType = configuration.ytConf(Schema)

  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortColumns)

  override def optionsAny: Map[String, Any] = {
    val optionsKeys = configuration.ytConf(Options)
    optionsKeys.collect { case key if !Options.excludeOptions.contains(key) =>
      key.drop(prefix.length + 1) -> Options.get(key, configuration)
    }.toMap
  }
}

object YtTableSparkSettings {
  private val prefix = "table_settings"

  case object SortColumns extends StringListConfigEntry(s"$prefix.sort_columns", Some(Nil))

  case object Schema extends StructTypeConfigEntry(s"$prefix.schema")

  case object OptimizeFor extends StringConfigEntry(s"$prefix.optimize_for")

  case object Dynamic extends BooleanConfigEntry(s"$prefix.dynamic")

  case object Options extends StringListConfigEntry(s"$prefix.options") {
    def isTableOption(key: String): Boolean = key.startsWith(prefix)

    private val transformOptions: Set[ConfigEntry[_]] = Set(Dynamic)

    def get(key: String, configuration: Configuration): Any = {
      val str = configuration.getYtConf(key).get
      transformOptions.collectFirst {
        case conf if conf.name == key => conf.get(str)
      }.getOrElse(str)
    }

    val excludeOptions: Set[String] = Set(SortColumns, Schema).map(_.name)
  }

  def isTableSorted(configuration: Configuration): Boolean = {
    configuration.getYtConf(SortColumns).exists(_.nonEmpty)
  }

  def deserialize(configuration: Configuration): YtTableSparkSettings = {
    YtTableSparkSettings(configuration)
  }

  def serialize(options: Map[String, String], schema: StructType, configuration: Configuration): Unit = {
    val tableOptions = options.filterKeys(Options.isTableOption)
    tableOptions.foreach { case (key, value) =>
      configuration.setYtConf(key, value)
    }
    configuration.setYtConf(Schema, schema)
    configuration.setYtConf(Options, tableOptions.keys.toSeq)
  }
}
