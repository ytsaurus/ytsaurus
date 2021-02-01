package ru.yandex.spark.yt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.serializers.{SchemaConverter, YtLogicalType}
import ru.yandex.spark.yt.wrapper.table.YtTableSettings

case class YtTableSparkSettings(configuration: Configuration) extends YtTableSettings {

  import YtTableSparkSettings._

  private def sortColumns: Seq[String] = configuration.ytConf(SortColumns)

  private def writeSchemaHints: Map[String, YtLogicalType] = configuration.ytConf(WriteSchemaHint)

  private def schema: StructType = configuration.ytConf(Schema)

  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortColumns, writeSchemaHints)

  override def optionsAny: Map[String, Any] = {
    val optionsKeys = configuration.ytConf(Options)
    optionsKeys.collect { case key if !Options.excludeOptions.contains(key) =>
      key -> Options.get(key, configuration)
    }.toMap
  }
}

object YtTableSparkSettings {
  // read
  case object OptimizedForScan extends BooleanConfigEntry("is_scan")

  case object ArrowEnabled extends BooleanConfigEntry("arrow_enabled", Some(true))

  // write
  case object IsTable extends BooleanConfigEntry("is_table", Some(false))

  case object SortColumns extends StringListConfigEntry("sort_columns", Some(Nil))

  case object WriteSchemaHint extends YtLogicalTypeMapConfigEntry("write_schema_hint", Some(Map.empty))

  case object Schema extends StructTypeConfigEntry("schema")

  case object OptimizeFor extends StringConfigEntry("optimize_for")

  case object Dynamic extends BooleanConfigEntry("dynamic")

  case object Path extends StringListConfigEntry("path")

  case object Options extends StringListConfigEntry("options") {

    private val transformOptions: Set[ConfigEntry[_]] = Set(Dynamic)

    def get(key: String, configuration: Configuration): Any = {
      val str = configuration.getYtConf(key).get
      transformOptions.collectFirst {
        case conf if conf.name == key => conf.get(str)
      }.getOrElse(str)
    }
    val excludeOptions: Set[String] = Set(SortColumns, Schema, Path).map(_.name)
  }

  def isTable(configuration: Configuration): Boolean = {
    configuration.ytConf(IsTable)
  }

  def isTableSorted(configuration: Configuration): Boolean = {
    configuration.getYtConf(SortColumns).exists(_.nonEmpty)
  }

  def deserialize(configuration: Configuration): YtTableSparkSettings = {
    YtTableSparkSettings(configuration)
  }

  def serialize(options: Map[String, String], schema: StructType, configuration: Configuration): Unit = {
    options.foreach { case (key, value) =>
      configuration.setYtConf(key, value)
    }
    configuration.setYtConf(Schema, schema)
    configuration.setYtConf(Options, options.keys.toSeq)
    configuration.setYtConf(IsTable, true)
  }
}
