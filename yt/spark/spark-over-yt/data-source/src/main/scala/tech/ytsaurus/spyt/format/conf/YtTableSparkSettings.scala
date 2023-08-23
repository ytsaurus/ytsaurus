package tech.ytsaurus.spyt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.spyt.fs.conf.{BooleanConfigEntry, ConfigEntry, LongConfigEntry, StringConfigEntry, StringListConfigEntry}
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.ysontree.YTreeNode

case class YtTableSparkSettings(configuration: Configuration) extends YtTableSettings {

  import YtTableSparkSettings._

  private def sortOption: SortOption = {
    val keys = configuration.ytConf(SortColumns)
    val uniqueKeys = configuration.ytConf(UniqueKeys)
    if (keys.isEmpty && uniqueKeys) {
      throw new IllegalArgumentException("Unique keys attribute can't be true for unordered table")
    }
    if (keys.nonEmpty) Sorted(keys, uniqueKeys) else Unordered
  }

  private def writeSchemaHints: Map[String, YtLogicalType] = configuration.ytConf(WriteSchemaHint)

  private def typeV3Format: Boolean = configuration.ytConf(WriteTypeV3)

  private def schema: StructType = configuration.ytConf(Schema)

  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(
    schema, sortOption, writeSchemaHints, typeV3Format)

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

  case object KeyPartitioned extends BooleanConfigEntry("key_partitioned")

  case object Dynamic extends BooleanConfigEntry("dynamic", Some(false))

  case object Transaction extends StringConfigEntry("transaction")

  case object Timestamp extends LongConfigEntry("timestamp")

  case object InconsistentReadEnabled extends BooleanConfigEntry("enable_inconsistent_read", Some(false))

  // write
  case object IsTable extends BooleanConfigEntry("is_table", Some(false))

  case object SortColumns extends StringListConfigEntry("sort_columns", Some(Nil))

  case object UniqueKeys extends BooleanConfigEntry("unique_keys", Some(false))

  case object InconsistentDynamicWrite extends BooleanConfigEntry("inconsistent_dynamic_write", Some(false))

  case object WriteSchemaHint extends YtLogicalTypeMapConfigEntry("write_schema_hint", Some(Map.empty))

  case object Schema extends StructTypeConfigEntry("schema")

  case object WriteTransaction extends StringConfigEntry("write_transaction")

  case object WriteTypeV3 extends BooleanConfigEntry("write_type_v3", Some(false))

  case object NullTypeAllowed extends BooleanConfigEntry("null_type_allowed", Some(false))

  case object OptimizeFor extends StringConfigEntry("optimize_for")

  case object Path extends StringListConfigEntry("path")

  case object Options extends StringListConfigEntry("options") {

    private val transformOptions: Set[ConfigEntry[_]] = Set(Dynamic)

    def get(key: String, configuration: Configuration): Any = {
      val str = configuration.getYtConf(key).get
      transformOptions.collectFirst {
        case conf if conf.name == key => conf.get(str)
      }.getOrElse(str)
    }

    val excludeOptions: Set[String] = Set(SortColumns, Schema, WriteTypeV3, NullTypeAllowed, Path).map(_.name)
  }

  def isTable(configuration: Configuration): Boolean = {
    configuration.ytConf(IsTable)
  }

  def isDynamicTable(configuration: Configuration): Boolean = {
    configuration.ytConf(Dynamic)
  }

  def isNullTypeAllowed(options: Map[String, String]): Boolean = {
    options.ytConf(NullTypeAllowed)
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
