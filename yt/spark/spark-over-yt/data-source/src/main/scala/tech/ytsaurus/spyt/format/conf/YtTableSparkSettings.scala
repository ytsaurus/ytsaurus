package tech.ytsaurus.spyt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.spyt.fs.conf.ConfigEntry
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeTypeV3}
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.ysontree.YTreeNode
import tech.ytsaurus.ysontree.YTreeTextSerializer

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
  import ConfigEntry.implicits._
  import ConfigEntry.{fromJsonTyped, toJsonTyped}

  implicit val structTypeAdapter: ValueAdapter[StructType] = new ValueAdapter[StructType] {
    override def get(value: String): StructType = SchemaConverter.stringToSparkType(value).asInstanceOf[StructType]

    override def set(value: StructType): String = SchemaConverter.sparkTypeToString(value)
  }

  implicit val logicalTypeMapAdapter: ValueAdapter[Map[String, YtLogicalType]] = new ValueAdapter[Map[String, YtLogicalType]] {
    override def get(value: String): Map[String, YtLogicalType] = {
      fromJsonTyped[Map[String, String]](value).mapValues(t => deserializeTypeV3(YTreeTextSerializer.deserialize(t)))
    }

    override def set(value: Map[String, YtLogicalType]): String = {
      toJsonTyped[Map[String, String]](value.mapValues(t => YTreeTextSerializer.serialize(serializeTypeV3(t))))
    }
  }
  
  // read
  case object OptimizedForScan extends ConfigEntry[Boolean]("is_scan")

  case object ArrowEnabled extends ConfigEntry[Boolean]("arrow_enabled", Some(true))

  case object KeyPartitioned extends ConfigEntry[Boolean]("key_partitioned")

  case object Dynamic extends ConfigEntry[Boolean]("dynamic")

  case object Transaction extends ConfigEntry[String]("transaction")

  case object Timestamp extends ConfigEntry[Long]("timestamp")

  case object InconsistentReadEnabled extends ConfigEntry[Boolean]("enable_inconsistent_read", Some(false))

  // write
  case object IsTable extends ConfigEntry[Boolean]("is_table", Some(false))

  case object SortColumns extends ConfigEntry[Seq[String]]("sort_columns", Some(Nil))

  case object UniqueKeys extends ConfigEntry[Boolean]("unique_keys", Some(false))

  case object InconsistentDynamicWrite extends ConfigEntry[Boolean]("inconsistent_dynamic_write", Some(false))

  case object WriteSchemaHint extends ConfigEntry[Map[String, YtLogicalType]]("write_schema_hint", Some(Map.empty))

  case object Schema extends ConfigEntry[StructType]("schema")

  case object WriteTransaction extends ConfigEntry[String]("write_transaction")

  case object WriteTypeV3 extends ConfigEntry[Boolean]("write_type_v3", Some(false))

  case object NullTypeAllowed extends ConfigEntry[Boolean]("null_type_allowed", Some(false))

  case object OptimizeFor extends ConfigEntry[String]("optimize_for")

  case object Path extends ConfigEntry[String]("path")

  case object Options extends ConfigEntry[Seq[String]]("options") {

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
