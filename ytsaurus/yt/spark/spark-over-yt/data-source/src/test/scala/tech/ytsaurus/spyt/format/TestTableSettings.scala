package tech.ytsaurus.spyt.format

import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Unordered}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.spyt.serializers.{SchemaConverter, YtLogicalType}
import tech.ytsaurus.ysontree.YTreeNode

case class TestTableSettings(schema: StructType,
                             isDynamic: Boolean = false,
                             sortOption: SortOption = Unordered,
                             writeSchemaHint: Map[String, YtLogicalType] = Map.empty,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortOption, writeSchemaHint)

  override def optionsAny: Map[String, Any] = otherOptions + ("dynamic" -> isDynamic)
}