package ru.yandex.spark.yt.format

import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.serializers.SchemaConverter.{SortOption, Unordered}
import ru.yandex.spark.yt.serializers.{SchemaConverter, YtLogicalType}
import ru.yandex.spark.yt.wrapper.table.YtTableSettings
import tech.ytsaurus.ysontree.YTreeNode

case class TestTableSettings(schema: StructType,
                             isDynamic: Boolean = false,
                             sortOption: SortOption = Unordered,
                             writeSchemaHint: Map[String, YtLogicalType] = Map.empty,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortOption, writeSchemaHint)

  override def optionsAny: Map[String, Any] = otherOptions + ("dynamic" -> isDynamic)
}