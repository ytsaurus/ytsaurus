package ru.yandex.spark.yt.format

import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.serializers.{SchemaConverter, YtLogicalType}
import ru.yandex.spark.yt.wrapper.table.YtTableSettings

case class TestTableSettings(schema: StructType,
                             isDynamic: Boolean = false,
                             sortColumns: Seq[String] = Nil,
                             writeSchemaHint: Map[String, YtLogicalType] = Map.empty,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def ytSchema: YTreeNode = SchemaConverter.ytLogicalSchema(schema, sortColumns, writeSchemaHint)

  override def optionsAny: Map[String, Any] = otherOptions + ("dynamic" -> isDynamic)
}