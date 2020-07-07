package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType.StructFieldMeta
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.fs.conf.{ConfigTypeConverter, YtLogicalType}
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

object SchemaConverter {
  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType] = None): StructType = {
    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.map { fieldSchema =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val stringDataType = fieldMap.getOrThrow("type").stringValue()
      val metadata = new MetadataBuilder()
      metadata.putString("original_name", originalName)
      structField(fieldName, stringDataType, schemaHint, metadata.build())
    })
  }

  def ytType(sparkType: DataType): ColumnValueType = {
    sparkType match {
      case ShortType => ColumnValueType.INT64
      case StringType => ColumnValueType.STRING
      case IntegerType => ColumnValueType.INT64
      case LongType => ColumnValueType.INT64
      case DoubleType => ColumnValueType.DOUBLE
      case BooleanType => ColumnValueType.BOOLEAN
      case _: ArrayType => ColumnValueType.ANY
      case _: StructType => ColumnValueType.ANY
      case _: MapType => ColumnValueType.ANY
      case BinaryType => ColumnValueType.ANY
    }
  }

  def structField(fieldName: String,
                  stringDataType: String,
                  metadata: Metadata): StructField = {
    StructField(fieldName, ConfigTypeConverter.sparkType(stringDataType), metadata = metadata)
  }

  def structField(fieldName: String,
                  stringDataType: String,
                  schemaHint: Option[StructType],
                  metadata: Metadata): StructField = {
    schemaHint
      .flatMap(_.find(_.name == fieldName.toLowerCase())
        .map(_.copy(name = fieldName, metadata = metadata))
      )
      .getOrElse(structField(fieldName, stringDataType, metadata))
  }

  def indexedDataType(dataType: DataType): IndexedDataType = {
    dataType match {
      case s@StructType(fields) => IndexedDataType.StructType(
        fields.zipWithIndex.map { case (f, i) =>
          f.name -> StructFieldMeta(i, indexedDataType(f.dataType), isNull = true)
        }.toMap,
        s
      )
      case a@ArrayType(elementType, _) => IndexedDataType.ArrayType(indexedDataType(elementType), a)
      case m@MapType(StringType, valueType, _) => IndexedDataType.MapType(indexedDataType(valueType), m)
      case other => IndexedDataType.AtomicType(other)
    }
  }

  def schemaHint(options: Map[String, String]): Option[StructType] = {
    val fields = options.collect { case (key, value) if key.contains("_hint") =>
      val name = key.dropRight("_hint".length)
      val dataType = ConfigTypeConverter.sparkType(value)
      StructField(name, dataType)
    }

    if (fields.nonEmpty) {
      Some(StructType(fields.toSeq))
    } else {
      None
    }
  }

  def serializeSchemaHint(schema: StructType): Map[String, String] = {
    schema.foldLeft(Seq.empty[(String, String)]) { case (result, f) =>
      (s"${f.name}_hint", ConfigTypeConverter.stringType(f.dataType)) +: result
    }.toMap
  }

  def ytLogicalType(sparkType: DataType): YtLogicalType = sparkType match {
    case ShortType => YtLogicalType.Int8
    case StringType => YtLogicalType.String
    case IntegerType => YtLogicalType.Int32
    case LongType => YtLogicalType.Int64
    case DoubleType => YtLogicalType.Double
    case BooleanType => YtLogicalType.Boolean
    case _: ArrayType => YtLogicalType.Any
    case _: StructType => YtLogicalType.Any
    case _: MapType => YtLogicalType.Any
    case BinaryType => YtLogicalType.Any
  }

  def ytLogicalSchema(sparkSchema: StructType, sortColumns: Seq[String]): YTreeNode = {
    import scala.collection.JavaConverters._

    val columns = sortColumns.map{ name =>
      val sparkField = sparkSchema(name)
      YTree.builder
        .beginMap
        .key("name").value(name)
        .key("type").value(ytLogicalType(sparkField.dataType).name)
        .key("required").value(!sparkField.nullable)
        .key("sort_order").value(ColumnSortOrder.ASCENDING.getName)
        .buildMap
    } ++ sparkSchema.flatMap{
      case field if !sortColumns.contains(field.name) =>
        Some(
          YTree.builder
            .beginMap
            .key("name").value(field.name)
            .key("type").value(ytLogicalType(field.dataType).name)
            .key("required").value(!field.nullable)
            .buildMap
        )
      case _ => None
    }

    YTree.builder
      .beginAttributes
      .key("strict").value(true)
      .key("unique_keys").value(false)
      .endAttributes
      .value(columns.asJava)
      .build
  }

  def ytSchema(sparkSchema: StructType, sortColumns: Seq[String]): YTreeNode = {
    tableSchema(sparkSchema, sortColumns).toYTree
  }

  def tableSchema(sparkSchema: StructType, sortColumns: Seq[String]): TableSchema = {
    val builder = new TableSchema.Builder()
      .setStrict(true)
      .setUniqueKeys(false)

    sortColumns.foreach { name =>
      val sparkField = sparkSchema(name)
      builder.add(new ColumnSchema(name, ytType(sparkField.dataType), ColumnSortOrder.ASCENDING))
    }
    sparkSchema.foreach { field =>
      if (!sortColumns.contains(field.name)) {
        builder.add(new ColumnSchema(field.name, ytType(field.dataType)))
      }
    }

    builder.build()
  }
}
