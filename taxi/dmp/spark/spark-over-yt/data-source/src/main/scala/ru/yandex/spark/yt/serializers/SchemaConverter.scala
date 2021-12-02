package ru.yandex.spark.yt.serializers

import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeMapNodeImpl
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType.StructFieldMeta
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode, YTreeStringNode}
import ru.yandex.spark.yt.common.utils.TypeUtils.isTuple
import ru.yandex.spark.yt.wrapper.YtJavaConverters
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
  }

  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType] = None): StructType = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.zipWithIndex.map { case (fieldSchema, index) =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val stringDataType = fieldMap.getOrThrow("type_v3") match {
        case m: YTreeMapNode =>
          m.getOrThrow("type_name").stringValue() match {
            case "optional" => m.getOrThrow("item").stringValue()
            case _ => "yson"
          }
        case s: YTreeStringNode =>
          s.stringValue()
      }
      val metadata = new MetadataBuilder()
      metadata.putString(MetadataFields.ORIGINAL_NAME, originalName)
      metadata.putLong(MetadataFields.KEY_ID, if (fieldMap.containsKey("sort_order")) index else -1)

      structField(fieldName, stringDataType, schemaHint, metadata.build())
    })
  }

  def structField(fieldName: String,
                  stringDataType: String,
                  metadata: Metadata): StructField = {
    StructField(fieldName, sparkType(stringDataType), metadata = metadata)
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
      case s@StructType(fields) if isTuple(s) =>
        val tupleElementTypes = fields.map(element => indexedDataType(element.dataType))
        IndexedDataType.TupleType(tupleElementTypes, s)
      case s@StructType(fields) =>
        IndexedDataType.StructType(
          fields.zipWithIndex.map { case (f, i) =>
            f.name -> StructFieldMeta(i, indexedDataType(f.dataType), isNull = true)
          }.toMap,
          s
        )
      case a@ArrayType(elementType, _) => IndexedDataType.ArrayType(indexedDataType(elementType), a)
      case m@MapType(keyType, valueType, _) => IndexedDataType.MapType(indexedDataType(keyType), indexedDataType(valueType), m)
      case other => IndexedDataType.AtomicType(other)
    }
  }

  def schemaHint(options: Map[String, String]): Option[StructType] = {
    val fields = options.collect { case (key, value) if key.contains("_hint") =>
      val name = key.dropRight("_hint".length)
      val dataType = sparkType(value)
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
      (s"${f.name}_hint", stringType(f.dataType)) +: result
    }.toMap
  }

  def ytLogicalType(sparkType: DataType): YtLogicalType = sparkType match {
    case ByteType => YtLogicalType.Int8
    case ShortType => YtLogicalType.Int16
    case IntegerType => YtLogicalType.Int32
    case LongType => YtLogicalType.Int64
    case StringType => YtLogicalType.String
    case FloatType => YtLogicalType.Float
    case DoubleType => YtLogicalType.Double
    case BooleanType => YtLogicalType.Boolean
    case _: ArrayType => YtLogicalType.Any
    case _: StructType => YtLogicalType.Any
    case _: MapType => YtLogicalType.Any
    case YsonType => YtLogicalType.Any
    case BinaryType => YtLogicalType.String
    case UInt64Type => YtLogicalType.Uint64
  }

  def ytLogicalSchema(sparkSchema: StructType, sortColumns: Seq[String], hint: Map[String, YtLogicalType]): YTreeNode = {
    import scala.collection.JavaConverters._

    def logicalType(field: StructField): YtLogicalType = {
      hint.getOrElse(field.name, ytLogicalType(field.dataType))
    }

    val columns = sortColumns.map { name =>
      val sparkField = sparkSchema(name)
      YTree.builder
        .beginMap
        .key("name").value(name)
        .key("type").value(logicalType(sparkField).name)
        .key("required").value(!sparkField.nullable)
        .key("sort_order").value(ColumnSortOrder.ASCENDING.getName)
        .buildMap
    } ++ sparkSchema.flatMap {
      case field if !sortColumns.contains(field.name) =>
        Some(
          YTree.builder
            .beginMap
            .key("name").value(field.name)
            .key("type").value(logicalType(field).name)
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

  def ytSchema(sparkSchema: StructType, sortColumns: Seq[String], hint: Map[String, YtLogicalType]): YTreeNode = {
    tableSchema(sparkSchema, sortColumns, hint).toYTree
  }

  def tableSchema(sparkSchema: StructType, sortColumns: Seq[String], hint: Map[String, YtLogicalType]): TableSchema = {
    val builder = new TableSchema.Builder()
      .setStrict(true)
      .setUniqueKeys(false)

    def columnType(field: StructField): ColumnValueType = {
      hint.getOrElse(field.name, ytLogicalType(field.dataType)).columnValueType
    }

    sortColumns.foreach { name =>
      val sparkField = sparkSchema(name)
      builder.add(new ColumnSchema(name, columnType(sparkField), ColumnSortOrder.ASCENDING))
    }
    sparkSchema.foreach { field =>
      if (!sortColumns.contains(field.name)) {
        builder.add(new ColumnSchema(field.name, columnType(field)))
      }
    }

    builder.build()
  }

  def sparkType(sType: String): DataType = {
    sType match {
      case name if name.startsWith("a#") =>
        ArrayType(sparkType(name.drop(2)))
      case name if name.startsWith("s#") =>
        val strFields = decode[Seq[(String, String)]](name.drop(2)) match {
          case Right(value) => value
          case Left(error) => throw new IllegalArgumentException(s"Unsupported type: $sType", error)
        }
        StructType(strFields.map { case (name, dt) => StructField(name, sparkType(dt)) })
      case name if name.startsWith("m#") =>
        val (keyType, valueType) = decode[(String, String)](name.drop(2)) match {
          case Right(value) => value
          case Left(error) => throw new IllegalArgumentException(s"Unsupported type: $sType", error)
        }
        MapType(sparkType(keyType), sparkType(valueType))
      case "binary" => BinaryType
      case "yson" => YsonType
      case _ => YtLogicalType.fromName(sType).sparkType
    }
  }

  def stringType(sparkType: DataType): String = {
    sparkType match {
      case BinaryType => "binary"
      case ArrayType(elementType, _) => "a#" + stringType(elementType)
      case StructType(fields) => "s#" + fields.map(f => f.name -> stringType(f.dataType)).asJson.noSpaces
      case MapType(keyType, valueType, _) => "m#" + Seq(keyType, valueType).map(stringType).asJson.noSpaces
      case _ => ytLogicalType(sparkType).name
    }
  }

  def checkSchema(schema: StructType): Unit = {
    schema.foreach { field =>
      if (!YtTable.supportsDataType(field.dataType)) {
        throw new IllegalArgumentException(
          s"YT data source does not support ${field.dataType.simpleString} data type.")
      }
    }
  }
}
