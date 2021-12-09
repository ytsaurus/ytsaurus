package ru.yandex.spark.yt.serializers

import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType.StructFieldMeta
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode, YTreeStringNode}
import ru.yandex.spark.yt.common.utils.TypeUtils.{isTuple, isVariant}
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
    val TAG = "tag"
  }

  import scala.collection.JavaConverters._

  private def getAvailableType(fieldMap: java.util.Map[String, YTreeNode], parsingTypeV3: Boolean): YtLogicalType = {
    if (fieldMap.containsKey("type_v3") && parsingTypeV3) {
      sparkTypeV3(fieldMap.get("type_v3"))
    } else if (fieldMap.containsKey("type")) {
      sparkTypeV1(fieldMap.get("type").stringValue())
    } else {
      throw new NoSuchElementException("No parsable data type description")
    }
  }

  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType] = None, parsingTypeV3: Boolean = true): StructType = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._

    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.zipWithIndex.map { case (fieldSchema, index) =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val metadata = new MetadataBuilder()
      metadata.putString(MetadataFields.ORIGINAL_NAME, originalName)
      metadata.putLong(MetadataFields.KEY_ID, if (fieldMap.containsKey("sort_order")) index else -1)

      structField(fieldName, getAvailableType(fieldMap, parsingTypeV3), schemaHint, metadata.build())
    })
  }

  def structField(fieldName: String,
                  ytType: YtLogicalType,
                  metadata: Metadata): StructField = {
    StructField(fieldName, ytType.sparkType, metadata = metadata, nullable = ytType.nullable)
  }

  def structField(fieldName: String,
                  ytType: => YtLogicalType,
                  schemaHint: Option[StructType] = None,
                  metadata: Metadata = Metadata.empty): StructField = {
    schemaHint
      .flatMap(_.find(_.name == fieldName.toLowerCase())
        .map(_.copy(name = fieldName, metadata = metadata))
      )
      .getOrElse(structField(fieldName, ytType, metadata))
  }

  def indexedDataType(dataType: DataType): IndexedDataType = {
    dataType match {
      case s@StructType(fields) if isTuple(s) =>
        val tupleElementTypes = fields.map(element => indexedDataType(element.dataType))
        IndexedDataType.TupleType(tupleElementTypes, s)
      case s@StructType(fields) if isVariant(s) =>
        val transformedStruct = StructType(fields.map(f => f.copy(name = f.name.substring(2))))
        indexedDataType(transformedStruct) match {
          case t: IndexedDataType.TupleType => IndexedDataType.VariantOverTupleType(t)
          case s: IndexedDataType.StructType => IndexedDataType.VariantOverStructType(s)
        }
      case s@StructType(fields) =>
        IndexedDataType.StructType(
          fields.zipWithIndex.map { case (f, i) =>
            f.name -> StructFieldMeta(i, indexedDataType(f.dataType), isNull = f.nullable)
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
      StructField(name, sparkType(value))
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
        .key("type").value(logicalType(sparkField).alias.name)
        .key("required").value(!sparkField.nullable)
        .key("sort_order").value(ColumnSortOrder.ASCENDING.getName)
        .buildMap
    } ++ sparkSchema.flatMap {
      case field if !sortColumns.contains(field.name) =>
        Some(
          YTree.builder
            .beginMap
            .key("name").value(field.name)
            .key("type").value(logicalType(field).alias.name)
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

  def sparkTypeV1(sType: String): YtLogicalType = {
    sType match {
      case name if name.startsWith("a#") =>
        YtLogicalType.Array(sparkTypeV1(name.drop(2)))
      case name if name.startsWith("s#") =>
        val strFields = decode[Seq[(String, String)]](name.drop(2)) match {
          case Right(value) => value
          case Left(error) => throw new IllegalArgumentException(s"Unsupported type: $sType", error)
        }
        YtLogicalType.Struct(strFields.map { case (name, dt) => (name, sparkTypeV1(dt)) })
      case name if name.startsWith("m#") =>
        val (keyType, valueType) = decode[(String, String)](name.drop(2)) match {
          case Right(value) => value
          case Left(error) => throw new IllegalArgumentException(s"Unsupported type: $sType", error)
        }
        YtLogicalType.Dict(sparkTypeV1(keyType), sparkTypeV1(valueType))
      case _ => YtLogicalType.fromName(sType)
    }
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
      case _ => ytLogicalType(sparkType).alias.name
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

  private def sparkTypeV3(node: YTreeNode): YtLogicalType = {
    def parseMembers(m: YTreeMapNode): Seq[(String, YtLogicalType)] = {
      m.getOrThrow("members").asList().asScala
        .map(member => (
          member.mapNode().getOrThrow("name").stringValue(),
          sparkTypeV3(member.mapNode().getOrThrow("type")))
        )
    }
    def parseElements(m: YTreeMapNode): Seq[YtLogicalType] = {
      m.getOrThrow("elements").asList().asScala.map {
        element => sparkTypeV3(element.mapNode().getOrThrow("type"))
      }
    }
    node match {
      case m: YTreeMapNode =>
        val alias = YtLogicalType.fromCompositeName(m.getOrThrow("type_name").stringValue())
        alias match {
          case YtLogicalType.Optional =>
            YtLogicalType.Optional(
              sparkTypeV3(m.getOrThrow("item")))
          case YtLogicalType.Dict =>
            YtLogicalType.Dict(
              sparkTypeV3(m.getOrThrow("key")),
              sparkTypeV3(m.getOrThrow("value")))
          case YtLogicalType.Array =>
            YtLogicalType.Array(sparkTypeV3(m.getOrThrow("item")))
          case YtLogicalType.Struct =>
            YtLogicalType.Struct(parseMembers(m))
          case YtLogicalType.Tuple =>
            YtLogicalType.Tuple(parseElements(m))
          case YtLogicalType.Tagged =>
            YtLogicalType.Tagged(
              sparkTypeV3(m.getOrThrow("item")), m.getOrThrow("tag").stringValue()
            )
          case YtLogicalType.Variant =>
            if (m.containsKey("members")) {
              YtLogicalType.VariantOverStruct(parseMembers(m))
            } else if (m.containsKey("elements")) {
              YtLogicalType.VariantOverTuple(parseElements(m))
            } else {
              throw new NoSuchElementException("Incorrect variant format")
            }
        }
      case s: YTreeStringNode =>
        YtLogicalType.fromName(s.stringValue())
    }
  }
}
