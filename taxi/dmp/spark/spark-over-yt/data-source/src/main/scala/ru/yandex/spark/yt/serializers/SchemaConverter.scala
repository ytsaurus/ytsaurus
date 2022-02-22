package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.IndexedDataType.StructFieldMeta
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.common.utils.TypeUtils.{isTuple, isVariant, isVariantOverTuple}
import ru.yandex.spark.yt.serializers.YtLogicalType.getStructField
import ru.yandex.spark.yt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeType, serializeTypeV3}
import ru.yandex.yt.ytclient.tables.{ColumnSortOrder, TableSchema}

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
    val TAG = "tag"
  }

  sealed trait SortOption {
    def keys: Seq[String]

    def uniqueKeys: Boolean
  }
  case class Sorted(keys: Seq[String], uniqueKeys: Boolean) extends SortOption {
    if (keys.isEmpty) throw new IllegalArgumentException("Sort columns can't be empty in sorted table")
  }
  case object Unordered extends SortOption {
    override def keys: Seq[String] = Nil

    override def uniqueKeys: Boolean = false
  }

  private def getAvailableType(fieldMap: java.util.Map[String, YTreeNode], parsingTypeV3: Boolean): YtLogicalType = {
    if (fieldMap.containsKey("type_v3") && parsingTypeV3) {
      deserializeTypeV3(fieldMap.get("type_v3"))
    } else if (fieldMap.containsKey("type")) {
      val requiredAttribute = fieldMap.get("required")
      val requiredValue = if (requiredAttribute != null) requiredAttribute.boolValue() else false
      wrapNullable(sparkTypeV1(fieldMap.get("type").stringValue()), !requiredValue)
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
    getStructField(fieldName, ytType, metadata)
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
      StructField(name, stringToSparkType(value))
    }

    if (fields.nonEmpty) {
      Some(StructType(fields.toSeq))
    } else {
      None
    }
  }

  def serializeSchemaHint(schema: StructType): Map[String, String] = {
    schema.foldLeft(Seq.empty[(String, String)]) { case (result, f) =>
      (s"${f.name}_hint", sparkTypeToString(f.dataType)) +: result
    }.toMap
  }

  private def wrapNullable(inner: YtLogicalType, flag: Boolean): YtLogicalType = {
    if (flag) {
      YtLogicalType.Optional(inner)
    } else {
      inner
    }
  }

  def ytLogicalTypeV3Variant(struct: StructType): YtLogicalType = {
    if (isVariantOverTuple(struct)) {
      YtLogicalType.VariantOverTuple {
        struct.fields.map(tF => wrapNullable(ytLogicalTypeV3(tF.dataType), tF.nullable))
      }
    } else {
      YtLogicalType.VariantOverStruct {
        struct.fields.map(sf => (sf.name.drop(2), wrapNullable(ytLogicalTypeV3(sf.dataType), sf.nullable)))
      }
    }
  }

  def ytLogicalTypeV3(sparkType: DataType): YtLogicalType = sparkType match {
    case NullType => YtLogicalType.Null
    case ByteType => YtLogicalType.Int8
    case ShortType => YtLogicalType.Int16
    case IntegerType => YtLogicalType.Int32
    case LongType => YtLogicalType.Int64
    case StringType => YtLogicalType.String
    case FloatType => YtLogicalType.Float
    case DoubleType => YtLogicalType.Double
    case BooleanType => YtLogicalType.Boolean
    case d: DecimalType =>
      val dT = if (d.precision > 35) applyYtLimitToSparkDecimal(d) else d
      YtLogicalType.Decimal(dT.precision, dT.scale)
    case aT: ArrayType =>
      YtLogicalType.Array(wrapNullable(ytLogicalTypeV3(aT.elementType), aT.containsNull))
    case sT: StructType if isTuple(sT) =>
      YtLogicalType.Tuple {
        sT.fields.map(tF => wrapNullable(ytLogicalTypeV3(tF.dataType), tF.nullable))
      }
    case sT: StructType if isVariant(sT) => ytLogicalTypeV3Variant(sT)
    case sT: StructType =>
      YtLogicalType.Struct {
        sT.fields.map(sf => (sf.name, wrapNullable(ytLogicalTypeV3(sf.dataType), sf.nullable)))
      }
    case mT: MapType =>
      YtLogicalType.Dict(
        ytLogicalTypeV3(mT.keyType),
        wrapNullable(ytLogicalTypeV3(mT.valueType), mT.valueContainsNull))
    case YsonType => YtLogicalType.Any
    case BinaryType => YtLogicalType.Binary
    case DateType => YtLogicalType.Date
    case TimestampType => YtLogicalType.Datetime
    case UInt64Type => YtLogicalType.Uint64
  }

  private def ytLogicalSchemaImpl(sparkSchema: StructType,
                                  sortOption: SortOption,
                                  hint: Map[String, YtLogicalType],
                                  typeV3Format: Boolean = false,
                                  isTableSchema: Boolean = false): YTreeNode = {
    import scala.collection.JavaConverters._

    def serializeColumn(field: StructField, sort: Boolean): YTreeNode = {
      val builder = YTree.builder
        .beginMap
        .key("name").value(field.name)
      val fieldType = hint.getOrElse(field.name, ytLogicalTypeV3(field.dataType))
      if (typeV3Format) {
        builder
          .key("type_v3").value(serializeTypeV3(wrapNullable(fieldType, field.nullable)))
      } else {
        builder
          .key("type").value(serializeType(fieldType, isTableSchema))
          .key("required").value(false)
      }
      if (sort) builder.key("sort_order").value(ColumnSortOrder.ASCENDING.getName)
      builder.buildMap
    }

    val sortColumnsSet = sortOption.keys.toSet
    val sortedFields =
      sortOption.keys
        .map(sparkSchema.apply)
        .map(f => serializeColumn(f, sort = true)
        ) ++ sparkSchema
        .filter(f => !sortColumnsSet.contains(f.name))
        .map(f => serializeColumn(f, sort = false))

    YTree.builder
      .beginAttributes
      .key("strict").value(true)
      .key("unique_keys").value(sortOption.uniqueKeys)
      .endAttributes
      .value(sortedFields.asJava)
      .build
  }

  def ytLogicalSchema(sparkSchema: StructType, sortOption: SortOption,
                      hint: Map[String, YtLogicalType], typeV3Format: Boolean = false): YTreeNode = {
    ytLogicalSchemaImpl(sparkSchema, sortOption, hint, typeV3Format)
  }

  def tableSchema(sparkSchema: StructType, sortOption: SortOption,
                  hint: Map[String, YtLogicalType], typeV3Format: Boolean = false): TableSchema = {
    TableSchema.fromYTree(ytLogicalSchemaImpl(sparkSchema, sortOption,
      hint, typeV3Format, isTableSchema = true))
  }

  def sparkTypeV1(sType: String): YtLogicalType = {
    YtLogicalType.fromName(sType)
  }

  def stringToSparkType(sType: String): DataType = {
    deserializeTypeV3(YTreeTextSerializer.deserialize(sType)).sparkType
  }

  def sparkTypeToString(sparkType: DataType): String = {
    YTreeTextSerializer.serialize(serializeTypeV3(ytLogicalTypeV3(sparkType), innerForm = true))
  }

  def checkSchema(schema: StructType): Unit = {
    schema.foreach { field =>
      if (!YtTable.supportsDataType(field.dataType)) {
        throw new IllegalArgumentException(
          s"YT data source does not support ${field.dataType.simpleString} data type.")
      }
    }
  }

  def applyYtLimitToSparkDecimal(dataType: DecimalType): DecimalType = {
    val precision = dataType.precision
    val scale = dataType.scale
    val overflow = precision - 35
    if (overflow > 0) {
      if (scale < overflow) {
        throw new IllegalArgumentException("Precision and scale couldn't be reduced for satisfying yt limitations")
      } else {
        DecimalType(precision - overflow, scale - overflow)
      }
    } else {
      dataType
    }
  }
}
