package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import tech.ytsaurus.spyt.serialization.IndexedDataType.StructFieldMeta
import tech.ytsaurus.spyt.common.utils.TypeUtils.{isTuple, isVariant, isVariantOverTuple}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.isNullTypeAllowed
import YtLogicalType.getStructField
import tech.ytsaurus.core.common.Decimal.textToBinary
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeType, serializeTypeV3}
import tech.ytsaurus.core.tables.{ColumnSortOrder, TableSchema}
import tech.ytsaurus.spyt.serialization.IndexedDataType
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeNode, YTreeTextSerializer}

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
    val TAG = "tag"
    val OPTIONAL = "optional"
    val ARROW_SUPPORTED = "arrow_supported"
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
      wrapSparkAttributes(sparkTypeV1(fieldMap.get("type").stringValue()), !requiredValue)
    } else {
      throw new NoSuchElementException("No parsable data type description")
    }
  }

  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType] = None, parsingTypeV3: Boolean = true): StructType = {
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._

    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.zipWithIndex.map { case (fieldSchema, index) =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val metadata = new MetadataBuilder()
      metadata.putString(MetadataFields.ORIGINAL_NAME, originalName)
      metadata.putLong(MetadataFields.KEY_ID, if (fieldMap.containsKey("sort_order")) index else -1)
      val ytType = getAvailableType(fieldMap, parsingTypeV3)
      metadata.putBoolean(MetadataFields.ARROW_SUPPORTED, ytType.arrowSupported)
      structField(fieldName, ytType, schemaHint, metadata.build())
    })
  }

  def prefixKeys(schema: StructType): Seq[String] = {
    keys(schema).takeWhile(_.isDefined).map(_.get)
  }

  def keys(schema: StructType): Seq[Option[String]] = {
    val keyMap = schema
      .fields
      .map(x =>
        if (x.metadata.contains(MetadataFields.KEY_ID))
          (x.metadata.getLong(MetadataFields.KEY_ID), x.metadata.getString(MetadataFields.ORIGINAL_NAME))
        else
          (-1L, x.name)
      )
      .toMap
    val max = if (keyMap.nonEmpty) keyMap.keys.max else -1
    (0L to max).map(keyMap.get)
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

  private def wrapSparkAttributes(inner: YtLogicalType, flag: Boolean,
                                    metadata: Option[Metadata] = None): YtLogicalType = {
    def wrapNullable(ytType: YtLogicalType): YtLogicalType = {
      val optionalO = metadata.flatMap { m =>
        if (m.contains(MetadataFields.OPTIONAL)) Some(m.getBoolean(MetadataFields.OPTIONAL)) else None
      }
      if (optionalO.getOrElse(flag)) YtLogicalType.Optional(ytType) else ytType
    }

    def wrapTagged(ytType: YtLogicalType): YtLogicalType = {
      val tagO = metadata.flatMap { m =>
        if (m.contains(MetadataFields.TAG)) Some(m.getString(MetadataFields.TAG)) else None
      }
      tagO.map(t => YtLogicalType.Tagged(ytType, t)).getOrElse(ytType)
    }

    wrapNullable(wrapTagged(inner))
  }

  private def ytLogicalTypeV3Variant(struct: StructType): YtLogicalType = {
    if (isVariantOverTuple(struct)) {
      YtLogicalType.VariantOverTuple {
        struct.fields.map(tF =>
          (wrapSparkAttributes(ytLogicalTypeV3(tF.dataType), tF.nullable, Some(tF.metadata)), tF.metadata))
      }
    } else {
      YtLogicalType.VariantOverStruct {
        struct.fields.map(sf => (sf.name.drop(2),
          wrapSparkAttributes(ytLogicalTypeV3(sf.dataType), sf.nullable, Some(sf.metadata)), sf.metadata))
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
      YtLogicalType.Array(wrapSparkAttributes(ytLogicalTypeV3(aT.elementType), aT.containsNull))
    case sT: StructType if isTuple(sT) =>
      YtLogicalType.Tuple {
        sT.fields.map(tF =>
          (wrapSparkAttributes(ytLogicalTypeV3(tF.dataType), tF.nullable, Some(tF.metadata)), tF.metadata))
      }
    case sT: StructType if isVariant(sT) => ytLogicalTypeV3Variant(sT)
    case sT: StructType =>
      YtLogicalType.Struct {
        sT.fields.map(sf => (sf.name,
          wrapSparkAttributes(ytLogicalTypeV3(sf.dataType), sf.nullable, Some(sf.metadata)), sf.metadata))
      }
    case mT: MapType =>
      YtLogicalType.Dict(
        ytLogicalTypeV3(mT.keyType),
        wrapSparkAttributes(ytLogicalTypeV3(mT.valueType), mT.valueContainsNull))
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
      val fieldType = hint.getOrElse(field.name,
        wrapSparkAttributes(ytLogicalTypeV3(field.dataType), field.nullable, Some(field.metadata)))
      if (typeV3Format) {
        builder
          .key("type_v3").value(serializeTypeV3(fieldType))
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

  def tableSchema(sparkSchema: StructType, sortOption: SortOption = Unordered,
                  hint: Map[String, YtLogicalType] = Map.empty, typeV3Format: Boolean = false): TableSchema = {
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

  def checkSchema(schema: StructType, options: Map[String, String]): Unit = {
    schema.foreach { field =>
      if (!YtTable.supportsDataType(field.dataType)) {
        throw new IllegalArgumentException(
          s"YT data source does not support ${field.dataType.simpleString} data type(column `${field.name}`).")
      }
      if (field.dataType == NullType && !isNullTypeAllowed(options)) {
        throw new IllegalArgumentException(
          s"Writing null data type(column `${field.name}`) is not allowed now, because of uselessness. " +
            s"If you are sure you can enable `null_type_allowed` option.")
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

  def decimalToBinary(ytType: Option[TiType], decimalType: DecimalType, decimalValue: Decimal): Array[Byte] = {
    val (precision, scale) = if (ytType.exists(_.isDecimal)) {
      val decimalHint = ytType.get.asDecimal()
      (decimalHint.getPrecision, decimalHint.getScale)
    } else {
      val dT = if (decimalType.precision > 35) applyYtLimitToSparkDecimal(decimalType) else decimalType
      (dT.precision, dT.scale)
    }
    val result = decimalValue.changePrecision(precision, scale)
    if (!result) {
      throw new IllegalArgumentException("Decimal value couldn't fit in yt limitations (precision <= 35)")
    }
    val binary = textToBinary(decimalValue.toBigDecimal.bigDecimal.toPlainString, precision, scale)
    binary
  }
}
