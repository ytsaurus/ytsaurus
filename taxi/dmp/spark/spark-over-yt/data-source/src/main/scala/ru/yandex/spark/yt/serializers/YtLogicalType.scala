package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import ru.yandex.type_info.StructType.Member
import ru.yandex.type_info.{TiType, TypeName}
import ru.yandex.yt.ytclient.tables.ColumnValueType

case class DataTypeHolder(dataType: DataType, nullable: Boolean = false)

sealed trait YtLogicalType {
  def value: Int = columnValueType.getValue

  def columnValueType: ColumnValueType

  def getNameV3(inner: Boolean): String = {
    if (inner) {
      alias.name
    } else {
      tiType.getTypeName.getWireName
    }
  }

  def getName(isColumnType: Boolean): String = {
    if (isColumnType) {
      columnValueType.getName
    } else {
      alias.name
    }
  }

  def tiType: TiType

  def sparkType: DataType

  def nullable: Boolean = false

  def dataTypeHolder: DataTypeHolder = DataTypeHolder(sparkType, nullable)

  def alias: YtLogicalTypeAlias
}

sealed trait YtLogicalTypeAlias {
  def name: String = aliases.head
  def aliases: Seq[String]
}

sealed abstract class AtomicYtLogicalType(name: String,
                                          override val value: Int,
                                          val columnValueType: ColumnValueType,
                                          val tiType: TiType,
                                          val sparkType: DataType,
                                          otherAliases: Seq[String] = Seq.empty)
  extends YtLogicalType with YtLogicalTypeAlias {

  override def alias: YtLogicalTypeAlias = this
  override def aliases: Seq[String] = name +: otherAliases
}

sealed trait CompositeYtLogicalType extends YtLogicalType {
  override def columnValueType: ColumnValueType = ColumnValueType.ANY

  override def getName(isColumnType: Boolean): String = columnValueType.getName
}

sealed abstract class CompositeYtLogicalTypeAlias(name: String,
                                                  otherAliases: Seq[String] = Seq.empty) extends YtLogicalTypeAlias {
  override def aliases: Seq[String] = name +: otherAliases
}

object YtLogicalType {
  case object Null extends AtomicYtLogicalType("null", 0x02, ColumnValueType.NULL, TiType.nullType(), NullType)

  case object Int64 extends AtomicYtLogicalType("int64", 0x03, ColumnValueType.INT64, TiType.int64(), LongType)
  case object Uint64 extends AtomicYtLogicalType("uint64", 0x04, ColumnValueType.UINT64, TiType.uint64(), UInt64Type)
  case object Float extends AtomicYtLogicalType("float", 0x05, ColumnValueType.DOUBLE, TiType.floatType(), FloatType)
  case object Double extends AtomicYtLogicalType("double", 0x05, ColumnValueType.DOUBLE, TiType.doubleType(), DoubleType)
  case object Boolean extends AtomicYtLogicalType("boolean", 0x06, ColumnValueType.BOOLEAN, TiType.bool(), BooleanType, Seq("bool"))

  case object String extends AtomicYtLogicalType("string", 0x10, ColumnValueType.STRING, TiType.string(), StringType)
  case object Binary extends AtomicYtLogicalType("binary", 0x10, ColumnValueType.STRING, TiType.string(), BinaryType) {
    override def getName(isColumnType: Boolean): String = columnValueType.getName

    override def getNameV3(inner: Boolean): String = {
      if (inner) alias.name else "string"
    }
  }
  case object Any extends AtomicYtLogicalType("any", 0x11, ColumnValueType.ANY, TiType.yson(), YsonType, Seq("yson")) {
    override def nullable: Boolean = true
  }

  case object Int8 extends AtomicYtLogicalType("int8", 0x1000, ColumnValueType.INT64, TiType.int8(), ByteType)
  case object Uint8 extends AtomicYtLogicalType("uint8", 0x1001, ColumnValueType.INT64, TiType.uint8(), ShortType)

  case object Int16 extends AtomicYtLogicalType("int16", 0x1003, ColumnValueType.INT64, TiType.int16(), ShortType)
  case object Uint16 extends AtomicYtLogicalType("uint16", 0x1004, ColumnValueType.INT64, TiType.uint16(), IntegerType)

  case object Int32 extends AtomicYtLogicalType("int32", 0x1005, ColumnValueType.INT64, TiType.int32(), IntegerType)
  case object Uint32 extends AtomicYtLogicalType("uint32", 0x1006, ColumnValueType.INT64, TiType.uint32(), LongType)

  case object Utf8 extends AtomicYtLogicalType("utf8", 0x1007, ColumnValueType.STRING, TiType.utf8(), StringType)

  case object Date extends AtomicYtLogicalType("date", 0x1008, ColumnValueType.INT64, TiType.date(), DateType)
  case object Datetime extends AtomicYtLogicalType("datetime", 0x1009, ColumnValueType.INT64, TiType.datetime(), TimestampType)
  case object Timestamp extends AtomicYtLogicalType("timestamp", 0x100a, ColumnValueType.INT64, TiType.timestamp(), LongType)
  case object Interval extends AtomicYtLogicalType("interval", 0x100b, ColumnValueType.INT64, TiType.interval(), LongType)

  case object Void extends AtomicYtLogicalType("void", 0x100c, ColumnValueType.NULL, TiType.voidType(), NullType) //?

  case class Decimal(precision: Int, scale: Int) extends CompositeYtLogicalType {
    override def sparkType: DataType = DecimalType(precision, scale)

    override def alias: CompositeYtLogicalTypeAlias = Decimal

    override def tiType: TiType = TiType.decimal(precision, scale)
  }

  case object Decimal extends CompositeYtLogicalTypeAlias("decimal")

  case class Optional(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def value: Int = inner.value

    override def columnValueType: ColumnValueType = inner.columnValueType

    override def tiType: TiType = TiType.optional(inner.tiType)

    override def sparkType: DataType = inner.sparkType

    override def nullable: Boolean = true

    override def alias: CompositeYtLogicalTypeAlias = Optional
  }

  case object Optional extends CompositeYtLogicalTypeAlias(TypeName.Optional.getWireName)

  case class Dict(dictKey: YtLogicalType, dictValue: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: DataType = MapType(dictKey.sparkType, dictValue.sparkType, dictValue.nullable)

    override def tiType: TiType = TiType.dict(dictKey.tiType, dictValue.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Dict
  }

  case object Dict extends CompositeYtLogicalTypeAlias(TypeName.Dict.getWireName)

  case class Array(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: DataType = ArrayType(inner.sparkType, inner.nullable)

    override def tiType: TiType = TiType.list(inner.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Array
  }

  case object Array extends CompositeYtLogicalTypeAlias(TypeName.List.getWireName)

  case class Struct(fields: Seq[(String, YtLogicalType)]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields
      .map { case (name, ytType) => getStructField(name, ytType) })

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.struct(
      fields.map{ case (name, ytType) => new Member(name, ytType.tiType)}.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Struct
  }

  case object Struct extends CompositeYtLogicalTypeAlias(TypeName.Struct.getWireName)

  case class Tuple(elements: Seq[YtLogicalType]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(elements.zipWithIndex
      .map { case (ytType, index) => getStructField(s"_${1 + index}", ytType) })

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.tuple(
      elements.map(e => e.tiType).asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Tuple
  }

  case object Tuple extends CompositeYtLogicalTypeAlias(TypeName.Tuple.getWireName)

  case class Tagged(inner: YtLogicalType, tag: String) extends CompositeYtLogicalType {
    override def sparkType: DataType = inner.sparkType

    override def tiType: TiType = TiType.tagged(inner.tiType, tag)

    override def alias: CompositeYtLogicalTypeAlias = Tagged
  }

  case object Tagged extends CompositeYtLogicalTypeAlias(TypeName.Tagged.getWireName)

  case class VariantOverStruct(fields: Seq[(String, YtLogicalType)]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields
      .map { case (name, ytType) => getStructField(s"_v$name", ytType) })

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.variantOverStruct(
      fields.map{ case (name, ytType) => new Member(name, ytType.tiType)}.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case class VariantOverTuple(fields: Seq[YtLogicalType]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields.zipWithIndex
      .map { case (ytType, index) => getStructField(s"_v_${1 + index}", ytType) })

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.variantOverTuple(
      fields.map(e => e.tiType).asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case object Variant extends CompositeYtLogicalTypeAlias(TypeName.Variant.getWireName)

  private lazy val atomicTypes = Seq(Null, Int64, Uint64, Float, Double, Boolean, String, Binary, Any,
    Int8, Uint8, Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Void)

  private lazy val compositeTypes = Seq(Optional, Dict, Array, Struct, Tuple,
    Tagged, Variant, Decimal)

  def fromName(name: String): YtLogicalType = {
    findOrThrow(name, atomicTypes)
  }

  def fromCompositeName(name: String): YtLogicalTypeAlias = {
    findOrThrow(name, compositeTypes)
  }

  private def findOrThrow[T <: YtLogicalTypeAlias](name: String, types: Seq[T]): T = {
    types.find(_.aliases.contains(name))
      .getOrElse(throw new IllegalArgumentException(s"Unknown logical yt type: $name"))
  }

  def getStructField(name: String, ytType: YtLogicalType, metadata: Metadata = Metadata.empty): StructField = {
    val metadataBuilder = new MetadataBuilder
    metadataBuilder.withMetadata(metadata)
    addInnerMetadata(metadataBuilder, ytType)
    StructField(
      name,
      ytType.sparkType,
      ytType.nullable,
      metadataBuilder.build()
    )
  }

  private def addInnerMetadata(metadataBuilder: MetadataBuilder, ytType: YtLogicalType): Unit = {
    ytType match {
      case t: Tagged => metadataBuilder.putString("tag", t.tag)
      case _ =>
    }
  }
}
