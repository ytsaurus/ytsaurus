package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import ru.yandex.yt.ytclient.tables.ColumnValueType

case class DataTypeHolder(dataType: DataType, nullable: Boolean = false)

sealed trait YtLogicalType {
  def value: Int = columnValueType.getValue

  def columnValueType: ColumnValueType

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
                                          val sparkType: DataType,
                                          otherAliases: Seq[String] = Seq.empty)
  extends YtLogicalType with YtLogicalTypeAlias {

  override def alias: YtLogicalTypeAlias = this
  override def aliases: Seq[String] = name +: otherAliases
}

sealed trait CompositeYtLogicalType extends YtLogicalType {
  override def columnValueType: ColumnValueType = ColumnValueType.COMPOSITE
}

sealed abstract class CompositeYtLogicalTypeAlias(name: String,
                                                  otherAliases: Seq[String] = Seq.empty) extends YtLogicalTypeAlias {
  override def aliases: Seq[String] = name +: otherAliases
}

object YtLogicalType {
  case object Null extends AtomicYtLogicalType("null", 0x02, ColumnValueType.NULL, NullType)

  case object Int64 extends AtomicYtLogicalType("int64", 0x03, ColumnValueType.INT64, LongType)
  case object Uint64 extends AtomicYtLogicalType("uint64", 0x04, ColumnValueType.UINT64, UInt64Type)
  case object Float extends AtomicYtLogicalType("float", 0x05, ColumnValueType.DOUBLE, FloatType)
  case object Double extends AtomicYtLogicalType("double", 0x05, ColumnValueType.DOUBLE, DoubleType)
  case object Boolean extends AtomicYtLogicalType("boolean", 0x06, ColumnValueType.BOOLEAN, BooleanType, Seq("bool"))

  case object String extends AtomicYtLogicalType("string", 0x10, ColumnValueType.STRING, StringType)
  case object Any extends AtomicYtLogicalType("any", 0x11, ColumnValueType.ANY, YsonType, Seq("yson")) {
    override def nullable: Boolean = true
  }

  case object Int8 extends AtomicYtLogicalType("int8", 0x1000, ColumnValueType.INT64, ByteType)
  case object Uint8 extends AtomicYtLogicalType("uint8", 0x1001, ColumnValueType.INT64, ShortType)

  case object Int16 extends AtomicYtLogicalType("int16", 0x1003, ColumnValueType.INT64, ShortType)
  case object Uint16 extends AtomicYtLogicalType("uint16", 0x1004, ColumnValueType.INT64, IntegerType)

  case object Int32 extends AtomicYtLogicalType("int32", 0x1005, ColumnValueType.INT64, IntegerType)
  case object Uint32 extends AtomicYtLogicalType("uint32", 0x1006, ColumnValueType.INT64, LongType)

  case object Utf8 extends AtomicYtLogicalType("utf8", 0x1007, ColumnValueType.STRING, StringType)

  case object Date extends AtomicYtLogicalType("date", 0x1008, ColumnValueType.INT64, DateType)
  case object Datetime extends AtomicYtLogicalType("datetime", 0x1009, ColumnValueType.INT64, TimestampType)
  case object Timestamp extends AtomicYtLogicalType("timestamp", 0x100a, ColumnValueType.INT64, LongType)
  case object Interval extends AtomicYtLogicalType("interval", 0x100b, ColumnValueType.INT64, LongType)

  case object Void extends AtomicYtLogicalType("void", 0x100c, ColumnValueType.NULL, NullType) //?

  case class Optional(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def value: Int = inner.value

    override def columnValueType: ColumnValueType = inner.columnValueType

    override def sparkType: DataType = inner.sparkType

    override def nullable: Boolean = true

    override def alias: CompositeYtLogicalTypeAlias = Optional
  }

  case object Optional extends CompositeYtLogicalTypeAlias("optional")

  case class Dict(dictKey: YtLogicalType, dictValue: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: DataType = MapType(dictKey.sparkType, dictValue.sparkType, dictValue.nullable)

    override def alias: CompositeYtLogicalTypeAlias = Dict
  }

  case object Dict extends CompositeYtLogicalTypeAlias("dict")

  case class Array(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: DataType = ArrayType(inner.sparkType, inner.nullable)

    override def alias: CompositeYtLogicalTypeAlias = Array
  }

  case object Array extends CompositeYtLogicalTypeAlias("list")

  case class Struct(fields: Seq[(String, YtLogicalType)]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields
      .map { case (name, ytType) => getStructField(name, ytType) })

    override def alias: CompositeYtLogicalTypeAlias = Struct
  }

  case object Struct extends CompositeYtLogicalTypeAlias("struct")

  case class Tuple(elements: Seq[YtLogicalType]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(elements.zipWithIndex
      .map { case (ytType, index) => getStructField(s"_${1 + index}", ytType) })

    override def alias: CompositeYtLogicalTypeAlias = Tuple
  }

  case object Tuple extends CompositeYtLogicalTypeAlias("tuple")

  case class Tagged(inner: YtLogicalType, tag: String) extends CompositeYtLogicalType {
    override def sparkType: DataType = inner.sparkType

    override def alias: CompositeYtLogicalTypeAlias = Tagged
  }

  case object Tagged extends CompositeYtLogicalTypeAlias("tagged")

  case class VariantOverStruct(fields: Seq[(String, YtLogicalType)]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields
      .map { case (name, ytType) => getStructField(s"_v$name", ytType) })

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case class VariantOverTuple(fields: Seq[YtLogicalType]) extends CompositeYtLogicalType {
    override def sparkType: DataType = StructType(fields.zipWithIndex
      .map { case (ytType, index) => getStructField(s"_v_${1 + index}", ytType) })

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case object Variant extends CompositeYtLogicalTypeAlias("variant")

  private lazy val atomicTypes = Seq(Null, Int64, Uint64, Float, Double, Boolean, String, Any, Int8, Uint8,
    Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Void)

  private lazy val compositeTypes = Seq(Optional, Dict, Array, Struct, Tuple,
    Tagged, Variant)

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

  private def getStructField(name: String, ytType: YtLogicalType): StructField = {
    val metadataBuilder = new MetadataBuilder
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
