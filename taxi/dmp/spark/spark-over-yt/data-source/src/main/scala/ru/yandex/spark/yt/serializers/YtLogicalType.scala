package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.YsonType
import ru.yandex.yt.ytclient.tables.ColumnValueType

sealed abstract class YtLogicalType(val name: String,
                                    val value: Int,
                                    val columnValueType: ColumnValueType,
                                    val sparkType: DataType) {
}

object YtLogicalType {
  case object Null extends YtLogicalType("null", 0x02, ColumnValueType.NULL, NullType)

  case object Int64 extends YtLogicalType("int64", 0x03, ColumnValueType.INT64, LongType)
  case object Uint64 extends YtLogicalType("uint64", 0x04, ColumnValueType.INT64, LongType)
  case object Double extends YtLogicalType("double", 0x05, ColumnValueType.DOUBLE, DoubleType)
  case object Boolean extends YtLogicalType("boolean", 0x06, ColumnValueType.BOOLEAN, BooleanType)

  case object String extends YtLogicalType("string", 0x10, ColumnValueType.STRING, StringType)
  case object Any extends YtLogicalType("any", 0x11, ColumnValueType.ANY, YsonType)

  case object Int8 extends YtLogicalType("int8", 0x1000, ColumnValueType.INT64, ByteType)
  case object Uint8 extends YtLogicalType("uint8", 0x1001, ColumnValueType.INT64, ShortType)

  case object Int16 extends YtLogicalType("int16", 0x1003, ColumnValueType.INT64, ShortType)
  case object Uint16 extends YtLogicalType("uint16", 0x1004, ColumnValueType.INT64, IntegerType)

  case object Int32 extends YtLogicalType("int32", 0x1005, ColumnValueType.INT64, IntegerType)
  case object Uint32 extends YtLogicalType("uint32", 0x1006, ColumnValueType.INT64, LongType)

  case object Utf8 extends YtLogicalType("utf8", 0x1007, ColumnValueType.STRING, StringType) //?

  case object Date extends YtLogicalType("date", 0x1008, ColumnValueType.INT64, DateType) //?
  case object Datetime extends YtLogicalType("datetime", 0x1009, ColumnValueType.INT64, LongType) //?
  case object Timestamp extends YtLogicalType("timestamp", 0x100a, ColumnValueType.INT64, LongType) //?
  case object Interval extends YtLogicalType("interval", 0x100b, ColumnValueType.INT64, LongType) //?

  case object Void extends YtLogicalType("void", 0x100c, ColumnValueType.NULL, NullType) //?

  private val values = Seq(Null, Int64, Uint64, Double, Boolean, String, Any, Int8, Uint8,
    Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Void)

  def fromName(name: String): YtLogicalType = {
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown logical yt type: $name"))
  }
}
