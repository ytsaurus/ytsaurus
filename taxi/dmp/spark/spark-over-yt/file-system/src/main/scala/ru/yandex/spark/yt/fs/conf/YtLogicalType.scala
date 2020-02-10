package ru.yandex.spark.yt.fs.conf

sealed abstract class YtLogicalType(val name: String, val value: Int)

object YtLogicalType {
  case object Null extends YtLogicalType("null", 0x02)

  case object Int64 extends YtLogicalType("int64", 0x03)
  case object Uint64 extends YtLogicalType("uint64", 0x04)
  case object Double extends YtLogicalType("double", 0x05)
  case object Boolean extends YtLogicalType("boolean", 0x06)

  case object String extends YtLogicalType("string", 0x10)
  case object Any extends YtLogicalType("any", 0x11)

  case object Int8 extends YtLogicalType("int8", 0x1000)
  case object Uint8 extends YtLogicalType("uint8", 0x1001)

  case object Int16 extends YtLogicalType("int16", 0x1003)
  case object Uint16 extends YtLogicalType("uint16", 0x1004)

  case object Int32 extends YtLogicalType("int32", 0x1005)
  case object Uint32 extends YtLogicalType("uint32", 0x1006)

  case object Utf8 extends YtLogicalType("utf8", 0x1007)

  case object Date extends YtLogicalType("date", 0x1008)
  case object Datetime extends YtLogicalType("datetime", 0x1009)
  case object Timestamp extends YtLogicalType("timestamp", 0x100a)
  case object Interval extends YtLogicalType("interval", 0x100b)

  case object Void extends YtLogicalType("void", 0x100c)



  private val values = Seq(Null, Int64, Uint64, Double, Boolean, String, Any, Int8, Uint8,
    Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Void)

  def fromName(name: String): YtLogicalType = {
    values.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown logical yt type: $name"))
  }
}
