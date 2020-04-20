package ru.yandex.spark.yt.fs.conf

import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.types._

object ConfigTypeConverter {
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
        MapType(StringType, sparkType(name.drop(2)))
      case _ =>
        import YtLogicalType._
        YtLogicalType.fromName(sType) match {
          case Null => NullType
          case Int64 => LongType
          case Uint64 => LongType
          case Double => DoubleType
          case Boolean => BooleanType
          case String => StringType
          case Any => BinaryType
          case Int8 => ShortType
          case Uint8 => ShortType
          case Int16 => IntegerType
          case Uint16 => IntegerType
          case Int32 => IntegerType
          case Uint32 => IntegerType
//          case Utf8 => StringType
//          case Date =>
//          case Datetime => IntegerType
//          case Timestamp => LongType
//          case Interval =>
//          case Void =>
          case _ => throw new IllegalArgumentException(s"Unsupported type: $sType")
        }
    }
  }

  def stringType(sparkType: DataType): String = {
    sparkType match {
      case StringType => YtLogicalType.String.name
      case ShortType => YtLogicalType.Int8.name
      case IntegerType => YtLogicalType.Int32.name
      case LongType => YtLogicalType.Int64.name
      case DoubleType => YtLogicalType.Double.name
      case BooleanType => YtLogicalType.Boolean.name
      case BinaryType => YtLogicalType.Any.name
      case ArrayType(elementType, _) => "a#" + stringType(elementType)
      case StructType(fields) => "s#" + fields.map(f => f.name -> stringType(f.dataType)).asJson.noSpaces
      case MapType(StringType, valueType, _) => "m#" + stringType(valueType)
    }
  }
}
