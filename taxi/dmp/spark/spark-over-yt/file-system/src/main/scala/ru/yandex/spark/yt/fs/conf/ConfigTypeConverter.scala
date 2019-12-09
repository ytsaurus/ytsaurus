package ru.yandex.spark.yt.fs.conf

import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.types._
import ru.yandex.yt.ytclient.tables.ColumnValueType

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
        ColumnValueType.fromName(sType) match {
          case ColumnValueType.STRING => StringType
          case ColumnValueType.INT64 => LongType
          case ColumnValueType.DOUBLE => DoubleType
          case ColumnValueType.BOOLEAN => BooleanType
          case ColumnValueType.UINT64 => LongType
          case ColumnValueType.ANY => BinaryType
          case _ => throw new IllegalArgumentException(s"Unsupported type: $sType")
        }
    }
  }

  def stringType(sparkType: DataType): String = {
    sparkType match {
      case StringType => ColumnValueType.STRING.getName
      case IntegerType => ColumnValueType.INT64.getName
      case LongType => ColumnValueType.INT64.getName
      case DoubleType => ColumnValueType.DOUBLE.getName
      case BooleanType => ColumnValueType.BOOLEAN.getName
      case BinaryType => ColumnValueType.ANY.getName
      case ArrayType(elementType, _) => "a#" + stringType(elementType)
      case StructType(fields) => "s#" + fields.map(f => f.name -> stringType(f.dataType)).asJson.noSpaces
      case MapType(StringType, valueType, _) => "m#" + stringType(valueType)
    }
  }
}
