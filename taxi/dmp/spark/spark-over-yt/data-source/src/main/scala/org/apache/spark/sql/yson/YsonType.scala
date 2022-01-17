package org.apache.spark.sql.yson

import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.{IndexedDataType, YsonDecoder}

class YsonType extends UserDefinedType[YsonBinary] with ValidatedCastType {
  override def pyUDT: String = "spyt.types.YsonType"

  override def sqlType: DataType = BinaryType

  override def serialize(obj: YsonBinary): Array[Byte] = {
    obj.bytes
  }

  override def deserialize(datum: Any): YsonBinary = datum match {
    case bytes: Array[Byte] => YsonBinary(bytes)
  }

  override def userClass: Class[YsonBinary] = classOf[YsonBinary]

  override def validate(datum: Any): Unit = datum match {
    case bytes: Array[Byte] =>
      try {
        YsonDecoder.decode(bytes, IndexedDataType.NoneType)
      } catch {
        case e: Throwable => throw new IllegalArgumentException(s"Illegal yson bytes", e)
      }

  }

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "yson"
}

case object YsonType extends YsonType

@SQLUserDefinedType(udt = classOf[YsonType])
case class YsonBinary(bytes: Array[Byte])
