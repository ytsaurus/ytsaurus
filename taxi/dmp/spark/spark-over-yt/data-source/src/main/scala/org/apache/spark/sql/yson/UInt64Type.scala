package org.apache.spark.sql.yson

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.{IndexedDataType, YsonDecoder}

import java.math.BigInteger

class UInt64Type extends UserDefinedType[UInt64Long] with ValidatedCastType {
  override def pyUDT: String = "spyt.types.UInt64Type"

  override def sqlType: DataType = LongType

  override def serialize(obj: UInt64Long): Long = {
    obj.toLong
  }

  override def deserialize(datum: Any): UInt64Long = datum match {
    case number: Long => UInt64Long(number)
  }

  override def userClass: Class[UInt64Long] = classOf[UInt64Long]

  override def validate(datum: Any): Unit = {}

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }
}

case object UInt64Type extends UInt64Type

@SQLUserDefinedType(udt = classOf[UInt64Type])
case class UInt64Long(value: Long) {
  def toLong: Long = value

  override def toString: String = java.lang.Long.toUnsignedString(value)
}

object UInt64Long {
  def apply(number: String): UInt64Long = {
    UInt64Long(java.lang.Long.parseUnsignedLong(number))
  }

  val toStringUdf: UserDefinedFunction = udf((number: UInt64Long) => number match {
    case null => null
    case _ => number.toString
  })

  val fromStringUdf: UserDefinedFunction = udf((number: String) => number match {
    case null => null
    case _ => UInt64Long(number)
  })
}
