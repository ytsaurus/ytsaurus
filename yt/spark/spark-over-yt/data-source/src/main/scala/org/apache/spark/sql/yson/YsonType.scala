package org.apache.spark.sql.yson

import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, ExprValue}
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.serialization.{IndexedDataType, YsonDecoder}

class YsonType extends UserDefinedType[YsonBinary] {
  override def pyUDT: String = "spyt.types.YsonType"

  override def sqlType: DataType = BinaryType

  override def serialize(obj: YsonBinary): Array[Byte] = {
    obj.bytes
  }

  override def deserialize(datum: Any): YsonBinary = datum match {
    case bytes: Array[Byte] => YsonBinary(bytes)
  }

  override def userClass: Class[YsonBinary] = classOf[YsonBinary]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "yson"

  override def equals(other: Any): Boolean = other match {
    case that: UserDefinedType[_] => this.acceptsType(that)
    case _ => false
  }
}

case object YsonType extends YsonType

@SQLUserDefinedType(udt = classOf[YsonType])
case class YsonBinary(bytes: Array[Byte])

object YsonBinary {
  // Actually - from a certain type to Binary with validation
  def cast(from: DataType): Any => Any = from match {
    case BinaryType => (datum: Any) => {
      validate(datum)
      datum
    }
  }

  def validate(datum: Any): Unit = datum match {
    case bytes: Array[Byte] =>
      try {
        YsonDecoder.decode(bytes, IndexedDataType.NoneType)
      } catch {
        case e: Throwable => throw new IllegalArgumentException(s"Illegal yson bytes", e)
      }
  }
}

object YsonCastToBinary extends (Any => Any) {
  override def apply(t: Any): Any = t.asInstanceOf[Array[Byte]]
}

object YsonCastToBinaryCode extends ((ExprValue, ExprValue, ExprValue) => Block) {
  def apply(c: ExprValue, evPrim: ExprValue, evNull: ExprValue): Block = code"$evPrim = $c;"
}

object BinaryCastToYsonCode extends ((ExprValue, ExprValue, ExprValue) => Block) {
  def apply(c: ExprValue, evPrim: ExprValue, evNull: ExprValue): Block =
    code"""
          org.apache.spark.sql.yson.YsonBinary$$.MODULE$$.validate($c);
          $evPrim = $c;
        """
}