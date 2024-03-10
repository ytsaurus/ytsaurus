package org.apache.spark.sql.yson

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, ExprValue}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.math.Numeric.LongIsIntegral
import scala.reflect.runtime.universe.typeTag

class UInt64Type private() extends IntegralType {
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering: Ordering[InternalType] = UInt64IsIntegral

  override def defaultSize: Int = 8

  private[spark] override def asNullable: UInt64Type = this

  override def catalogString: String = "uint64"

  override def simpleString: String = "uint64"

  private[sql] val numeric = UInt64IsIntegral

  private[sql] val integral = UInt64IsIntegral

}

object UInt64IsIntegral extends LongIsIntegral with Ordering[Long] {
  override def compare(x: Long, y: Long): Int = java.lang.Long.compareUnsigned(x, y)
}

case object UInt64Type extends UInt64Type

case class UInt64Long(value: Long) {
  def toLong: Long = value

  override def toString: String = UInt64Long.toString(value)

  override def hashCode(): Int = value.toInt
}

object UInt64Long {
  def apply(number: String): UInt64Long = {
    UInt64Long(fromString(number))
  }

  val toStringUdf: UserDefinedFunction = udf((number: UInt64Long) => number match {
    case null => null
    case _ => number.toString
  })

  val fromStringUdf: UserDefinedFunction = udf((number: String) => number match {
    case null => null
    case _ => UInt64Long(number)
  })

  def fromString(number: String): Long = java.lang.Long.parseUnsignedLong(number)

  def toString(value: Long): String = java.lang.Long.toUnsignedString(value)

  def createSerializer(inputObject: Expression): Expression = {
    Invoke(inputObject, "toLong", UInt64Type)
  }

  def createDeserializer(path: Expression): Expression = {
    StaticInvoke(
      UInt64Long.getClass,
      ObjectType(classOf[UInt64Long]),
      "apply",
      path :: Nil,
      returnNullable = false)
  }

  // Actually - from a certain type to Long
  def cast(from: DataType): Any => Any = from match {
    case StringType => s => fromString(s.asInstanceOf[UTF8String].toString)
    case BooleanType => b => if (b.asInstanceOf[Boolean]) 1L else 0L
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }
}

object UInt64CastToString extends (Any => Any) {
  def apply(t: Any): Any = UTF8String.fromString(UInt64Long.toString(t.asInstanceOf[Long]))
}

object UInt64CastToStringCode extends ((ExprValue, ExprValue, ExprValue) => Block) {
  def apply(c: ExprValue, evPrim: ExprValue, evNull: ExprValue): Block =
    code"$evPrim = UTF8String.fromString(org.apache.spark.sql.yson.UInt64Long$$.MODULE$$.toString($c));"
}
