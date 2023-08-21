package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, HashExpression, InterpretedHashFunction}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.v2.YtUtils.getClass
import org.apache.spark.sql.yson.UInt64Type
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.unsafe.Platform
import org.slf4j.LoggerFactory

case class CityHash(children: Seq[Expression], seed: Long) extends HashExpression[Long] {
  def this(arguments: Seq[Expression]) = this(arguments, 0L)

  override def dataType: DataType = LongType

  override def prettyName: String = "cityhash"

  override protected def hasherClassName: String = classOf[CityHashImpl].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Long): Long = {
    CityHashFunction.hash(value, dataType, seed)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CityHash =
    copy(children = newChildren)
}

object CityHashFunction extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = CityHashImpl.hashInt(i, seed)

  override protected def hashLong(l: Long, seed: Long): Long = CityHashImpl.hashLong(l, seed)

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    CityHashImpl.hashUnsafeBytes(base, offset, len, seed)
  }
}

object CityHash {
  @scala.annotation.varargs
  def cityHashUdf(source: Column*): Column = {
    new Column(new CityHash(source.map(_.expr))).cast(UInt64Type)
  }

  def registerFunction(spark: SparkSession): Unit = {
    spark.sessionState.functionRegistry.registerFunction(
      new FunctionIdentifier("cityhash"),
      new ExpressionInfo("tech.ytsaurus.spyt.common.utils.CityHash", "cityhash"),
      (children: Seq[Expression]) =>
        Cast(new CityHash(children), UInt64Type)
    )
  }
}
