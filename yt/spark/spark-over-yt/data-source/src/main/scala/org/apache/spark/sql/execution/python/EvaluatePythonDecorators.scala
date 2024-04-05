package org.apache.spark.sql.execution.python

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.python.EvaluatePython")
object EvaluatePythonDecorators {

  @DecoratedMethod
  def makeFromJava(dataType: DataType): Any => Any = dataType match {
    case UInt64Type => EvaluatePythonUint64MakeFromJava
    case other => __makeFromJava(other)
  }

  def __makeFromJava(dataType: DataType): Any => Any = ???
}


object EvaluatePythonUint64MakeFromJava extends (Any => Any) {

  override def apply(input: Any): Any = input match {
    case null => null
    case c: Byte => c.toLong
    case c: Short => c.toLong
    case c: Int => c.toLong
    case c: Long => c
    case _ => null
  }
}