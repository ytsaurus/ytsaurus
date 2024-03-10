package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

@Subclass
@OriginClass("org.apache.spark.sql.catalyst.expressions.HashExpression")
abstract class HashExpressionSpyt[E] extends HashExpression[E] {

  override protected def computeHash(input: String,
                                     dataType: DataType,
                                     result: String,
                                     ctx: CodegenContext): String = dataType match {
    case UInt64Type => genHashLong(input, result)
    case _ => super.computeHash(input, dataType, result, ctx)
  }
}
