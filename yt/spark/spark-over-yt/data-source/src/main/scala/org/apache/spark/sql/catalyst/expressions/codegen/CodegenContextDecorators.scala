package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext")
class CodegenContextDecorators {

  @DecoratedMethod
  def genComp(dataType: DataType, c1: String, c2: String): String = dataType match {
    case UInt64Type => s"java.lang.Long.compareUnsigned($c1, $c2)"
    case _ => __genComp(dataType, c1, c2)
  }

  def __genComp(dataType: DataType, c1: String, c2: String): String = ???

}
