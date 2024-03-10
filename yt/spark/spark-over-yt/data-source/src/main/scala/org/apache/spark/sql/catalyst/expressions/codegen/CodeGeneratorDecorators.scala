package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.JAVA_LONG
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator")
object CodeGeneratorDecorators {

  @DecoratedMethod
  def javaType(dt: DataType): String = dt match {
    case UInt64Type => JAVA_LONG
    case _ => __javaType(dt)
  }

  def __javaType(dt: DataType): String = ???

  @DecoratedMethod
  def javaClass(dt: DataType): Class[_] = dt match {
    case UInt64Type => java.lang.Long.TYPE
    case _ => __javaClass(dt)
  }

  def __javaClass(dt: DataType): Class[_] = ???
}
