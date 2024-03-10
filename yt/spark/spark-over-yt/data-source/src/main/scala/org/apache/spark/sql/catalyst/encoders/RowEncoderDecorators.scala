package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.encoders.RowEncoder")
object RowEncoderDecorators {

  @DecoratedMethod
  def externalDataTypeFor(dt: DataType): DataType = dt match {
    case UInt64Type => ObjectType(classOf[UInt64Long])
    case _ => __externalDataTypeFor(dt)
  }

  def __externalDataTypeFor(dt: DataType): DataType = ???

  @DecoratedMethod
  private def serializerFor(inputObject: Expression, inputType: DataType): Expression = inputType match {
    case UInt64Type => UInt64Long.createSerializer(inputObject)
    case _ => __serializerFor(inputObject, inputType)
  }
  private def __serializerFor(inputObject: Expression, inputType: DataType): Expression = ???

  @DecoratedMethod(
    signature = "(Lorg/apache/spark/sql/catalyst/expressions/Expression;Lorg/apache/spark/sql/types/DataType;)Lorg/apache/spark/sql/catalyst/expressions/Expression;"
  )
  private def deserializerFor(input: Expression, dataType: DataType): Expression = dataType match {
    case UInt64Type => UInt64Long.createDeserializer(input)
    case _ => __deserializerFor(input, dataType)
  }
  private def __deserializerFor(input: Expression, dataType: DataType): Expression = ???
}
