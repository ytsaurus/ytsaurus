package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.SpecificInternalRow")
class SpecificInternalRowDecorators {

  @DecoratedMethod
  private[this] def dataTypeToMutableValue(dataType: DataType): MutableValue = dataType match {
    case UInt64Type => new MutableLong
    case _ => __dataTypeToMutableValue(dataType)
  }

  private[this] def __dataTypeToMutableValue(dataType: DataType): MutableValue = ???
}
