package org.apache.spark.sql.types

import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.types.DataType")
object DataTypeDecorators {

  @DecoratedMethod
  private def nameToType(name: String): DataType = name match {
    case "uint64" => UInt64Type
    case _ => __nameToType(name)
  }

  private def __nameToType(name: String): DataType = ???
}
