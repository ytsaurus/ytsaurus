package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.yson.{BinaryCastToYsonCode, UInt64CastToString, UInt64CastToStringCode, UInt64Long, UInt64Type, YsonBinary, YsonCastToBinary, YsonCastToBinaryCode, YsonType}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.CastBase")
class CastBaseDecorators {

  @DecoratedMethod
  private[this] def castToString(from: DataType): Any => Any = from match {
    case UInt64Type => UInt64CastToString
    case _ => __castToString(from)
  }

  private[this] def __castToString(from: DataType): Any => Any = ???

  @DecoratedMethod
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case YsonType => YsonCastToBinary
    case _ => __castToBinary(from)
  }
  private[this] def __castToBinary(from: DataType): Any => Any = ???

  @DecoratedMethod
  protected[this] def cast(from: DataType, to: DataType): Any => Any = {
    if (DataType.equalsStructurally(from, to)) {
      __cast(from, to)
    } else if (from == NullType) {
      _ => throw QueryExecutionErrors.cannotCastFromNullTypeError(to)
    } else {
      to match {
        case UInt64Type => UInt64Long.cast(from)
        case YsonType => YsonBinary.cast(from)
        case _ => __cast(from, to)
      }
    }
  }
  protected[this] def __cast(from: DataType, to: DataType): Any => Any = ???

  protected[this] type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  @DecoratedMethod
  private[this] def nullSafeCastFunction(from: DataType, to: DataType, ctx: CodegenContext): CastFunction = to match {
    case YsonType if !(from == NullType || to == from) => BinaryCastToYsonCode
    case _ => __nullSafeCastFunction(from, to, ctx)
  }

  private[this] def __nullSafeCastFunction(from: DataType, to: DataType, ctx: CodegenContext): CastFunction = ???

  @DecoratedMethod
  private[this] def castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case UInt64Type => UInt64CastToStringCode
    case _ => __castToStringCode(from, ctx)
  }

  private[this] def __castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = ???

  @DecoratedMethod
  private[this] def castToBinaryCode(from: DataType): CastFunction = from match {
    case YsonType => YsonCastToBinaryCode
  }
  private[this] def __castToBinaryCode(from: DataType): CastFunction = ???
}
