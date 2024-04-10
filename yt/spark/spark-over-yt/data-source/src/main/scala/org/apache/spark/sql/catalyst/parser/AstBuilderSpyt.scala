package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.yson.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import java.util.Locale

@Subclass
@OriginClass("org.apache.spark.sql.catalyst.parser.AstBuilder")
class AstBuilderSpyt extends AstBuilder {

  override def visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = {

    val uint64Opt = withOrigin(ctx) {
      val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
      dataType match {
        case "uint64" => Some(UInt64Type)
        case _ => None
      }
    }

    if (uint64Opt.isDefined) {
      uint64Opt.get
    } else {
      super.visitPrimitiveDataType(ctx)
    }
  }

}
