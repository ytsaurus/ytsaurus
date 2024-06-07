#include "format.h"

#include <Parsers/formatAST.h>

namespace DB {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(NYT::TStringBuilderBase* builder, const DB::DataTypePtr& dataType, TStringBuf spec)
{
    FormatValue(builder, TStringBuf{dataType->getName()}, spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace DB
