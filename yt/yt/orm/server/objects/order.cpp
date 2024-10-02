#include "order.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectOrderByExpression& expression, TStringBuf spec)
{
    FormatValue(
        builder,
        Format(
            "{Expression: %v, Descending: %v}",
            expression.Expression,
            expression.Descending),
        spec);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectOrderBy& orderBy, TStringBuf spec)
{
    TString fmt = "%";
    fmt += spec;
    Format(builder, TRuntimeFormat{fmt}, orderBy.Expressions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
