#include "format.h"

#include <yt/library/re2/re2.h>

#include <Parsers/formatAST.h>

namespace NYT::NClickHouseServer {

using namespace NRe2;

////////////////////////////////////////////////////////////////////////////////

TString MaybeTruncateSubquery(TString query)
{
    static const auto ytSubqueryRegex = New<TRe2>("ytSubquery\\([^()]*\\)");
    static constexpr const char* replacement = "ytSubquery(...)";
    RE2::GlobalReplace(&query, *ytSubqueryRegex, replacement);
    return query;
}

TString SerializeAndMaybeTruncateSubquery(const DB::IAST& ast)
{
    return MaybeTruncateSubquery(TString(DB::serializeAST(ast)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const DB::DataTypePtr& dataType)
{
    return TString(dataType->getName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace DB
