#include "function_helpers.h"

#include <Interpreters/evaluateConstantExpression.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TString EvaluateStringExpression(const DB::ASTPtr& expr, DB::ContextPtr context)
{
    auto [value, _] = DB::evaluateConstantExpression(expr, context);
    return TString(value.safeGet<std::string>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
