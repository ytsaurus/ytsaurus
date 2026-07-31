#include "function_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TString EvaluateStringExpression(const DB::ASTPtr& expr, DB::ContextPtr context)
{
    auto [value, _] = DB::evaluateConstantExpression(expr, context);
    return TString(value.safeGet<std::string>());
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TInstant> ParseDateTimeArg(const DB::ASTPtr& arg, DB::ContextPtr context)
{
    auto [field, dataType] = DB::evaluateConstantExpression(arg, context);
    if (DB::WhichDataType(dataType).isString()) {
        const auto& value = field.safeGet<std::string>();
        if (value.empty()) {
            return std::nullopt;
        }
        return TInstant::ParseIso8601(value);
    }
    auto utcArg = DB::makeASTFunction(
        "toString",
        DB::makeASTFunction("toTimeZone", arg, std::make_shared<DB::ASTLiteral>(DB::Field("UTC"))));
    auto utcString = EvaluateStringExpression(utcArg, context);
    if (utcString.empty()) {
        return std::nullopt;
    }
    return TInstant::ParseIso8601(utcString);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
