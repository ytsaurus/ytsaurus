#include "query_evaluator.h"

#include <yt/ytlib/query_client/folding_profiler.h>
#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/client/table_client/unversioned_value.h>

namespace NYP::NServer::NQueryHelpers {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContext CreateQueryEvaluationContext(
    NAst::TExpressionPtr astExpression,
    const TTableSchema& schema)
{
    auto expressionSource = FormatExpression(*astExpression);

    auto astHead = NAst::TAstHead::MakeExpression();
    astHead.Ast = std::move(astExpression);

    TParsedSource parsedSource(
        std::move(expressionSource),
        std::move(astHead));

    TQueryEvaluationContext context;

    context.Expression = PrepareExpression(
        parsedSource,
        schema,
        BuiltinTypeInferrersMap,
        nullptr);

    context.ExpressionCallback = Profile(
        context.Expression,
        schema,
        nullptr,
        &context.Variables,
        BuiltinFunctionProfilers)();

    return context;
}

////////////////////////////////////////////////////////////////////////////////

TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    const TValue* inputValues,
    TExpressionContext* expressionContext)
{
    // Pre-zero value to avoid garbage after evaluator.
    auto outputValue = MakeUnversionedSentinelValue(EValueType::Null);
    evaluationContext.ExpressionCallback(
        evaluationContext.Variables.GetLiteralValues(),
        evaluationContext.Variables.GetOpaqueData(),
        &outputValue,
        inputValues,
        expressionContext);
    return outputValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NQueryHelpers
