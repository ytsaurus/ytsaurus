#include "query_evaluator.h"

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContext CreateQueryEvaluationContext(
    const NAst::TExpressionPtr& astExpression,
    const TTableSchemaPtr& schema)
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
        *schema,
        BuiltinTypeInferrersMap,
        nullptr);

    context.ExpressionCallback = Profile(
        context.Expression,
        schema,
        nullptr,
        &context.Variables,
        BuiltinFunctionProfilers)();

    // YTORM-553 Initialize variables.
    context.Variables.GetLiteralValues();

    return context;
}

////////////////////////////////////////////////////////////////////////////////

TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    TRange<TValue> inputValues,
    TRowBuffer* expressionContext)
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

} // namespace NYT::NOrm::NQuery
