#include "query_evaluator.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContext::~TQueryEvaluationContext()
{
    // NB: Function contexts should be destroyed before the Expression since Expression hosts destructors.
    Variables.Clear();
}

// TODO(dtorilov): Consider enabling WebAssembly for ORM.

TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    const TParsedSource& parsedSource,
    const TTableSchemaPtr& schema)
{
    return CreateQueryEvaluationContext(
        PrepareExpression(
            parsedSource,
            *schema),
        schema);
}

Y_WEAK TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    TConstExpressionPtr /*expression*/,
    const TTableSchemaPtr& /*schema*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    TRange<TValue> inputValues,
    const TRowBufferPtr& rowBuffer)
{
    // Pre-zero value to avoid garbage after evaluator.
    auto outputValue = MakeUnversionedNullValue();
    evaluationContext.Instance.Run(
        evaluationContext.Variables.GetLiteralValues(),
        evaluationContext.Variables.GetOpaqueData(),
        evaluationContext.Variables.GetOpaqueDataSizes(),
        &outputValue,
        inputValues,
        rowBuffer);
    return outputValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
