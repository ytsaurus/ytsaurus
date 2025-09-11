#include "query_evaluator.h"

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContext::~TQueryEvaluationContext()
{
    // NB: Function contexts should be destroyed before the Expression since Expression hosts destructors.
    Variables.Clear();
}

// TODO(dtorilov): Consider enabling WebAssembly for ORM.

std::unique_ptr<TQueryEvaluationContext> CreateQueryEvaluationContext(
    const TParsedSource& parsedSource,
    const TTableSchemaPtr& schema)
{
    auto context = std::make_unique<TQueryEvaluationContext>();

    context->Expression = PrepareExpression(
        parsedSource,
        *schema);

    context->Image = Profile(
        context->Expression,
        schema,
        /*id*/ nullptr,
        &context->Variables,
        /*useCanonicalNullRelations*/ false,
        /*executionBackend*/ NCodegen::EExecutionBackend::Native,
        GetBuiltinFunctionProfilers())();

    context->Instance = context->Image.Instantiate();

    // YTORM-553 Initialize variables.
    context->Variables.GetLiteralValues();

    return context;
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
