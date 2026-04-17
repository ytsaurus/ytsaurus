#include "folding_profiler.h"

#include <yt/yt/library/query/engine_api/query_evaluator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    TConstExpressionPtr expression,
    const TTableSchemaPtr& schema)
{
    auto context = New<TQueryEvaluationContext>();
    context->Expression = std::move(expression);

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

} // namespace NYT::NOrm::NQuery
