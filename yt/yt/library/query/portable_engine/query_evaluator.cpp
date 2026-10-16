#include "builtin_registry.h"
#include "evaluation_helpers.h"
#include "program.h"

#include <yt/yt/library/query/engine_api/query_evaluator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    TConstExpressionPtr expression,
    const TTableSchemaPtr& schema)
{
    auto context = New<TQueryEvaluationContext>();
    context->Expression = std::move(expression);
    context->Image = NPortable::CreateExpressionImage(NPortable::CompileExpression(
        context->Expression,
        *schema,
        NPortable::GetBuiltinExpressionRegistry()));
    context->Instance = context->Image.Instantiate();
    // Initialize lazy literal storage before sharing the context.
    context->Variables.GetLiteralValues();
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
