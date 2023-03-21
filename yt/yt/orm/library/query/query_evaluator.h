#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TQueryEvaluationContext
{
    NQueryClient::TConstExpressionPtr Expression;
    NQueryClient::TCGVariables Variables;
    NQueryClient::TCGExpressionCallback ExpressionCallback;
};

TQueryEvaluationContext CreateQueryEvaluationContext(
    const NQueryClient::NAst::TExpressionPtr& astExpression,
    const NQueryClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    const NQueryClient::TValue* inputValues,
    NQueryClient::TExpressionContext* expressionContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
