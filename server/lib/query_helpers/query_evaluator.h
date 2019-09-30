#pragma once

#include <yp/server/lib/misc/public.h>

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/evaluation_helpers.h>

namespace NYP::NServer::NQueryHelpers {

////////////////////////////////////////////////////////////////////////////////

struct TQueryEvaluationContext
{
    NQueryClient::TConstExpressionPtr Expression;
    NQueryClient::TCGVariables Variables;
    NQueryClient::TCGExpressionCallback ExpressionCallback;
};

TQueryEvaluationContext CreateQueryEvaluationContext(
    NQueryClient::NAst::TExpressionPtr astExpression,
    const NQueryClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    const NQueryClient::TValue* inputValues,
    NQueryClient::TExpressionContext* expressionContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NQueryHelpers
