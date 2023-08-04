#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

class TQueryEvaluationContext
    : public TNonCopyable
{
public:
    NQueryClient::TConstExpressionPtr Expression;
    NQueryClient::TCGVariables Variables;
    NQueryClient::TCGExpressionCallback ExpressionCallback;

    ~TQueryEvaluationContext();
};

std::unique_ptr<TQueryEvaluationContext> CreateQueryEvaluationContext(
    const NQueryClient::NAst::TExpressionPtr& astExpression,
    const NQueryClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    TRange<NQueryClient::TValue> inputValues,
    NQueryClient::TRowBuffer* expressionContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
