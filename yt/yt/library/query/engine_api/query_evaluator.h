#pragma once

#include "evaluation_helpers.h"

#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryEvaluationContext final
    : public TNonCopyable
{
    TConstExpressionPtr Expression;
    TCGVariables Variables;
    TCGExpressionImage Image;
    TCGExpressionInstance Instance;

    ~TQueryEvaluationContext();
};

using TQueryEvaluationContextPtr = TIntrusivePtr<TQueryEvaluationContext>;

TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    const TParsedSource& parsedSource,
    const TTableSchemaPtr& schema);

TQueryEvaluationContextPtr CreateQueryEvaluationContext(
    TConstExpressionPtr expression,
    const TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

TValue EvaluateQuery(
    const TQueryEvaluationContext& evaluationContext,
    TRange<TValue> inputValues,
    const TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
