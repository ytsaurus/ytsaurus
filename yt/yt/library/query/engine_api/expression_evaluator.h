#pragma once

#include "evaluation_helpers.h"
#include "public.h"

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluator
    : public TRefCounted
{
public:
    static TExpressionEvaluatorPtr Create(
        const TParsedSource& expression,
        const TTableSchemaPtr& schema,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers);

    TValue Evaluate(TRow row, const TRowBufferPtr& rowBuffer) const;

private:
    TCGExpressionImage Image_;
    TCGExpressionInstance Instance_;
    TCGVariables Variables_;

    TExpressionEvaluator(TCGExpressionImage image, TCGVariables variables);

    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TExpressionEvaluator)

////////////////////////////////////////////////////////////////////////////////

struct IExpressionEvaluatorCache
    : public virtual TRefCounted
{
    virtual TExpressionEvaluatorPtr Find(const TParsedSource& parsedSource, const TTableSchemaPtr& schema) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExpressionEvaluatorCache)

IExpressionEvaluatorCachePtr CreateExpressionEvaluatorCache(
    TExpressionEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers = GetBuiltinTypeInferrers(),
    TConstFunctionProfilerMapPtr profilers = GetBuiltinFunctionProfilers());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
