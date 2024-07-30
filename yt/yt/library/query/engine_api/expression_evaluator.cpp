#include "expression_evaluator.h"

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryClient {

using NTableClient::TMutableVersionedRow;
using NTableClient::TMutableUnversionedRow;
using NTableClient::TUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TExpressionEvaluatorPtr TExpressionEvaluator::Create(
    const TParsedSource& /*expression*/,
    const TTableSchemaPtr& /*schema*/,
    const TConstTypeInferrerMapPtr& /*typeInferrers*/,
    const TConstFunctionProfilerMapPtr& /*profilers*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/expression_evaluator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TExpressionEvaluator::TExpressionEvaluator(
    TCGExpressionImage image,
    TCGVariables variables)
    : Image_(std::move(image))
    , Instance_(Image_.Instantiate())
    , Variables_(std::move(variables))
{ }

////////////////////////////////////////////////////////////////////////////////

TValue TExpressionEvaluator::Evaluate(TRow row, const TRowBufferPtr& rowBuffer) const
{
    auto value = MakeUnversionedNullValue();

    Instance_.Run(
        Variables_.GetLiteralValues(),
        Variables_.GetOpaqueData(),
        Variables_.GetOpaqueDataSizes(),
        &value,
        row.Elements(),
        rowBuffer);

    return value;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IExpressionEvaluatorCachePtr CreateExpressionEvaluatorCache(
    TExpressionEvaluatorCacheConfigPtr /*config*/,
    TConstTypeInferrerMapPtr /*typeInferrers*/,
    TConstFunctionProfilerMapPtr /*profilers*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/expression_evaluator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
