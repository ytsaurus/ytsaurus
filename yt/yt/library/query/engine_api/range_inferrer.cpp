#include "range_inferrer.h"

namespace NYT::NQueryClient {

Y_WEAK TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr /*predicate*/,
    const TTableSchemaPtr& /*schema*/,
    const TKeyColumns& /*keyColumns*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/range_inferrer.cpp.
    THROW_ERROR_EXCEPTION("Error query/engine library is not linked; consider PEERDIR'ing yt/yt/library/query/engine");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

