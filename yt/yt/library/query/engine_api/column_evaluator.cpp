#include "column_evaluator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr /*config*/,
    TConstTypeInferrerMapPtr /*typeInferrers*/,
    TConstFunctionProfilerMapPtr /*profilers*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/column_evaluator.cpp.
    THROW_ERROR_EXCEPTION("Error query/engine library is not linked; consider PEERDIR'ing yt/yt/library/query/engine");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

