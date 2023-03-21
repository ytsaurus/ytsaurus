#include "evaluator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IEvaluatorPtr CreateEvaluator(TExecutorConfigPtr /*config*/, const NProfiling::TProfiler& /*profiler*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/evaluator.cpp.
    THROW_ERROR_EXCEPTION("Error query/engine library is not linked; consider PEERDIR'ing yt/yt/library/query/engine");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

