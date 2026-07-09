#include "computation.h"
#include "source_computation.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// The adapter computations are referenced from a pipeline spec by these type names; the
// `processing_function` parameter then names the user function to host. Each marks
// RequiresProcessingFunction, so the spec validator demands a registered function.
YT_FLOW_DEFINE_COMPUTATION(TProcessFunctionComputation);
YT_FLOW_DEFINE_COMPUTATION(TProcessFunctionSwiftMapComputation);
YT_FLOW_DEFINE_COMPUTATION(TProcessFunctionSourceComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
