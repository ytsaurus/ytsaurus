#pragma once

#include <yt/yt/flow/library/cpp/common/source.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Builds a source context with a fixed partition, a test logger and a sync status profiler.
// The caller sets the time provider, the source spec and the source key.
TSourceContextPtr CreateTestSourceContext(IInvokerPtr serializedInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
