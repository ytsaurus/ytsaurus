#include "tvm_service.h"
#include "config.h"

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NAuth {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

Y_WEAK ITvmServicePtr CreateTvmService(
    TTvmServiceConfigPtr /*config*/,
    TProfiler /*profiler*/)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

Y_WEAK IDynamicTvmServicePtr CreateDynamicTvmService(
    TTvmServiceConfigPtr /*config*/,
    TProfiler /*profiler*/)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TStringBuf RemoveTicketSignature(TStringBuf /*ticketBody*/)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
