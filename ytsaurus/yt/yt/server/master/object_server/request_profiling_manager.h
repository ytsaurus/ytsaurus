#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TRequestProfilingCounters
    : public TRefCounted
{
    explicit TRequestProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter TotalReadRequestCounter;
    NProfiling::TCounter TotalWriteRequestCounter;
    NProfiling::TCounter LocalReadRequestCounter;
    NProfiling::TCounter LocalWriteRequestCounter;
    NProfiling::TCounter LeaderFallbackRequestCounter;
    NProfiling::TCounter CrossCellForwardingRequestCounter;
    NProfiling::TCounter AutomatonForwardingRequestCounter;
};

DEFINE_REFCOUNTED_TYPE(TRequestProfilingCounters)

////////////////////////////////////////////////////////////////////////////////

struct IRequestProfilingManager
    : public TRefCounted
{
    virtual TRequestProfilingCountersPtr GetCounters(const TString& user, const TString& method) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRequestProfilingManager)

////////////////////////////////////////////////////////////////////////////////

IRequestProfilingManagerPtr CreateRequestProfilingManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
