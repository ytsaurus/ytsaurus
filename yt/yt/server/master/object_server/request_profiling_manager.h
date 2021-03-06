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
    NProfiling::TCounter IntraCellForwardingRequestCounter;
    NProfiling::TCounter CrossCellForwardingRequestCounter;
    NProfiling::TCounter AutomatonForwardingRequestCounter;
};

DEFINE_REFCOUNTED_TYPE(TRequestProfilingCounters)

////////////////////////////////////////////////////////////////////////////////

class TRequestProfilingManager
    : public TRefCounted
{
public:
    TRequestProfilingManager();

    ~TRequestProfilingManager();

    TRequestProfilingCountersPtr GetCounters(const TString& user, const TString& method);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TRequestProfilingManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
