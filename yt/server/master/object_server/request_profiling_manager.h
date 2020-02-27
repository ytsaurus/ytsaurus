#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TRequestProfilingCounters
    : public TIntrinsicRefCounted
{
    explicit TRequestProfilingCounters(const NProfiling::TTagIdList& tagIds);

    NProfiling::TMonotonicCounter TotalReadRequestCounter;
    NProfiling::TMonotonicCounter TotalWriteRequestCounter;
    NProfiling::TMonotonicCounter LocalReadRequestCounter;
    NProfiling::TMonotonicCounter LocalWriteRequestCounter;
    NProfiling::TMonotonicCounter LeaderFallbackRequestCounter;
    NProfiling::TMonotonicCounter IntraCellForwardingRequestCounter;
    NProfiling::TMonotonicCounter CrossCellForwardingRequestCounter;
    NProfiling::TMonotonicCounter AutomatonForwardingRequestCounter;
    NProfiling::TMonotonicCounter LocalMutationScheduleTimeCounter;
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
