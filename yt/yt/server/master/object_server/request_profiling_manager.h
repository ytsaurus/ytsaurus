#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TRequestProfilingCounters
    : public TRefCounted
{
    explicit TRequestProfilingCounters(const NProfiling::TTagIdList& tagIds);

    NProfiling::TShardedMonotonicCounter TotalReadRequestCounter;
    NProfiling::TShardedMonotonicCounter TotalWriteRequestCounter;
    NProfiling::TShardedMonotonicCounter LocalReadRequestCounter;
    NProfiling::TShardedMonotonicCounter LocalWriteRequestCounter;
    NProfiling::TShardedMonotonicCounter LeaderFallbackRequestCounter;
    NProfiling::TShardedMonotonicCounter IntraCellForwardingRequestCounter;
    NProfiling::TShardedMonotonicCounter CrossCellForwardingRequestCounter;
    NProfiling::TShardedMonotonicCounter AutomatonForwardingRequestCounter;
    NProfiling::TShardedMonotonicCounter LocalMutationScheduleTimeCounter;
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
