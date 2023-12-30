#include "request_profiling_manager.h"

#include <yt/yt/server/master/object_server/private.h>

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NObjectServer {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TRequestProfilingCounters::TRequestProfilingCounters(const TProfiler& profiler)
    : TotalReadRequestCounter(profiler.Counter("/total_read_request_count"))
    , TotalWriteRequestCounter(profiler.Counter("/total_write_request_count"))
    , LocalReadRequestCounter(profiler.Counter("/local_read_request_count"))
    , LocalWriteRequestCounter(profiler.Counter("/local_write_request_count"))
    , LeaderFallbackRequestCounter(profiler.Counter("/leader_fallback_request_count"))
    , CrossCellForwardingRequestCounter(profiler.Counter("/cross_cell_forwarding_request_count"))
    , AutomatonForwardingRequestCounter(profiler.Counter("/automaton_forwarding_request_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

class TRequestProfilingManager
    : public IRequestProfilingManager
{
public:
    TRequestProfilingCountersPtr GetCounters(const TString& user, const TString& method) override
    {
        auto key = std::tuple(user, method);
        return *KeyToCounters_.FindOrInsert(key, [&] {
            return New<TRequestProfilingCounters>(
                ObjectServerProfiler
                    .WithHot()
                    .WithSparse()
                    .WithTag("user", user)
                    .WithTag("method", method));
        }).first;
    }

private:
    // (user, method)
    using TKey = std::tuple<TString, TString>;
    NConcurrency::TSyncMap<TKey, TRequestProfilingCountersPtr> KeyToCounters_;
};

////////////////////////////////////////////////////////////////////////////////

IRequestProfilingManagerPtr CreateRequestProfilingManager()
{
    return New<TRequestProfilingManager>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
