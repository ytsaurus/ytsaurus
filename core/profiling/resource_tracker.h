#pragma once

#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
    #define RESOURCE_TRACKER_ENABLED
#endif

class TResourceTracker
    : public TRefCounted
{
public:
    explicit TResourceTracker(IInvokerPtr invoker);

    void Start();

private:
    i64 TicksPerSecond;
    TInstant LastUpdateTime;

    struct TJiffies
    {
        i64 PreviousUser;
        i64 PreviousSystem;
    };

    THashMap<TString, TJiffies> ThreadNameToJiffies;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;

    void EnqueueUsage();

    void EnqueueCpuUsage();
    void EnqueueMemoryUsage();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
