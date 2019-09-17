#pragma once

#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/public.h>

namespace NYT::NProfiling {

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

    double GetUserCpu();
    double GetSystemCpu();
    double GetCpuWait();

private:
    i64 TicksPerSecond_;
    TInstant LastUpdateTime_;

    std::atomic<double> LastUserCpu_{0.0};
    std::atomic<double> LastSystemCpu_{0.0};
    std::atomic<double> LastCpuWait_{0.0};

    struct TJiffies
    {
        i64 PreviousUser;
        i64 PreviousSystem;
        i64 PreviousWait;
    };

    THashMap<TString, TJiffies> ThreadNameToJiffies_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void EnqueueUsage();

    void EnqueueCpuUsage();
    void EnqueueMemoryUsage();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
