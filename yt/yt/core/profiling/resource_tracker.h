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

    // Value below are in percents.
    std::atomic<double> LastUserCpu_{0.0};
    std::atomic<double> LastSystemCpu_{0.0};
    std::atomic<double> LastCpuWait_{0.0};

    struct TTimings
    {
        i64 UserJiffies = 0;
        i64 SystemJiffies = 0;
        i64 CpuWaitNsec = 0;

        TTimings operator+(const TTimings& other) const;
        TTimings operator-(const TTimings& other) const;
    };

    struct TThreadStats
    {
        TString ThreadName;
        TTimings Timings;
    };

    // thread id -> stats
    using TThreadMap = THashMap<TString, TThreadStats>;

    TThreadMap TidToStats_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void EnqueueUsage();

    void EnqueueCpuUsage();
    void EnqueueMemoryUsage();

    TThreadMap ReadThreadStats();
    void EnqueueAggregatedTimings(
        const TThreadMap& oldTidToStats,
        const TThreadMap& newTidToStats,
        i64 timeDeltaUsec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
