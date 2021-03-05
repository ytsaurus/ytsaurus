#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TResourceTracker
    : public NProfiling::ISensorProducer
{
public:
    explicit TResourceTracker();

    double GetUserCpu();
    double GetSystemCpu();
    double GetCpuWait();

    virtual void Collect(ISensorWriter* writer) override;

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

        TTimings operator-(const TTimings& other) const;
        TTimings& operator+=(const TTimings& other);
    };

    struct TThreadInfo
    {
        TString ThreadName;
        TTimings Timings;
        bool IsYtThread = true;
        //! This key is IsYtThread ? ThreadName : ThreadName + "@".
        //! It allows to distinguish YT threads from non-YT threads that
        //! inherited parent YT thread name.
        TString ProfilingKey;
    };

    // thread id -> stats
    using TThreadMap = THashMap<TString, TThreadInfo>;

    TThreadMap TidToInfo_;

    void EnqueueUsage();

    void EnqueueThreadStats();

    bool ProcessThread(TString tid, TThreadInfo* result);
    TThreadMap ProcessThreads();

    void CollectAggregatedTimings(
        ISensorWriter* writer,
        const TThreadMap& oldTidToInfo,
        const TThreadMap& newTidToInfo,
        i64 timeDeltaUsec);

    void CollectThreadCounts(
        ISensorWriter* writer,
        const TThreadMap& tidToInfo) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
