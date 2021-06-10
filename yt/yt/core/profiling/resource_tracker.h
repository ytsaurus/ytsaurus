#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TResourceTracker
    : public NProfiling::ISensorProducer
{
    struct TPerfThreadInfo final
    {
        struct TPerfFDInfo
        {
            int FD;
            bool Enabled;
        };

        std::array<TPerfFDInfo, TEnumTraits<EPerfEvents>::DomainSize> EventInfos{};
    };

public:
    explicit TResourceTracker();

    double GetUserCpu();
    double GetSystemCpu();
    double GetCpuWait();

    virtual void CollectSensors(ISensorWriter* writer) override;

    void Configure(const TProfileManagerConfigPtr& config);
    void Reconfigure(const TProfileManagerConfigPtr& config, const TProfileManagerDynamicConfigPtr& dynamicConfig);

    using TThreadPerfInfoMap = THashMap<TString, TPerfThreadInfo>;
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

    struct TPerfCounters final
    {
        std::array<ui64, TEnumTraits<EPerfEvents>::DomainSize> Counters{};

        TPerfCounters& operator+=(const TPerfCounters& other)
        {
            for (int index = 0; index < std::ssize(Counters); ++index) {
                Counters[index] += other.Counters[index];
            }

            return *this;
        }
    };

    struct TThreadInfo
    {
        TString ThreadName;
        TTimings Timings;
        TPerfCounters PerfCounters;
        bool IsYtThread = true;
        //! This key is IsYtThread ? ThreadName : ThreadName + "@".
        //! It allows to distinguish YT threads from non-YT threads that
        //! inherited parent YT thread name.
        TString ProfilingKey;
    };

    struct TEventConfigs
    {
        std::array<std::atomic<bool>, TEnumTraits<EPerfEvents>::DomainSize> Enabled{};
    };

    struct TEventConfigSnapshot 
    {
        std::array<bool, TEnumTraits<EPerfEvents>::DomainSize> Enabled{};
    };

    // thread id -> stats
    using TThreadMap = THashMap<TString, TThreadInfo>;

    TThreadMap TidToInfo_;
    TThreadPerfInfoMap TidToPerfInfo_;

    TEventConfigs EventConfigs{};
    TEventConfigSnapshot EventConfigSnapshot{};

    void EnqueueUsage();

    void EnqueueThreadStats();

    bool ProcessThread(TString tid, TThreadInfo* result);
    TThreadMap ProcessThreads();

    void CollectSensorsAggregatedTimings(
        ISensorWriter* writer,
        const TThreadMap& oldTidToInfo,
        const TThreadMap& newTidToInfo,
        i64 timeDeltaUsec);

    void CollectSensorsThreadCounts(
        ISensorWriter* writer,
        const TThreadMap& tidToInfo) const;

    void FetchPerfStats(
        const TStringBuf tidString, 
        TResourceTracker::TThreadPerfInfoMap& perfInfoMap, 
        TResourceTracker::TPerfCounters& counters);
    
    void CollectPerfMetrics(
        ISensorWriter* writer,
        const TResourceTracker::TThreadMap& oldTidToInfo,
        const TResourceTracker::TThreadMap& newTidToInfo);
    
    void CreateEventConfigSnapshot() noexcept;

    void SetPerfEventsConfiguration(const THashSet<EPerfEvents>& enabledEvents);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
