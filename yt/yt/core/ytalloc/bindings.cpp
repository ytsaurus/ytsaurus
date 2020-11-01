#include "bindings.h"
#include "config.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/string_builder.h>
#include <yt/core/misc/stack_trace.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/libunwind/libunwind.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <util/system/env.h>

#include <cstdio>

namespace NYT::NYTAlloc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

const NLogging::TLogger& GetLogger()
{
    struct TSingleton
    {
        NLogging::TLogger Logger{"YTAlloc"};
    };

    return LeakySingleton<TSingleton>()->Logger;
}

NLogging::ELogLevel SeverityToLevel(NYTAlloc::ELogEventSeverity severity)
{
    switch (severity) {

        case ELogEventSeverity::Debug:   return NLogging::ELogLevel::Debug;
        case ELogEventSeverity::Info:    return NLogging::ELogLevel::Info;
        case ELogEventSeverity::Warning: return NLogging::ELogLevel::Warning;
        case ELogEventSeverity::Error:   return NLogging::ELogLevel::Error;
        default:                         Y_UNREACHABLE();
    }
}

void LogHandler(const NYTAlloc::TLogEvent& event)
{
    YT_LOG_EVENT(GetLogger(), SeverityToLevel(event.Severity), "%v", event.Message);
}

} // namespace

void EnableYTLogging()
{
    EnableLogging(LogHandler);
}

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

class TProfilingStatisticsPusher
    : public TRefCounted
{
public:
    TProfilingStatisticsPusher()
        : Executor_(New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TProfilingStatisticsPusher::OnProfiling, MakeWeak(this)),
            ProfilingPeriod))
    {
        Executor_->Start();
    }

private:
    Y_DECLARE_SINGLETON_FRIEND()

    const TPeriodicExecutorPtr Executor_;

    const NProfiling::TEnumMemberTagCache<ETimingEventType> TimingEventTypeTagCache{"type"};
    const NProfiling::TProfiler Profiler_{"/yt_alloc"};


    void OnProfiling()
    {
        PushSystemAllocationStatistics();
        PushTotalAllocationStatistics();
        PushSmallAllocationStatistics();
        PushLargeAllocationStatistics();
        PushHugeAllocationStatistics();
        PushUndumpableAllocationStatistics();
        PushTimingStatistics();
    }


    template <class TCounters>
    static void PushAllocationCounterStatistics(const NProfiling::TProfiler& profiler, const TCounters& counters)
    {
        using T = typename TCounters::TIndex;
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            profiler.Enqueue("/" + FormatEnum(counter), counters[counter], NProfiling::EMetricType::Gauge);
        }
    }

    void PushSystemAllocationStatistics()
    {
        auto counters = GetSystemAllocationCounters();
        auto profiler = Profiler_.AppendPath("/system");
        PushAllocationCounterStatistics(profiler, counters);
    }

    void PushTotalAllocationStatistics()
    {
        auto counters = GetTotalAllocationCounters();
        auto profiler = Profiler_.AppendPath("/total");
        PushAllocationCounterStatistics(profiler, counters);
    }

    void PushHugeAllocationStatistics()
    {
        auto counters = GetHugeAllocationCounters();
        auto profiler = Profiler_.AppendPath("/huge");
        PushAllocationCounterStatistics(profiler, counters);
    }

    void PushUndumpableAllocationStatistics()
    {
        auto counters = GetUndumpableAllocationCounters();
        auto profiler = Profiler_.AppendPath("/undumpable");
        PushAllocationCounterStatistics(profiler, counters);
    }

    void PushSmallArenaStatistics(
        size_t rank,
        const TEnumIndexedVector<ESmallArenaCounter, ssize_t>& counters)
    {
        auto profiler = Profiler_.AppendPath("/small_arena").AddTags(
            {
                NProfiling::TProfileManager::Get()->RegisterTag("rank", rank)
            });
        PushAllocationCounterStatistics(profiler, counters);
    }

    void PushSmallAllocationStatistics()
    {
        auto counters = GetSmallAllocationCounters();
        auto profiler = Profiler_.AppendPath("/small");
        PushAllocationCounterStatistics(profiler, counters);

        auto arenaCounters = GetSmallArenaAllocationCounters();
        for (size_t rank = 1; rank < SmallRankCount; ++rank) {
            PushSmallArenaStatistics(rank, arenaCounters[rank]);
        }
    }

    void PushLargeArenaStatistics(
        size_t rank,
        const TEnumIndexedVector<ELargeArenaCounter, ssize_t>& counters)
    {
        auto profiler = Profiler_.AppendPath("/large_arena").AddTags(
            {
                NProfiling::TProfileManager::Get()->RegisterTag("rank", rank)
            });
        PushAllocationCounterStatistics(profiler, counters);

        auto bytesFreed = counters[ELargeArenaCounter::BytesFreed];
        auto bytesReleased = counters[ELargeArenaCounter::PagesReleased] * PageSize;
        int poolHitRatio;
        if (bytesFreed == 0) {
            poolHitRatio = 100;
        } else if (bytesReleased > bytesFreed) {
            poolHitRatio = 0;
        } else {
            poolHitRatio = 100 - bytesReleased * 100 / bytesFreed;
        }
        profiler.Enqueue("/pool_hit_ratio", poolHitRatio, NProfiling::EMetricType::Gauge);
    }

    void PushLargeAllocationStatistics()
    {
        auto counters = GetLargeAllocationCounters();
        auto profiler = Profiler_.AppendPath("/large");
        PushAllocationCounterStatistics(profiler, counters);

        auto arenaCounters = GetLargeArenaAllocationCounters();
        for (size_t rank = MinLargeRank; rank < LargeRankCount; ++rank) {
            PushLargeArenaStatistics(rank, arenaCounters[rank]);
        }
    }

    void PushTimingStatistics()
    {
        auto timingEventCounters = GetTimingEventCounters();
        for (auto type : TEnumTraits<ETimingEventType>::GetDomainValues()) {
            auto profiler = Profiler_.AppendPath("/timing_events").AddTags(
                {
                    TimingEventTypeTagCache.GetTag(type)
                });
            const auto& counters = timingEventCounters[type];
            profiler.Enqueue("/count", counters.Count, NProfiling::EMetricType::Gauge);
            profiler.Enqueue("/size", counters.Size, NProfiling::EMetricType::Gauge);
        }
    }
};

void EnableYTProfiling()
{
    RefCountedSingleton<TProfilingStatisticsPusher>();
}

////////////////////////////////////////////////////////////////////////////////

void Configure(const TYTAllocConfigPtr& config)
{
    if (config->SmallArenasToProfile) {
        for (size_t rank = 1; rank < SmallRankCount; ++rank) {
            SetSmallArenaAllocationProfilingEnabled(rank, false);
        }
        for (auto rank : *config->SmallArenasToProfile) {
            if (rank < 1 || rank >= SmallRankCount) {
                THROW_ERROR_EXCEPTION("Unable to enable allocation profiling for small arena %v since its rank is out of range",
                    rank);
            }
            SetSmallArenaAllocationProfilingEnabled(rank, true);
        }
    }

    if (config->LargeArenasToProfile) {
        for (size_t rank = 1; rank < LargeRankCount; ++rank) {
            SetLargeArenaAllocationProfilingEnabled(rank, false);
        }
        for (auto rank : *config->LargeArenasToProfile) {
            if (rank < 1 || rank >= LargeRankCount) {
                THROW_ERROR_EXCEPTION("Unable to enable allocation profiling for large arena %v since its rank is out of range",
                    rank);
            }
            SetLargeArenaAllocationProfilingEnabled(rank, true);
        }
    }

    if (config->EnableAllocationProfiling) {
        SetAllocationProfilingEnabled(*config->EnableAllocationProfiling);
    }
    
    if (config->AllocationProfilingSamplingRate) {
        SetAllocationProfilingSamplingRate(*config->AllocationProfilingSamplingRate);
    }
    
    if (config->ProfilingBacktraceDepth) {
        SetProfilingBacktraceDepth(*config->ProfilingBacktraceDepth);
    }
    
    if (config->MinProfilingBytesUsedToReport) {
        SetMinProfilingBytesUsedToReport(*config->MinProfilingBytesUsedToReport);
    }
    
    if (config->StockpileInterval) {
        SetStockpileInterval(*config->StockpileInterval);
    }
    
    if (config->StockpileThreadCount) {
        SetStockpileThreadCount(*config->StockpileThreadCount);
    }
    
    if (config->StockpileSize) {
        SetStockpileSize(*config->StockpileSize);
    }
    
    if (config->EnableEagerMemoryRelease) {
        SetEnableEagerMemoryRelease(*config->EnableEagerMemoryRelease);
    }
}

bool ConfigureFromEnv()
{
    const auto& Logger = GetLogger();

    static const TString ConfigEnvVarName = "YT_ALLOC_CONFIG";
    auto configVarValue = GetEnv(ConfigEnvVarName);
    if (!configVarValue) {
        YT_LOG_DEBUG("No %v environment variable is found",
            ConfigEnvVarName);
        return false;
    }

    TYTAllocConfigPtr config;
    try {
        config = ConvertTo<TYTAllocConfigPtr>(NYson::TYsonString(configVarValue));
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error parsing environment variable %v",
            ConfigEnvVarName);
        return false;
    }

    YT_LOG_DEBUG("%v environment variable parsed successfully",
        ConfigEnvVarName);

    try {
        Configure(config);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error applying configuration parsed from environment variable");
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void InitializeLibunwindInterop()
{
    SetBacktraceProvider(NLibunwind::GetStackTrace);
    SetBacktraceFormatter(FormatStackTrace);
}

TString FormatAllocationCounters()
{
    TStringBuilder builder;

    auto formatCounters = [&] (const auto& counters) {
        using T = typename std::decay_t<decltype(counters)>::TIndex;
        builder.AppendString("{");
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            delimitedBuilder->AppendFormat("%v: %v", counter, counters[counter]);
        }
        builder.AppendString("}");
    };

    builder.AppendString("Total = {");
    formatCounters(GetTotalAllocationCounters());

    builder.AppendString("}, System = {");
    formatCounters(GetSystemAllocationCounters());

    builder.AppendString("}, Small = {");
    formatCounters(GetSmallAllocationCounters());

    builder.AppendString("}, Large = {");
    formatCounters(GetLargeAllocationCounters());

    builder.AppendString("}, Huge = {");
    formatCounters(GetHugeAllocationCounters());

    builder.AppendString("}");
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
