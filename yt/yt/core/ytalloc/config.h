#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

class TYTAllocConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableAllocationProfiling;
    double AllocationProfilingSamplingRate;
    std::vector<int> SmallArenasToProfile;
    std::vector<int> LargeArenasToProfile;
    std::optional<int> ProfilingBacktraceDepth;
    std::optional<size_t> MinProfilingBytesUsedToReport;
    std::optional<TDuration> StockpileInterval;
    std::optional<int> StockpileThreadCount;
    std::optional<size_t> StockpileSize;
    std::optional<bool> EnableEagerMemoryRelease;

    TYTAllocConfig()
    {
        RegisterParameter("enable_allocation_profiling", EnableAllocationProfiling)
            .Default(false);
        RegisterParameter("allocation_profiling_sampling_rate", AllocationProfilingSamplingRate)
            .Default(1.0)
            .InRange(0.0, 1.0);
        RegisterParameter("small_arenas_to_profile", SmallArenasToProfile)
            .Default({});
        RegisterParameter("large_arenas_to_profile", LargeArenasToProfile)
            .Default({});
        RegisterParameter("profiling_backtrace_depth", ProfilingBacktraceDepth)
            .InRange(1, MaxAllocationProfilingBacktraceDepth)
            .Default();
        RegisterParameter("min_profiling_bytes_used_to_report", MinProfilingBytesUsedToReport)
            .GreaterThan(0)
            .Default();
        RegisterParameter("stockpile_interval", StockpileInterval)
            .Default();
        RegisterParameter("stockpile_thread_count", StockpileThreadCount)
            .Default();
        RegisterParameter("stockpile_size", StockpileSize)
            .GreaterThan(0)
            .Default();
        RegisterParameter("enable_eager_memory_release", EnableEagerMemoryRelease)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TYTAllocConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
