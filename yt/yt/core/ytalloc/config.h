#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

class TYTAllocConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<bool> EnableAllocationProfiling;
    std::optional<double> AllocationProfilingSamplingRate;
    std::optional<std::vector<int>> SmallArenasToProfile;
    std::optional<std::vector<int>> LargeArenasToProfile;
    std::optional<int> ProfilingBacktraceDepth;
    std::optional<size_t> MinProfilingBytesUsedToReport;
    std::optional<TDuration> StockpileInterval;
    std::optional<int> StockpileThreadCount;
    std::optional<size_t> StockpileSize;
    std::optional<bool> EnableEagerMemoryRelease;
    std::optional<double> LargeUnreclaimableCoeff;
    std::optional<size_t> MinLargeUnreclaimableBytes;
    std::optional<size_t> MaxLargeUnreclaimableBytes;

    TYTAllocConfig()
    {
        RegisterParameter("enable_allocation_profiling", EnableAllocationProfiling)
            .Default();
        RegisterParameter("allocation_profiling_sampling_rate", AllocationProfilingSamplingRate)
            .InRange(0.0, 1.0)
            .Default();
        RegisterParameter("small_arenas_to_profile", SmallArenasToProfile)
            .Default();
        RegisterParameter("large_arenas_to_profile", LargeArenasToProfile)
            .Default();
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
            .Default();
        RegisterParameter("large_unreclaimable_coeff", LargeUnreclaimableCoeff)
            .Default();
        RegisterParameter("min_large_unreclaimable_bytes", MinLargeUnreclaimableBytes)
            .Default();
        RegisterParameter("max_large_unreclaimable_bytes", MaxLargeUnreclaimableBytes)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TYTAllocConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
