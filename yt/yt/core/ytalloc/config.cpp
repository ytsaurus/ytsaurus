#include "config.h"

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

TYTAllocConfig::TYTAllocConfig()
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
    RegisterParameter("enable_madvise_populate", EnableMadvisePopulate)
        .Default();
    RegisterParameter("large_unreclaimable_coeff", LargeUnreclaimableCoeff)
        .Default();
    RegisterParameter("min_large_unreclaimable_bytes", MinLargeUnreclaimableBytes)
        .Default();
    RegisterParameter("max_large_unreclaimable_bytes", MaxLargeUnreclaimableBytes)
        .Default();
}


TYTProfilingConfig::TYTProfilingConfig()
{
    RegisterParameter("enable_detailed_allocation_statistics", EnableDetailedAllocationStatistics)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
