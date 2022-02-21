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
    std::optional<bool> EnableMadvisePopulate;
    std::optional<double> LargeUnreclaimableCoeff;
    std::optional<size_t> MinLargeUnreclaimableBytes;
    std::optional<size_t> MaxLargeUnreclaimableBytes;

    TYTAllocConfig();
};

DEFINE_REFCOUNTED_TYPE(TYTAllocConfig)

class TYTProfilingConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<bool> EnableDetailedAllocationStatistics;

    TYTProfilingConfig();
};

DEFINE_REFCOUNTED_TYPE(TYTProfilingConfig)


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
