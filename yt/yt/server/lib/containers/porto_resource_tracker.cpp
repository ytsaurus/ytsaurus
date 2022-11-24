#include "porto_resource_tracker.h"
#include "private.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/net/address.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/server/lib/containers/instance.h>
#include <yt/yt/server/lib/containers/porto_executor.h>
#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/ytlib/cgroup/cgroup.h>

namespace NYT::NContainers {

static const NLogging::TLogger &Logger = NContainers::ContainersLogger;

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

static ui64 GetFieldOrThrow(
    const NContainers::TResourceUsage &usage,
    NContainers::EStatField field)
{
    auto it = usage.find(field);
    if (it == usage.end()) {
        THROW_ERROR_EXCEPTION("Resource usage is missing %Qlv field", field);
    }
    const auto &errorOrValue = it->second;
    if (errorOrValue.FindMatching(NContainers::EPortoErrorCode::NotSupported)) {
        return 0;
    }
    if (!errorOrValue.IsOK()) {
        THROW_ERROR_EXCEPTION("Error getting %Qlv resource usage field", field)
            << errorOrValue;
    }
    return errorOrValue.Value();
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceTracker::TPortoResourceTracker(
    NContainers::IInstancePtr instance,
    TDuration updatePeriod)
    : Instance_(std::move(instance))
    , UpdatePeriod_(updatePeriod)
{ }

TCpuStatistics TPortoResourceTracker::ExtractCpuStatistics() const
{
    // NB: Job proxy uses last sample of CPU statistics but we are interested in
    // peak thread count value.
    PeakThreadCount_ = std::max<ui64>(
        PeakThreadCount_,
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::ThreadCount));
    ui64 systemTimeNs = GetFieldOrThrow(
        ResourceUsage_,
        NContainers::EStatField::CpuUsageSystem);
    ui64 userTimeNs =
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::CpuUsage) -
        systemTimeNs;
    ui64 waitTimeNs =
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::CpuWait);

    return TCpuStatistics{
        .UserTime = TDuration::MicroSeconds(userTimeNs / 1000),
        .SystemTime = TDuration::MicroSeconds(systemTimeNs / 1000),
        .WaitTime = TDuration::MicroSeconds(waitTimeNs / 1000),
        .ThrottledTime = TDuration::MicroSeconds(
            GetFieldOrThrow(
                ResourceUsage_,
                NContainers::EStatField::CpuThrottled) / 1000),
        .ContextSwitches = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::ContextSwitches),
        .PeakThreadCount = PeakThreadCount_,
    };
}

TMemoryStatistics TPortoResourceTracker::ExtractMemoryStatistics() const
{
    return TMemoryStatistics{
        .Rss = GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::Rss),
        .MappedFile = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::MappedFiles),
        .MajorPageFaults = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::MajorFaults),
    };
}

TBlockIOStatistics TPortoResourceTracker::ExtractBlockIOStatistics() const
{
    return TBlockIOStatistics{
        .BytesRead = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::IOReadByte),
        .BytesWritten = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::IOWriteByte),
        .IOTotal = GetFieldOrThrow(
            ResourceUsage_,
            NContainers::EStatField::IOOperations),
    };
}

TNetworkStatistics TPortoResourceTracker::ExtractNetworkStatistics() const
{
    return TNetworkStatistics{
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetTxBytes),
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetTxPackets),
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetTxDrops),
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetRxBytes),
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetRxPackets),
        GetFieldOrThrow(ResourceUsage_, NContainers::EStatField::NetRxDrops),
    };
}

TTotalStatistics TPortoResourceTracker::ExtractTotalStatistics() const
{
    return TTotalStatistics{
        .CpuStatistics = ExtractCpuStatistics(),
        .MemoryStatistics = ExtractMemoryStatistics(),
        .BlockIOStatistics = ExtractBlockIOStatistics(),
        .NetworkStatistics = ExtractNetworkStatistics(),
    };
}

TCpuStatistics TPortoResourceTracker::GetCpuStatistics() const
{
    return GetStatistics(
        CachedCpuStatistics_, 
        "CPU", 
        [&] {
            return ExtractCpuStatistics();
        });
}

TMemoryStatistics TPortoResourceTracker::GetMemoryStatistics() const
{
    return GetStatistics(
        CachedMemoryStatistics_, 
        "memory", 
        [&] {
            return ExtractMemoryStatistics();
        });
}

TBlockIOStatistics TPortoResourceTracker::GetBlockIOStatistics() const
{
    return GetStatistics(
        CachedBlockIOStatistics_, 
        "block IO", 
        [&] {
            return ExtractBlockIOStatistics();
        });
}

TNetworkStatistics TPortoResourceTracker::GetNetworkStatistics() const
{
    return GetStatistics(
        CachedNetworkStatistics_, 
        "network", 
        [&] {
            return ExtractNetworkStatistics();
        });
}

TTotalStatistics TPortoResourceTracker::GetTotalStatistics() const
{
    return GetStatistics(
        CachedTotalStatistics_, 
        "total", 
        [&] {
            return ExtractTotalStatistics();
        });
}

template <class T, class F>
T TPortoResourceTracker::GetStatistics(
    std::optional<T> &cachedStatistics,
    const TString &statisticsKind,
    F extractor) const
{
    UpdateResourceUsageStatisticsIfExpired();

    auto guard = Guard(SpinLock_);
    try {
        auto newStatistics = extractor();
        cachedStatistics = newStatistics;
        return newStatistics;
    } catch (const std::exception &ex) {
        if (!cachedStatistics) {
            THROW_ERROR_EXCEPTION("Unable to get %v statistics", statisticsKind)
                << ex;
        }
        YT_LOG_WARNING(
            ex,
            "Unable to get %v statistics; using the last one",
            statisticsKind);
        return *cachedStatistics;
    }
}

bool TPortoResourceTracker::AreResourceUsageStatisticsExpired() const
{
    return TInstant::Now() - LastUpdateTime_.load() > UpdatePeriod_;
}

TInstant TPortoResourceTracker::GetLastUpdateTime() const
{
    return LastUpdateTime_.load();
}

void TPortoResourceTracker::UpdateResourceUsageStatisticsIfExpired() const
{
    if (AreResourceUsageStatisticsExpired()) {
        DoUpdateResourceUsage();
    }
}

void TPortoResourceTracker::DoUpdateResourceUsage() const
{
    // TODO(don-dron): Why not all fields? Try to remove search by fields.
    auto resourceUsage = Instance_->GetResourceUsage({
        NContainers::EStatField::CpuUsage,
        NContainers::EStatField::CpuUsageSystem,
        NContainers::EStatField::CpuWait,
        NContainers::EStatField::CpuThrottled,
        NContainers::EStatField::ContextSwitches,
        NContainers::EStatField::IOReadByte,
        NContainers::EStatField::IOWriteByte,
        NContainers::EStatField::IOOperations,
        NContainers::EStatField::Rss,
        NContainers::EStatField::MappedFiles,
        NContainers::EStatField::MajorFaults,
        NContainers::EStatField::ThreadCount,
        NContainers::EStatField::NetTxBytes,
        NContainers::EStatField::NetTxPackets,
        NContainers::EStatField::NetTxDrops,
        NContainers::EStatField::NetRxBytes,
        NContainers::EStatField::NetRxPackets,
        NContainers::EStatField::NetRxDrops
    });

    {
        auto guard = Guard(SpinLock_);
        ResourceUsage_ = resourceUsage;
        LastUpdateTime_.store(TInstant::Now());
    }
}

////////////////////////////////////////////////////////////////////////////////

#endif

} // namespace NYT::NContainers
