#include "porto_resource_tracker.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/library/containers/cgroup.h>
#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/public.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NContainers {

using namespace NConcurrency;
using namespace NProfiling;

static constexpr auto& Logger = ContainersLogger;

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

struct TPortoProfilers
    : public TRefCounted
{
    TPortoResourceProfilerPtr DaemonProfiler;
    TPortoResourceProfilerPtr ContainerProfiler;

    TPortoProfilers(
        TPortoResourceProfilerPtr daemonProfiler,
        TPortoResourceProfilerPtr containerProfiler)
        : DaemonProfiler(std::move(daemonProfiler))
        , ContainerProfiler(std::move(containerProfiler))
    { }
};

DEFINE_REFCOUNTED_TYPE(TPortoProfilers)

////////////////////////////////////////////////////////////////////////////////

template<class T>
static typename T::mapped_type GetFieldOrError(
    const T& containerStats,
    EStatField field)
{
    auto it = containerStats.find(field);
    if (it == containerStats.end()) {
        return TError("Resource usage is missing %Qlv field", field);
    }
    const auto& errorOrValue = it->second;
    if (errorOrValue.FindMatching(EPortoErrorCode::NotSupported)) {
        return TError("Property %Qlv not supported in Porto response", field);
    }
    return errorOrValue;
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceTracker::TPortoResourceTracker(
    IInstancePtr instance,
    TDuration updatePeriod,
    bool isDeltaTracker,
    bool isForceUpdate)
    : Instance_(std::move(instance))
    , UpdatePeriod_(updatePeriod)
    , IsDeltaTracker_(isDeltaTracker)
    , IsForceUpdate_(isForceUpdate)
{
    ResourceUsage_.ContainerStats = {
        {EStatField::IOReadByte, 0},
        {EStatField::IOWriteByte, 0},
        {EStatField::IOBytesLimit, 0},
        {EStatField::IOReadOps, 0},
        {EStatField::IOWriteOps, 0},
        {EStatField::IOOps, 0},
        {EStatField::IOOpsLimit, 0},
        {EStatField::IOTotalTime, 0},
        {EStatField::IOWaitTime, 0}
    };
    ResourceUsageDelta_ = ResourceUsage_;
}

static TErrorOr<TDuration> ExtractDuration(TErrorOr<i64> timeNs)
{
    if (timeNs.IsOK()) {
        return TErrorOr<TDuration>(TDuration::MicroSeconds(timeNs.Value() / 1000));
    } else {
        return TError(timeNs);
    }
}

TCpuStatistics TPortoResourceTracker::ExtractCpuStatistics(const TResourceUsage& resourceUsage) const
{
    // NB: Job proxy uses last sample of CPU statistics but we are interested in
    // peak thread count value.
    auto currentThreadCountPeak = GetFieldOrError(resourceUsage.ContainerStats, EStatField::ThreadCount);

    PeakThreadCount_ = currentThreadCountPeak.IsOK() && PeakThreadCount_.IsOK()
        ? std::max<i64>(
            PeakThreadCount_.Value(),
            currentThreadCountPeak.Value())
        : currentThreadCountPeak.IsOK() ? currentThreadCountPeak : PeakThreadCount_;

    auto burstTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuBurstUsage);
    auto totalTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuUsage);
    auto systemTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuSystemUsage);
    auto userTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuUserUsage);
    auto waitTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuWait);
    auto throttledNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuThrottled);
    auto cfsThrottledNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuCfsThrottled);
    auto limitTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuLimit);
    auto guaranteeTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::CpuGuarantee);

    return TCpuStatistics{
        .BurstUsageTime = ExtractDuration(burstTimeNs),
        .TotalUsageTime = ExtractDuration(totalTimeNs),
        .UserUsageTime = ExtractDuration(userTimeNs),
        .SystemUsageTime = ExtractDuration(systemTimeNs),
        .WaitTime = ExtractDuration(waitTimeNs),
        .ThrottledTime = ExtractDuration(throttledNs),
        .CfsThrottledTime = ExtractDuration(cfsThrottledNs),
        .ThreadCount = GetFieldOrError(resourceUsage.ContainerStats, EStatField::ThreadCount),
        .ContextSwitches = GetFieldOrError(resourceUsage.ContainerStats, EStatField::ContextSwitches),
        .ContextSwitchesDelta = GetFieldOrError(resourceUsage.ContainerStats, EStatField::ContextSwitchesDelta),
        .PeakThreadCount = PeakThreadCount_,
        .LimitTime = ExtractDuration(limitTimeNs),
        .GuaranteeTime = ExtractDuration(guaranteeTimeNs),
    };
}

TMemoryStatistics TPortoResourceTracker::ExtractMemoryStatistics(const TResourceUsage& resourceUsage) const
{
    return TMemoryStatistics{
        .ResidentAnon = GetFieldOrError(resourceUsage.ContainerStats, EStatField::ResidentAnon),
        .TmpfsUsage = GetFieldOrError(resourceUsage.ContainerStats, EStatField::TmpfsUsage),
        .MappedFile = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MappedFile),
        .MinorPageFaults = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MinorPageFaults),
        .MajorPageFaults = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MajorPageFaults),
        .FileCacheUsage = GetFieldOrError(resourceUsage.ContainerStats, EStatField::FileCacheUsage),
        .AnonUsage = GetFieldOrError(resourceUsage.ContainerStats, EStatField::AnonMemoryUsage),
        .AnonLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::AnonMemoryLimit),
        .MemoryUsage = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MemoryUsage),
        .MemoryGuarantee = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MemoryGuarantee),
        .MemoryLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MemoryLimit),
        .MaxMemoryUsage = GetFieldOrError(resourceUsage.ContainerStats, EStatField::MaxMemoryUsage),
        .OomKills = GetFieldOrError(resourceUsage.ContainerStats, EStatField::OomKills),
        .OomKillsTotal = GetFieldOrError(resourceUsage.ContainerStats, EStatField::OomKillsTotal),
    };
}

TBlockIOStatistics TPortoResourceTracker::ExtractBlockIOStatistics(const TResourceUsage& resourceUsage) const
{
    return TBlockIOStatistics{
        .TotalIOStatistics = ExtractTotalBlockIOStatistics(resourceUsage),
        .DeviceIOStatistics = ExtractBlockIOPerDeviceStatistics(resourceUsage),
    };
}

TBlockIOStatistics::TIOStatistics TPortoResourceTracker::ExtractTotalBlockIOStatistics(const TResourceUsage& resourceUsage) const
{
    auto totalTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOTotalTime);
    auto waitTimeNs = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOWaitTime);

    return TBlockIOStatistics::TIOStatistics{
        .IOReadByte = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOReadByte),
        .IOWriteByte = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOWriteByte),
        .IOBytesLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOBytesLimit),
        .IOReadOps = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOReadOps),
        .IOWriteOps = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOWriteOps),
        .IOOps = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOOps),
        .IOOpsLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::IOOpsLimit),
        .IOTotalTime = ExtractDuration(totalTimeNs),
        .IOWaitTime = ExtractDuration(waitTimeNs),
    };
}

TBlockIOStatistics::TDeviceIOStatistics TPortoResourceTracker::ExtractBlockIOPerDeviceStatistics(
    const TResourceUsage& resourceUsage) const
{
    TBlockIOStatistics::TDeviceIOStatistics statistics;

    const auto writeStatisticsToHashMap = [&] <class T> (EStatField field, T TBlockIOStatistics::TIOStatistics::* statField) {
        auto statisticsVector = GetFieldOrError(resourceUsage.ContainerTaggedStats, field);
        if (statisticsVector.IsOK()) {
            for (const auto& [deviceName, value] : statisticsVector.Value()) {
                if constexpr (std::is_same_v<T, TErrorOr<TDuration>>) {
                    statistics[deviceName].*statField = ExtractDuration(value);
                } else {
                    statistics[deviceName].*statField = value;
                }
            }
        }
    };

    writeStatisticsToHashMap(EStatField::IOReadByte, &TBlockIOStatistics::TIOStatistics::IOReadByte);
    writeStatisticsToHashMap(EStatField::IOWriteByte, &TBlockIOStatistics::TIOStatistics::IOWriteByte);
    writeStatisticsToHashMap(EStatField::IOBytesLimit, &TBlockIOStatistics::TIOStatistics::IOBytesLimit);
    writeStatisticsToHashMap(EStatField::IOReadOps, &TBlockIOStatistics::TIOStatistics::IOReadOps);
    writeStatisticsToHashMap(EStatField::IOWriteOps, &TBlockIOStatistics::TIOStatistics::IOWriteOps);
    writeStatisticsToHashMap(EStatField::IOOps, &TBlockIOStatistics::TIOStatistics::IOOps);
    writeStatisticsToHashMap(EStatField::IOOpsLimit, &TBlockIOStatistics::TIOStatistics::IOOpsLimit);
    writeStatisticsToHashMap(EStatField::IOTotalTime, &TBlockIOStatistics::TIOStatistics::IOTotalTime);
    writeStatisticsToHashMap(EStatField::IOWaitTime, &TBlockIOStatistics::TIOStatistics::IOWaitTime);

    return statistics;
}

TNetworkStatistics TPortoResourceTracker::ExtractNetworkStatistics(const TResourceUsage& resourceUsage) const
{
    return TNetworkStatistics{
        .TxBytes = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetTxBytes),
        .TxPackets = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetTxPackets),
        .TxDrops = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetTxDrops),
        .TxLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetTxLimit),

        .RxBytes = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetRxBytes),
        .RxPackets = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetRxPackets),
        .RxDrops = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetRxDrops),
        .RxLimit = GetFieldOrError(resourceUsage.ContainerStats, EStatField::NetRxLimit),
    };
}

TVolumeStatistics TPortoResourceTracker::ExtractVolumeStatistics(const TResourceUsage& resourceUsage) const
{
    auto volumeCounts = GetFieldOrError(resourceUsage.ContainerTaggedStats, EStatField::VolumeCounts).ValueOrDefault({});

    std::vector<std::pair<TString, i64>> convertedVolumeCounts;
    convertedVolumeCounts.reserve(volumeCounts.size());

    for (const auto& [deviceName, value] : volumeCounts) {
        convertedVolumeCounts.emplace_back(deviceName, value);
    }

    return TVolumeStatistics{
        .VolumeCounts = convertedVolumeCounts,
    };
}

TLayerStatistics TPortoResourceTracker::ExtractLayerStatistics(const TResourceUsage& resourceUsage) const
{
    return TLayerStatistics{
        .LayerCounts = GetFieldOrError(resourceUsage.ContainerStats, EStatField::LayerCounts),
    };
}

TTotalStatistics TPortoResourceTracker::ExtractTotalStatistics(const TResourceUsage& resourceUsage) const
{
    return TTotalStatistics{
        .CpuStatistics = ExtractCpuStatistics(resourceUsage),
        .MemoryStatistics = ExtractMemoryStatistics(resourceUsage),
        .BlockIOStatistics = ExtractBlockIOStatistics(resourceUsage),
        .NetworkStatistics = ExtractNetworkStatistics(resourceUsage),
        .VolumeStatistics = ExtractVolumeStatistics(resourceUsage),
        .LayerStatistics = ExtractLayerStatistics(resourceUsage),
    };
}

TCpuStatistics TPortoResourceTracker::GetCpuStatistics() const
{
    return GetStatistics(
        CachedCpuStatistics_,
        "CPU",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractCpuStatistics(resourceUsage);
        });
}

TMemoryStatistics TPortoResourceTracker::GetMemoryStatistics() const
{
    return GetStatistics(
        CachedMemoryStatistics_,
        "memory",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractMemoryStatistics(resourceUsage);
        });
}

TBlockIOStatistics TPortoResourceTracker::GetBlockIOStatistics() const
{
    return GetStatistics(
        CachedBlockIOStatistics_,
        "block IO",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractBlockIOStatistics(resourceUsage);
        });
}

TNetworkStatistics TPortoResourceTracker::GetNetworkStatistics() const
{
    return GetStatistics(
        CachedNetworkStatistics_,
        "network",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractNetworkStatistics(resourceUsage);
        });
}

TTotalStatistics TPortoResourceTracker::GetTotalStatistics() const
{
    return GetStatistics(
        CachedTotalStatistics_,
        "total",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractTotalStatistics(resourceUsage);
        });
}

template <class T, class F>
T TPortoResourceTracker::GetStatistics(
    std::optional<T>& cachedStatistics,
    const TString& statisticsKind,
    F extractor) const
{
    UpdateResourceUsageStatisticsIfExpired();

    auto guard = Guard(SpinLock_);
    try {
        auto newStatistics = extractor(IsDeltaTracker_ ? ResourceUsageDelta_ : ResourceUsage_);
        cachedStatistics = newStatistics;
        return newStatistics;
    } catch (const std::exception& ex) {
        if (!cachedStatistics) {
            THROW_ERROR_EXCEPTION("Unable to get %v statistics", statisticsKind)
                << ex;
        }
        YT_LOG_WARNING(ex, "Unable to get %v statistics; using the last one", statisticsKind);
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
    if (IsForceUpdate_ || AreResourceUsageStatisticsExpired()) {
        DoUpdateResourceUsage();
    }
}

TErrorOr<i64> TPortoResourceTracker::CalculateCounterDelta(
    const TErrorOr<i64>& oldValue,
    const TErrorOr<i64>& newValue) const
{
    if (oldValue.IsOK() && newValue.IsOK()) {
        return newValue.Value() - oldValue.Value();
    } else if (newValue.IsOK()) {
        // It is better to return an error than an incorrect value.
        return oldValue;
    } else {
        return newValue;
    }
}

static bool IsCumulativeStatistics(EStatField statistic)
{
    return
        statistic == EStatField::CpuBurstUsage ||
        statistic == EStatField::CpuUsage ||
        statistic == EStatField::CpuUserUsage ||
        statistic == EStatField::CpuSystemUsage ||
        statistic == EStatField::CpuWait ||
        statistic == EStatField::CpuThrottled ||
        statistic == EStatField::CpuCfsThrottled ||

        statistic == EStatField::ContextSwitches ||

        statistic == EStatField::MinorPageFaults ||
        statistic == EStatField::MajorPageFaults ||

        statistic == EStatField::IOReadByte ||
        statistic == EStatField::IOWriteByte ||
        statistic == EStatField::IOReadOps ||
        statistic == EStatField::IOWriteOps ||
        statistic == EStatField::IOOps ||
        statistic == EStatField::IOTotalTime ||
        statistic == EStatField::IOWaitTime ||

        statistic == EStatField::NetTxBytes ||
        statistic == EStatField::NetTxPackets ||
        statistic == EStatField::NetTxDrops ||
        statistic == EStatField::NetRxBytes ||
        statistic == EStatField::NetRxPackets ||
        statistic == EStatField::NetRxDrops;
}

void TPortoResourceTracker::ReCalculateResourceUsage(const TResourceUsage& newResourceUsage) const
{
    auto guard = Guard(SpinLock_);

    TResourceUsage resourceUsage;
    TResourceUsage resourceUsageDelta;

    for (const auto& stat : InstanceStatFields) {
        TErrorOr<i64> oldValue;
        TErrorOr<i64> newValue;

        if (auto newValueIt = newResourceUsage.ContainerStats.find(stat); newValueIt.IsEnd()) {
            newValue = TError("Missing property %Qlv in Porto response", stat)
                << TErrorAttribute("container", Instance_->GetName());
        } else {
            newValue = newValueIt->second;
        }

        if (auto oldValueIt = ResourceUsage_.ContainerStats.find(stat); oldValueIt.IsEnd()) {
            oldValue = newValue;
        } else {
            oldValue = oldValueIt->second;
        }

        if (newValue.IsOK()) {
            resourceUsage.ContainerStats[stat] = newValue;
        } else {
            resourceUsage.ContainerStats[stat] = oldValue;
        }

        if (IsCumulativeStatistics(stat)) {
            resourceUsageDelta.ContainerStats[stat] = CalculateCounterDelta(oldValue, newValue);
        } else {
            if (newValue.IsOK()) {
                resourceUsageDelta.ContainerStats[stat] = newValue;
            } else {
                resourceUsageDelta.ContainerStats[stat] = oldValue;
            }
        }
    }

    resourceUsage.ContainerTaggedStats = newResourceUsage.ContainerTaggedStats;
    resourceUsageDelta.ContainerTaggedStats = newResourceUsage.ContainerTaggedStats;

    ResourceUsage_ = resourceUsage;
    ResourceUsageDelta_ = resourceUsageDelta;
    LastUpdateTime_.store(TInstant::Now());
}

void TPortoResourceTracker::DoUpdateResourceUsage() const
{
    try {
        ReCalculateResourceUsage(Instance_->GetResourceUsage());
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(
            ex,
            "Couldn't get metrics from Porto");
    }
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceProfiler::TPortoResourceProfiler(
    TPortoResourceTrackerPtr tracker,
    TPodSpecConfigPtr podSpec,
    const TProfiler& profiler)
    : ResourceTracker_(std::move(tracker))
    , PodSpec_(std::move(podSpec))
    , UpdateBufferActionQueue_(New<TActionQueue>("PortoResourceProfiler"))
    , UpdateBufferPeriodicExecutor_(New<TPeriodicExecutor>(
        UpdateBufferActionQueue_->GetInvoker(),
        BIND(&TPortoResourceProfiler::DoUpdateBuffer, MakeWeak(this)),
        ResourceUsageUpdatePeriod))
{
    profiler.AddProducer("", MakeStrong(this));

    UpdateBufferPeriodicExecutor_->Start();
}

TPortoResourceProfiler::~TPortoResourceProfiler()
{
    YT_UNUSED_FUTURE(UpdateBufferPeriodicExecutor_->Stop());
}

static void WriteGaugeIfOk(
    ISensorWriter* writer,
    const TString& path,
    TErrorOr<i64> valueOrError)
{
    if (valueOrError.IsOK()) {
        i64 value = static_cast<i64>(valueOrError.Value());

        if (value >= 0) {
            writer->AddGauge(path, value);
        }
    }
}

static void WriteCumulativeGaugeIfOk(
    ISensorWriter* writer,
    const TString& path,
    TErrorOr<i64> valueOrError,
    i64 timeDeltaUsec)
{
    if (valueOrError.IsOK()) {
        i64 value = static_cast<i64>(valueOrError.Value());

        if (value >= 0) {
            writer->AddGauge(path,
                1.0 * value * ResourceUsageUpdatePeriod.MicroSeconds() / timeDeltaUsec);
        }
    }
}

void TPortoResourceProfiler::WriteCpuMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    {
        if (totalStatistics.CpuStatistics.BurstUsageTime.IsOK()) {
            i64 burstUsageTimeUs = totalStatistics.CpuStatistics.BurstUsageTime.Value().MicroSeconds();
            double burstUsagePercent = std::max<double>(0.0, 100. * burstUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/burst", burstUsagePercent);
        }

        if (totalStatistics.CpuStatistics.UserUsageTime.IsOK()) {
            i64 userUsageTimeUs = totalStatistics.CpuStatistics.UserUsageTime.Value().MicroSeconds();
            double userUsagePercent = std::max<double>(0.0, 100. * userUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/user", userUsagePercent);
        }

        if (totalStatistics.CpuStatistics.SystemUsageTime.IsOK()) {
            i64 systemUsageTimeUs = totalStatistics.CpuStatistics.SystemUsageTime.Value().MicroSeconds();
            double systemUsagePercent = std::max<double>(0.0, 100. * systemUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/system", systemUsagePercent);
        }

        if (totalStatistics.CpuStatistics.WaitTime.IsOK()) {
            i64 waitTimeUs = totalStatistics.CpuStatistics.WaitTime.Value().MicroSeconds();
            double waitPercent = std::max<double>(0.0, 100. * waitTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/wait", waitPercent);
        }

        if (totalStatistics.CpuStatistics.ThrottledTime.IsOK()) {
            i64 throttledTimeUs = totalStatistics.CpuStatistics.ThrottledTime.Value().MicroSeconds();
            double throttledPercent = std::max<double>(0.0, 100. * throttledTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/throttled", throttledPercent);
        }

        if (totalStatistics.CpuStatistics.CfsThrottledTime.IsOK()) {
            i64 cfsThrottledTimeUs = totalStatistics.CpuStatistics.CfsThrottledTime.Value().MicroSeconds();
            double cfsThrottledPercent = std::max<double>(0.0, 100. * cfsThrottledTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/cfs_throttled", cfsThrottledPercent);
        }

        if (totalStatistics.CpuStatistics.TotalUsageTime.IsOK()) {
            i64 totalUsageTimeUs = totalStatistics.CpuStatistics.TotalUsageTime.Value().MicroSeconds();
            double totalUsagePercent = std::max<double>(0.0, 100. * totalUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/total", totalUsagePercent);
        }

        if (totalStatistics.CpuStatistics.GuaranteeTime.IsOK()) {
            i64 guaranteeTimeUs = totalStatistics.CpuStatistics.GuaranteeTime.Value().MicroSeconds();
            double guaranteePercent = std::max<double>(0.0, (100. * guaranteeTimeUs) / (1'000'000L));
            writer->AddGauge("/cpu/guarantee", guaranteePercent);
        }

        if (totalStatistics.CpuStatistics.LimitTime.IsOK()) {
            i64 limitTimeUs = totalStatistics.CpuStatistics.LimitTime.Value().MicroSeconds();
            double limitPercent = std::max<double>(0.0, (100. * limitTimeUs) / (1'000'000L));
            writer->AddGauge("/cpu/limit", limitPercent);
        }
    }

    if (PodSpec_->CpuToVCpuFactor) {
        auto factor = *PodSpec_->CpuToVCpuFactor;

        writer->AddGauge("/cpu_to_vcpu_factor", factor);

        if (totalStatistics.CpuStatistics.BurstUsageTime.IsOK()) {
            i64 burstUsageTimeUs = totalStatistics.CpuStatistics.BurstUsageTime.Value().MicroSeconds();
            double burstUsagePercent = std::max<double>(0.0, 100. * burstUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/burst", burstUsagePercent);
        }

        if (totalStatistics.CpuStatistics.UserUsageTime.IsOK()) {
            i64 userUsageTimeUs = totalStatistics.CpuStatistics.UserUsageTime.Value().MicroSeconds();
            double userUsagePercent = std::max<double>(0.0, 100. * userUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/user", userUsagePercent);
        }

        if (totalStatistics.CpuStatistics.SystemUsageTime.IsOK()) {
            i64 systemUsageTimeUs = totalStatistics.CpuStatistics.SystemUsageTime.Value().MicroSeconds();
            double systemUsagePercent = std::max<double>(0.0, 100. * systemUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/system", systemUsagePercent);
        }

        if (totalStatistics.CpuStatistics.WaitTime.IsOK()) {
            i64 waitTimeUs = totalStatistics.CpuStatistics.WaitTime.Value().MicroSeconds();
            double waitPercent = std::max<double>(0.0, 100. * waitTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/wait", waitPercent);
        }

        if (totalStatistics.CpuStatistics.ThrottledTime.IsOK()) {
            i64 throttledTimeUs = totalStatistics.CpuStatistics.ThrottledTime.Value().MicroSeconds();
            double throttledPercent = std::max<double>(0.0, 100. * throttledTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/throttled", throttledPercent);
        }

        if (totalStatistics.CpuStatistics.CfsThrottledTime.IsOK()) {
            i64 cfsThrottledTimeUs = totalStatistics.CpuStatistics.CfsThrottledTime.Value().MicroSeconds();
            double cfsThrottledPercent = std::max<double>(0.0, 100. * cfsThrottledTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/cfs_throttled", cfsThrottledPercent);
        }

        if (totalStatistics.CpuStatistics.TotalUsageTime.IsOK()) {
            i64 totalUsageTimeUs = totalStatistics.CpuStatistics.TotalUsageTime.Value().MicroSeconds();
            double totalUsagePercent = std::max<double>(0.0, 100. * totalUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/total", totalUsagePercent);
        }

        if (totalStatistics.CpuStatistics.GuaranteeTime.IsOK()) {
            i64 guaranteeTimeUs = totalStatistics.CpuStatistics.GuaranteeTime.Value().MicroSeconds();
            double guaranteePercent = std::max<double>(0.0, 100. * guaranteeTimeUs * factor / 1'000'000L);
            writer->AddGauge("/vcpu/guarantee", guaranteePercent);
        }

        if (totalStatistics.CpuStatistics.LimitTime.IsOK()) {
            i64 limitTimeUs = totalStatistics.CpuStatistics.LimitTime.Value().MicroSeconds();
            double limitPercent = std::max<double>(0.0, 100. * limitTimeUs * factor / 1'000'000L);
            writer->AddGauge("/vcpu/limit", limitPercent);
        }
    }

    WriteGaugeIfOk(writer, "/cpu/thread_count", totalStatistics.CpuStatistics.ThreadCount);
    WriteGaugeIfOk(writer, "/cpu/context_switches", totalStatistics.CpuStatistics.ContextSwitches);
}

void TPortoResourceProfiler::WriteMemoryMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(writer,
        "/memory/minor_page_faults",
        totalStatistics.MemoryStatistics.MinorPageFaults,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/memory/major_page_faults",
        totalStatistics.MemoryStatistics.MajorPageFaults,
        timeDeltaUsec);

    WriteGaugeIfOk(writer, "/memory/oom_kills", totalStatistics.MemoryStatistics.OomKills);
    WriteGaugeIfOk(writer, "/memory/oom_kills_total", totalStatistics.MemoryStatistics.OomKillsTotal);

    WriteGaugeIfOk(writer, "/memory/file_cache_usage", totalStatistics.MemoryStatistics.FileCacheUsage);
    WriteGaugeIfOk(writer, "/memory/anon_usage", totalStatistics.MemoryStatistics.AnonUsage);
    WriteGaugeIfOk(writer, "/memory/anon_limit", totalStatistics.MemoryStatistics.AnonLimit);
    WriteGaugeIfOk(writer, "/memory/memory_usage", totalStatistics.MemoryStatistics.MemoryUsage);
    WriteGaugeIfOk(writer, "/memory/memory_guarantee", totalStatistics.MemoryStatistics.MemoryGuarantee);
    WriteGaugeIfOk(writer, "/memory/memory_limit", totalStatistics.MemoryStatistics.MemoryLimit);
}

void TPortoResourceProfiler::WriteVolumeMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 /*timeDeltaUsec*/)
{
    for (const auto& [key, count] : totalStatistics.VolumeStatistics.VolumeCounts) {
        auto guard = TWithTagGuard(writer, "backend", key);
        writer->AddGauge("/volume/count", count);
    }
}

void TPortoResourceProfiler::WriteBlockingIOMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteBlockingIOMetrics(writer, totalStatistics.BlockIOStatistics.TotalIOStatistics, timeDeltaUsec);
}

void TPortoResourceProfiler::WriteBlockingIOPerDeviceMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    for (const auto& [device, statistics] : totalStatistics.BlockIOStatistics.DeviceIOStatistics) {
        auto guard = TWithTagGuard(writer, "device_name", device);
        WriteBlockingIOMetrics(writer, statistics, timeDeltaUsec);
    }
}

void TPortoResourceProfiler::WriteBlockingIOMetrics(
    ISensorWriter* writer,
    const TBlockIOStatistics::TIOStatistics& blockIOStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(writer,
        "/io/read_bytes",
        blockIOStatistics.IOReadByte,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/write_bytes",
        blockIOStatistics.IOWriteByte,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/read_ops",
        blockIOStatistics.IOReadOps,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/write_ops",
        blockIOStatistics.IOWriteOps,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/ops",
        blockIOStatistics.IOOps,
        timeDeltaUsec);

    WriteGaugeIfOk(writer,
        "/io/bytes_limit",
        blockIOStatistics.IOBytesLimit);
    WriteGaugeIfOk(writer,
        "/io/ops_limit",
        blockIOStatistics.IOOpsLimit);

    if (blockIOStatistics.IOTotalTime.IsOK()) {
        i64 totalTimeUs = blockIOStatistics.IOTotalTime.Value().MicroSeconds();
        double totalPercent = std::max<double>(0.0, 100. * totalTimeUs / timeDeltaUsec);
        writer->AddGauge("/io/total", totalPercent);
    }

    if (blockIOStatistics.IOWaitTime.IsOK()) {
        i64 waitTimeUs = blockIOStatistics.IOWaitTime.Value().MicroSeconds();
        double waitPercent = std::max<double>(0.0, 100. * waitTimeUs / timeDeltaUsec);
        writer->AddGauge("/io/wait", waitPercent);
    }
}

void TPortoResourceProfiler::WriteLayerMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 /*timeDeltaUsec*/)
{
    if (totalStatistics.LayerStatistics.LayerCounts.IsOK()) {
        writer->AddGauge("/layer/count", totalStatistics.LayerStatistics.LayerCounts.Value());
    }
}

void TPortoResourceProfiler::WriteNetworkMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_bytes",
        totalStatistics.NetworkStatistics.RxBytes,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_drops",
        totalStatistics.NetworkStatistics.RxDrops,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_packets",
        totalStatistics.NetworkStatistics.RxPackets,
        timeDeltaUsec);
    WriteGaugeIfOk(
        writer,
        "/network/rx_limit",
        totalStatistics.NetworkStatistics.RxLimit);

    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_bytes",
        totalStatistics.NetworkStatistics.TxBytes,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_drops",
        totalStatistics.NetworkStatistics.TxDrops,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_packets",
        totalStatistics.NetworkStatistics.TxPackets,
        timeDeltaUsec);
    WriteGaugeIfOk(
        writer,
        "/network/tx_limit",
        totalStatistics.NetworkStatistics.TxLimit);
}

void TPortoResourceProfiler::DoUpdateBuffer()
{
    TSensorBuffer buffer;
    CollectSensors(&buffer);
    Update(std::move(buffer));
}

void TPortoResourceProfiler::CollectSensors(ISensorWriter* writer)
{
    i64 lastUpdate = ResourceTracker_->GetLastUpdateTime().MicroSeconds();

    auto totalStatistics = ResourceTracker_->GetTotalStatistics();
    i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - lastUpdate;

    WriteCpuMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteMemoryMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteBlockingIOMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteBlockingIOPerDeviceMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteNetworkMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteVolumeMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteLayerMetrics(writer, totalStatistics, timeDeltaUsec);
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceProfilerPtr CreatePortoProfilerWithTags(
    const IInstancePtr& instance,
    const TString containerCategory,
    const TPodSpecConfigPtr& podSpec)
{
    auto portoResourceTracker = New<TPortoResourceTracker>(
        instance,
        ResourceUsageUpdatePeriod,
        true,
        true);

    return New<TPortoResourceProfiler>(
        portoResourceTracker,
        podSpec,
        TProfiler("/porto")
            .WithTag("container_category", containerCategory));
}

////////////////////////////////////////////////////////////////////////////////

#endif

#ifdef __linux__
void EnablePortoResourceTracker(const TPodSpecConfigPtr& podSpec)
{
    BIND([=] {
        auto executor = CreatePortoExecutor(New<TPortoExecutorDynamicConfig>(), "porto-tracker");

        executor->SubscribeFailed(BIND([=] (const TError& error) {
            YT_LOG_ERROR(error, "Fatal error during Porto polling");
        }));

        LeakyRefCountedSingleton<TPortoProfilers>(
            CreatePortoProfilerWithTags(GetSelfPortoInstance(executor), "daemon", podSpec),
            CreatePortoProfilerWithTags(GetRootPortoInstance(executor), "pod", podSpec));
    }).AsyncVia(GetCurrentInvoker())
    .Run()
    .Subscribe(BIND([] (const TError& error) {
        YT_LOG_ERROR_IF(!error.IsOK(), error, "Failed to enable Porto profiler");
    }));
}
#else
void EnablePortoResourceTracker(const TPodSpecConfigPtr& /*podSpec*/)
{
    YT_LOG_WARNING("Porto resource tracker not supported");
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
