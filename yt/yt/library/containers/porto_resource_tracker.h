#pragma once

#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cgroup.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/profiling/producer.h>

namespace NYT::NContainers {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ResourceUsageUpdatePeriod = TDuration::MilliSeconds(1000);

////////////////////////////////////////////////////////////////////////////////

using TCpuStatistics = TCpuAccounting::TStatistics;
using TBlockIOStatistics = TBlockIO::TStatistics;
using TMemoryStatistics = TMemory::TStatistics;
using TNetworkStatistics = TNetwork::TStatistics;

////////////////////////////////////////////////////////////////////////////////

struct TVolumeStatistics
{
    std::vector<std::pair<TString, i64>> VolumeCounts;
};

////////////////////////////////////////////////////////////////////////////////

struct TLayerStatistics
{
    TErrorOr<i64> LayerCounts;
};

////////////////////////////////////////////////////////////////////////////////

struct TTotalStatistics
{
    TCpuStatistics CpuStatistics;
    TMemoryStatistics MemoryStatistics;
    TBlockIOStatistics BlockIOStatistics;
    TNetworkStatistics NetworkStatistics;
    TVolumeStatistics VolumeStatistics;
    TLayerStatistics LayerStatistics;
};

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

class TPortoResourceTracker
    : public TRefCounted
{
public:
    TPortoResourceTracker(
        IInstancePtr instance,
        TDuration updatePeriod,
        bool isDeltaTracker = false,
        bool isForceUpdate = false);

    TCpuStatistics GetCpuStatistics() const;

    TBlockIOStatistics GetBlockIOStatistics() const;

    TMemoryStatistics GetMemoryStatistics() const;

    TNetworkStatistics GetNetworkStatistics() const;

    TTotalStatistics GetTotalStatistics() const;

    bool AreResourceUsageStatisticsExpired() const;

    TInstant GetLastUpdateTime() const;

private:
    const IInstancePtr Instance_;
    const TDuration UpdatePeriod_;
    const bool IsDeltaTracker_;
    const bool IsForceUpdate_;

    mutable std::atomic<TInstant> LastUpdateTime_ = {};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    mutable TResourceUsage ResourceUsage_;
    mutable TResourceUsage ResourceUsageDelta_;

    mutable std::optional<TCpuStatistics> CachedCpuStatistics_;
    mutable std::optional<TMemoryStatistics> CachedMemoryStatistics_;
    mutable std::optional<TBlockIOStatistics> CachedBlockIOStatistics_;
    mutable std::optional<TNetworkStatistics> CachedNetworkStatistics_;
    mutable std::optional<TTotalStatistics> CachedTotalStatistics_;
    mutable TErrorOr<i64> PeakThreadCount_ = 0;

    template <class T, class F>
    T GetStatistics(
        std::optional<T>& cachedStatistics,
        const TString& statisticsKind,
        F extractor) const;

    TCpuStatistics ExtractCpuStatistics(const TResourceUsage& resourceUsage) const;
    TMemoryStatistics ExtractMemoryStatistics(const TResourceUsage& resourceUsage) const;
    TBlockIOStatistics ExtractBlockIOStatistics(const TResourceUsage& resourceUsage) const;
    TBlockIOStatistics::TIOStatistics ExtractTotalBlockIOStatistics(const TResourceUsage& resourceUsage) const;
    TBlockIOStatistics::TDeviceIOStatistics ExtractBlockIOPerDeviceStatistics(const TResourceUsage& resourceUsage) const;
    TNetworkStatistics ExtractNetworkStatistics(const TResourceUsage& resourceUsage) const;
    TVolumeStatistics ExtractVolumeStatistics(const TResourceUsage& resourceUsage) const;
    TLayerStatistics ExtractLayerStatistics(const TResourceUsage& resourceUsage) const;
    TTotalStatistics ExtractTotalStatistics(const TResourceUsage& resourceUsage) const;

    TErrorOr<i64> CalculateCounterDelta(
        const TErrorOr<i64>& oldValue,
        const TErrorOr<i64>& newValue) const;

    void ReCalculateResourceUsage(const TResourceUsage& newResourceUsage) const;

    void UpdateResourceUsageStatisticsIfExpired() const;

    void DoUpdateResourceUsage() const;
};

DEFINE_REFCOUNTED_TYPE(TPortoResourceTracker)

////////////////////////////////////////////////////////////////////////////////

class TPortoResourceProfiler
    : public TBufferedProducer
{
public:
    TPortoResourceProfiler(
        TPortoResourceTrackerPtr tracker,
        TPodSpecConfigPtr podSpec,
        const TProfiler& profiler = TProfiler{"/porto"});

    ~TPortoResourceProfiler();

    void CollectSensors(ISensorWriter* writer) override;

private:
    const TPortoResourceTrackerPtr ResourceTracker_;
    const TPodSpecConfigPtr PodSpec_;
    const NConcurrency::TActionQueuePtr UpdateBufferActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr UpdateBufferPeriodicExecutor_;

    void WriteCpuMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteMemoryMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteBlockingIOMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteBlockingIOPerDeviceMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteNetworkMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteVolumeMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteLayerMetrics(
        ISensorWriter* writer,
        TTotalStatistics& totalStatistics,
        i64 timeDeltaUsec);

    void WriteBlockingIOMetrics(
        ISensorWriter* writer,
        const TBlockIOStatistics::TIOStatistics& blockIOStatistics,
        i64 timeDeltaUsec);

    void DoUpdateBuffer();
};

DECLARE_REFCOUNTED_TYPE(TPortoResourceProfiler)
DEFINE_REFCOUNTED_TYPE(TPortoResourceProfiler)

////////////////////////////////////////////////////////////////////////////////

#endif

void EnablePortoResourceTracker(const TPodSpecConfigPtr& podSpec);

} // namespace NYT::NContainers
