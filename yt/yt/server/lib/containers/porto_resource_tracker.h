#pragma once

#include <yt/yt/server/lib/containers/instance.h>
#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/ytlib/cgroup/cgroup.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/net/address.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/profiling/producer.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ResourceUsageUpdatePeriod = TDuration::MilliSeconds(1000);

////////////////////////////////////////////////////////////////////////////////

using TCpuStatistics = NCGroup::TCpuAccounting::TStatistics;
using TBlockIOStatistics = NCGroup::TBlockIO::TStatistics;
using TMemoryStatistics = NCGroup::TMemory::TStatistics;
using TNetworkStatistics = NCGroup::TNetwork::TStatistics;

struct TTotalStatistics
{
public:
    TCpuStatistics CpuStatistics;
    TMemoryStatistics MemoryStatistics;
    TBlockIOStatistics BlockIOStatistics;
    TNetworkStatistics NetworkStatistics;
};

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

class TPortoResourceTracker 
    : public TRefCounted
{
public:
    TPortoResourceTracker(
        NContainers::IInstancePtr instance,
        TDuration updatePeriod);

    TCpuStatistics GetCpuStatistics() const;

    TBlockIOStatistics GetBlockIOStatistics() const;

    TMemoryStatistics GetMemoryStatistics() const;

    TNetworkStatistics GetNetworkStatistics() const;

    TTotalStatistics GetTotalStatistics() const;

    bool AreResourceUsageStatisticsExpired() const;

    TInstant GetLastUpdateTime() const;

private:
    const NContainers::IInstancePtr Instance_;
    const TDuration UpdatePeriod_;

    mutable std::atomic<TInstant> LastUpdateTime_ = {};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    mutable NContainers::TResourceUsage ResourceUsage_;
    mutable std::optional<TCpuStatistics> CachedCpuStatistics_;
    mutable std::optional<TMemoryStatistics> CachedMemoryStatistics_;
    mutable std::optional<TBlockIOStatistics> CachedBlockIOStatistics_;
    mutable std::optional<TNetworkStatistics> CachedNetworkStatistics_;
    mutable std::optional<TTotalStatistics> CachedTotalStatistics_;

    mutable ui64 PeakThreadCount_ = 0;

    template <class T, class F>
    T GetStatistics(
        std::optional<T> &cachedStatistics,
        const TString &statisticsKind,
        F func) const;

    TCpuStatistics ExtractCpuStatistics() const;
    TMemoryStatistics ExtractMemoryStatistics() const;
    TBlockIOStatistics ExtractBlockIOStatistics() const;
    TNetworkStatistics ExtractNetworkStatistics() const;
    TTotalStatistics ExtractTotalStatistics() const;

    void UpdateResourceUsageStatisticsIfExpired() const;

    void DoUpdateResourceUsage() const;
};

DECLARE_REFCOUNTED_TYPE(TPortoResourceTracker)
DEFINE_REFCOUNTED_TYPE(TPortoResourceTracker)

////////////////////////////////////////////////////////////////////////////////

#endif

} // namespace NYT::NContainers
