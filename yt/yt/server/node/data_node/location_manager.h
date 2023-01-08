#pragma once

#include "public.h"

#include <yt/yt/library/containers/disk_manager/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TLocationLivenessInfo
{
    TStoreLocationPtr Location;
    TString DiskId;
    bool IsLocationPendingDecommission;
    bool IsDiskAlive;
};

////////////////////////////////////////////////////////////////////////////////

class TLocationManager
    : public TRefCounted
{
public:
    TLocationManager(
        TChunkStorePtr chunkStore,
        IInvokerPtr controlInvoker,
        NContainers::TDiskInfoProviderPtr diskInfoProvider);

    TFuture<std::vector<TLocationLivenessInfo>> GetLocationsLiveliness();

    TFuture<std::vector<TGuid>> ReleaseLocations(const THashSet<TGuid>& locationUuids);

    TFuture<std::vector<TErrorOr<void>>> RecoverDisks(const THashSet<TString>& diskIds);

private:
    const TChunkStorePtr ChunkStore_;
    const IInvokerPtr ControlInvoker_;
    const NContainers::TDiskInfoProviderPtr DiskInfoProvider_;

    std::vector<TLocationLivenessInfo> MapLocationToLivelinessInfo(
        const std::vector<NContainers::TDiskInfo>& failedDisks);

    std::vector<TGuid> MarkLocationsForDecommissioning(
        const std::vector<NContainers::TDiskInfo>& disks,
        const THashSet<TGuid>& locationUuids);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TLocationManager)

////////////////////////////////////////////////////////////////////////////////

class TLocationHealthChecker
    : public TRefCounted
{
public:
    TLocationHealthChecker(
        TLocationManagerPtr locationManager,
        IInvokerPtr invoker,
        TLocationHealthCheckerConfigPtr config);

    void Start();

    void OnHealthCheck();

    void OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig);

private:
    const TLocationHealthCheckerConfigPtr Config_;
    bool Enabled_;

    const IInvokerPtr Invoker_;

    const TLocationManagerPtr LocationManager_;
    NConcurrency::TPeriodicExecutorPtr HealthCheckerExecutor_;
};

DEFINE_REFCOUNTED_TYPE(TLocationHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
