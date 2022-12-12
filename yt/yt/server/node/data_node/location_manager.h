#pragma once

#include "public.h"

#include <yt/yt/library/containers/disk_manager/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TLocationManager
    : public TRefCounted
{
public:
    TLocationManager(
        TChunkStorePtr chunkStore,
        IInvokerPtr controlInvoker,
        NContainers::TDiskInfoProviderPtr diskInfoProvider);

    TFuture<std::vector<TStoreLocationPtr>> GetFailedLocations();

private:
    const TChunkStorePtr ChunkStore_;
    const IInvokerPtr ControlInvoker_;
    const NContainers::TDiskInfoProviderPtr DiskInfoProvider_;

    std::vector<TStoreLocationPtr> GetDiskLocations(
        const std::vector<NContainers::TDiskInfo>& failedDisks);

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
    std::atomic<bool> Enabled_;

    const IInvokerPtr Invoker_;

    const TLocationManagerPtr LocationManager_;
    NConcurrency::TPeriodicExecutorPtr HealthCheckerExecutor_;
};

DEFINE_REFCOUNTED_TYPE(TLocationHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
