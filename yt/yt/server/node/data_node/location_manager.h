#pragma once

#include "public.h"

#include "disk_location.h"

#include <yt/yt/library/disk_manager/public.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TLocationLivenessInfo
{
    TStoreLocationPtr Location;
    TString DiskId;
    ELocationState LocationState;
    bool IsDiskAlive;
    NDiskManager::EDiskState DiskState;
};

////////////////////////////////////////////////////////////////////////////////

class TLocationManager
    : public TRefCounted
{
public:
    TLocationManager(
        IBootstrap* bootstrap,
        TChunkStorePtr chunkStore,
        IInvokerPtr controlInvoker,
        NDiskManager::IDiskInfoProviderPtr diskInfoProvider);

    void SetFailedDiskAlerts(std::vector<TError> alerts);

    void SetWaitingReplacementDiskAlerts(std::vector<TError> alerts);

    void SetFailedUnlinkedDiskIds(std::vector<TString> diskIds);

    TFuture<std::vector<NDiskManager::TDiskInfo>> GetDiskInfos();

    TFuture<bool> GetHotSwapEnabled();

    TFuture<std::vector<TGuid>> ResurrectChunkLocations(const THashSet<TGuid>& locationUuids);

    TFuture<std::vector<TGuid>> DisableChunkLocations(const THashSet<TGuid>& locationUuids);

    TFuture<std::vector<TGuid>> DestroyChunkLocations(
        bool recoverUnlinkedDisks,
        const THashSet<TGuid>& locationUuids);

    TFuture<void> FailDiskByName(
        const TString& diskName,
        const TError& error);

    TFuture<void> UpdateDiskCache();

    TFuture<void> RecoverDisk(const TString& diskId);

    NYTree::IYPathServicePtr GetOrchidService();

    std::vector<TLocationLivenessInfo> MapLocationToLivenessInfo(
        const std::vector<NDiskManager::TDiskInfo>& diskInfos);

private:
    const NDiskManager::IDiskInfoProviderPtr DiskInfoProvider_;
    const TChunkStorePtr ChunkStore_;
    const IInvokerPtr ControlInvoker_;

    NThreading::TAtomicObject<std::vector<TError>> DiskFailedAlerts_;
    NThreading::TAtomicObject<std::vector<TError>> DiskWaitingReplacementAlerts_;
    NThreading::TAtomicObject<std::vector<TString>> FailedUnlinkedDiskIds_;

    NYTree::IYPathServicePtr CreateOrchidService();

    void PopulateAlerts(std::vector<TError>* alerts);

    void BuildOrchid(NYson::IYsonConsumer* consumer);

    std::vector<TGuid> DoResurrectLocations(const THashSet<TGuid>& locationUuids);

    std::vector<TGuid> DoDisableLocations(const THashSet<TGuid>& locationUuids);

    std::vector<TGuid> DoDestroyLocations(bool recoverUnlinkedDisks, const THashSet<TGuid>& locationUuids);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TLocationManager)

////////////////////////////////////////////////////////////////////////////////

class TLocationHealthChecker
    : public TRefCounted
{
public:
    TLocationHealthChecker(
        TChunkStorePtr chunkStore,
        TLocationManagerPtr locationManager,
        IInvokerPtr invoker,
        TRestartManagerPtr restartManager,
        const NProfiling::TProfiler& profiler);

    void Initialize();

    void Start();

    void OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig);

private:
    TAtomicIntrusivePtr<TLocationHealthCheckerDynamicConfig> DynamicConfig_;

    const TChunkStorePtr ChunkStore_;
    const TLocationManagerPtr LocationManager_;
    const IInvokerPtr Invoker_;
    const TRestartManagerPtr RestartManager_;

    NConcurrency::TPeriodicExecutorPtr HealthCheckerExecutor_;
    const NProfiling::TProfiler Profiler_;

    TEnumIndexedArray<NDiskManager::EDiskState, TEnumIndexedArray<NDiskManager::EStorageClass, NProfiling::TGauge>> Gauges_;

    void OnHealthCheck();

    void OnDiskHealthCheck(const std::vector<NDiskManager::TDiskInfo>& diskInfos);

    void OnLocationsHealthCheck();

    void PushCounters(std::vector<NDiskManager::TDiskInfo> diskInfos);

    void HandleHotSwap(std::vector<NDiskManager::TDiskInfo> disks);

    void OnDiskHealthCheckFailed(
        const TStoreLocationPtr& location,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TLocationHealthChecker)

////////////////////////////////////////////////////////////////////////////////

TLocationHealthCheckerPtr CreateLocationHealthChecker(
    TChunkStorePtr chunkStore,
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TRestartManagerPtr restartManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
