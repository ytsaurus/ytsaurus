#pragma once

#include "private.h"
#include "preparation_options.h"
#include "volume.h"
#include "volume_counters.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/public.h>
#include <yt/yt/server/node/data_node/location.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using NServer::TDiskHealthCheckerPtr;
using NServer::TDiskHealthCheckerConfigPtr;
using NProfiling::TTagSet;
using NProfiling::TEventTimerGuard;

class TOverlayData;

////////////////////////////////////////////////////////////////////////////////

class TLayerLocation
    : public NNode::TDiskLocation
{
public:
    TLayerLocation(
        NDataNode::TLayerLocationConfigPtr locationConfig,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TDiskHealthCheckerConfigPtr healthCheckerConfig,
        NContainers::IPortoExecutorPtr volumeExecutor,
        NContainers::IPortoExecutorPtr layerExecutor,
        const TString& id);

    TFuture<void> Initialize();

    TFuture<TVolumeMeta> CreateNbdVolume(
        TGuid tag,
        TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options);

    TFuture<TVolumeMeta> CreateOverlayVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray);

    TFuture<TVolumeMeta> CreateSquashFSVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath);

    TFuture<TVolumeMeta> CreateTmpfsVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParams tmpfsVolume);

    void Disable(const TError& error, bool persistentDisable = true);

    TFuture<void> GetVolumeReleaseEvent();

    TError GetAlert();

    std::vector<TLayerMeta> GetAllLayers() const;

    i64 GetAvailableSpace();

    i64 GetCapacity();

    int GetLayerCount() const;

    TLayerLocationPerformanceCounters& GetPerformanceCounters();

    TFuture<TLayerMeta> ImportLayer(
        const TArtifactKey& artifactKey,
        const TString& archivePath,
        const TString& container,
        TLayerId layerId,
        TGuid tag);

    bool IsFull();

    bool IsLayerImportInProgress() const;

    void OnDynamicConfigChanged(
        const TLayerCacheDynamicConfigPtr& oldConfig,
        const TLayerCacheDynamicConfigPtr& newConfig);

    TFuture<void> RemoveLayer(const TLayerId& layerId);

    TFuture<void> RemoveVolume(TTagSet tagSet, TVolumeId volumeId);

    //! TODO(yuryalekseev): Remove me when slot rbind is removed.
    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const TString& slotPath);

    bool ResidesOnTmpfs() const;

    TFuture<void> LinkVolume(
        TGuid tag,
        const TString& source,
        const TString& target);

    TFuture<void> UnlinkVolume(
        const TString& source,
        const TString& target);

    int GetVolumeCount() const;

    i64 GetUsedSpace() const;

private:
    const NDataNode::TLayerLocationConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    TAtomicIntrusivePtr<TLayerCacheDynamicConfig> DynamicConfig_;
    const NContainers::IPortoExecutorPtr VolumeExecutor_;
    const NContainers::IPortoExecutorPtr LayerExecutor_;

    const NConcurrency::TActionQueuePtr LocationQueue_;
    const TString VolumesPath_;
    const TString VolumesMetaPath_;
    const TString LayersPath_;
    const TString LayersMetaPath_;
    const TString PlacePath_;

    TDiskHealthCheckerPtr HealthChecker_;

    TLayerLocationPerformanceCounters PerformanceCounters_;

    std::atomic<int> LayerImportsInProgress_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    THashMap<TLayerId, TLayerMeta> Layers_;
    THashMap<TVolumeId, TVolumeMeta> Volumes_;

    TPromise<void> VolumesReleaseEvent_ = MakePromise<void>(TError());

    mutable i64 AvailableSpace_ = 0;
    i64 UsedSpace_ = 0;
    TError Alert_;

    std::string GetLayerPath(const TLayerId& id) const;

    std::string GetLayerMetaPath(const TLayerId& id) const;

    std::string GetVolumePath(const TVolumeId& id) const;

    std::string GetVolumeMetaPath(const TVolumeId& id) const;

    void ValidateEnabled() const;

    THashSet<TLayerId> LoadLayerIds();

    void LoadLayers();

    i64 GetQuota() const;

    void DoInitialize();

    void DoFinalizeLayerImport(const TLayerMeta& layerMeta, TGuid tag);

    TLayerMeta DoImportLayer(
        const TArtifactKey& artifactKey,
        const TString& archivePath,
        const TString& container,
        TLayerId layerId,
        TGuid tag);

    void DoRemoveLayer(const TLayerId& layerId);

    TVolumeMeta DoCreateVolume(
        TGuid tag,
        TTagSet tagSet,
        std::optional<TEventTimerGuard> volumeCreateTimeGuard,
        TVolumeMeta volumeMeta,
        THashMap<TString, TString>&& volumeProperties);

    TVolumeMeta DoCreateNbdVolume(
        TGuid tag,
        TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options);

    TVolumeMeta DoCreateOverlayVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray);

    TVolumeMeta DoCreateSquashFSVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath);

    TVolumeMeta DoCreateTmpfsVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParams volumeParams);

    void DoRemoveVolume(TTagSet tagSet, TVolumeId volumeId);

    void DoLinkVolume(
        TGuid tag,
        const TString& source,
        const TString& target);

    void DoUnlinkVolume(
        const TString& source,
        const TString& target);

    void RemoveVolumes(TDuration timeout = TDuration::Minutes(30));
};

DECLARE_REFCOUNTED_CLASS(TLayerLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
