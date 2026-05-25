#pragma once

#include "preparation_options.h"
#include "private.h"
#include "volume.h"
#include "volume_counters.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TOverlayData;

////////////////////////////////////////////////////////////////////////////////

class TLayerLocation
    : public NNode::TDiskLocation
{
public:
    TLayerLocation(
        NDataNode::TLayerLocationConfigPtr locationConfig,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        NServer::TDiskHealthCheckerConfigPtr healthCheckerConfig,
        NContainers::IPortoExecutorPtr volumeExecutor,
        NContainers::IPortoExecutorPtr layerExecutor,
        NContainers::IPortoExecutorPtr fastLayerExecutor,
        const std::string& id);

    TFuture<void> Initialize();

    TFuture<TVolumeMeta> CreateNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options);

    TFuture<TVolumeMeta> CreateOverlayVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        int userId,
        const std::optional<std::string>& placePath,
        std::optional<i64> diskSpaceLimit,
        std::optional<i64> inodeLimit,
        const std::vector<TOverlayData>& overlayDataArray,
        bool placeInUserSlot);

    TFuture<TVolumeMeta> CreateSquashFSVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath);

    TFuture<TVolumeMeta> CreateLoopVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        TLocalDiskVolumeParamsPtr tmpfsVolume);

    TFuture<TVolumeMeta> CreateTmpfsVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParamsPtr tmpfsVolume);

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
        const std::string& archivePath,
        const std::string& container,
        TLayerId layerId,
        TGuid tag);

    bool IsFull();

    bool IsLayerImportInProgress() const;

    void OnDynamicConfigChanged(
        const TLayerCacheDynamicConfigPtr& oldConfig,
        const TLayerCacheDynamicConfigPtr& newConfig);

    TFuture<void> RemoveLayer(const TLayerId& layerId);

    TFuture<void> RemoveVolume(
        NProfiling::TTagSet tagSet,
        TVolumeId volumeId,
        std::optional<std::string> portoPlacePath);

    //! TODO(yuryalekseev): Remove me when slot rbind is removed.
    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const std::string& slotPath);

    bool ResidesOnTmpfs() const;

    TFuture<void> LinkVolume(
        TGuid tag,
        const std::string& source,
        const std::string& target);

    TFuture<void> UnlinkVolume(
        const std::string& source,
        const std::string& target);

    int GetVolumeCount() const;

    i64 GetUsedSpace() const;

    //! Remove volumes planted at a given place.
    void RemoveVolumes(
        const std::string& place,
        TDuration timeout);

    //! Remove layers planted at a given place.
    void RemoveLayers(
        const std::string& place,
        TDuration timeout);

private:
    const NDataNode::TLayerLocationConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    TAtomicIntrusivePtr<TLayerCacheDynamicConfig> DynamicConfig_;
    const NContainers::IPortoExecutorPtr VolumeExecutor_;
    const NContainers::IPortoExecutorPtr LayerExecutor_;
    const NContainers::IPortoExecutorPtr FastLayerExecutor_;

    const NConcurrency::TActionQueuePtr LocationQueue_;
    const std::string VolumesPath_;
    const std::string VolumesMetaPath_;
    const std::string LayersPath_;
    const std::string LayersMetaPath_;
    const std::string PlacePath_;

    NServer::TDiskHealthCheckerPtr HealthChecker_;

    TLayerLocationPerformanceCounters PerformanceCounters_;

    std::atomic<int> LayerImportsInProgress_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    THashMap<TLayerId, TLayerMeta> LayerIdToMeta_;
    THashMap<TVolumeId, TVolumeMeta> VolumeIdToMeta_;

    TPromise<void> VolumesReleasePromise_ = MakePromise<void>(TError());

    mutable i64 AvailableSpace_ = 0;
    i64 UsedSpace_ = 0;

    TError Alert_;

    std::string GetLayerPath(const TLayerId& id) const;

    std::string GetLayerMetaPath(const TLayerId& id) const;

    std::string GetVolumePath(
        const TVolumeId& id,
        const std::optional<std::string>& portoPlacePath = std::nullopt) const;

    std::string GetVolumeMetaPath(
        const TVolumeId& id,
        const std::optional<std::string>& portoPlacePath = std::nullopt) const;

    void ValidateEnabled() const;

    THashSet<TLayerId> LoadLayerIds();

    void LoadLayers();

    i64 GetQuota() const;

    void DoInitialize();

    void DoFinalizeLayerImport(const TLayerMeta& layerMeta, TGuid tag);

    TLayerMeta DoImportLayer(
        const TArtifactKey& artifactKey,
        const std::string& archivePath,
        const std::string& container,
        TLayerId layerId,
        TGuid tag);

    void DoRemoveLayer(const TLayerId& layerId);

    TVolumeMeta DoCreateVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        std::optional<NProfiling::TEventTimerGuard> volumeCreateTimeGuard,
        TVolumeMeta volumeMeta,
        THashMap<std::string, std::string> volumeProperties,
        std::optional<std::string> portoPlacePath = std::nullopt);

    TVolumeMeta DoCreateNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options);

    TVolumeMeta DoCreateOverlayVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        int userId,
        const std::optional<std::string>& placePath,
        std::optional<i64> diskSpaceLimit,
        std::optional<i64> inodeLimit,
        const std::vector<TOverlayData>& overlayDataArray,
        bool placeInUserSlot);

    TVolumeMeta DoCreateSquashFSVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath);

    TVolumeMeta DoCreateLoopVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        TLocalDiskVolumeParamsPtr volumeParams);

    TVolumeMeta DoCreateTmpfsVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParamsPtr volumeParams);

    void DoRemoveVolume(
        NProfiling::TTagSet tagSet,
        TVolumeId volumeId,
        std::optional<std::string> portoPlacePath = std::nullopt);

    void DoLinkVolume(
        TGuid tag,
        const std::string& source,
        const std::string& target);

    void DoUnlinkVolume(
        const std::string& source,
        const std::string& target);

    //! Remove volumes planted in VolumesPath_.
    void RemoveVolumes(TDuration timeout);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
