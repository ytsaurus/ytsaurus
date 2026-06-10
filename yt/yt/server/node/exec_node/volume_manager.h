#pragma once

#include "preparation_options.h"
#include "public.h"
#include "volume.h"
#include "volume_artifact.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! This class creates all volumes for job.
struct IVolumeManager
    : public virtual TRefCounted
{
    //! Create root overlayfs volume from pre-prepared overlay layer data.
    virtual TFuture<IVolumePtr> PrepareVolume(
        std::vector<TOverlayData> overlayDataArray,
        const TVolumePreparationOptions& options) = 0;

    //! Prepare non-root volumes from pre-prepared overlay layer data.
    //! perVolumeOverlayData[i] is the overlay data for volumes[i].
    virtual TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        const std::optional<std::string>& sandboxPath,
        const TJobId& jobId,
        const std::vector<TBaseVolumeParamsPtr>& volumes,
        std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) = 0;

    //! Prepare overlay layers (download/import) for a set of artifact keys.
    //! Returns one future per layer; each resolves to TOverlayData.
    virtual std::vector<TFuture<TOverlayData>> PrepareOverlayLayers(
        TJobId jobId,
        std::vector<TOverlayLayerPreparationOptions> layerOptions,
        const TArtifactDownloadOptions& artifactDownloadOptions) = 0;

    //! TODO(yuryalekeev): Remove this method after we get rid of rbind volume.
    virtual TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const std::string& slotPath) = 0;

    //! Link volumes into destination directory.
    virtual TFuture<void> LinkVolumes(
        const std::string& destinationDirectory,
        const std::vector<TVolumeResultPtr>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) = 0;

    //! Remove volumes planted at a given place, excluding the given porto mount paths.
    virtual TFuture<void> RemoveVolumes(
        const std::string& place,
        TDuration timeout,
        const THashSet<std::string>& excludedVolumePaths = {}) = 0;

    //! Remove layers planted at a given place.
    virtual TFuture<void> RemoveLayers(const std::string& place, TDuration timeout) = 0;

    virtual bool IsLayerCached(const TArtifactKey& artifactKey) const = 0;

    virtual void BuildOrchid(NYTree::TFluentAny fluent) const = 0;

    virtual void ClearCaches() const = 0;

    virtual void MarkLayersAsNotRemovable() const = 0;

    virtual TFuture<void> GetVolumeReleaseEvent() = 0;

    virtual TFuture<void> DisableLayerCache(const TError& reason) = 0;

    virtual bool IsEnabled() const = 0;

    virtual void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr& oldConfig,
        const TVolumeManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

//! Porto volume manager initialization invokes a number of async operations
//! including population of on-disk and tmpfs layer cache.
TFuture<IVolumeManagerPtr> CreatePortoVolumeManager(
    NDataNode::TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IVolumeArtifactCachePtr artifactCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

TFuture<IVolumeManagerPtr> CreateSimpleVolumeManager(
    const std::vector<TSlotLocationConfigPtr>& locations,
    IInvokerPtr invoker,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
