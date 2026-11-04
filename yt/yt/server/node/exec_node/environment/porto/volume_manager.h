#pragma once

#include "overlay_data.h"
#include "public.h"

#include <yt/yt/server/node/exec_node/volume_manager.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IPortoVolumeManager
    : public IVolumeManager
{
    virtual TFuture<void> RemoveVolumes(
        const std::string& place,
        TDuration timeout,
        const THashSet<std::string>& excludedVolumePaths = {}) = 0;

    virtual TFuture<void> RemoveLayers(const std::string& place, TDuration timeout) = 0;

    virtual void ClearCaches() const = 0;

    virtual void MarkLayersAsNotRemovable() const = 0;

    //! Create root overlayfs volume from pre-prepared overlay layer data.
    virtual TFuture<IVolumePtr> PrepareVolume(
        std::vector<TOverlayData> overlayDataArray,
        TBaseVolumeParamsPtr volumeParams,
        const TVolumePreparationContext& context,
        const TVolumePreparationOptions& options = {}) = 0;

    //! Prepare non-root volumes from pre-prepared overlay layer data.
    //! perVolumeOverlayData[i] is the overlay data for volumes[i].
    virtual TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        const std::optional<std::string>& sandboxPath,
        const TJobId& jobId,
        const std::vector<TBaseVolumeParamsPtr>& volumes,
        std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
        const std::vector<TVolumeMountPtr>& volumeMounts,
        const TVolumePreparationContext& context) = 0;

    //! Prepare overlay layers (download/import) for a set of artifact keys.
    //! Returns one future per layer; each resolves to TOverlayData.
    virtual std::vector<TFuture<TOverlayData>> PrepareOverlayLayers(
        TJobId jobId,
        std::vector<TOverlayLayerPreparationOptions> layerOptions,
        const TArtifactDownloadContext& artifactDownloadContext) = 0;

    //! TODO(yuryalekeev): Remove this method after we get rid of rbind volume.
    virtual TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const std::string& slotPath) = 0;

    //! Link volumes into destination directory.
    virtual TFuture<void> LinkVolumes(
        const std::string& destinationDirectory,
        const std::vector<TVolumeResultPtr>& volumes,
        const std::vector<TVolumeMountPtr>& volumeMounts) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

//! Porto volume manager initialization invokes a number of async operations
//! including population of on-disk and tmpfs layer cache.
TFuture<IPortoVolumeManagerPtr> CreatePortoVolumeManager(
    NDataNode::TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IVolumeArtifactCachePtr artifactCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
