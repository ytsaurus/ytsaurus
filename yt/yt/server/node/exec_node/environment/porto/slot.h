#pragma once

#include "overlay_data.h"
#include "public.h"

#include <yt/yt/server/node/exec_node/environment/common/slot.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoUserSlot
    : public TUserSlotBase
{
public:
    explicit TPortoUserSlot(TUserSlotContext context);

    void CleanJobWorkspaceResources(const TJobFSSecretary& fsSecretary) override;

    void CleanAllocationWorkspace() override;

    std::vector<TFuture<TOverlayData>> PrepareLayers(
        TJobId jobId,
        const std::vector<TOverlayLayerPreparationOptions>& layerOptions,
        const TArtifactDownloadContext& artifactDownloadContext);

    TFuture<IVolumePtr> PrepareRootVolume(
        std::vector<TOverlayData> overlayDataArray,
        const TBaseVolumeParamsPtr& volumeParams,
        const TVolumePreparationOptions& options);

    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume);

    TFuture<IVolumePtr> PrepareGpuCheckVolume(
        std::vector<TOverlayData> overlayDataArray,
        const TBaseVolumeParamsPtr& volumeParams,
        const TVolumePreparationOptions& options);

    TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        TJobId jobId,
        const IVolumePtr& rootVolume,
        const std::vector<TBaseVolumeParamsPtr>& volumeParams,
        std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
        const std::vector<TVolumeMountPtr>& volumeMounts);

    TFuture<void> LinkVolumes(
        const IVolumePtr& rootVolume,
        const std::vector<TVolumeResultPtr>& volumeResults,
        const std::vector<TVolumeMountPtr>& volumeMounts);

    TFuture<void> CreateRootVolumeDirectories(const IVolumePtr& rootVolume, int userId) const;
    TFuture<void> ValidateRootFS(const IVolumePtr& rootVolume) const override;

private:
    TNonNullPtr<IPortoVolumeManager> GetVolumeManager() const;
};

DEFINE_REFCOUNTED_TYPE(TPortoUserSlot)

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreatePortoUserSlot(TUserSlotContext context);

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
