#include "job_fs_secretary.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void TPortoJobFSSecretary::AddPreparedLayers(TPreparedLayers layers)
{
    const auto& Logger = GetLogger();
    YT_TLOG_DEBUG("Adding prepared layers")
        .With("NewLayerCount", size(layers.ArtifactKeyToOverlayData))
        .With("ExistingLayerCount", size(PreparedLayers_.ArtifactKeyToOverlayData));

    for (auto& [key, overlayData] : layers.ArtifactKeyToOverlayData) {
        EmplaceOrCrash(
            PreparedLayers_.ArtifactKeyToOverlayData,
            std::move(key),
            std::move(overlayData));
    }
}

bool TPortoJobFSSecretary::HasPreparedLayer(const TArtifactKey& key) const
{
    return PreparedLayers_.ArtifactKeyToOverlayData.contains(key);
}

std::vector<TOverlayData> TPortoJobFSSecretary::GetPreparedRootVolumeOverlayData() const
{
    if (!GetRootVolumeParams()) {
        return {};
    }

    auto overlayDataArray = GetPreparedOverlayData(*GetRootVolumeParams());
    if (GetVirtualSandboxOptions()) {
        overlayDataArray.push_back(GetOrCrash(
            PreparedLayers_.ArtifactKeyToOverlayData,
            GetVirtualSandboxOptions()->ArtifactKey));
    }
    return overlayDataArray;
}

std::vector<TOverlayData> TPortoJobFSSecretary::GetPreparedGpuCheckVolumeOverlayData() const
{
    if (!GetGpuCheckVolumeParams()) {
        return {};
    }
    return GetPreparedOverlayData(*GetGpuCheckVolumeParams());
}

std::vector<TOverlayData> TPortoJobFSSecretary::GetPreparedNonRootVolumeOverlayData(const TBaseVolumeParams& params) const
{
    return GetPreparedOverlayData(params);
}

std::vector<TOverlayData> TPortoJobFSSecretary::GetPreparedOverlayData(const TBaseVolumeParams& params) const
{
    std::vector<TOverlayData> overlayDataArray;
    overlayDataArray.reserve(size(params.LayerArtifactKeys.GetAll()));
    for (const auto& key : params.LayerArtifactKeys.GetAll()) {
        overlayDataArray.push_back(GetOrCrash(PreparedLayers_.ArtifactKeyToOverlayData, key));
    }
    return overlayDataArray;
}

void TPortoJobFSSecretary::ReleaseWorkspaceResources()
{
    const auto& Logger = GetLogger();
    YT_TLOG_DEBUG("Releasing prepared layers")
        .With("LayerCount", size(PreparedLayers_.ArtifactKeyToOverlayData));
    PreparedLayers_ = {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
