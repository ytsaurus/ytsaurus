#pragma once

#include "overlay_data.h"
#include "public.h"

#include <yt/yt/server/node/exec_node/artifact.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! Prepared overlay layer data, indexed by artifact key.
struct TPreparedLayers
{
    THashMap<TArtifactKey, TOverlayData> ArtifactKeyToOverlayData;
};

class TPortoJobFSSecretary
    : public TJobFSSecretary
{
public:
    using TJobFSSecretary::TJobFSSecretary;

    void AddPreparedLayers(TPreparedLayers layers);
    bool HasPreparedLayer(const TArtifactKey& key) const;

    std::vector<TOverlayData> GetPreparedRootVolumeOverlayData() const;
    std::vector<TOverlayData> GetPreparedGpuCheckVolumeOverlayData() const;
    std::vector<TOverlayData> GetPreparedNonRootVolumeOverlayData(const TBaseVolumeParams& params) const;

    void ReleaseWorkspaceResources() override;

private:
    TPreparedLayers PreparedLayers_;

    std::vector<TOverlayData> GetPreparedOverlayData(const TBaseVolumeParams& params) const;
};

DEFINE_REFCOUNTED_TYPE(TPortoJobFSSecretary)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
