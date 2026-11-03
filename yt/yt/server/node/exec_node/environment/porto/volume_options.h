#pragma once

#include "overlay_data.h"

#include <yt/yt/server/node/exec_node/volume_options.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TPrepareLayerOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    TArtifactDownloadContext ArtifactDownloadContext;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareSquashFSVolumeOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    TArtifactDownloadContext ArtifactDownloadContext;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRootOverlayVolumeOptions
{
    TJobId JobId;
    TUserSandboxOptions UserSandboxOptions;
    std::vector<TOverlayData> OverlayDataArray;
    TBaseVolumeParamsPtr RootVolumeParams;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
