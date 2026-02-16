#pragma once

#include "preparation_options.h"
#include "volume.h"

#include <yt/yt/server/node/exec_node/artifact.h>

#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TCreateNbdVolumeOptions
{
    TJobId JobId;
    TString DeviceId;
    TString Filesystem;
    bool IsReadOnly;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRONbdVolumeOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    NNbd::IImageReaderPtr ImageReader;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRWNbdVolumeOptions
{
    TJobId JobId;

    i64 Size;
    int MediumIndex;
    NNbd::EFilesystemType Filesystem;
    TString DeviceId;
    NRpc::IChannelPtr DataNodeChannel;
    NChunkClient::TSessionId SessionId;

    //! Params to connect to chosen data nodes.
    TDuration DataNodeRpcTimeout;
    std::optional<std::string> DataNodeAddress;

    //! Params for NBD requests to data nodes.
    TDuration DataNodeNbdServiceRpcTimeout;
    TDuration DataNodeNbdServiceMakeTimeout;

    //! Params to get suitable data nodes from master.
    TDuration MasterRpcTimeout;
    int MinDataNodeCount;
    int MaxDataNodeCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareLayerOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    TArtifactDownloadOptions ArtifactDownloadOptions;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareSquashFSVolumeOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    TArtifactDownloadOptions ArtifactDownloadOptions;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareOverlayVolumeOptions
{
    TJobId JobId;
    TUserSandboxOptions UserSandboxOptions;
    std::vector<TOverlayData> OverlayDataArray;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
