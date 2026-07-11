#pragma once

#include "preparation_options.h"
#include "volume.h"

#include <yt/yt/server/node/exec_node/artifact.h>

#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/public.h>
#include <yt/yt/server/lib/nbd/image/public.h>

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TCreateNbdVolumeOptions
{
    TJobId JobId;

    std::string DeviceId;
    std::string Filesystem;

    bool IsReadOnly = true;

    //! Block size (I/O alignment) reported to Porto so it configures the kernel NBD device's logical
    //! block size accordingly (Porto defaults to 512 otherwise). For a block-granular backend this is
    //! its block size, so the kernel aligns I/O and does any sub-block read-modify-write itself.
    i64 BlockSize = 512;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRONbdVolumeOptions
{
    TJobId JobId;
    TArtifactKey ArtifactKey;
    NNbd::NImage::IImageReaderPtr ImageReader;
};

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRWNbdVolumeOptions
{
    TJobId JobId;

    i64 Size = 0;
    int MediumIndex = 0;
    NNbd::EFilesystemType Filesystem = NNbd::EFilesystemType::Unknown;
    std::string DeviceId;
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
    int MinDataNodeCount = 0;
    int MaxDataNodeCount = 0;

    //! Number of TCP connections to use for NBD RPC requests.
    int MultiplexingParallelism = DefaultNbdMultiplexingParallelism;
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
