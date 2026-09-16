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

#include <variant>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TCreateNbdVolumeOptions
{
    TJobId JobId;

    std::string DeviceId;
    std::string FilesystemType;

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

//! Chunk-backed device: the request params plus the data node session opened for it.
struct TChunkNbdVolumeOptions
{
    TChunkNbdVolumeSpec Spec;

    //! Filled in once a suitable data node is found.
    NRpc::IChannelPtr DataNodeChannel;
    NChunkClient::TSessionId SessionId;
};

using TRWNbdVolumeBackendOptions = std::variant<TChunkNbdVolumeOptions>;

////////////////////////////////////////////////////////////////////////////////

struct TPrepareRWNbdVolumeOptions
{
    TJobId JobId;

    //! Identifier of NBD disk within NBD server.
    std::string DeviceId;

    //! Volume params.
    i64 DeviceSize = 0;
    NNbd::EFilesystemType FilesystemType = NNbd::EFilesystemType::Unknown;

    TRWNbdVolumeBackendOptions BackendOptions;
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
