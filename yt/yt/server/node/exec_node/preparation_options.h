#pragma once

#include "artifact.h"
#include "volume_helpers.h"
#include "private.h"

#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/public.h>
#include <yt/yt/server/lib/nbd/image/public.h>

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/core/actions/callback.h>

#include <util/generic/string.h>

#include <util/system/types.h>

#include <variant>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TVirtualSandboxOptions
{
    std::string NbdDeviceId;
    TArtifactKey ArtifactKey;
    NNbd::NImage::IImageReaderPtr Reader;
};

////////////////////////////////////////////////////////////////////////////////

struct TOverlayLayerPreparationOptions
{
    TArtifactKey ArtifactKey;
    NNbd::NImage::IImageReaderPtr ImageReader;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkNbdVolumeSpec
{
    int MediumIndex = 0;

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

    bool operator==(const TChunkNbdVolumeSpec&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TChunkNbdVolumeSpec& volumeSpec, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

using TRWNbdVolumeBackendSpec = std::variant<TChunkNbdVolumeSpec>;

//! Sandbox NBD root volume as requested by the job spec.
struct TSandboxNbdRootVolumeSpec
{
    //! Identifier of NBD disk within NBD server.
    std::string DeviceId;

    //! Volume params.
    i64 DeviceSize = 0;
    NNbd::EFilesystemType FilesystemType = NNbd::EFilesystemType::Ext4;

    TRWNbdVolumeBackendSpec BackendSpec;

    bool operator==(const TSandboxNbdRootVolumeSpec&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeSpec& volumeSpec, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): refactor this class and its usages.
// For example: it looks weird as an agrument in PrepareVolume in TVolumeManager,
// and some of the options is irrelevant for TVolumeManager..
struct TUserSandboxOptions
{
    std::vector<TVolumeMountPtr> JobVolumeMounts;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
    bool DisableRbindRootVolume = false;
    bool EnableDiskQuota = true;
    int UserId = 0;
    std::optional<TVirtualSandboxOptions> VirtualSandboxOptions;
    std::string SlotPath;

    TCallback<void(const TError&)> DiskOverdraftCallback;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumePreparationOptions
{
    TJobId JobId;
    TUserSandboxOptions UserSandboxOptions;
    TArtifactDownloadOptions ArtifactDownloadOptions;
    std::optional<TSandboxNbdRootVolumeSpec> SandboxNbdRootVolumeSpec;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
