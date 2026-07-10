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

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TVirtualSandboxData
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

//! Data necessary to create NBD root volume.
struct TSandboxNbdRootVolumeData
{
    //! Identifier of NBD disk within NBD server.
    std::string DeviceId;

    //! Volume params.
    i64 Size = 0;
    int MediumIndex = 0;
    NNbd::EFilesystemType FsType = NNbd::EFilesystemType::Ext4;

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

    //! Number of TCP connections to use for NBD RPC requests.
    int MultiplexingParallelism = DefaultNbdMultiplexingParallelism;

    bool operator==(const TSandboxNbdRootVolumeData&) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeData& data, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): refactor this class and its usages.
// For example: it looks weird as an agrument in PrepareVolume in TVolumeManager,
// and some of the options is irrelevant for TVolumeManager..
struct TUserSandboxOptions
{
    std::vector<TVolumeMountPtr> JobVolumeMounts;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
    bool EnableRootVolumeDiskQuota = false;
    bool EnableDiskQuota = true;
    int UserId = 0;
    std::optional<TVirtualSandboxData> VirtualSandboxData;
    std::string SlotPath;

    TCallback<void(const TError&)> DiskOverdraftCallback;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumePreparationOptions
{
    TJobId JobId;
    TUserSandboxOptions UserSandboxOptions;
    TArtifactDownloadOptions ArtifactDownloadOptions;
    std::optional<TSandboxNbdRootVolumeData> SandboxNbdRootVolumeData;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
