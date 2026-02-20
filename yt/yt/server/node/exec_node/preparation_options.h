#pragma once

#include "artifact.h"
#include "private.h"

#include <yt/yt/server/lib/nbd/public.h>
#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/core/actions/callback.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TVirtualSandboxData
{
    TString NbdDeviceId;
    TArtifactKey ArtifactKey;
    NNbd::IImageReaderPtr Reader;
};

////////////////////////////////////////////////////////////////////////////////

//! Data necessary to create NBD root volume.
struct TSandboxNbdRootVolumeData
{
    //! Identifier of NBD disk within NBD server.
    TString DeviceId;

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
};

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): refactor this class and its usages.
// For example: it looks weird as an agrument in PrepareVolume in TVolumeManager,
// and some of the options is irrelevant for TVolumeManager..
struct TUserSandboxOptions
{
    std::vector<TTmpfsVolumeParams> TmpfsVolumes;
    std::vector<NScheduler::TVolumeMountPtr> JobVolumeMounts;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
    bool EnableRootVolumeDiskQuota = false;
    bool EnableDiskQuota = true;
    int UserId = 0;
    std::optional<TVirtualSandboxData> VirtualSandboxData;
    std::optional<TSandboxNbdRootVolumeData> SandboxNbdRootVolumeData;
    std::optional<std::string> SlotPath;

    TCallback<void(const TError&)> DiskOverdraftCallback;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumePreparationOptions
{
    TJobId JobId;
    TUserSandboxOptions UserSandboxOptions;
    TArtifactDownloadOptions ArtifactDownloadOptions;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
