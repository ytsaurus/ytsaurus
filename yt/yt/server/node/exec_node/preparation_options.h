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

// TODO(ignat): refactor this class and its usages.
// For example: it looks weird as an agrument in PrepareVolume in TVolumeManager,
// and some of the options is irrelevant for TVolumeManager..
struct TUserSandboxOptions
{
    std::vector<TVolumeMountPtr> JobVolumeMounts;
    TBaseVolumeParamsPtr RootVolumeParams;
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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
