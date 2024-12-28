#pragma once

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/nbd/image_reader.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/actions/callback.h>

#include <optional>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TAllocationId;
using NScheduler::EAllocationState;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobProxyResources;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

struct TTmpfsVolume
{
    TString Path;
    i64 Size;
};

struct TVirtualSandboxData
{
    TString NbdExportId;
    NNbd::IImageReaderPtr Reader;
};

struct TUserSandboxOptions
{
    std::vector<TTmpfsVolume> TmpfsVolumes;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
    bool EnableRootVolumeDiskQuota = false;
    int UserId = 0;
    std::optional<TVirtualSandboxData> VirtualSandboxData;

    TCallback<void(const TError&)> DiskOverdraftCallback;
};

extern const TString ProxyConfigFileName;

DECLARE_REFCOUNTED_STRUCT(IJobController)

DECLARE_REFCOUNTED_CLASS(TJobWorkspaceBuilder)
struct TJobWorkspaceBuildingContext;
struct TJobWorkspaceBuildingResult;

DECLARE_REFCOUNTED_CLASS(TJobGpuChecker)
struct TJobGpuCheckerSettings;
struct TJobGpuCheckerResult;

DECLARE_REFCOUNTED_CLASS(TChunkCache)
DECLARE_REFCOUNTED_CLASS(TCacheLocation)
DECLARE_REFCOUNTED_CLASS(TCachedBlobChunk)
DECLARE_REFCOUNTED_STRUCT(IVolumeArtifact)
DECLARE_REFCOUNTED_STRUCT(IVolumeChunkCache)

DECLARE_REFCOUNTED_CLASS(TGpuManager)

DECLARE_REFCOUNTED_STRUCT(IVolume)
DECLARE_REFCOUNTED_STRUCT(IVolumeManager)
DECLARE_REFCOUNTED_STRUCT(IPlainVolumeManager)

DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

DECLARE_REFCOUNTED_CLASS(TSlotManager)
DECLARE_REFCOUNTED_CLASS(TSlotLocation)
DECLARE_REFCOUNTED_STRUCT(IJobDirectoryManager)

DECLARE_REFCOUNTED_STRUCT(IJobInputCache)

DECLARE_REFCOUNTED_STRUCT(IUserSlot)
DECLARE_REFCOUNTED_CLASS(TGpuSlot)

DECLARE_REFCOUNTED_CLASS(TControllerAgentConnectorPool)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

DECLARE_REFCOUNTED_STRUCT(IJobEnvironment)

DECLARE_REFCOUNTED_STRUCT(IJobProxyLogManager)

DECLARE_REFCOUNTED_CLASS(IThrottlerManager)

DEFINE_ENUM(ESlotType,
    //! With cpu_policy=normal
    ((Common)    (0))
    //! With cpu_polcy=idle
    ((Idle)      (1))
);

struct TNumaNodeInfo
{
    i64 NumaNodeId;
    TString CpuSet;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVolumeType,
    ((Overlay)  (0))
    ((Nbd)      (1))
    ((SquashFS) (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
