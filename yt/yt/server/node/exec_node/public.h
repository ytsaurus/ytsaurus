#pragma once

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/actions/callback.h>

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

////////////////////////////////////////////////////////////////////////////////

extern const TString ProxyConfigFileName;

DECLARE_REFCOUNTED_STRUCT(IJobController)

DECLARE_REFCOUNTED_CLASS(TJobWorkspaceBuilder)
struct TJobWorkspaceBuildingContext;
struct TJobWorkspaceBuildingResult;

DECLARE_REFCOUNTED_CLASS(TJobGpuChecker)
struct TJobGpuCheckerSettings;
struct TJobGpuCheckerResult;

DECLARE_REFCOUNTED_CLASS(TArtifactCache)
DECLARE_REFCOUNTED_CLASS(TCacheLocation)
DECLARE_REFCOUNTED_STRUCT(IVolumeArtifact)
DECLARE_REFCOUNTED_STRUCT(IVolumeArtifactCache)

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
    std::string CpuSet;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVolumeType,
    ((Local)    (0))
    ((Nbd)      (1))
    // Reserved (2))
    ((Tmpfs)    (3))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
