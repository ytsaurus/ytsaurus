#pragma once

#include <yt/server/lib/exec_agent/public.h>

#include <yt/core/misc/optional.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobProxyResources;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TUserSandboxOptions
{
    std::optional<TString> TmpfsPath;
    std::optional<i64> TmpfsSizeLimit;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
};

DEFINE_ENUM(EErrorCode,
    ((ConfigCreationFailed)          (1100))
    ((AbortByScheduler)              (1101))
    ((ResourceOverdraft)             (1102))
    ((WaitingJobTimeout)             (1103))
    ((SlotNotFound)                  (1104))
    ((JobEnvironmentDisabled)        (1105))
    ((JobProxyConnectionFailed)      (1106))
    ((ArtifactCopyingFailed)         (1107))
    ((NodeDirectoryPreparationFailed)(1108))
    ((SlotLocationDisabled)          (1109))
    ((QuotaSettingFailed)            (1110))
    ((RootVolumePreparationFailed)   (1111))
    ((NotEnoughDiskSpace)            (1112))
    ((ArtifactDownloadFailed)        (1113))
);

extern const TString ProxyConfigFileName;

DECLARE_REFCOUNTED_CLASS(TSlotManager)
DECLARE_REFCOUNTED_CLASS(TSlotLocation)
DECLARE_REFCOUNTED_STRUCT(IJobDirectoryManager)

DECLARE_REFCOUNTED_STRUCT(ISlot)

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

DECLARE_REFCOUNTED_STRUCT(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
