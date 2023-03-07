#pragma once

#include <yt/server/lib/exec_agent/public.h>
#include <yt/server/lib/job_agent/public.h>

#include <yt/core/misc/optional.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobProxyResources;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTmpfsVolume
{
    TString Path;
    i64 Size;
};

struct TUserSandboxOptions
{
    std::vector<TTmpfsVolume> TmpfsVolumes;
    std::optional<i64> InodeLimit;
    std::optional<i64> DiskSpaceLimit;
};

extern const TString ProxyConfigFileName;

DECLARE_REFCOUNTED_CLASS(TSlotManager)
DECLARE_REFCOUNTED_CLASS(TSlotLocation)
DECLARE_REFCOUNTED_STRUCT(IJobDirectoryManager)

DECLARE_REFCOUNTED_STRUCT(ISlot)

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

DECLARE_REFCOUNTED_STRUCT(IJobEnvironment)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
