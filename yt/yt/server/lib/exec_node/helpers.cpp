#include "helpers.h"

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

const std::string& GetSandboxRelPath(ESandboxKind sandboxKind)
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    YT_VERIFY(!sandboxName.empty());
    return sandboxName;
}

////////////////////////////////////////////////////////////////////////////////

TErrorCode GetDisableErrorCode(
    const TError& error,
    TErrorCode fallbackErrorCode)
{
    if (NFS::IsOutOfDiskSpaceError(error) ||
        error.FindMatching(NContainers::EPortoErrorCode::NoSpace).has_value())
    {
        return EErrorCode::NotEnoughDiskSpace;
    }

    return fallbackErrorCode;
}

////////////////////////////////////////////////////////////////////////////////

EAllocationState JobStateToAllocationState(EJobState jobState)
{
    switch (jobState) {
        case EJobState::None:
            return EAllocationState::Scheduled;
        case EJobState::Waiting:
            return EAllocationState::Waiting;
        case EJobState::Running:
            return EAllocationState::Running;
        case EJobState::Aborting:
            return EAllocationState::Finishing;
        case EJobState::Completed:
        case EJobState::Failed:
        case EJobState::Aborted:
            return EAllocationState::Finished;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
