#include "helpers.h"

#include <library/cpp/yt/string/enum.h>

namespace NYT::NExecNode {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TString GetSandboxRelPath(ESandboxKind sandboxKind)
{
    return FormatEnum(sandboxKind);
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
