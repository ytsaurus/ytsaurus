#include "structs.h"

namespace NYT::NScheduler {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TJobStartDescriptor::TJobStartDescriptor(
    TJobId id,
    EJobType type,
    const TJobResources& resourceLimits,
    bool interruptible)
    : Id(id)
    , Type(type)
    , ResourceLimits(resourceLimits)
    , Interruptible(interruptible)
{ }

////////////////////////////////////////////////////////////////////////////////

void TControllerScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

bool TControllerScheduleJobResult::IsBackoffNeeded() const
{
    return
        !StartDescriptor &&
        Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleJobFailReason::NoLocalJobs] == 0 &&
        Failed[EScheduleJobFailReason::NodeBanned] == 0 &&
        Failed[EScheduleJobFailReason::DataBalancingViolation] == 0;
}

bool TControllerScheduleJobResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleJobFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleJobFailReason::JobSpecThrottling] > 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
