#include "gpu_allocation_scheduler_helpers.h"

#include "gpu_allocation_scheduler_structs.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

bool IsAssignmentPreliminary(const TGpuSchedulerAssignmentPtr& /*assignment*/)
{
    // TODO(eshcherbin): Should be true only for assignments without a running allocation.
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
