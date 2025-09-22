#include "helpers.h"

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

bool IsAssignmentPreliminary(const TAssignmentPtr& /*assignment*/)
{
    // TODO(eshcherbin): Should be true only for assignments without a running allocation.
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
