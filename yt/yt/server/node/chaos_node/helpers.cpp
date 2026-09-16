#include "helpers.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

ESuspensionStatus GetSuspensionStatus(bool suspended, bool completelySuspended)
{
    if (completelySuspended) {
        return ESuspensionStatus::Suspended;
    }

    if (!suspended) {
        return ESuspensionStatus::Normal;
    }

    return ESuspensionStatus::Suspending;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
