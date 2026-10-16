#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESuspensionStatus,
    ((Normal)       (0))
    ((Suspending)   (1))
    ((Suspended)    (2))
);

ESuspensionStatus GetSuspensionStatus(bool suspended, bool completelySuspended);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
