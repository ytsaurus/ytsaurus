#pragma once

#include <yt/yt/core/misc/error_code.h>

// TODO(ignat): migrate to enum class
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFairShareType,
    (Regular)
    (PromisedGuarantee)
);

DEFINE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

DEFINE_ENUM(EIntegralGuaranteeType,
    (None)
    (Burst)
    (Relaxed)
);

// NB(eshcherbin): Lower tier index means higher priority.
DEFINE_ENUM(EStrongGuaranteeTier,
    ((PriorityPools) (0))
    ((RegularPools)  (1))
    ((Operations)    (2))
);

YT_DEFINE_ERROR_ENUM(
    ((PoolTreeGuaranteesOvercommit)                       (29000))
    ((NestedPromisedGuaranteeFairSharePools)              (29001))
    ((PriorityStrongGuaranteeAdjustmentPoolsWithoutDonor) (29002))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

