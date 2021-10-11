#pragma once

// TODO(ignat): migrate to enum class
#include <library/cpp/ytalloc/core/misc/enum.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

DEFINE_ENUM(EIntegralGuaranteeType,
    (None)
    (Burst)
    (Relaxed)
);
    
DEFINE_ENUM(EErrorCode,
    ((PoolTreeGuaranteesOvercommit) (29000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

