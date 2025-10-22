#pragma once

#include "public.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

struct TLsmTabletNodeConfig
    : public TRefCounted
{
    // Store flusher.
    double ForcedRotationMemoryRatio;
    i64 MinForcedFlushDataSize;

    // Partition balancer.
    TDuration ResamplingPeriod;

    // Store compactor.
    TDuration CompactionBackoffTime;
    double StarvingTablesTasksRatio;
    TDuration BackgroundTaskHistoryWindow;
};

DEFINE_REFCOUNTED_TYPE(TLsmTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
