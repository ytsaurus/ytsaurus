#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletIOCategory,
    ((SelectRows)         (0))
    ((LookupRows)         (1))
    ((StoreFlush)         (2))
    ((Preload)            (3))
    ((Compaction)         (4))
    ((Partitioning)       (5))
    ((DictionaryBuilding) (6))
    ((Replication)        (7))
    ((ReplicationLogTrim) (8))
    ((PullRows)           (9))
    ((ReadDynamicStore)  (10))
    ((FetchTableRows)    (11))
);

void PackBaggageFromTabletSnapshot(
    const NTracing::TTraceContextPtr& context,
    ETabletIOCategory category,
    const TTabletSnapshotPtr& tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

NYT::NDistributedThrottler::EDistributedThrottlerMode GetDistributedThrottledMode(
    ETabletDistributedThrottlerKind kind);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
