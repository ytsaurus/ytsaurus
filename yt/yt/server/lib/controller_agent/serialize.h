#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 20.3 start here
    ((RemovePartitionedTables)              (300426))
    ((OverrideTimestampInInputChunks)       (300427))
    ((UserJobMonitoring)                    (300428))
    // 20.4 start here
    ((NewSlices)                            (300501))
    ((FixForeignSliceDataWeight)            (300502))
    ((MemoryReserveFactorOverride)          (300503))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
