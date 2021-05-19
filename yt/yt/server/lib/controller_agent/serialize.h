#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 20.3 start here
    ((RemovePartitionedTables)              (300426))
    ((OverrideTimestampInInputChunks)       (300427))
    ((UserJobMonitoring)                    (300428))
    ((PerTableCurrentChunkCount)            (300429))
    // 21.1 start here
    ((NewSlices)                            (300501))
    ((FixForeignSliceDataWeight)            (300502))
    ((MemoryReserveFactorOverride)          (300503))
    ((SimplifyForeignDataProcessing)        (300504))
    ((AutoMergePendingJobCount)             (300505))
    ((NoOrderedDynamicStoreInterrupts)      (300506))
    ((CorrectLoggerSerialization)           (300507))
    ((TimeStatistics)                       (300508))
    ((SliceIndex)                           (300509))
    // 21.2 start here
    ((CorrectLoggerSerialization_21_2)      (300602))
    ((Actually21_2StartsHere)               (300603))
    ((PersistStandardStreamDescriptors)     (300604))
    ((JSCInitialPrimaryDataWeight)          (300605))
    ((MultiChunkPoolOptions)                (300606))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
