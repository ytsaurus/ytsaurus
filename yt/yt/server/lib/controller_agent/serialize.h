#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    ((JobMetricsJobStateFilter)             (300195))
    // 19.7 starts here
    ((ExternalizedTransactions)             (300200))
    ((DynamicTableWriterConfig)             (300201))
    ((JobSpeculationTimeout)                (300202))
    // 19.8 starts here
    ((GroupedSpeculativeJobs)               (300220))
    ((ImproveSpeculativeHeuristic)          (300221))
    // 20.1 starts here
    ((TabletIndexInMROverOrderedDynTables)  (300230))
    ((JobHasCompetitorsFlag)                (300231))
    ((RetainedJobsCoreInfoCount)            (300232))
    ((ControllerWritesJobArchive)           (300233))
    ((SliceForeignChunks)                   (300234))
    ((CompletedRowCount)                    (300235))
    // 20.2 starts here
    ((DynamicStoreRead)                     (300300))
    ((SortedOperationsSliceSize)            (300301))
    ((ChunkCountInUserObject)               (300302))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
