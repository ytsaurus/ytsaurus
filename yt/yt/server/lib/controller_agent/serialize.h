#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

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
    ((JobTaskName)                          (300303))
    ((ApproximateColumnarStatistics)        (300304))
    ((AutoMergeEnabled)                     (300305))
    // 20.3 start here
    ((BernoulliSampler)                     (300400))
    ((RefactorReduce)                       (300401))
    ((StartAndCompletionTimeInTask)         (300402))
    ((DataFlowInProgress)                   (300403))
    ((MultiChunkPool)                       (300404))
    ((JoinSinks)                            (300405))
    ((JoinAutoMergeTasks)                   (300406))
    ((RefCountedChunkPools)                 (300407))
    ((DynamicMultiPool)                     (300408))
    ((VintageControllerAgent)               (300409))
    ((JoinSortControllerTasks)              (300410))
    ((RemoveTaskGroups)                     (300411))
    ((FixLostInMultiPool)                   (300412))
    ((HierarchicalPartitions)               (300413))
    ((SingleJobOrderedPool)                 (300414))
    ((MergeRowCountValidation)              (300415))
    ((FixDataFlowGraphPersistence)          (300416))
    ((JobResourcesIntToLong)                (300417))
    ((SchemafulMapReduce)                   (300418))
    ((PartitionedTables)                    (300419))
    ((FailedJobCount)                       (300420))
    ((PerTaskInputDataSizeHistograms)       (300421))
    ((ForeignDataSliceWeight)               (300422))
    ((JobSplitterInTask)                    (300423))
    ((BarrierlessSort)                      (300424))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
