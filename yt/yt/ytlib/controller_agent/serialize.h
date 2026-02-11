#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 24.1 starts here
    ((BumpTo_24_1)                           (301500))
    ((InterruptionReasonInJoblet)            (301501))
    ((PersistMonitoringCounts)               (301502))
    ((WaitingForResourcesDuration)           (301503))
    ((ForceAllowJobInterruption)             (301504))
    ((BatchRowCount_24_1)                    (301505))
    ((InputManagerIntroduction)              (301506))
    ((ChunkSliceStatistics)                  (301507))
    ((AllocationMap)                         (301508))
    ((SingleChunkTeleportStrategy)           (301509))
    ((OutputNodeDirectory)                   (301510))
    ((RemoteInputForOperations)              (301511))
    ((JobDeterminismValidation)              (301512))
    ((MultipleJobsInAllocation)              (301513))
    ((OperationIncarnationInJoblet)          (301514))
    ((TeleportedOutputRowCount)              (301515))
    // 24.2 starts here
    ((BumpTo_24_2)                           (301600))
    ((DropLegacyWirePartitionKeys)           (301601))
    ((VersionedMapReduceWrite)               (301602))
    ((IntroduceGangManager)                  (301603))
    ((DropOriginalTableSchemaRevision)       (301604))
    ((AcoName)                               (301605))
    ((DisableShrinkingJobs)                  (301606))
    ((MultipleOrderedTasks)                  (301607))
    ((RemoteCopyDynamicTableWithHunks)       (301608))
    ((JobFailTolerance)                      (301609))
    ((NewJobsForbiddenReason)                (301610))
    // 25.1 starts here
    ((BumpTo_25_1)                           (301700))
    ((OperationIncarnationIsStrongTypedef)   (301701))
    ((PhoenixSchema)                         (301702))
    ((PreserveJobCookieForAllocationInGangs) (301703))
    ((ThrottlingOfRemoteReads)               (301704))
    ((TableWriteBufferEstimation)            (301705))
    ((OperationIncarnationIsOptional)        (301706))
    ((GroupedNeededResources)                (301707))
    ((MonitoringDescriptorsPreserving)       (301708))
    ((ValidateLivePreviewChunks)             (301709))
    ((IsolateManiacsInSlicing)               (301710))
    ((MaxCompressedDataSizePerJob)           (301711))
    ((DynamicVanillaJobCount)                (301712))
    ((AddAddressesToJob)                     (301713))
    ((NewOrderedChunkPoolSlicing)            (301714))
    ((DropSupportLocality)                   (301715))
    ((PrepareGpuCheckFSDuration)             (301716))
    ((DropShouldSlicePrimaryTableByKeys)     (301717))
    ((NewUnorderedChunkPoolSlicing)          (301718))
    // 25.2 starts here
    ((BumpTo_25_2)                           (301800))
    ((CompressedDataSizePerJob)              (301801))
    ((DropSolidFromChunkStripe)              (301802))
    ((GangRanks)                             (301803))
    ((ChunkStripeKeyNoIndex)                 (301804))
    ((OrderedAndSortedJobSizeAdjuster)       (301805))
    ((RemoveAddressFromJob)                  (301806))
    ((IntroduceInputStatistics)              (301807))
    ((DropUnusedFieldInJobSizeConstraints)   (301808))
    ((DropOutputOrder)                       (301809))
    ((AddSliceCountStatistics)               (301810))
    // 25.3 starts here
    ((BumpTo_25_3)                           (301900))
    ((DropRedundantFieldsInSortedChunkPool)  (301901))
    ((DropDuplicateOutputNodeDirectory)      (301902))
    ((PersistentCompletedRankCount)          (301903))
    ((JobEnvironmentPreparationStatistics)   (301904))
    ((RlsInOperations)                       (301905))
    ((FixRlsSnapshots)                       (301906))
    // 25.4 starts here
    ((BumpTo_25_4)                           (302100))
    ((PrimaryCompressedDataSizePerJob)       (302101))
    ((RemoveOldOrderedChunkPoolSlicing)      (302102))
    ((RemoveOldUnorderedChunkPoolSlicing)    (302103))
    ((DistributedJobManagers)                (302104))
    ((PerTableInputQuery)                    (302105))
    ((RemoveUnusedLocalityStatistics)        (302106))
    ((RefactorChunkStripeList)               (302107))
    ((DropIsFinalOutput)                     (302108))
    ((MoveWaitingChunkCount)                 (302109))
    ((RemoveParentPartitionTag)              (302110))
    ((RefactorTPartition)                    (302111))
    ((SimplifyStreamDescriptor)              (302112))
    ((JobOutputDigestOptimization)           (302113))
    ((PrepareTmpfsVolumes)                   (302114))
    ((DivideTPartitionIntoTwoTypes)          (302115))
    ((AlwaysSetChunkPoolOutput)              (302116))
    ((MovePoolsToTasksInSortController)      (302117))
    ((FixWaitingChunkCountInInputManager)    (302118))
    ((UnifyFinalPartitionDispatching)        (302119))
    ((StorePhysicalChildPartitionBounds)     (302120))
    ((UseMultiplePartitionTagsInChunkStripe) (302121))
    ((CreateFinalPartitionsLazily)           (302122))
    ((SupportMergingFinalPartitions)         (302123))
    ((DropUnusedOutputCookieGenerator)       (302124))
    ((StorePreemptibleProgressInJoblets)     (302125))
    // 26.1 starts here
    ((BumpTo_26_1)                           (302200))
    ((UseCounterGuardsInChunkPoolMerger)     (302201))
    ((FileRlsReadSpec)                       (302202))
    ((DropExcessFieldsInOrderedController)   (302203))
    ((ChunkPoolStatistics)                   (302204))
    ((FixSimpleSort)                         (302205))
    ((ValidateRootFS)                        (302206))
    ((FixOutputChunkPoolIndexSerialization)  (302207))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
