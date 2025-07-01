#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 22.4 starts here
    ((BumpTo_22_4)                          (301100))
    ((ReworkJobProfilerSpec)                (301101))
    ((PersistDataStatistics)                (301102))
    ((ChunkFormat)                          (301103))
    ((ProbingBaseLayer)                     (301105))
    ((ProbingBaseLayerPersistLostJobs)      (301106))
    ((ProbingBaseLayerPersistAlertCounts)   (301107))
    // 23.1 starts here
    ((InputStreamDescriptors)               (301200))
    ((DoNotPersistJobReleaseFlags)          (301201))
    ((JobStateInJoblet)                     (301202))
    ((InitialMinNeededResources)            (301203))
    ((JobExperiment)                        (301204))
    ((PersistInputChunkMappingLogger)       (301205))
    // 23.2 starts here
    ((AddChunkSchemas)                      (301300))
    ((AddFilesystemAttribute)               (301301))
    ((AddAccessMethodAttribute)             (301302))
    ((VirtualTableSchema)                   (301303))
    ((UnwrapTimestamp)                      (301304))
    ((AttributeBasedAccessControl)          (301305))
    ((NewLivePreview)                       (301306))
    ((LivePreviewAnnotation)                (301307))
    ((JobErrorInJobSummary)                 (301308))
    // 23.2 continues here (YT-21342)
    ((NodeJobStartTimeInJoblet)             (301408))
    ((JobAbortsUntilOperationFailure)       (301409))
    ((BatchRowCount_23_2)                   (301410))
    // 24.1 starts here
    ((BumpTo_24_1)                          (301500))
    ((InterruptionReasonInJoblet)           (301501))
    ((PersistMonitoringCounts)              (301502))
    ((WaitingForResourcesDuration)          (301503))
    ((ForceAllowJobInterruption)            (301504))
    ((BatchRowCount_24_1)                   (301505))
    ((InputManagerIntroduction)             (301506))
    ((ChunkSliceStatistics)                 (301507))
    ((AllocationMap)                        (301508))
    ((SingleChunkTeleportStrategy)          (301509))
    ((OutputNodeDirectory)                  (301510))
    ((RemoteInputForOperations)             (301511))
    ((JobDeterminismValidation)             (301512))
    ((MultipleJobsInAllocation)             (301513))
    ((OperationIncarnationInJoblet)         (301514))
    ((TeleportedOutputRowCount)             (301515))
    // 24.2 starts here
    ((BumpTo_24_2)                          (301600))
    ((DropLegacyWirePartitionKeys)          (301601))
    ((VersionedMapReduceWrite)              (301602))
    ((IntroduceGangManager)                 (301603))
    ((DropOriginalTableSchemaRevision)      (301604))
    ((AcoName)                              (301605))
    ((DisableShrinkingJobs)                 (301606))
    ((MultipleOrderedTasks)                 (301607))
    ((UnusedSnapshotVersion)                (301608))
    ((JobFailTolerance)                     (301609))
    ((NewJobsForbiddenReason)               (301610))
    ((RemoteCopyDynamicTableWithHunks)      (301611))
    ((OffshoreReplicas)                     (301612))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
