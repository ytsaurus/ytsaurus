#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 22.3 start here
    ((DropUnusedOperationId)                (301001))
    ((ChangeUnorderedMergeInheritance)      (301002))
    ((DoNotPersistStatistics)               (301003))
    ((SeparateMultipliers)                  (301004))
    ((SwitchIntermediateMedium)             (301005))
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
    ((BatchRowCount_23_2)                   (301309))
    // 24.1 starts here
    ((NodeJobStartTimeInJoblet)             (301408))
    ((JobAbortsUntilOperationFailure)       (301409))
    ((BatchRowCount_24_1)                   (301410))            
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
