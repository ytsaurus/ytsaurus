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
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
