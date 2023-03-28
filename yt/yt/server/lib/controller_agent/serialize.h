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
    ((InputStreamDescriptors)               (301104))
    ((ProbingBaseLayer)                     (301105))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
