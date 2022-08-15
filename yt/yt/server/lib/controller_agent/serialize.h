#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 22.2 start here
    ((MajorUpdateTo22_2)                    (300901))
    ((DropUnavailableInputChunkCount)       (300902))
    ((ResourceOverdraftState)               (300903))
    ((ResourceOverdraftJobProxy)            (300904))
    ((ResourceOverdraftJobId)               (300905))
    ((DropLogAndProfile)                    (300906))
    ((ProbingJobsFix)                       (300907))
    ((IsStartedFlag)                        (300908))
    // 22.3 start here
    ((DropUnusedOperationId)                (301001))
    ((ChangeUnorderedMergeInheritance)      (301002))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
