#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 22.3 start here
    ((DropUnusedOperationId)                (301001))
    ((ChangeUnorderedMergeInheritance)      (301002))
    ((DoNotPersistStatistics)               (301003))
    // 22.4 starts here
    ((BumpTo_22_4)                          (301100))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
