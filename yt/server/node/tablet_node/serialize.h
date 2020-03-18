#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletReign,
    ((SafeReplicatedLogSchema)       (100012)) // savrus
    ((BulkInsert)                    (100013)) // savrus
    ((GiantTabletProblem)            (100014)) // akozhikhov
    ((ChunkViewsForPivots)           (100015)) // akozhikhov
    ((BulkInsertOverwrite)           (100016)) // ifsmirnov
    ((ChunkViewWideRange_YT_12532)   (100017)) // ifsmirnov
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
