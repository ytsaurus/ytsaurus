#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletReign,
    ((LongForgottenBase)             (100008)) // aozeritsky
    ((SaveLastCommitTimestamp)       (100009)) // savrus
    ((AddTabletCellLifeState)        (100010)) // savrus
    ((SerializeChunkReadRange)       (100011)) // ifsmirnov
    ((SafeReplicatedLogSchema)       (100012)) // savrus
    ((BulkInsert)                    (100013)) // savrus
    ((GiantTabletProblem)            (100014)) // akozhikhov
    ((ChunkViewsForPivots)           (100015)) // akozhikhov
    ((BulkInsertOverwrite)           (100016)) // ifsmirnov
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
