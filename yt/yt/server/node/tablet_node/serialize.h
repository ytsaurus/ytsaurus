#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletReign,
    ((SafeReplicatedLogSchema)            (100012)) // savrus
    ((BulkInsert)                         (100013)) // savrus
    ((GiantTabletProblem)                 (100014)) // akozhikhov
    ((ChunkViewsForPivots)                (100015)) // akozhikhov
    ((BulkInsertOverwrite)                (100016)) // ifsmirnov
    ((ChunkViewWideRange_YT_12532)        (100017)) // ifsmirnov
    ((DynamicStoreRead)                   (100100)) // ifsmirnov
    ((AuthenticationIdentity)             (100101)) // babenko
    ((MountHint)                          (100102)) // ifsmirnov
    ((ReplicationBarrier_YT_14346)        (100103)) // babenko
    ((RowBufferEmptyRowDeserialization)   (100200)) // max42
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
