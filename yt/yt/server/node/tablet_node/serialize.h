#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

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
    ((Hunks)                              (100201)) // babenko
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
