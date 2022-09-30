#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// If reign change is disallowed, tablet node will crash if cell snapshot reign
// differs from node reign. This is useful for local mode where occasional cell
// state migration may end up with a disaster.
void SetReignChangeAllowed(bool allowed);
bool IsReignChangeAllowed();

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletReign,
    // 22.2 starts here.
    ((CumulativeDataWeight)               (100500)) // achulkov2
    ((CommitSignature)                    (100501)) // gritukan
    ((MountConfig)                        (100502)) // ifsmirnov
    ((BackupsReplicated)                  (100503)) // ifsmirnov
    ((PersistLastReadLockTimestamp)       (100504)) // gritukan
    ((SuspendTabletCells)                 (100505)) // gritukan
    ((FixSuspendTabletCells)              (100506)) // gritukan
    ((AnotherFixSuspendTabletCells)       (100507)) // gritukan
    ((ReplicationTxLocksTablet)           (100508)) // gritukan
    ((FixCommittedReplicationRowIndex)    (100509)) // ifsmirnov
    ((DoNotLockUnmountingTablet)          (100510)) // gritukan
    ((SavePreparedReplicatorTxs)          (100511)) // ifsmirnov
    ((DoNotWriteToUnmountedTablet)        (100512)) // gritukan
    ((SometimesWriteToUnmountedTablet)    (100513)) // gritukan
    ((AdvanceReplicationRound222)         (100514)) // savrus
    // 22.3 starts here.
    ((TabletWriteManager)                 (100600)) // gritukan
    ((HunkTablets)                        (100601)) // gritukan
    ((AdvanceReplicationRound)            (100602)) // savrus
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
