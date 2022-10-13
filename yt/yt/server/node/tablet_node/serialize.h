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
    // 22.3 starts here.
    ((TabletWriteManager)                 (100600)) // gritukan
    ((HunkTablets)                        (100601)) // gritukan
    ((AdvanceReplicationRound)            (100602)) // savrus
    ((ReworkTabletLocks)                  (100603)) // gritukan
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
