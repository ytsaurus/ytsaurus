#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/serialize.h>

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
    ((MountConfigExperiments)             (100604)) // ifsmirnov
    // 23.1 starts here.
    ((LockingState)                       (100700)) // gritukan
    ((JournalHunks)                       (100701)) // aleksandra-zh
    ((FixHunkStorage)                     (100702)) // gritukan
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    explicit TSaveContext(NHydra::ICheckpointableOutputStream* output);

    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    explicit TLoadContext(NHydra::ICheckpointableInputStream* input);

    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
