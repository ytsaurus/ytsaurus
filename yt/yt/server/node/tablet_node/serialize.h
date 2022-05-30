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
    // 22.1 starts here.
    ((NewLockMasks)                       (100400)) // gritukan
    ((Chaos)                              (100401)) // savrus
    ((BackupsSorted)                      (100402)) // ifsmirnov
    ((MaxClipTimestamp)                   (100403)) // ifsmirnov
    ((SerializeForeign)                   (100404)) // savrus
    ((SerializeReplicationProgress)       (100405)) // savrus
    ((LongReplicationRound)               (100406)) // savrus
    // Late 22.1 starts here
    ((BackupsOrdered)                     (100450)) // ifsmirnov
    // 22.2 starts here.
    ((CumulativeDataWeight)               (100500)) // achulkov2
    ((CommitSignature)                    (100501)) // gritukan
    ((MountConfig)                        (100502)) // ifsmirnov
    ((BackupsReplicated)                  (100503)) // ifsmirnov
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
