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
    ((TabletWriteManager)                          (100600)) // gritukan
    ((HunkTablets)                                 (100601)) // gritukan
    ((AdvanceReplicationRound)                     (100602)) // savrus
    ((ReworkTabletLocks)                           (100603)) // gritukan
    ((MountConfigExperiments)                      (100604)) // ifsmirnov
    // 23.1 starts here.
    ((LockingState)                                (100700)) // gritukan
    ((JournalHunks)                                (100701)) // aleksandra-zh
    ((FixHunkStorage)                              (100702)) // gritukan
    ((SendDynamicStoreInBackup)                    (100703)) // ifsmirnov
    ((FixBulkInsertAtomicityNone)                  (100704)) // ifsmirnov
    ((JournalHunksCommitted)                       (100705)) // aleksandra-zh
    ((RestoreHunkLocks)                            (100706)) // aleksandra-zh
    ((RegisterTxActionsShouldPersistTx_23_1)       (100707)) // ifsmirnov
    // 23.2 starts here.
    ((Avenues)                                     (100800)) // ifsmirnov
    ((TabletIdGenerator)                           (100801)) // ifsmirnov
    ((FixRaceBetweenCompactionUnmount)             (100802)) // dave11ar
    ((InMemoryModeAndBundleInExperimentDescriptor) (100803)) // dave11ar
    ((RegisterTxActionsShouldPersistTx)            (100804)) // ifsmirnov
    ((PersistLastExclusiveLockTimestamp)           (100805)) // ponasenko-rs
    ((ValueDictionaryCompression_23_2)             (100806)) // akozhikhov
    ((HunkValueDictionaryCompression_23_2)         (100807)) // akozhikhov
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    TSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger);

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
