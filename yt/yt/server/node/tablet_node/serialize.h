#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/serialize.h>

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
    ((AbortTransactionOnCorruptedChaosProgress)    (100808)) // osidorkin
    // 24.1 starts here.
    ((NoMountRevisionCheckInBulkInsert)            (100900)) // ifsmirnov
    ((SharedWriteLocks)                            (100901)) // ponasenko-rs
    ((TabletPrerequisites)                         (100902)) // gritukan
    ((ValueDictionaryCompression)                  (100903)) // akozhikhov
    ((SaneTxActionAbort)                           (100904)) // kvk1920
    ((HunkValueDictionaryCompression)              (100905)) // akozhikhov
    ((SaneTxActionAbortFix)                        (100906)) // kvk1920
    ((SmoothTabletMovement)                        (100907)) // ifsmirnov
    ((DistributedTabletPrerequisites)              (100908)) // gritukan
    ((HiveManagerLamportTimestamp)                 (100909)) // danilalexeev
    ((ChunkReplicaAlwaysPrecache)                  (100910)) // osidorkin
    ((FixCDWComputationForChaosReplicas)           (100911)) // akozhikhov
    ((SmoothMovementDynamicStoreRead_24_1)         (100912)) // ifsmirnov
    // 24.2 starts here.
    ((Start_24_2)                                  (101000)) // ponasenko-rs
    ((AddTabletCustomRuntimeData)                  (101001)) // gryzlov-ad
    ((SmoothMovementDynamicStoreRead)              (101002)) // ifsmirnov
);

static_assert(TEnumTraits<ETabletReign>::IsMonotonic, "Tablet reign enum is not monotonic");

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
    using NHydra::TLoadContext::TLoadContext;

    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
