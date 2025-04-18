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
    // 24.2 starts here.
    ((Start_24_2)                                  (101000)) // ponasenko-rs
    ((AddTabletCustomRuntimeData)                  (101001)) // gryzlov-ad
    ((SmoothMovementDynamicStoreRead)              (101002)) // ifsmirnov
    // 25.1 starts here.
    ((Start_25_1)                                  (101100)) // ponasenko-rs
    ((DropSamplingTimeCompats)                     (101101)) // sabdenovch
    ((SmoothMovementForwardWrites)                 (101102)) // ifsmirnov
    ((FixPrepareToggleHunkTabletStore)             (101103)) // akozhikhov
    ((CancelTabletTransition_25_1)                 (101104)) // ifsmirnov
    ((PerRowSequencer_25_1_NOOP)                   (101105)) // ponasenko-rs
    ((FixHunkStorageUnmountRoutine_25_1)           (101106)) // akozhikhov
    // 25.2 starts here.
    ((Start_25_2)                                  (101200)) // ponasenko-rs
    ((CancelTabletTransition)                      (101201)) // ifsmirnov
    ((PerRowSequencer)                             (101202)) // ponasenko-rs
    ((FixHunkStorageUnmountRoutine)                (101203)) // akozhikhov
    ((AddTabletMountTime)                          (101204)) // alexelexa
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

    NTableClient::TSchemaData CurrentTabletWriteManagerSchemaData;
    bool CurrentTabletVersionedWriteIsUnversioned;

    ETabletReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
