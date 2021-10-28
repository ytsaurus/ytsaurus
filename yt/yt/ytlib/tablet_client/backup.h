#pragma once

#include "public.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

//! Backup state of a table as a whole. Stored both at native and external cells.
DEFINE_ENUM(ETableBackupState,
    //! Regular table with no restrictions.
    ((None)                        (0))

    //! Backup destination. Table cannot be mounted, resharded etc.
    ((BackupCompleted)             (1))

    //! Table was restored and can be mounted and resharded but still has
    //! some restrictions, e.g. cannot be altered to static.
    //! When secondary cell removes restrictions table transitions to |None| backup state.
    ((RestoredWithRestrictions)    (2))
)

//! Backup state of individual tables. Stored at the external cell. Also aggregate value
//! for all tables of the table is stored.
//!
//! Aggregation rules:
//!  - if there is at least one |BackupFailed|/|RestoreFailed| state, so is aggregate;
//!  - otherwise, if all states are the same, so is aggregate;
//!  - otherwise, aggregate is |Mixed|.
DEFINE_ENUM(ETabletBackupState,
    ((None)                        (0))

    // Source states.
    //! Barrier timestamp was sent to tablet cell, pending confirmation.
    ((BarrierRequested)            (1))
    //! Tablet cell confirmed barrier timestamp.
    ((BarrierConfirmed)            (2))

    // Destination states.
    //! Table was cloned but backup is not yet finished.
    ((BackupStarted)               (3))
    //! Backup is created but is invalid.
    ((BackupFailed)                (4))
    //! Backup is completed.
    ((BackupCompleted)             (5))

    // Restore states.
    //! Table was cloned but restore is not yet finished.
    ((RestoreStarted)              (6))
    //! Table is restored but the result is invalid.
    ((RestoreFailed)               (7))
    // NB: When restore is completed the table transitions to |None| state.

    // Only applied to aggregate state.
    ((Mixed)                       (8))
)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
