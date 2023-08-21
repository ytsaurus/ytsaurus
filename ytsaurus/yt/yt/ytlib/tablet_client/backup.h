#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/proto/backup.pb.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

//! Backup state of a table as a whole. Stored both at native and external cells.
DEFINE_ENUM(ETableBackupState,
    //! Regular table with no restrictions.
    ((None)                        (0))

    //! Backup destination. Table cannot be mounted, resharded etc.
    ((BackupCompleted)             (1))
);

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
    //! Checkpoint timestamp was sent to tablet cell, pending confirmation.
    ((CheckpointRequested)         (1))
    //! Tablet cell confirmed checkpoint timestamp.
    ((CheckpointConfirmed)         (2))
    //! Tablet cell rejected checkpoint timestamp.
    ((CheckpointRejected)          (3))
    //! Backup was interrupted by unmount.
    ((CheckpointInterrupted)       (10))

    // Destination states.
    //! Table was cloned but backup is not yet finished.
    ((BackupStarted)               (4))
    //! Backup is created but is invalid.
    ((BackupFailed)                (5))
    //! Backup is completed.
    ((BackupCompleted)             (6))

    // Restore states.
    //! Table was cloned but restore is not yet finished.
    ((RestoreStarted)              (7))
    //! Table is restored but the result is invalid.
    ((RestoreFailed)               (8))
    // NB: When restore is completed the table transitions to |None| state.

    // Only applied to aggregate state.
    ((Mixed)                       (9))
);

//! Describes the backup mode of a certain table.
DEFINE_ENUM(EBackupMode,
    // Sentinel, not used.
    ((None)                           (0))

    ((Sorted)                         (1))
    ((OrderedStrongCommitOrdering)    (2))
    ((OrderedExact)                   (3))
    ((OrderedAtLeast)                 (4))
    ((OrderedAtMost)                  (5))
    ((SortedSyncReplica)              (6))
    ((SortedAsyncReplica)             (7))
    ((ReplicatedSorted)               (8))
);

////////////////////////////////////////////////////////////////////////////////

struct TTableReplicaBackupDescriptor
{
    TTableReplicaId ReplicaId;
    ETableReplicaMode Mode = ETableReplicaMode::Sync;
    TString ReplicaPath;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(
    NProto::TTableReplicaBackupDescriptor* protoDescriptor,
    const TTableReplicaBackupDescriptor& descriptor);

void FromProto(
    TTableReplicaBackupDescriptor* descriptor,
    const NProto::TTableReplicaBackupDescriptor& protoDescriptor);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
