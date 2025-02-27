#pragma once

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TAddStoreDescriptor;
class TRemoveStoreDescriptor;
class TAddHunkChunkDescriptor;
class TRemoveHunkChunkDescriptor;
class TMountHint;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;

using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::TDynamicStoreId;
using NTabletClient::NullStoreId;
using NTabletClient::TPartitionId;
using NTabletClient::NullPartitionId;

using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::ETableReplicaMode;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::MinTimestamp;
using NTransactionClient::MaxTimestamp;
using NTransactionClient::SyncLastCommittedTimestamp;
using NTransactionClient::AsyncLastCommittedTimestamp;
using NTransactionClient::AllCommittedTimestamp;
using NTransactionClient::TTransactionActionData;
using NTransactionClient::TTransactionSignature;
using NTransactionClient::TTransactionGeneration;
using NTransactionClient::InitialTransactionSignature;
using NTransactionClient::InitialTransactionGeneration;
using NTransactionClient::FinalTransactionSignature;

using NTableClient::EValueType;
using NTableClient::TLegacyKey;
using NTableClient::TLegacyOwningKey;
using NTableClient::TUnversionedValue;
using NTableClient::TUnversionedValueRange;
using NTableClient::TVersionedValue;
using NTableClient::TUnversionedRow;
using NTableClient::TUnversionedOwningRow;
using NTableClient::TVersionedRow;
using NTableClient::TVersionedOwningRow;
using NTableClient::TColumnFilter;
using NTableClient::TTableSchema;
using NTableClient::TColumnSchema;
using NTableClient::TChunkReaderPerformanceCounters;

using NTabletClient::TTableReplicaId;
using NTabletClient::ETableReplicaState;

using NTransactionSupervisor::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPartitionState,
    (Normal)             // nothing special is happening
    (Splitting)          // split mutation is submitted
    (Merging)            // merge mutation is submitted
    (Compacting)         // compaction is in progress
    (Partitioning)       // partitioning is in progress
    (Sampling)           // sampling is in progress
);

DEFINE_AMBIGUOUS_ENUM_WITH_UNDERLYING_TYPE(ETabletState, int,
    // The only good state admitting read and write requests.
    ((Mounted)                  (0))

    // Unmount workflow.
    ((UnmountFirst)             (1)) // sentinel
    ((UnmountWaitingForLocks)   (1))
    ((UnmountFlushPending)      (2)) // transient, transition to UnmountFlushing is pending
    ((UnmountFlushing)          (3))
    ((UnmountPending)           (4)) // transient, transition to Unmounted is pending
    ((Unmounted)                (5))
    ((UnmountLast)              (5)) // sentinel

    // Freeze workflow.
    ((FreezeFirst)              (6)) // sentinel
    ((FreezeWaitingForLocks)    (6))
    ((FreezeFlushPending)       (7)) // transient, transition to FreezeFlushing is pending
    ((FreezeFlushing)           (8))
    ((FreezePending)            (9)) // transient, transition to Frozen is pending
    ((Frozen)                  (10))
    ((FreezeLast)              (10)) // sentinel

    ((Orphaned)               (100))
);

DEFINE_ENUM(EStoreType,
    ((SortedDynamic)    (0))
    ((SortedChunk)      (1))
    ((OrderedDynamic)   (2))
    ((OrderedChunk)     (3))
    ((HunkChunk)        (4))
);

DEFINE_ENUM(EStoreState,
    ((Undefined)           (-1)) // not initialized
    ((ActiveDynamic)        (0)) // dynamic, can receive updates
    ((PassiveDynamic)       (1)) // dynamic, rotated and cannot receive more updates

    ((Persistent)           (2)) // stored in a chunk

    ((RemovePrepared)       (7)) // tablet stores update transaction has been prepared

    ((Orphaned)             (9)) // belongs to a forcefully removed tablet
    ((Removed)             (10)) // removed by rotation but still locked
);

DEFINE_ENUM(EHunkChunkState,
    ((Active)          (0))
    ((RemovePrepared)  (1))
    ((Removed)         (2))
);

// See comments at TSmoothMovementTracker for description of stage transitions.
DEFINE_ENUM(ESmoothMovementStage,
    ((None)                              (0))

    // Source: accepts all writes. Waits for running tablet stores updates to finish
    // but does not accept new tablet stores updates.
    // Target: rejects reads and writes.
    ((TargetAllocated)                   (1))

    // Source: rejects all writes and compactions, except for common dynamic store
    // flushes. Waits for transactions to finish.
    // Target: n/a.
    ((WaitingForLocksBeforeActivation)   (2))

    // Source: Reads, writes and compactions are allowed. Writes are forwarded
    // to the target.
    // Target: rejects reads and writes. Wait for common dynamic stores flush.
    ((TargetActivated)                   (3))

    // Source: waits for running tablet stores updates to finish, do not accept new.
    // Accepts reads, rejects writes.
    // Target: rejects reads and writes, waits for source to confirm servant switch.
    ((ServantSwitchRequested)            (4))

    // Source: rejects all writes and compactions. Waits for transactions to finish.
    // Target: n/a.
    ((WaitingForLocksBeforeSwitch)       (5))

    // Source: rejects reads and writes, responds with redirect error code to
    // all client requests. No more source-target avenue messages can be sent.
    // Target: fully functional now, accepts reads and writes, may perform compaction.
    // Waits for client cache invalidation to deactivate source.
    ((ServantSwitched)                   (6))

    // Source: n/a.
    // Target: intermediate state needed to send source deactivation message.
    ((SourceDeactivationRequested)       (7))
);

DEFINE_ENUM(ESmoothMovementRole,
    ((None)     (0))
    ((Source)   (1))
    ((Target)   (2))
);

DEFINE_ENUM(EStoreFlushState,
    (None)
    (Running)
    (Complete)
);

DEFINE_ENUM(EStoreCompactionState,
    (None)
    (Running)
    (Complete)
);

DEFINE_ENUM(EHunkChunkSweepState,
    (None)
    (Running)
    (Complete)
);

DEFINE_ENUM(EStorePreloadState,
    (None)
    (Scheduled)
    (Running)
    (Complete)
    (Failed)
);

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Mutation)
    (Read)
    (Write)
);

DEFINE_ENUM(EDynamicTableProfilingMode,
    (Disabled)
    (Path)
    (Tag)
    (PathLetters)
);

DEFINE_ENUM(ETabletDynamicMemoryType,
    (Active)
    (Passive)
    (Backing)
    (Other)
);

DEFINE_ENUM(EPeriodicCompactionMode,
    // Periodically compact each store independently.
    (Store)
    // Periodically compact all stores in each partition together.
    (Partition)
);

DEFINE_ENUM(EHunkStoreState,
    ((Undefined)              (0)) // not initialized
    ((Allocated)              (1)) // ready to receive data
    ((Active)                 (2)) // ready to receive data; used by tablet for new writes
    ((Passive)                (3)) // rotated and cannot receive data
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTabletHydraManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TRelativeReplicationThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(TRowDigestCompactionConfig)
DECLARE_REFCOUNTED_STRUCT(TBuiltinTableMountConfig)
DECLARE_REFCOUNTED_STRUCT(TCustomTableMountConfig)
DECLARE_REFCOUNTED_STRUCT(TTableMountConfig)
DECLARE_REFCOUNTED_STRUCT(TTransactionManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletCellWriteManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletHunkLockManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreBackgroundActivityOrchidConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreFlusherConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreFlusherDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreCompactorConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreCompactorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreTrimmerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(THunkChunkSweeperDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TInMemoryManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TInMemoryManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TPartitionBalancerConfig)
DECLARE_REFCOUNTED_STRUCT(TPartitionBalancerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TCompressionDictionaryBuilderConfig)
DECLARE_REFCOUNTED_STRUCT(TCompressionDictionaryBuilderDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSmoothMovementTrackerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSmoothMovementTrackerTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TSecurityManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TSecurityManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletStoreReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletHunkReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TResourceLimitsConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicatorHintConfig)
DECLARE_REFCOUNTED_STRUCT(THintManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletHunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TTableIOConfigPatch)
DECLARE_REFCOUNTED_CLASS(TTableConfigPatch)
DECLARE_REFCOUNTED_CLASS(TTableConfigExperiment)
DECLARE_REFCOUNTED_CLASS(TClusterTableConfigPatchSet)
DECLARE_REFCOUNTED_STRUCT(TBackupManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(THunkStorageMountConfig)
DECLARE_REFCOUNTED_STRUCT(THunkStoreWriterConfig)
DECLARE_REFCOUNTED_CLASS(THunkStoreWriterOptions)
DECLARE_REFCOUNTED_CLASS(TServiceMethod)
DECLARE_REFCOUNTED_STRUCT(TServiceMethodConfig)
DECLARE_REFCOUNTED_STRUCT(TOverloadTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TOverloadControllerConfig)
DECLARE_REFCOUNTED_STRUCT(TStatisticsReporterConfig)
DECLARE_REFCOUNTED_STRUCT(TMediumThrottlersConfig)
DECLARE_REFCOUNTED_STRUCT(TErrorManagerConfig)

using TTabletStoreWriterConfig = NTableClient::TTableWriterConfig;
using TTabletStoreWriterConfigPtr = NTableClient::TTableWriterConfigPtr;

using TTabletStoreWriterOptions = NTableClient::TTableWriterOptions;
using TTabletStoreWriterOptionsPtr = NTableClient::TTableWriterOptionsPtr;

using TTabletHunkWriterOptions = NChunkClient::TMultiChunkWriterOptions;
using TTabletHunkWriterOptionsPtr = NChunkClient::TMultiChunkWriterOptionsPtr;

//! This is the hard limit.
//! Moreover, it is quite expensive to be graceful in preventing it from being exceeded.
//! The soft limit, thus, is significantly smaller.
constexpr i64 HardRevisionsPerDynamicStoreLimit = 1ULL << 26;
constexpr i64 SoftRevisionsPerDynamicStoreLimit = 1ULL << 25;

constexpr int DefaultMaxOverlappingStoreCount = 30;

//! Number of potential dynamic stores per tablet (that is, created stores and
//! preallocated ids). This number of stores is initially sent by the master
//! during mount, then store flusher tries to maintain it further.
//!
//! NB: Changing this constant requires promoting master reign.
constexpr int DynamicStoreIdPoolSize = 2;

//! Limit on the number of dynamic stores per tablet.
constexpr int DynamicStoreCountLimit = 10;

//! Sentinel index for Eden partition.
constexpr int EdenIndex = -1;

DEFINE_ENUM(ETabletNodeThrottlerKind,
    //! Controls outgoing bandwidth used by store flushes.
    (StoreFlushOut)
    //! Controls incoming bandwidth used by store compactions.
    (StoreCompactionAndPartitioningIn)
    //! Controls outgoing bandwidth used by store compactions.
    (StoreCompactionAndPartitioningOut)
    //! Controls incoming bandwidth used by table replication.
    (ReplicationIn)
    //! Controls outgoing bandwidth used by table replication.
    (ReplicationOut)
    //! Controls incoming bandwidth used by in-memory tables preload.
    (StaticStorePreloadIn)
    //! Controls outgoing bandwidth used by dynamic store remote reads.
    (DynamicStoreReadOut)
    //! Controls incoming bandwidth used by lookups and selects
    //! that corresponds to tablet and data node interaction.
    (UserBackendIn)
    //! Controls outgoing bandwidth generated by user rpc responses.
    (UserBackendOut)
    //! Controls outgoing bandwidth used by remote changelogs.
    (ChangelogOut)
    //! Controls outgoing bandwidth used by remote snapshot store.
    (SnapshotOut)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
