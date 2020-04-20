#pragma once

#include <yt/server/lib/hive/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TAddStoreDescriptor;
class TRemoveStoreDescriptor;

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

using NTabletClient::TTransactionSignature;
using NTabletClient::InitialTransactionSignature;
using NTabletClient::FinalTransactionSignature;

using NTabletClient::TTabletCellConfig;
using NTabletClient::TTabletCellConfigPtr;
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

using NTableClient::EValueType;
using NTableClient::TKey;
using NTableClient::TOwningKey;
using NTableClient::TUnversionedValue;
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

using NHiveServer::ETransactionState;

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
    ((FreezeFlushPending)       (7)) // transient, transition to UnmountFlushing is pending
    ((FreezeFlushing)           (8))
    ((FreezePending)            (9)) // transient, transition to Unmounted is pending
    ((Frozen)                  (10))
    ((FreezeLast)              (10)) // sentinel

    ((Orphaned)               (100))
);

DEFINE_ENUM(EStoreType,
    (SortedDynamic)
    (SortedChunk)
    (OrderedDynamic)
    (OrderedChunk)
);

DEFINE_ENUM(EStoreState,
    ((Undefined)           (-1)) // not initailized
    ((ActiveDynamic)        (0)) // dynamic, can receive updates
    ((PassiveDynamic)       (1)) // dynamic, rotated and cannot receive more updates

    ((Persistent)           (2)) // stored in a chunk

    ((RemovePrepared)       (7)) // tablet stores update transaction has been prepared

    ((Orphaned)             (9)) // belongs to a forcefully removed tablet
    ((Removed)             (10)) // removed by rotation but still locked
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

DEFINE_ENUM(EStorePreloadState,
    (None)
    (Scheduled)
    (Running)
    (Complete)
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
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletHydraManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletManagerConfig)
DECLARE_REFCOUNTED_CLASS(TStoreFlusherConfig)
DECLARE_REFCOUNTED_CLASS(TStoreCompactorConfig)
DECLARE_REFCOUNTED_CLASS(TInMemoryManagerConfig)
DECLARE_REFCOUNTED_CLASS(TPartitionBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TTabletNodeConfig)

using TTabletChunkWriterConfig = NTableClient::TTableWriterConfig;
using TTabletChunkWriterConfigPtr = NTableClient::TTableWriterConfigPtr;

using TTabletWriterOptions = NTableClient::TTableWriterOptions;
using TTabletWriterOptionsPtr = NTableClient::TTableWriterOptionsPtr;

//! This is the hard limit.
//! Moreover, it is quite expensive to be graceful in preventing it from being exceeded.
//! The soft limit, thus, is significantly smaller.
constexpr i64 HardRevisionsPerDynamicStoreLimit = 1ULL << 26;
constexpr i64 SoftRevisionsPerDynamicStoreLimit = 1ULL << 25;

constexpr int DefaultMaxOverlappingStoreCount = 30;

// Changing this constant requires promoting master reign.
constexpr int DynamicStoreIdPoolSize = 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
