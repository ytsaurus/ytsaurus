#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/table_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <server/hydra/public.h>

#include <server/hive/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;

using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::NullStoreId;
using NTabletClient::TPartitionId;
using NTabletClient::NullPartitionId;

using NTabletClient::TTabletCellConfig;
using NTabletClient::TTabletCellConfigPtr;
using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::SyncLastCommittedTimestamp;
using NTransactionClient::AsyncLastCommittedTimestamp;
using NTransactionClient::AllCommittedTimestamp;

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

using NHive::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPartitionState,
    (Normal)             // nothing special is happening
    (Splitting)          // split mutation is submitted
    (Merging)            // merge mutation is submitted
    (Compacting)         // compaction is in progress
    (Partitioning)       // partitioning is in progress
    (Sampling)           // sampling is in progress
);

DEFINE_ENUM(ETabletState,
    // The only good state admitting read and write requests.
    ((Mounted)           (0))

    // NB: All states below are for unmounting workflow only!
    ((WaitingForLocks)   (1))
    ((FlushPending)      (2)) // transient, transition to Flushing is pending
    ((Flushing)          (3))
    ((UnmountPending)    (4)) // transient, transition to Unmounted is pending
    ((Unmounted)         (5))
);

DEFINE_ENUM(EStoreType,
    (DynamicMemory)
    (Chunk)
);

DEFINE_ENUM(EStoreState,
    ((ActiveDynamic)        (0)) // dynamic, can receive updates
    ((PassiveDynamic)       (1)) // dynamic, rotated and cannot receive more updates

    ((Persistent)           (2)) // stored in a chunk

    ((RemoveCommitting)     (7)) // UpdateTabletStores request sent to master

    ((Orphaned)             (9)) // belongs to a forcefully removed tablet
    ((Removed)             (10)) // removed by rotation but still locked
);

DEFINE_ENUM(EStoreFlushState,
    (None)
    (Running)
    (Complete)
    (Failed)
);

DEFINE_ENUM(EStoreCompactionState,
    (None)
    (Running)
    (Complete)
    (Failed)
);

DEFINE_ENUM(EStoreRemovalState,
    (None)
    (Pending)
    (Failed)
);

DEFINE_ENUM(EStorePreloadState,
    (Disabled)
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

DEFINE_ENUM(EInMemoryMode,
    (None)
    (Compressed)
    (Uncompressed)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletHydraManageConfig)
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

DECLARE_REFCOUNTED_CLASS(TSlotManager)
DECLARE_REFCOUNTED_CLASS(TTabletSlot)
DECLARE_REFCOUNTED_CLASS(TTabletAutomaton)

class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_CLASS(TTabletManager)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

class TPartition;
class TTablet;

DECLARE_REFCOUNTED_STRUCT(TKeyList)
DECLARE_REFCOUNTED_STRUCT(TPartitionSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletPerformanceCounters)

class TTransaction;

DECLARE_REFCOUNTED_STRUCT(IStore)

DECLARE_REFCOUNTED_CLASS(TDynamicMemoryStore)
DECLARE_REFCOUNTED_CLASS(TChunkStore)
DECLARE_REFCOUNTED_CLASS(TStoreManager)
DECLARE_REFCOUNTED_CLASS(TSecurityManager)

DECLARE_REFCOUNTED_STRUCT(TInterceptedChunkData)
DECLARE_REFCOUNTED_CLASS(TInMemoryManager)

struct TDynamicRowHeader;
class TDynamicRow;
struct TDynamicRowRef;

union TDynamicValueData;
struct TDynamicValue;

struct TEditListHeader;
template <class T>
class TEditList;
using TValueList = TEditList<TDynamicValue>;
using TRevisionList = TEditList<ui32>;

using TTabletWriterOptions = NTableClient::TTableWriterOptions;
using TTabletWriterOptionsPtr = NTableClient::TTableWriterOptionsPtr;

//! This is the hard limit.
//! Moreover, it is quite expensive to be graceful in preventing it from being exceeded.
//! The soft limit, thus, is significantly smaller.
static const i64 HardRevisionsPerDynamicMemoryStoreLimit = 1ULL << 26;
static const i64 SoftRevisionsPerDynamicMemoryStoreLimit = 1ULL << 25;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
