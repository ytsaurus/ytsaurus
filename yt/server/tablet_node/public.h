#pragma once

#include <yt/server/hive/public.h>

#include <yt/server/hydra/public.h>

#include <yt/server/tablet_server/public.h>

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

using NTabletServer::TTableReplicaId;
using NTabletServer::ETableReplicaState;

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

DEFINE_ENUM(ETabletState,
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

bool IsInUnmountWorkflow(ETabletState state);
bool IsInFreezeWorkflow(ETabletState state);

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

DECLARE_REFCOUNTED_CLASS(TSlotManager)
DECLARE_REFCOUNTED_CLASS(TTabletSlot)
DECLARE_REFCOUNTED_CLASS(TTabletAutomaton)
DECLARE_REFCOUNTED_STRUCT(TRuntimeTabletCellData)

class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_CLASS(TTabletManager)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

class TPartition;
class TTableReplicaInfo;

DECLARE_REFCOUNTED_STRUCT(TRuntimeTabletData)
DECLARE_REFCOUNTED_STRUCT(TRuntimeTableReplicaData)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TDirectObjectIdHash)

DECLARE_REFCOUNTED_STRUCT(TSampleKeyList)
DECLARE_REFCOUNTED_STRUCT(TPartitionSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTableReplicaSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletPerformanceCounters)

DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, ::THash<TTransactionId>)

DECLARE_REFCOUNTED_STRUCT(IStore)
DECLARE_REFCOUNTED_STRUCT(IDynamicStore)
DECLARE_REFCOUNTED_STRUCT(IChunkStore)
DECLARE_REFCOUNTED_STRUCT(ISortedStore)
DECLARE_REFCOUNTED_STRUCT(IOrderedStore)

DECLARE_REFCOUNTED_CLASS(TSortedDynamicStore)
DECLARE_REFCOUNTED_CLASS(TSortedChunkStore)

DECLARE_REFCOUNTED_CLASS(TOrderedDynamicStore)
DECLARE_REFCOUNTED_CLASS(TOrderedChunkStore)

DECLARE_REFCOUNTED_STRUCT(IStoreManager)
DECLARE_REFCOUNTED_STRUCT(ISortedStoreManager)
DECLARE_REFCOUNTED_STRUCT(IOrderedStoreManager)

DECLARE_REFCOUNTED_CLASS(TSortedStoreManager)
DECLARE_REFCOUNTED_CLASS(TOrderedStoreManager)
DECLARE_REFCOUNTED_CLASS(TReplicatedStoreManager)

DECLARE_REFCOUNTED_CLASS(TSecurityManager)

DECLARE_REFCOUNTED_STRUCT(TInMemoryChunkData)
DECLARE_REFCOUNTED_STRUCT(IInMemoryManager)
DECLARE_REFCOUNTED_STRUCT(IRemoteInMemoryBlockCache)

DECLARE_REFCOUNTED_CLASS(TVersionedChunkMetaManager)

DECLARE_REFCOUNTED_CLASS(TTableReplicator)

struct TSortedDynamicRowHeader;
class TSortedDynamicRow;

union TDynamicValueData;
struct TDynamicValue;

struct TEditListHeader;
template <class T>
class TEditList;
using TValueList = TEditList<TDynamicValue>;
using TRevisionList = TEditList<ui32>;

using TTabletChunkWriterConfig = NTableClient::TTableWriterConfig;
using TTabletChunkWriterConfigPtr = NTableClient::TTableWriterConfigPtr;

using TTabletWriterOptions = NTableClient::TTableWriterOptions;
using TTabletWriterOptionsPtr = NTableClient::TTableWriterOptionsPtr;

struct ITabletContext;

using TSyncReplicaIdList = SmallVector<TTableReplicaId, 2>;

//! This is the hard limit.
//! Moreover, it is quite expensive to be graceful in preventing it from being exceeded.
//! The soft limit, thus, is significantly smaller.
constexpr i64 HardRevisionsPerDynamicStoreLimit = 1ULL << 26;
constexpr i64 SoftRevisionsPerDynamicStoreLimit = 1ULL << 25;

constexpr int EdenIndex = -1;

extern const TString UnknownProfilingTag;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
