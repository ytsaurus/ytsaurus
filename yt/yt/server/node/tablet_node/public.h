#pragma once

#include <yt/server/lib/tablet_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

bool IsInUnmountWorkflow(ETabletState state);
bool IsInFreezeWorkflow(ETabletState state);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletDynamicMemoryType,
    (Active)
    (Passive)
    (Backing)
    (Other)
);

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

DECLARE_REFCOUNTED_CLASS(TReaderProfiler)
DECLARE_REFCOUNTED_CLASS(TWriterProfiler)
DECLARE_REFCOUNTED_STRUCT(IStoreManager)
DECLARE_REFCOUNTED_STRUCT(ISortedStoreManager)
DECLARE_REFCOUNTED_STRUCT(IOrderedStoreManager)

DECLARE_REFCOUNTED_CLASS(TSortedStoreManager)
DECLARE_REFCOUNTED_CLASS(TOrderedStoreManager)
DECLARE_REFCOUNTED_CLASS(TReplicatedStoreManager)

DECLARE_REFCOUNTED_CLASS(TLockManager)
using TLockManagerEpoch = i64;

DECLARE_REFCOUNTED_CLASS(TSecurityManager)

DECLARE_REFCOUNTED_STRUCT(TInMemoryChunkData)
DECLARE_REFCOUNTED_STRUCT(IInMemoryManager)
DECLARE_REFCOUNTED_STRUCT(IRemoteInMemoryBlockCache)

DECLARE_REFCOUNTED_CLASS(TVersionedChunkMetaManager)

DECLARE_REFCOUNTED_CLASS(TTableReplicator)

DECLARE_REFCOUNTED_CLASS(TStoreCompactor)

DECLARE_REFCOUNTED_STRUCT(TRowCache)

struct TSortedDynamicRowHeader;
class TSortedDynamicRow;

union TDynamicValueData;
struct TDynamicValue;

struct TEditListHeader;
template <class T>
class TEditList;
using TValueList = TEditList<TDynamicValue>;
using TRevisionList = TEditList<ui32>;

struct ITabletContext;

struct TWriteContext;

using TSyncReplicaIdList = SmallVector<TTableReplicaId, 2>;

constexpr int EdenIndex = -1;

extern const TString UnknownProfilingTag;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
