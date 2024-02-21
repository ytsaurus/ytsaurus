#pragma once

#include <yt/yt/server/lib/tablet_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TIdGenerator;
class TReqReplicateTabletContent;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

bool IsInUnmountWorkflow(ETabletState state);
bool IsInFreezeWorkflow(ETabletState state);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletDistributedThrottlerKind,
    // RPS throttlers.
    (StoresUpdate)

    // Throughput throttlers.
    (Lookup)
    (Select)
    (CompactionRead)
    (Write)

    // Disk IO.
    (ChangelogMediumWrite)
    (BlobMediumWrite)
);

DEFINE_ENUM(EHunkCompactionReason,
    (None)
    (ForcedCompaction)
    (GarbageRatioTooHigh)
    (HunkChunkTooSmall)
);

DEFINE_ENUM(ETabletWriteMode,
    ((Direct)         (0))
    ((Pull)           (1))
);

DEFINE_ENUM(EBackupStage,
    ((None)                           (0))
    ((TimestampReceived)              (1))
    ((FeasibilityConfirmed)           (2))
    ((AwaitingReplicationFinish)      (3))
    ((RespondedToMasterSuccess)       (4))
    ((RespondedToMasterFailure)       (5))
);

DEFINE_ENUM(ETabletLockType,
    // Lock of this kind is acquired on leader during #Write RPC call
    // and is released when corresponding mutation is committed or
    // when epoch finishes.
    ((TransientWrite)                (0))
    // Lock of this kind is acquired by transaction that transiently
    // affects tablet and is released when corresponding transaction
    // is finished (committed, aborted or serialized) or when epoch
    // finishes.
    ((TransientTransaction)          (1))
    // Lock of this kind is acquired by transaction that persistently
    // affects tablet and is released when corresponding transaction
    // is finished (committed, aborted or serialized).
    // Note that transaction may hold both transient and persistent
    // locks on tablet.
    ((PersistentTransaction)         (2))
);

DEFINE_ENUM(EChunkViewSizeStatus,
    (CompactionRequired)
    (CompactionNotRequired)
);

using TTabletDistributedThrottlersVector = TEnumIndexedArray<
    ETabletDistributedThrottlerKind,
    NConcurrency::IReconfigurableThroughputThrottlerPtr>;
using THunkStoreId = NChunkClient::TChunkId;

struct IBootstrap;

DECLARE_REFCOUNTED_STRUCT(ITabletSnapshotStore)
DECLARE_REFCOUNTED_STRUCT(ISlotManager)
DECLARE_REFCOUNTED_STRUCT(IMasterConnector)
DECLARE_REFCOUNTED_STRUCT(IHintManager)
DECLARE_REFCOUNTED_STRUCT(ITabletHedgingManagerRegistry)
DECLARE_REFCOUNTED_STRUCT(IHedgingManagerRegistry)
DECLARE_REFCOUNTED_STRUCT(ITabletSlot)
DECLARE_REFCOUNTED_STRUCT(IRelativeReplicationThrottler)
DECLARE_REFCOUNTED_STRUCT(IMutationForwarder)

DECLARE_REFCOUNTED_CLASS(TTableDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TMutationForwarderThunk)
DECLARE_REFCOUNTED_CLASS(TTabletAutomaton)

DECLARE_REFCOUNTED_STRUCT(TRuntimeTabletCellData)

class TSaveContext;
class TLoadContext;
enum class ETabletReign;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, ETabletReign>;

DECLARE_REFCOUNTED_STRUCT(ITabletManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(ITabletCellWriteManager)
DECLARE_REFCOUNTED_STRUCT(ITabletCellWriteManagerHost)
DECLARE_REFCOUNTED_STRUCT(ITabletWriteManager)
DECLARE_REFCOUNTED_STRUCT(ITabletWriteManagerHost)
DECLARE_REFCOUNTED_STRUCT(IBackupManager)
DECLARE_REFCOUNTED_STRUCT(ISmoothMovementTracker)
DECLARE_REFCOUNTED_STRUCT(ISmoothMovementTrackerHost)

class TPartition;
class TTableReplicaInfo;

struct TTableSettings;

DECLARE_REFCOUNTED_STRUCT(ITransactionManagerHost)

DECLARE_REFCOUNTED_STRUCT(TRuntimeTabletData)
DECLARE_REFCOUNTED_STRUCT(TRuntimeTableReplicaData)
DECLARE_REFCOUNTED_STRUCT(TChaosTabletData)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TDirectObjectIdHash)

DECLARE_REFCOUNTED_STRUCT(TSampleKeyList)
DECLARE_REFCOUNTED_STRUCT(TPartitionSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTableReplicaSnapshot)
DECLARE_REFCOUNTED_STRUCT(TRefCountedReplicationProgress)
DECLARE_REFCOUNTED_CLASS(TTableProfiler)
DECLARE_REFCOUNTED_CLASS(TChunkIOProfiler)

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

DECLARE_REFCOUNTED_CLASS(THunkChunk)

DECLARE_REFCOUNTED_STRUCT(IHunkLockManager)

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

DECLARE_REFCOUNTED_CLASS(TPreloadedBlockCache)

DECLARE_REFCOUNTED_STRUCT(TInMemoryChunkData)
DECLARE_REFCOUNTED_STRUCT(IInMemoryManager)
DECLARE_REFCOUNTED_STRUCT(IRemoteInMemoryBlockCache)
DECLARE_REFCOUNTED_STRUCT(IVersionedChunkMetaManager)
DECLARE_REFCOUNTED_CLASS(TVersionedChunkMetaCacheEntry)

DECLARE_REFCOUNTED_CLASS(TTableReplicator)

DECLARE_REFCOUNTED_STRUCT(ITablePuller)
DECLARE_REFCOUNTED_STRUCT(IChaosAgent)

DECLARE_REFCOUNTED_CLASS(TStatisticsReporter)

DECLARE_REFCOUNTED_STRUCT(IStoreCompactor)
DECLARE_REFCOUNTED_STRUCT(IStoreFlusher)
DECLARE_REFCOUNTED_STRUCT(IStoreRotator)
DECLARE_REFCOUNTED_STRUCT(IStoreTrimmer)
DECLARE_REFCOUNTED_STRUCT(IPartitionBalancer)
DECLARE_REFCOUNTED_STRUCT(IBackingStoreCleaner)
DECLARE_REFCOUNTED_STRUCT(IHunkChunkSweeper)
DECLARE_REFCOUNTED_STRUCT(ILsmInterop)
DECLARE_REFCOUNTED_STRUCT(ICompressionDictionaryBuilder)

DECLARE_REFCOUNTED_STRUCT(IStructuredLogger)
DECLARE_REFCOUNTED_STRUCT(IPerTabletStructuredLogger)

DECLARE_REFCOUNTED_CLASS(TRowCache)

DECLARE_REFCOUNTED_STRUCT(IDistributedThrottlerManager)

DECLARE_REFCOUNTED_STRUCT(IBackendChunkReadersHolder)

DECLARE_REFCOUNTED_CLASS(TOverloadController)
DECLARE_REFCOUNTED_CLASS(TMeanWaitTimeTracker)
DECLARE_REFCOUNTED_CLASS(TCongestionController)
DECLARE_REFCOUNTED_CLASS(TCompactionHintFetcher)

DECLARE_REFCOUNTED_CLASS(TErrorManager)
DECLARE_REFCOUNTED_CLASS(TMediumThrottlerManager);

struct TSortedDynamicRowHeader;
class TSortedDynamicRow;

struct ITabletContext;

struct TWriteContext;

using TSyncReplicaIdList = TCompactVector<TTableReplicaId, 2>;

using TObjectId = TGuid;

DEFINE_ENUM(EObjectLockMode,
    ((Exclusive)              (1))
    ((Shared)                 (2))
);

DECLARE_REFCOUNTED_STRUCT(ICompressionDictionaryManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(THunkTablet, TTabletId, NObjectClient::TDirectObjectIdHash)

DECLARE_REFCOUNTED_STRUCT(IHunkTabletHost)
DECLARE_REFCOUNTED_STRUCT(IHunkTabletManager)
DECLARE_REFCOUNTED_STRUCT(IHunkTabletScanner)

DECLARE_REFCOUNTED_CLASS(THunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
