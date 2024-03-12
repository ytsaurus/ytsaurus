#pragma once

#include "lock_manager.h"
#include "chaos_agent.h"
#include "object_detail.h"
#include "partition.h"
#include "store.h"
#include "sorted_dynamic_comparer.h"
#include "tablet_profiling.h"
#include "tablet_write_manager.h"
#include "row_cache.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/lib/lsm/statistics.h>

#include <yt/yt/server/lib/tablet_node/table_settings.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>
#include <yt/yt/ytlib/table_client/tablet_snapshot.h>

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/concurrency/async_barrier.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/query/base/public.h>

#include <library/cpp/yt/small_containers/compact_set.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/ref_tracked.h>

#include <atomic>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Cf. TRuntimeTabletData.
struct TRuntimeTableReplicaData
    : public TRefCounted
{
    std::atomic<ETableReplicaMode> Mode = ETableReplicaMode::Async;
    std::atomic<i64> CurrentReplicationRowIndex = 0;
    std::atomic<i64> CommittedReplicationRowIndex = 0;
    std::atomic<TTimestamp> CurrentReplicationTimestamp = NullTimestamp;
    std::atomic<TTimestamp> LastReplicationTimestamp = NullTimestamp;
    std::atomic<i64> PreparedReplicationRowIndex = -1;
    std::atomic<bool> PreserveTimestamps = true;
    std::atomic<NTransactionClient::EAtomicity> Atomicity = NTransactionClient::EAtomicity::Full;
    TAtomicObject<TError> Error;
    std::atomic<NTabletClient::ETableReplicaStatus> Status = NTabletClient::ETableReplicaStatus::Unknown;

    void Populate(NTabletClient::NProto::TTableReplicaStatistics* statistics) const;
    void MergeFrom(const NTabletClient::NProto::TTableReplicaStatistics& statistics);
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTableReplicaData)

////////////////////////////////////////////////////////////////////////////////

struct TTableReplicaSnapshot
    : public TRefCounted
{
    NTransactionClient::TTimestamp StartReplicationTimestamp;
    TRuntimeTableReplicaDataPtr RuntimeData;
    TReplicaCounters Counters;
};

DEFINE_REFCOUNTED_TYPE(TTableReplicaSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct TChaosTabletData
    : public TRefCounted
{
    std::atomic<ui64> ReplicationRound = 0;
    TAtomicObject<THashMap<TTabletId, i64>> CurrentReplicationRowIndexes;
    TTransactionId PreparedWritePulledRowsTransactionId;
    TTransactionId PreparedAdvanceReplicationProgressTransactionId;
};

DEFINE_REFCOUNTED_TYPE(TChaosTabletData)

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedReplicationProgress
    : public NChaosClient::TReplicationProgress
    , public TRefCounted
{
    TRefCountedReplicationProgress() = default;

    explicit TRefCountedReplicationProgress(
        const NChaosClient::TReplicationProgress& progress);
    explicit TRefCountedReplicationProgress(
        NChaosClient::TReplicationProgress&& progress);

    TRefCountedReplicationProgress& operator=(
        const NChaosClient::TReplicationProgress& progress);
    TRefCountedReplicationProgress& operator=(
        NChaosClient::TReplicationProgress&& progress);
};

DEFINE_REFCOUNTED_TYPE(TRefCountedReplicationProgress)

////////////////////////////////////////////////////////////////////////////////

struct TTabletErrors
{
    TEnumIndexedArray<NTabletClient::ETabletBackgroundActivity, TAtomicObject<TError>> BackgroundErrors;
    TAtomicObject<TError> ConfigError;

    template <class TCallback>
    void ForEachError(TCallback&& callback) const;
};

////////////////////////////////////////////////////////////////////////////////

//! All fields must be atomic since they're being accessed both
//! from the writer and from readers concurrently.
struct TRuntimeTabletData
    : public TRefCounted
{
    std::atomic<i64> TotalRowCount = 0;
    std::atomic<i64> TrimmedRowCount = 0;
    std::atomic<i64> DelayedLocklessRowCount = 0;
    std::atomic<TTimestamp> LastCommitTimestamp = NullTimestamp;
    std::atomic<TTimestamp> LastWriteTimestamp = NullTimestamp;
    std::atomic<TTimestamp> UnflushedTimestamp = MinTimestamp;
    std::atomic<TTimestamp> BackupCheckpointTimestamp = NullTimestamp;
    std::atomic<TInstant> ModificationTime = NProfiling::GetInstant();
    std::atomic<TInstant> AccessTime = TInstant::Zero();
    std::atomic<ETabletWriteMode> WriteMode = ETabletWriteMode::Direct;
    std::atomic<NChaosClient::TReplicationEra> ReplicationEra = NChaosClient::InvalidReplicationEra;
    TAtomicIntrusivePtr<TRefCountedReplicationProgress> ReplicationProgress;
    TAtomicIntrusivePtr<NChaosClient::TReplicationCard> ReplicationCard;
    TEnumIndexedArray<ETabletDynamicMemoryType, std::atomic<i64>> DynamicMemoryUsagePerType;
    TTabletErrors Errors;
    NConcurrency::TAsyncBarrier PreparedTransactionBarrier;
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTabletData)

////////////////////////////////////////////////////////////////////////////////

struct TPreloadStatistics
{
    int PendingStoreCount = 0;
    int CompletedStoreCount = 0;
    int FailedStoreCount = 0;

    TPreloadStatistics& operator+=(const TPreloadStatistics& other);
};

////////////////////////////////////////////////////////////////////////////////

struct TCompressionDictionaryInfo
{
    NChunkClient::TChunkId ChunkId = NChunkClient::NullChunkId;

    // NB: These are not persisted.
    TInstant RebuildBackoffTime;
    bool BuildingInProgress = false;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

using TCompressionDictionaryInfos = TEnumIndexedArray<
    NTableClient::EDictionaryCompressionPolicy,
    TCompressionDictionaryInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TTabletSnapshot
    : public NTableClient::TTabletSnapshot
{
    NHydra::TCellId CellId;
    NHydra::ISimpleHydraManagerPtr HydraManager;
    NTabletClient::TTabletId TabletId;
    TString LoggingTag;
    NYPath::TYPath TablePath;
    TTableSettings Settings;
    TRawTableSettings RawSettings;
    TLegacyOwningKey PivotKey;
    TLegacyOwningKey NextPivotKey;
    NTableClient::TTableSchemaPtr PhysicalSchema;
    NTableClient::TTableSchemaPtr QuerySchema;
    // Chunk and table schemas here are identical, since in order to change it, tablet needs to be unmounted,
    // and if it is unmounted then a new chunk can't be written.
    NTableClient::TMasterTableSchemaId SchemaId;
    NTableClient::TSchemaData TableSchemaData;
    NTableClient::TSchemaData KeysSchemaData;
    NTransactionClient::EAtomicity Atomicity;
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    int HashTableSize = 0;
    int OverlappingStoreCount = 0;
    int EdenOverlappingStoreCount = 0;
    int CriticalPartitionCount = 0;
    NTransactionClient::TTimestamp RetainedTimestamp = NTransactionClient::MinTimestamp;

    TPartitionSnapshotPtr Eden;
    IStorePtr ActiveStore;

    using TPartitionList = std::vector<TPartitionSnapshotPtr>;
    using TPartitionListIterator = TPartitionList::iterator;
    TPartitionList PartitionList;

    std::vector<IOrderedStorePtr> OrderedStores;
    i64 TotalRowCount = 0;

    std::vector<TWeakPtr<ISortedStore>> LockedStores;

    std::vector<TDynamicStoreId> PreallocatedDynamicStoreIds;

    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;

    TSortedDynamicRowKeyComparer RowKeyComparer;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator;

    TRuntimeTabletDataPtr TabletRuntimeData;
    TRuntimeTabletCellDataPtr TabletCellRuntimeData;

    TChaosTabletDataPtr TabletChaosData;

    THashMap<TTableReplicaId, TTableReplicaSnapshotPtr> Replicas;

    NTableClient::TTabletPerformanceCountersPtr PerformanceCounters;
    TTableProfilerPtr TableProfiler;

    //! Local throttlers.
    NConcurrency::IReconfigurableThroughputThrottlerPtr FlushThrottler;
    NConcurrency::IReconfigurableThroughputThrottlerPtr CompactionThrottler;
    NConcurrency::IReconfigurableThroughputThrottlerPtr PartitioningThrottler;

    //! Distributed throttlers.
    TTabletDistributedThrottlersVector DistributedThrottlers;

    TLockManagerPtr LockManager;
    TLockManagerEpoch LockManagerEpoch;
    TRowCachePtr RowCache;
    ui32 StoreFlushIndex;

    NChunkClient::TConsistentReplicaPlacementHash ConsistentChunkReplicaPlacementHash = NChunkClient::NullConsistentReplicaPlacementHash;

    NChunkClient::IChunkFragmentReaderPtr ChunkFragmentReader;

    ITabletHedgingManagerRegistryPtr HedgingManagerRegistry;

    TCompressionDictionaryInfos CompressionDictionaryInfos;
    NTableClient::IDictionaryCompressionFactoryPtr DictionaryCompressionFactory;

    TString TabletCellBundle;

    std::atomic<bool> Unregistered = false;

    //! Returns a range of partitions intersecting with the range |[lowerBound, upperBound)|.
    std::pair<TPartitionListIterator, TPartitionListIterator> GetIntersectingPartitions(
        const TLegacyKey& lowerBound,
        const TLegacyKey& upperBound);

    //! Returns a partition possibly containing a given #key or
    //! |nullptr| is there's none.
    TPartitionSnapshotPtr FindContainingPartition(TLegacyKey key);

    //! For sorted tablets only.
    //! This includes both regular and locked Eden stores.
    std::vector<ISortedStorePtr> GetEdenStores();

    //! Returns true if |id| corresponds to a preallocated dynamic store
    //! which has not been created yet.
    bool IsPreallocatedDynamicStoreId(TDynamicStoreId storeId) const;

    //! Returns a dynamic store with given |storeId| or |nullptr| if there is none.
    IDynamicStorePtr FindDynamicStore(TDynamicStoreId storeId) const;

    //! Returns a dynamic store with given |storeId| or throws if there is none.
    IDynamicStorePtr GetDynamicStoreOrThrow(TDynamicStoreId storeId) const;

    TTableReplicaSnapshotPtr FindReplicaSnapshot(TTableReplicaId replicaId);

    void ValidateCellId(NElection::TCellId cellId);
    void ValidateMountRevision(NHydra::TRevision mountRevision);
    void WaitOnLocks(TTimestamp timestamp) const;
};

DEFINE_REFCOUNTED_TYPE(TTabletSnapshot)

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletRetainedTimestamp(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp);
void ValidateTabletMounted(TTablet* tablet);

////////////////////////////////////////////////////////////////////////////////

struct ITabletContext
{
    virtual ~ITabletContext() = default;

    virtual NObjectClient::TCellId GetCellId() const = 0;
    virtual const TString& GetTabletCellBundleName() const = 0;
    virtual NHydra::EPeerState GetAutomatonState() const = 0;
    virtual int GetAutomatonTerm() const = 0;
    virtual IInvokerPtr GetControlInvoker() const = 0;
    virtual IInvokerPtr GetAutomatonInvoker() const = 0;
    virtual NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const = 0;
    virtual NQueryClient::IRowComparerProviderPtr GetRowComparerProvider() const = 0;
    virtual NApi::NNative::IClientPtr GetClient() const = 0;
    virtual NClusterNode::TClusterNodeDynamicConfigManagerPtr GetDynamicConfigManager() const = 0;
    virtual NObjectClient::TObjectId GenerateIdDeprecated(NObjectClient::EObjectType type) const = 0;
    virtual IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const NTabletNode::NProto::TAddStoreDescriptor* descriptor) const = 0;
    virtual THunkChunkPtr CreateHunkChunk(
        TTablet* tablet,
        NChunkClient::TChunkId chunkId,
        const NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor) const = 0;
    virtual ITransactionManagerPtr GetTransactionManager() const = 0;
    virtual NRpc::IServerPtr GetLocalRpcServer() const = 0;
    virtual TString GetLocalHostName() const = 0;
    virtual NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const = 0;
    virtual INodeMemoryTrackerPtr GetMemoryUsageTracker() const = 0;
    virtual NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() const = 0;
    virtual IHedgingManagerRegistryPtr GetHedgingManagerRegistry() const = 0;
    virtual ITabletWriteManagerHostPtr GetTabletWriteManagerHost() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaInfo
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TTablet*, Tablet);
    DEFINE_BYVAL_RO_PROPERTY(TTableReplicaId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TString, ClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, ReplicaPath);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartReplicationTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionId, PreparedReplicationTransactionId);

    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaState, State, ETableReplicaState::None);

    DEFINE_BYVAL_RW_PROPERTY(TTableReplicatorPtr, Replicator);
    DEFINE_BYVAL_RW_PROPERTY(TReplicaCounters, Counters);

public:
    TTableReplicaInfo() = default;
    TTableReplicaInfo(
        TTablet* tablet,
        TTableReplicaId id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    ETableReplicaMode GetMode() const;
    void SetMode(ETableReplicaMode value);

    NTransactionClient::EAtomicity GetAtomicity() const;
    void SetAtomicity(NTransactionClient::EAtomicity value);

    bool GetPreserveTimestamps() const;
    void SetPreserveTimestamps(bool value);

    i64 GetCurrentReplicationRowIndex() const;
    void SetCurrentReplicationRowIndex(i64 value);

    TTimestamp GetCurrentReplicationTimestamp() const;
    void SetCurrentReplicationTimestamp(TTimestamp value);

    i64 GetPreparedReplicationRowIndex() const;
    void SetPreparedReplicationRowIndex(i64 value);

    i64 GetCommittedReplicationRowIndex() const;
    void SetCommittedReplicationRowIndex(i64 value);

    TError GetError() const;
    void SetError(TError error);

    TTableReplicaSnapshotPtr BuildSnapshot() const;

    void PopulateStatistics(NTabletClient::NProto::TTableReplicaStatistics* statistics) const;
    void MergeFromStatistics(const NTabletClient::NProto::TTableReplicaStatistics& statistics);

    NTabletClient::ETableReplicaStatus GetStatus() const;
    void RecomputeReplicaStatus();

private:
    const TRuntimeTableReplicaDataPtr RuntimeData_ = New<TRuntimeTableReplicaData>();
};

////////////////////////////////////////////////////////////////////////////////

class TBackupMetadata
{
public:
    // If non-null then there is a backup task in progress. All store flushes
    // and compactions should ensure that the most recent version before this
    // timestamp is preserved (that is, consistent read is possible).
    // Persistent.
    DECLARE_BYVAL_RO_PROPERTY(TTimestamp, CheckpointTimestamp);

    // Last backup checkpoint timestamp that was passed by the tablet.
    // Transactions with earlier start timestamp must not be committed.
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, LastPassedCheckpointTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::EBackupMode, BackupMode);

    // Represent the stage of checkpoint confirmation process.
    // NB: Stage transitions may happen at different moments with respect to
    // checkpoint timestamp. E.g. sometimes it is safe to transition to
    // FeasibilityConfirmed state after checkpoint timestamp has already happened.
    // It is safer to check BackupCheckpointTimestamp against NullTimestamp
    // to see if backup is in progress.
    DEFINE_BYVAL_RW_PROPERTY(EBackupStage, BackupStage, EBackupStage::None);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<NApi::TClusterTag>, ClockClusterTag);

    DEFINE_BYREF_RW_PROPERTY(
        std::vector<NTabletClient::TTableReplicaBackupDescriptor>,
        ReplicaBackupDescriptors);

public:
    void Persist(const TPersistenceContext& context);

private:
    TTimestamp CheckpointTimestamp_ = NullTimestamp;

    // SetCheckpointTimestamp should be called only via TTablet's methods
    // since setting it implies other actions, e.g. merge_rows_on_flush
    // becomes temporarily disabled. Thus private.
    void SetCheckpointTimestamp(TTimestamp timestamp);

    friend class TTablet;
};

////////////////////////////////////////////////////////////////////////////////

class TIdGenerator
{
public:
    TIdGenerator() = default;

    TIdGenerator(NObjectClient::TCellTag cellTag, ui64 counter, ui64 seed);

    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);

    // Used in tests.
    static TIdGenerator CreateDummy();

    void Persist(const TPersistenceContext& context);

    friend void ToProto(
        NProto::TIdGenerator* protoIdGenerator,
        const TIdGenerator& idGenerator);
    friend void FromProto(
        TIdGenerator* idGenerator,
        const NProto::TIdGenerator& protoIdGenerator);

private:
    ui16 CellTag_;
    ui64 Counter_{};
    ui64 Seed_{};
};

////////////////////////////////////////////////////////////////////////////////

class TSmoothMovementData
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ESmoothMovementRole, Role);
    DEFINE_BYVAL_RW_PROPERTY(ESmoothMovementStage, Stage);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellId, SiblingCellId);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, SiblingMountRevision);
    DEFINE_BYVAL_RW_PROPERTY(NHiveServer::TAvenueEndpointId, SiblingAvenueEndpointId);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TStoreId>, CommonDynamicStoreIds);

    // Transient.
    DEFINE_BYVAL_RW_PROPERTY(bool, StageChangeScheduled);

public:
    void ValidateWriteToTablet() const;
    bool IsTabletStoresUpdateAllowed(bool isCommonFlush) const;
    bool ShouldForwardMutation() const;

    void Persist(const TPersistenceContext& context);

    void BuildOrchidYson(NYTree::TFluentMap fluent) const;
};

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NHydra::TRevision, MountRevision);
    DEFINE_BYVAL_RO_PROPERTY(NObjectClient::TObjectId, TableId);
    DEFINE_BYVAL_RO_PROPERTY(NYPath::TYPath, TablePath);
    DEFINE_BYVAL_RW_PROPERTY(NHiveServer::TAvenueEndpointId, MasterAvenueEndpointId);

    DEFINE_BYVAL_RO_PROPERTY(NObjectClient::TObjectId, SchemaId);
    DEFINE_BYVAL_RO_PROPERTY(NTableClient::TTableSchemaPtr, TableSchema);
    DEFINE_BYVAL_RO_PROPERTY(NTableClient::TTableSchemaPtr, PhysicalSchema);

    DEFINE_BYREF_RO_PROPERTY(NTableClient::TSchemaData, TableSchemaData);
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TSchemaData, KeysSchemaData);

    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, ColumnIndexToLockIndex);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, LockIndexToName);

    DEFINE_BYVAL_RO_PROPERTY(TLegacyOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TLegacyOwningKey, NextPivotKey);

    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RO_PROPERTY(TCancelableContextPtr, CancelableContext);

    // NB: Avoid keeping IStorePtr to simplify store removal.
    DEFINE_BYREF_RW_PROPERTY(std::deque<TStoreId>, PreloadStoreIds);

    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::EAtomicity, Atomicity);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::ECommitOrdering, CommitOrdering);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::TTableReplicaId, UpstreamReplicaId);

    DEFINE_BYVAL_RO_PROPERTY(int, HashTableSize);

    DEFINE_BYVAL_RO_PROPERTY(int, OverlappingStoreCount);
    DEFINE_BYVAL_RO_PROPERTY(int, EdenOverlappingStoreCount);
    DEFINE_BYVAL_RO_PROPERTY(int, CriticalPartitionCount);

    DEFINE_BYVAL_RW_PROPERTY(IDynamicStorePtr, ActiveStore);
    DEFINE_BYVAL_RW_PROPERTY(int, DynamicStoreCount);

    // NB: This field is transient.
    // Flush index of last rotated (last passive dynamic) store.
    DEFINE_BYVAL_RW_PROPERTY(ui32, StoreFlushIndex, 0);

    using TReplicaMap = THashMap<TTableReplicaId, TTableReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicaMap, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp);

    DEFINE_BYVAL_RO_PROPERTY(NConcurrency::TAsyncSemaphorePtr, StoresUpdateCommitSemaphore);

    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, LastDiscardStoresRevision);

    DEFINE_BYVAL_RW_PROPERTY(TTransactionId, StoresUpdatePreparedTransactionId);

    DEFINE_BYVAL_RO_PROPERTY(TTableProfilerPtr, TableProfiler, TTableProfiler::GetDisabled());

    DEFINE_BYREF_RO_PROPERTY(NTableClient::TTabletPerformanceCountersPtr, PerformanceCounters, New<NTableClient::TTabletPerformanceCounters>());
    DEFINE_BYREF_RO_PROPERTY(TRuntimeTabletDataPtr, RuntimeData, New<TRuntimeTabletData>());

    DEFINE_BYREF_RO_PROPERTY(std::deque<TDynamicStoreId>, DynamicStoreIdPool);
    DEFINE_BYVAL_RW_PROPERTY(bool, DynamicStoreIdRequested);

    DEFINE_BYREF_RW_PROPERTY(TTabletDistributedThrottlersVector, DistributedThrottlers);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastFullStructuredHeartbeatTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastIncrementalStructuredHeartbeatTime);

    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::IChunkFragmentReaderPtr, ChunkFragmentReader);

    // The number of in-flight write mutations issued by normal users.
    DEFINE_BYVAL_RW_PROPERTY(int, InFlightUserMutationCount);
    // The number of in-flight write mutations issued by replicator.
    DEFINE_BYVAL_RW_PROPERTY(int, InFlightReplicatorMutationCount);
    // The number of pending write records issued by normal users.
    DEFINE_BYVAL_RW_PROPERTY(int, PendingUserWriteRecordCount);
    // The number of pending write records issued by replicator.
    DEFINE_BYVAL_RW_PROPERTY(int, PendingReplicatorWriteRecordCount);

    using TTransactionIdSet = TCompactSet<TTransactionId, 4>;
    // Ids of prepared transactions issued by replicator.
    DEFINE_BYREF_RW_PROPERTY(TTransactionIdSet, PreparedReplicatorTransactionIds);

    DEFINE_BYVAL_RW_PROPERTY(IChaosAgentPtr, ChaosAgent);
    DEFINE_BYVAL_RW_PROPERTY(ITablePullerPtr, TablePuller);
    DEFINE_BYREF_RW_PROPERTY(NChaosClient::TReplicationProgress, ReplicationProgress);
    DEFINE_BYREF_RO_PROPERTY(TChaosTabletDataPtr, ChaosData, New<TChaosTabletData>());

    DEFINE_BYREF_RW_PROPERTY(TBackupMetadata, BackupMetadata);

    DEFINE_BYVAL_RO_PROPERTY(ITabletWriteManagerPtr, TabletWriteManager);

    // Accessors for frequent fields of backup metadata.
    DECLARE_BYVAL_RW_PROPERTY(TTimestamp, BackupCheckpointTimestamp);
    DECLARE_BYVAL_RO_PROPERTY(NTabletClient::EBackupMode, BackupMode);
    DECLARE_BYVAL_RW_PROPERTY(EBackupStage, BackupStage);

    DEFINE_BYVAL_RW_PROPERTY(i64, NonActiveStoresUnmergedRowCount);

    DEFINE_BYVAL_RW_PROPERTY(bool, OutOfBandRotationRequested);

    DEFINE_BYREF_RW_PROPERTY(ITabletHedgingManagerRegistryPtr, HedgingManagerRegistry);

    DEFINE_BYREF_RW_PROPERTY(TRawTableSettings, RawSettings);

    DEFINE_BYREF_RW_PROPERTY(NLsm::TTabletLsmStatistics, LsmStatistics);

    DEFINE_BYREF_RW_PROPERTY(TSmoothMovementData, SmoothMovementData);

public:
    TTablet(
        TTabletId tabletId,
        ITabletContext* context);
    TTablet(
        TTabletId tabletId,
        TTableSettings settings,
        NHydra::TRevision mountRevision,
        NObjectClient::TObjectId tableId,
        const NYPath::TYPath& path,
        ITabletContext* context,
        TIdGenerator idGenerator,
        NObjectClient::TObjectId schemaId,
        NTableClient::TTableSchemaPtr schema,
        TLegacyOwningKey pivotKey,
        TLegacyOwningKey nextPivotKey,
        NTransactionClient::EAtomicity atomicity,
        NTransactionClient::ECommitOrdering commitOrdering,
        NTabletClient::TTableReplicaId upstreamReplicaId,
        TTimestamp retainedTimestamp,
        i64 cumulativeDataWeight);

    ETabletState GetPersistentState() const;

    const TTableSettings& GetSettings() const;
    void SetSettings(TTableSettings settings);

    const IStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(IStoreManagerPtr storeManager);

    const TLockManagerPtr& GetLockManager() const;

    const IPerTabletStructuredLoggerPtr& GetStructuredLogger() const;
    void SetStructuredLogger(IPerTabletStructuredLoggerPtr storeManager);

    using TPartitionList = std::vector<std::unique_ptr<TPartition>>;
    const TPartitionList& PartitionList() const;
    TPartition* GetEden() const;
    void CreateInitialPartition();
    TPartition* FindPartition(TPartitionId partitionId);
    TPartition* GetPartition(TPartitionId partitionId);
    void MergePartitions(int firstIndex, int lastIndex, TDuration splitDelay);
    void SplitPartition(int index, const std::vector<TLegacyOwningKey>& pivotKeys, TDuration mergeDelay);
    //! Finds a partition fully containing the range |[minKey, maxKey]|.
    //! Returns the Eden if no such partition exists.
    TPartition* GetContainingPartition(const TLegacyOwningKey& minKey, const TLegacyOwningKey& maxKey);

    const THashMap<TStoreId, IStorePtr>& StoreIdMap() const;
    const std::map<i64, IOrderedStorePtr>& StoreRowIndexMap() const;
    void AddStore(IStorePtr store, bool onFlush, TPartitionId partitionIdHint = {});
    void RemoveStore(IStorePtr store);
    IStorePtr FindStore(TStoreId id);
    IStorePtr GetStore(TStoreId id);
    IStorePtr GetStoreOrThrow(TStoreId id);

    const THashMap<NChunkClient::TChunkId, THunkChunkPtr>& HunkChunkMap() const;
    void AddHunkChunk(THunkChunkPtr hunkChunk);
    void RemoveHunkChunk(THunkChunkPtr hunkChunk);
    THunkChunkPtr FindHunkChunk(NChunkClient::TChunkId id);
    THunkChunkPtr GetHunkChunk(NChunkClient::TChunkId id);
    THunkChunkPtr GetHunkChunkOrThrow(NChunkClient::TChunkId id);

    void AttachCompressionDictionary(NTableClient::EDictionaryCompressionPolicy policy, NChunkClient::TChunkId id);

    void UpdatePreparedStoreRefCount(const THunkChunkPtr& hunkChunk, int delta);
    void UpdateHunkChunkRef(const THunkChunkRef& ref, int delta);
    const THashSet<THunkChunkPtr>& DanglingHunkChunks() const;
    void UpdateDanglingHunkChunks(const THunkChunkPtr& hunkChunk);

    TTableReplicaInfo* FindReplicaInfo(TTableReplicaId id);
    TTableReplicaInfo* GetReplicaInfoOrThrow(TTableReplicaId id);

    NChaosClient::TReplicationCardId GetReplicationCardId() const;

    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    void Clear();
    void OnAfterSnapshotLoaded();

    bool IsPhysicallySorted() const;
    bool IsPhysicallyOrdered() const;
    bool IsReplicated() const;
    bool IsPhysicallyLog() const;

    int GetColumnLockCount() const;

    // Only applicable to physically ordered tablets.
    i64 GetTotalRowCount() const;
    void UpdateTotalRowCount();

    // Only applicable to replicated tablets (these are always physically ordered).
    i64 GetDelayedLocklessRowCount();
    void SetDelayedLocklessRowCount(i64 value);

    // Only applicable to ordered tablets.
    i64 GetTrimmedRowCount() const;
    void SetTrimmedRowCount(i64 value);

    // Only applicable to ordered tablets.
    i64 GetCumulativeDataWeight() const;
    void IncreaseCumulativeDataWeight(i64 delta);

    TTimestamp GetLastCommitTimestamp() const;
    void UpdateLastCommitTimestamp(TTimestamp value);

    TTimestamp GetLastWriteTimestamp() const;
    void UpdateLastWriteTimestamp(TTimestamp value);

    TTimestamp GetUnflushedTimestamp() const;

    void Reconfigure(const ITabletSlotPtr& slot);

    void StartEpoch(const ITabletSlotPtr& slot);
    void StopEpoch();

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    TTabletSnapshotPtr BuildSnapshot(
        const ITabletSlotPtr& slot,
        std::optional<TLockManagerEpoch> epoch = std::nullopt) const;

    const TSortedDynamicRowKeyComparer& GetRowKeyComparer() const;

    void ValidateMountRevision(NHydra::TRevision mountRevision);

    void UpdateUnflushedTimestamp() const;

    // Returns |true| if tablet either participates in smooth movement and holds master avenue connection
    // or does not participate in it at all.
    bool IsActiveServant() const;

    void PopulateReplicateTabletContentRequest(NProto::TReqReplicateTabletContent* request);
    void LoadReplicatedContent(const NProto::TReqReplicateTabletContent* request);

    // Lock stuff.

    //! Acquires tablet lock of a given kind.
    //! Returns total number of locks acquired (of all kinds).
    i64 Lock(ETabletLockType lockType);
    //! Releases tablet lock of a given kind.
    //! Returns total number of locks acquired (of all kinds).
    i64 Unlock(ETabletLockType lockType);

    //! Returns total number of locks acquired (of all kinds).
    i64 GetTotalTabletLockCount() const;
    //! Returns total number of transient locks acquired (of all kinds).
    i64 GetTransientTabletLockCount() const;
    //! Returns number of acquired locks of a given kind.
    i64 GetTabletLockCount(ETabletLockType lockType) const;

    void UpdateReplicaCounters();

    const TString& GetLoggingTag() const;

    std::optional<TString> GetPoolTagByMemoryCategory(EMemoryCategory category) const;

    int GetEdenStoreCount() const;

    void PushDynamicStoreIdToPool(TDynamicStoreId storeId);
    TDynamicStoreId PopDynamicStoreIdFromPool();
    void ClearDynamicStoreIdPool();

    NTabletNode::NProto::TMountHint GetMountHint() const;

    NChunkClient::TConsistentReplicaPlacementHash GetConsistentChunkReplicaPlacementHash() const;

    void ThrottleTabletStoresUpdate(
        const ITabletSlotPtr& slot,
        const NLogging::TLogger& Logger) const;

    // COMPAT(babenko)
    static TTabletHunkWriterOptionsPtr CreateFallbackHunkWriterOptions(const TTabletStoreWriterOptionsPtr& storeWriterOptions);

    const TRowCachePtr& GetRowCache() const;

    void RecomputeReplicaStatuses();

    void RecomputeCommittedReplicationRowIndices();

    void CheckedSetBackupStage(EBackupStage previous, EBackupStage next);

    void RecomputeNonActiveStoresUnmergedRowCount();

    void UpdateUnmergedRowCount();

    TTimestamp GetOrderedChaosReplicationMinTimestamp();

    const IHunkLockManagerPtr& GetHunkLockManager() const;

    bool IsDictionaryBuildingInProgress(
        NTableClient::EDictionaryCompressionPolicy policy) const;
    void SetDictionaryBuildingInProgress(
        NTableClient::EDictionaryCompressionPolicy policy, bool flag);
    TInstant GetCompressionDictionaryRebuildBackoffTime(
        NTableClient::EDictionaryCompressionPolicy policy) const;
    void SetCompressionDictionaryRebuildBackoffTime(
        NTableClient::EDictionaryCompressionPolicy policy,
        TInstant backoffTime);

private:
    ITabletContext* const Context_;
    TIdGenerator IdGenerator_;

    const TLockManagerPtr LockManager_;
    const IHunkLockManagerPtr HunkLockManager_;
    const NLogging::TLogger Logger;

    TTableSettings Settings_;

    TString LoggingTag_;

    IStoreManagerPtr StoreManager_;

    TEnumIndexedArray<EAutomatonThreadQueue, IInvokerPtr> EpochAutomatonInvokers_;

    std::unique_ptr<TPartition> Eden_;

    TPartitionList PartitionList_;
    THashMap<TPartitionId, TPartition*> PartitionMap_;

    THashMap<TStoreId, IStorePtr> StoreIdMap_;
    std::map<i64, IOrderedStorePtr> StoreRowIndexMap_;

    THashMap<NChunkClient::TChunkId, THunkChunkPtr> HunkChunkMap_;
    THashSet<THunkChunkPtr> DanglingHunkChunks_;

    TSortedDynamicRowKeyComparer RowKeyComparer_;

    TCompressionDictionaryInfos CompressionDictionaryInfos_;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    TRowCachePtr RowCache_;

    //! Number of tablet locks, per lock type.
    TEnumIndexedArray<ETabletLockType, i64> TabletLockCount_;

    //! Total number of tablet locks of all types.
    i64 TotalTabletLockCount_ = 0;

    IPerTabletStructuredLoggerPtr StructuredLogger_;

    NConcurrency::IReconfigurableThroughputThrottlerPtr FlushThrottler_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr CompactionThrottler_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr PartitioningThrottler_;

    TTabletCounters TabletCounters_;
    i64 CumulativeDataWeight_ = 0;

    void Initialize();

    TPartition* GetContainingPartition(const ISortedStorePtr& store);

    void UpdateTabletSizeMetrics();
    void UpdateOverlappingStoreCount();
    int ComputeEdenOverlappingStoreCount() const;
    int ComputeDynamicStoreCount() const;

    void ReconfigureLocalThrottlers();
    void ReconfigureDistributedThrottlers(const ITabletSlotPtr& slot);
    void ReconfigureChunkFragmentReader(const ITabletSlotPtr& slot);
    void ReconfigureStructuredLogger();
    void ReconfigureProfiling();
    void ReconfigureRowCache(const ITabletSlotPtr& slot);
    void InvalidateChunkReaders();
    void ReconfigureHedgingManagerRegistry();
    void ResetRowDigestRequestTime();
    void ReconfigureChangelogWriteThrottler(const ITabletSlotPtr& slot);
    void ReconfigureCompressionDictionaries();
};

////////////////////////////////////////////////////////////////////////////////

void BuildTableSettingsOrchidYson(const TTableSettings& options, NYTree::TFluentMap fluent);

NConcurrency::IThroughputThrottlerPtr GetBlobMediumWriteThrottler(
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& dynamicConfigManager,
    const TTabletSnapshotPtr& tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define TABLET_INL_H_
#include "tablet-inl.h"
#undef TABLET_INL_H_
