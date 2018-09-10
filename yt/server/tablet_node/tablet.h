#pragma once

#include "public.h"
#include "sorted_dynamic_comparer.h"
#include "partition.h"
#include "object_detail.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/atomic_object.h>

#include <atomic>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Cf. TRuntimeTabletData.
struct TRuntimeTableReplicaData
    : public TIntrinsicRefCounted
{
    std::atomic<ETableReplicaMode> Mode = {ETableReplicaMode::Async};
    std::atomic<i64> CurrentReplicationRowIndex = {0};
    std::atomic<TTimestamp> CurrentReplicationTimestamp = {NullTimestamp};
    std::atomic<TTimestamp> LastReplicationTimestamp = {NullTimestamp};
    std::atomic<i64> PreparedReplicationRowIndex = {-1};
    std::atomic<bool> PreserveTimestamps = {true};
    std::atomic<NTransactionClient::EAtomicity> Atomicity = {NTransactionClient::EAtomicity::Full};
    TAtomicObject<TError> Error;

    void Populate(NTabletClient::NProto::TTableReplicaStatistics* statistics) const;
    void MergeFrom(const NTabletClient::NProto::TTableReplicaStatistics& statistics);
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTableReplicaData)

////////////////////////////////////////////////////////////////////////////////

struct TReplicaCounters
{
    TReplicaCounters() = default;
    explicit TReplicaCounters(const NProfiling::TTagIdList& list);

    NProfiling::TAggregateGauge LagRowCount;
    NProfiling::TAggregateGauge LagTime;
    NProfiling::TAggregateGauge ReplicationTransactionStartTime;
    NProfiling::TAggregateGauge ReplicationTransactionCommitTime;
    NProfiling::TAggregateGauge ReplicationRowsReadTime;
    NProfiling::TAggregateGauge ReplicationRowsWriteTime;
    NProfiling::TAggregateGauge ReplicationBatchRowCount;
    NProfiling::TAggregateGauge ReplicationBatchDataWeight;

    const NProfiling::TTagIdList Tags;
};

extern TReplicaCounters NullReplicaCounters;

////////////////////////////////////////////////////////////////////////////////

struct TTableReplicaSnapshot
    : public TIntrinsicRefCounted
{
    NTransactionClient::TTimestamp StartReplicationTimestamp;
    TRuntimeTableReplicaDataPtr RuntimeData;
    TReplicaCounters* Counters = &NullReplicaCounters;
};

DEFINE_REFCOUNTED_TYPE(TTableReplicaSnapshot)

////////////////////////////////////////////////////////////////////////////////

//! All fields must be atomic since they're being accessed both
//! from the writer and from readers concurrently.
struct TRuntimeTabletData
    : public TIntrinsicRefCounted
{
    std::atomic<i64> TotalRowCount = {0};
    std::atomic<i64> TrimmedRowCount = {0};
    std::atomic<TTimestamp> LastCommitTimestamp = {NullTimestamp};
    std::atomic<TTimestamp> LastWriteTimestamp = {NullTimestamp};
    std::atomic<TTimestamp> UnflushedTimestamp = {MinTimestamp};
    std::atomic<i64> DynamicMemoryPoolSize = {0};
    TEnumIndexedVector<TAtomicObject<TError>, NTabletClient::ETabletBackgroundActivity> Errors;
};

DEFINE_REFCOUNTED_TYPE(TRuntimeTabletData)

////////////////////////////////////////////////////////////////////////////////

struct TTabletSnapshot
    : public TIntrinsicRefCounted
{
    NHydra::TCellId CellId;
    NHydra::IHydraManagerPtr HydraManager;
    TTabletManagerPtr TabletManager;
    TTabletId TabletId;
    i64 MountRevision = 0;
    NYPath::TYPath TablePath;
    NObjectClient::TObjectId TableId;
    TTableMountConfigPtr Config;
    TTabletChunkWriterConfigPtr WriterConfig;
    TTabletWriterOptionsPtr WriterOptions;
    TOwningKey PivotKey;
    TOwningKey NextPivotKey;
    NTableClient::TTableSchema TableSchema;
    NTableClient::TTableSchema PhysicalSchema;
    NTableClient::TTableSchema QuerySchema;
    NTableClient::TSchemaData PhysicalSchemaData;
    NTableClient::TSchemaData KeysSchemaData;
    NTransactionClient::EAtomicity Atomicity;
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    int HashTableSize = 0;
    int OverlappingStoreCount = 0;
    int CriticalPartitionCount = 0;
    NTransactionClient::TTimestamp RetainedTimestamp = NTransactionClient::MinTimestamp;

    TPartitionSnapshotPtr Eden;

    using TPartitionList = std::vector<TPartitionSnapshotPtr>;
    using TPartitionListIterator = TPartitionList::iterator;
    TPartitionList PartitionList;

    std::vector<IOrderedStorePtr> OrderedStores;

    std::vector<TWeakPtr<ISortedStore>> LockedStores;

    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;

    TSortedDynamicRowKeyComparer RowKeyComparer;

    TTabletPerformanceCountersPtr PerformanceCounters;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator;

    TRuntimeTabletDataPtr RuntimeData;

    THashMap<TTableReplicaId, TTableReplicaSnapshotPtr> Replicas;

    NProfiling::TTagIdList ProfilerTags;
    NProfiling::TTagIdList DiskProfilerTags;

    //! Returns a range of partitions intersecting with the range |[lowerBound, upperBound)|.
    std::pair<TPartitionListIterator, TPartitionListIterator> GetIntersectingPartitions(
        const TKey& lowerBound,
        const TKey& upperBound);

    //! Returns a partition possibly containing a given #key or
    //! |nullptr| is there's none.
    TPartitionSnapshotPtr FindContainingPartition(TKey key);

    //! For sorted tablets only.
    //! This includes both regular and locked Eden stores.
    std::vector<ISortedStorePtr> GetEdenStores();

    TTableReplicaSnapshotPtr FindReplicaSnapshot(const TTableReplicaId& replicaId);

    void ValidateCellId(const NElection::TCellId& cellId);
    void ValidateMountRevision(i64 mountRevision);
    bool IsProfilingEnabled() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletSnapshot)

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletRetainedTimestamp(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounters
    : public TChunkReaderPerformanceCounters
{
    std::atomic<i64> DynamicRowReadCount = {0};
    std::atomic<i64> DynamicRowReadDataWeightCount = {0};
    std::atomic<i64> DynamicRowLookupCount = {0};
    std::atomic<i64> DynamicRowLookupDataWeightCount = {0};
    std::atomic<i64> DynamicRowWriteCount = {0};
    std::atomic<i64> DynamicRowWriteDataWeightCount = {0};
    std::atomic<i64> DynamicRowDeleteCount = {0};
    std::atomic<i64> UnmergedRowReadCount = {0};
    std::atomic<i64> MergedRowReadCount = {0};
    std::atomic<i64> CompactionDataWeightCount = {0};
    std::atomic<i64> PartitioningDataWeightCount = {0};
    std::atomic<i64> LookupErrorCount = {0};
    std::atomic<i64> WriteErrorCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TTabletPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCounters
{
    TTabletCounters(const NProfiling::TTagIdList& list);

    NProfiling::TAggregateGauge OverlappingStoreCount;
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletContext
{
    virtual ~ITabletContext() = default;

    virtual NObjectClient::TCellId GetCellId() = 0;
    virtual NHydra::EPeerState GetAutomatonState() = 0;
    virtual NQueryClient::TColumnEvaluatorCachePtr GetColumnEvaluatorCache() = 0;
    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;
    virtual IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        const TStoreId& storeId,
        const NTabletNode::NProto::TAddStoreDescriptor* descriptor) = 0;
    virtual TTransactionManagerPtr GetTransactionManager() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaInfo
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTableReplicaId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TString, ClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, ReplicaPath);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartReplicationTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionId, PreparedReplicationTransactionId);

    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaState, State, ETableReplicaState::None);

    DEFINE_BYVAL_RW_PROPERTY(TTableReplicatorPtr, Replicator);
    DEFINE_BYVAL_RW_PROPERTY(TReplicaCounters*, Counters, &NullReplicaCounters);

public:
    TTableReplicaInfo() = default;
    explicit TTableReplicaInfo(const TTableReplicaId& id);

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

    TTableReplicaSnapshotPtr BuildSnapshot() const;

    void PopulateStatistics(NTabletClient::NProto::TTableReplicaStatistics* statistics) const;
    void MergeFromStatistics(const NTabletClient::NProto::TTableReplicaStatistics& statistics);

private:
    const TRuntimeTableReplicaDataPtr RuntimeData_ = New<TRuntimeTableReplicaData>();

};

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, MountRevision);
    DEFINE_BYVAL_RO_PROPERTY(NObjectClient::TObjectId, TableId);
    DEFINE_BYVAL_RO_PROPERTY(NYPath::TYPath, TablePath);

    DEFINE_BYREF_RO_PROPERTY(NTableClient::TTableSchema, TableSchema);
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TTableSchema, PhysicalSchema);

    DEFINE_BYREF_RO_PROPERTY(NTableClient::TSchemaData, PhysicalSchemaData);
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TSchemaData, KeysSchemaData);

    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, ColumnIndexToLockIndex);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, LockIndexToName);

    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, NextPivotKey);

    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RO_PROPERTY(TCancelableContextPtr, CancelableContext);

    // NB: Avoid keeping IStorePtr to simplify store removal.
    DEFINE_BYREF_RW_PROPERTY(std::deque<TStoreId>, PreloadStoreIds);

    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::EAtomicity, Atomicity);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::ECommitOrdering, CommitOrdering);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::TTableReplicaId, UpstreamReplicaId);

    DEFINE_BYVAL_RO_PROPERTY(int, HashTableSize);

    DEFINE_BYVAL_RO_PROPERTY(int, OverlappingStoreCount);
    DEFINE_BYVAL_RO_PROPERTY(int, CriticalPartitionCount);

    DEFINE_BYVAL_RW_PROPERTY(IDynamicStorePtr, ActiveStore);

    using TReplicaMap = THashMap<TTableReplicaId, TTableReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicaMap, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp);

    DEFINE_BYVAL_RO_PROPERTY(NConcurrency::TAsyncSemaphorePtr, StoresUpdateCommitSemaphore);

    DEFINE_BYVAL_RO_PROPERTY(NProfiling::TTagIdList, ProfilerTags);
    DEFINE_BYVAL_RO_PROPERTY(NProfiling::TTagIdList, DiskProfilerTags);

    DEFINE_BYREF_RO_PROPERTY(TTabletPerformanceCountersPtr, PerformanceCounters, New<TTabletPerformanceCounters>());
    DEFINE_BYREF_RO_PROPERTY(TRuntimeTabletDataPtr, RuntimeData, New<TRuntimeTabletData>());

public:
    TTablet(
        const TTabletId& tabletId,
        ITabletContext* context);
    TTablet(
        TTableMountConfigPtr config,
        TTabletChunkReaderConfigPtr readerConfig,
        TTabletChunkWriterConfigPtr writerConfig,
        TTabletWriterOptionsPtr writerOptions,
        const TTabletId& tabletId,
        i64 mountRevision,
        const NObjectClient::TObjectId& tableId,
        const NYPath::TYPath& path,
        ITabletContext* context,
        const NTableClient::TTableSchema& schema,
        TOwningKey pivotKey,
        TOwningKey nextPivotKey,
        NTransactionClient::EAtomicity atomicity,
        NTransactionClient::ECommitOrdering commitOrdering,
        const NTabletClient::TTableReplicaId& upstreamReplicaId);

    ETabletState GetPersistentState() const;

    const TTableMountConfigPtr& GetConfig() const;
    void SetConfig(TTableMountConfigPtr config);

    const TTabletChunkReaderConfigPtr& GetReaderConfig() const;
    void SetReaderConfig(TTabletChunkReaderConfigPtr config);

    const TTabletChunkWriterConfigPtr& GetWriterConfig() const;
    void SetWriterConfig(TTabletChunkWriterConfigPtr config);

    const TTabletWriterOptionsPtr& GetWriterOptions() const;
    void SetWriterOptions(TTabletWriterOptionsPtr options);

    const IStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(IStoreManagerPtr storeManager);

    using TPartitionList = std::vector<std::unique_ptr<TPartition>>;
    const TPartitionList& PartitionList() const;
    TPartition* GetEden() const;
    void CreateInitialPartition();
    TPartition* FindPartition(const TPartitionId& partitionId);
    TPartition* GetPartition(const TPartitionId& partitionId);
    void MergePartitions(int firstIndex, int lastIndex);
    void SplitPartition(int index, const std::vector<TOwningKey>& pivotKeys);
    //! Finds a partition fully containing the range |[minKey, maxKey]|.
    //! Returns the Eden if no such partition exists.
    TPartition* GetContainingPartition(const TOwningKey& minKey, const TOwningKey& maxKey);

    const THashMap<TStoreId, IStorePtr>& StoreIdMap() const;
    const std::map<i64, IOrderedStorePtr>& StoreRowIndexMap() const;
    void AddStore(IStorePtr store);
    void RemoveStore(IStorePtr store);
    IStorePtr FindStore(const TStoreId& id);
    IStorePtr GetStore(const TStoreId& id);
    IStorePtr GetStoreOrThrow(const TStoreId& id);

    TTableReplicaInfo* FindReplicaInfo(const TTableReplicaId& id);
    TTableReplicaInfo* GetReplicaInfoOrThrow(const TTableReplicaId& id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    bool IsPhysicallySorted() const;
    bool IsPhysicallyOrdered() const;
    bool IsReplicated() const;

    int GetColumnLockCount() const;

    // Only applicable to ordered tablets.
    i64 GetTotalRowCount() const;
    void UpdateTotalRowCount();

    // Only applicable to ordered tablets.
    i64 GetTrimmedRowCount() const;
    void SetTrimmedRowCount(i64 value);

    TTimestamp GetLastCommitTimestamp() const;
    void UpdateLastCommitTimestamp(TTimestamp value);

    TTimestamp GetLastWriteTimestamp() const;
    void UpdateLastWriteTimestamp(TTimestamp value);

    TTimestamp GetUnflushedTimestamp() const;

    void StartEpoch(TTabletSlotPtr slot);
    void StopEpoch();
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    TTabletSnapshotPtr BuildSnapshot(TTabletSlotPtr slot) const;

    const TSortedDynamicRowKeyComparer& GetRowKeyComparer() const;

    void ValidateMountRevision(i64 mountRevision);

    void UpdateUnflushedTimestamp() const;

    i64 Lock();
    i64 Unlock();
    i64 GetTabletLockCount() const;

    void FillProfilerTags(const TCellId& cellId);
    void UpdateReplicaCounters();
    bool IsProfilingEnabled() const;

private:
    TTableMountConfigPtr Config_;
    TTabletChunkReaderConfigPtr ReaderConfig_;
    TTabletChunkWriterConfigPtr WriterConfig_;
    TTabletWriterOptionsPtr WriterOptions_;

    IStoreManagerPtr StoreManager_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;

    std::unique_ptr<TPartition> Eden_;

    TPartitionList PartitionList_;
    THashMap<TPartitionId, TPartition*> PartitionMap_;

    THashMap<TStoreId, IStorePtr> StoreIdMap_;
    std::map<i64, IOrderedStorePtr> StoreRowIndexMap_;

    TSortedDynamicRowKeyComparer RowKeyComparer_;

    int ColumnLockCount_ = -1;

    ITabletContext* const Context_;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    i64 TabletLockCount_ = 0;

    TTabletCounters* ProfilerCounters_ = nullptr;

    void Initialize();

    TPartition* GetContainingPartition(const ISortedStorePtr& store);

    void UpdateOverlappingStoreCount();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
