#pragma once

#include "public.h"
#include "sorted_dynamic_comparer.h"
#include "partition.h"
#include "object_detail.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <atomic>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Trimmed row counter is shared between a tablet and its snapshots.
//! It's being concurrently updated from the automaton thread
//! and read from query-serving thread pool.
using TTrimmedRowCounter = std::atomic<i64>;
using TTrimmedRowCounterPtr = std::shared_ptr<TTrimmedRowCounter>;

struct TTabletSnapshot
    : public TIntrinsicRefCounted
{
    NHydra::TCellId CellId;
    NHydra::IHydraManagerPtr HydraManager;
    TTabletManagerPtr TabletManager;
    TTabletId TabletId;
    i64 MountRevision = 0;
    NObjectClient::TObjectId TableId;
    TTableMountConfigPtr Config;
    TTabletWriterOptionsPtr WriterOptions;
    TOwningKey PivotKey;
    TOwningKey NextPivotKey;
    //! This is just a copy of the table's schema.
    //! Can be both sorted and not sorted (ordered).
    NTableClient::TTableSchema TableSchema;
    //! This schema is always sorted.
    //! For sorted tablets, this is same as #TableSchema.
    //! For ordered tablets, this is the extended ordered schema.
    NTableClient::TTableSchema QuerySchema;
    NTransactionClient::EAtomicity Atomicity;
    int HashTableSize = 0;
    int OverlappingStoreCount = 0;

    TPartitionSnapshotPtr Eden;

    using TPartitionList = std::vector<TPartitionSnapshotPtr>;
    using TPartitionListIterator = TPartitionList::iterator;
    TPartitionList PartitionList;

    std::vector<IOrderedStorePtr> StoreList;
    TTrimmedRowCounterPtr TrimmedRowCounter;

    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;

    TSortedDynamicRowKeyComparer RowKeyComparer;

    TTabletPerformanceCountersPtr PerformanceCounters;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator;

    //! Returns a range of partitions intersecting with the range |[lowerBound, upperBound)|.
    std::pair<TPartitionListIterator, TPartitionListIterator> GetIntersectingPartitions(
        const TOwningKey& lowerBound,
        const TOwningKey& upperBound);

    //! Returns a partition possibly containing a given #key or
    //! |nullptr| is there's none.
    TPartitionSnapshotPtr FindContainingPartition(TKey key);

    void ValiateCellId(const NElection::TCellId& cellId);
    void ValiateMountRevision(i64 mountRevision);
};

DEFINE_REFCOUNTED_TYPE(TTabletSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounters
    : public TChunkReaderPerformanceCounters
{
    std::atomic<i64> DynamicRowReadCount = {0};
    std::atomic<i64> DynamicRowLookupCount = {0};
    std::atomic<i64> DynamicRowWriteCount = {0};
    std::atomic<i64> DynamicRowDeleteCount = {0};
    std::atomic<i64> UnmergedRowReadCount = {0};
    std::atomic<i64> MergedRowReadCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TTabletPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

struct ITabletContext
{
    virtual ~ITabletContext() = default;

    virtual NObjectClient::TCellId GetCellId() = 0;
    virtual NQueryClient::TColumnEvaluatorCachePtr GetColumnEvaluatorCache() = 0;
    virtual NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) = 0;
    virtual IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        const TStoreId& storeId,
        const NTabletNode::NProto::TAddStoreDescriptor* descriptor) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, MountRevision);
    DEFINE_BYVAL_RO_PROPERTY(NObjectClient::TObjectId, TableId);

    DEFINE_BYREF_RO_PROPERTY(NTableClient::TTableSchema, Schema);

    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, ColumnIndexToLockIndex);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, LockIndexToName);

    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, NextPivotKey);

    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RO_PROPERTY(TCancelableContextPtr, CancelableContext);

    // NB: Avoid keeping IStorePtr to simplify store removal.
    DEFINE_BYREF_RW_PROPERTY(std::deque<TStoreId>, PreloadStoreIds);

    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::EAtomicity, Atomicity);

    DEFINE_BYVAL_RO_PROPERTY(int, HashTableSize);

    DEFINE_BYVAL_RO_PROPERTY(int, OverlappingStoreCount);

    DEFINE_BYVAL_RW_PROPERTY(IDynamicStorePtr, ActiveStore);

public:
    TTablet(
        const TTabletId& tabletId,
        ITabletContext* context);
    TTablet(
        TTableMountConfigPtr config,
        TTabletWriterOptionsPtr writerOptions,
        const TTabletId& tabletId,
        i64 mountRevision,
        const NObjectClient::TObjectId& tableId,
        ITabletContext* context,
        const NTableClient::TTableSchema& schema,
        TOwningKey pivotKey,
        TOwningKey nextPivotKey,
        NTransactionClient::EAtomicity atomicity);

    ETabletState GetPersistentState() const;

    const TTableMountConfigPtr& GetConfig() const;
    void SetConfig(TTableMountConfigPtr config);

    const TTabletWriterOptionsPtr& GetWriterOptions() const;
    void SetWriterOptions(TTabletWriterOptionsPtr options);

    const IStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(IStoreManagerPtr storeManager);

    const TTabletPerformanceCountersPtr& GetPerformanceCounters() const;

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

    const yhash_map<TStoreId, IStorePtr>& StoreIdMap() const;
    const std::map<i64, IOrderedStorePtr>& StoreRowIndexMap() const;
    void AddStore(IStorePtr store);
    void RemoveStore(IStorePtr store);
    IStorePtr FindStore(const TStoreId& id);
    IStorePtr GetStore(const TStoreId& id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    bool IsSorted() const;
    bool IsOrdered() const;

    int GetSchemaColumnCount() const;
    int GetKeyColumnCount() const;
    int GetColumnLockCount() const;

    // Only applicable to ordered tablets.
    i64 GetTotalRowCount() const;

    // Only applicable to ordered tablets.
    i64 GetTrimmedRowCount() const;
    void SetTrimmedRowCount(i64 value);

    void StartEpoch(TTabletSlotPtr slot);
    void StopEpoch();
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default);

    TTabletSnapshotPtr BuildSnapshot(TTabletSlotPtr slot) const;

    const TSortedDynamicRowKeyComparer& GetRowKeyComparer() const;

    void ValidateMountRevision(i64 mountRevision);

private:
    const TTrimmedRowCounterPtr TrimmedRowCounter_ = std::make_shared<TTrimmedRowCounter>(0);

    TTableMountConfigPtr Config_;
    TTabletWriterOptionsPtr WriterOptions_;

    IStoreManagerPtr StoreManager_;

    TTabletPerformanceCountersPtr PerformanceCounters_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;

    std::unique_ptr<TPartition> Eden_;

    TPartitionList PartitionList_;
    yhash_map<TPartitionId, TPartition*> PartitionMap_;

    yhash_map<TStoreId, IStorePtr> StoreIdMap_;
    std::map<i64, IOrderedStorePtr> StoreRowIndexMap_;

    TSortedDynamicRowKeyComparer RowKeyComparer_;

    int ColumnLockCount_ = -1;

    ITabletContext* const Context_;

    NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;


    void Initialize();

    TPartition* GetContainingPartition(const ISortedStorePtr& store);

 	void UpdateOverlappingStoreCount();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
