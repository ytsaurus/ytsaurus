#pragma once

#include "public.h"
#include "partition.h"
#include "dynamic_memory_store_comparer.h"

#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>

#include <core/actions/cancelable_context.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <server/hydra/entity_map.h>

#include <atomic>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTabletSnapshot
    : public TIntrinsicRefCounted
{
    TTabletId TabletId;
    NObjectClient::TObjectId TableId;
    TTabletSlotPtr Slot;
    TTableMountConfigPtr Config;
    TTabletWriterOptionsPtr WriterOptions;
    TOwningKey PivotKey;
    TOwningKey NextPivotKey;
    NVersionedTableClient::TTableSchema Schema;
    NVersionedTableClient::TKeyColumns KeyColumns;
    NTransactionClient::EAtomicity Atomicity;

    TPartitionSnapshotPtr Eden;

    typedef std::vector<TPartitionSnapshotPtr> TPartitionList;
    typedef TPartitionList::iterator TPartitionListIterator;
    TPartitionList Partitions;

    int StoreCount = 0;
    int StorePreloadPendingCount = 0;
    int StorePreloadCompletedCount = 0;
    int StorePreloadFailedCount = 0;

    TDynamicRowKeyComparer RowKeyComparer;

    TTabletPerformanceCountersPtr PerformanceCounters;

    //! Returns a range of partitions intersecting with the range |[lowerBound, upperBound)|.
    std::pair<TPartitionListIterator, TPartitionListIterator> GetIntersectingPartitions(
        const TOwningKey& lowerBound,
        const TOwningKey& upperBound);

    //! Returns a partition possibly containing a given #key or
    //! |nullptr| is there's none.
    TPartitionSnapshotPtr FindContainingPartition(TKey key);
};

DEFINE_REFCOUNTED_TYPE(TTabletSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounters
    : public TChunkReaderPerformanceCounters
{
    std::atomic<i64> DynamicMemoryRowReadCount = {0};
    std::atomic<i64> DynamicMemoryRowLookupCount = {0};
    std::atomic<i64> DynamicMemoryRowWriteCount = {0};
    std::atomic<i64> DynamicMemoryRowDeleteCount = {0};
    std::atomic<i64> UnmergedRowReadCount = {0};
    std::atomic<i64> MergedRowReadCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TTabletPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NHydra::TEntityBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, TabletId);
    DEFINE_BYVAL_RO_PROPERTY(NObjectClient::TObjectId, TableId);
    DEFINE_BYVAL_RO_PROPERTY(TTabletSlotPtr, Slot);

    DEFINE_BYVAL_RO_PROPERTY(TTabletSnapshotPtr, Snapshot);

    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TTableSchema, Schema);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);

    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, ColumnIndexToLockIndex);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, LockIndexToName);

    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, NextPivotKey);
    
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RO_PROPERTY(TCancelableContextPtr, CancelableContext);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastPartitioningTime);

    // NB: Avoid keeping IStorePtr to simplify store removal.
    DEFINE_BYREF_RW_PROPERTY(std::deque<TStoreId>, PreloadStoreIds);

    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::EAtomicity, Atomicity);

public:
    TTablet(
        const TTabletId& tabletId,
        TTabletSlotPtr slot);
    TTablet(
        TTableMountConfigPtr config,
        TTabletWriterOptionsPtr writerOptions,
        const TTabletId& tabletId,
        const NObjectClient::TObjectId& tableId,
        TTabletSlotPtr slot,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        TOwningKey pivotKey,
        TOwningKey nextPivotKey,
        NTransactionClient::EAtomicity atomicity);

    ~TTablet();

    ETabletState GetPersistentState() const;

    const TTableMountConfigPtr& GetConfig() const;
    void SetConfig(TTableMountConfigPtr config);

    const TTabletWriterOptionsPtr& GetWriterOptions() const;
    void SetWriterOptions(TTabletWriterOptionsPtr options);

    const TStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(TStoreManagerPtr storeManager);

    const TTabletPerformanceCountersPtr& GetPerformanceCounters() const;

    typedef std::vector<std::unique_ptr<TPartition>> TPartitionList;
    const TPartitionList& Partitions() const;
    TPartition* GetEden() const;
    void CreateInitialPartition();
    TPartition* FindPartitionByPivotKey(const TOwningKey& pivotKey);
    TPartition* GetPartitionByPivotKey(const TOwningKey& pivotKey);
    TPartition* FindPartitionById(const TPartitionId& partitionId);
    TPartition* GetPartitionById(const TPartitionId& partitionId);
    void MergePartitions(int firstIndex, int lastIndex);
    void SplitPartition(int index, const std::vector<TOwningKey>& pivotKeys);

    //! Finds a partition fully containing the range |[minKey, maxKey]|.
    //! Returns the Eden if no such partition exists.
    TPartition* GetContainingPartition(
        const TOwningKey& minKey,
        const TOwningKey& maxKey);

    const yhash_map<TStoreId, IStorePtr>& Stores() const;
    void AddStore(IStorePtr store);
    void RemoveStore(IStorePtr store);
    IStorePtr FindStore(const TStoreId& id);
    IStorePtr GetStore(const TStoreId& id);

    const TDynamicMemoryStorePtr& GetActiveStore() const;
    void SetActiveStore(TDynamicMemoryStorePtr store);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    int GetSchemaColumnCount() const;
    int GetKeyColumnCount() const;
    int GetColumnLockCount() const;

    void StartEpoch(TTabletSlotPtr slot);
    void StopEpoch();
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default);

    TTabletSnapshotPtr RebuildSnapshot();
    void ResetSnapshot();

    const TDynamicRowKeyComparer& GetRowKeyComparer() const;

private:
    TTableMountConfigPtr Config_;
    TTabletWriterOptionsPtr WriterOptions_;

    TStoreManagerPtr StoreManager_;

    TTabletPerformanceCountersPtr PerformanceCounters_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;

    std::unique_ptr<TPartition> Eden_;

    TPartitionList PartitionList_;
    yhash_map<TPartitionId, TPartition*> PartitionMap_;

    yhash_map<TStoreId, IStorePtr> Stores_;
    TDynamicMemoryStorePtr ActiveStore_;

    TDynamicRowKeyComparer RowKeyComparer_;

    int ColumnLockCount_ = -1;


    void Initialize();

    TPartition* GetContainingPartition(IStorePtr store);
    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
