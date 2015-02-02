#pragma once

#include "public.h"
#include "partition.h"
#include "dynamic_memory_store_comparer.h"

#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>

#include <core/actions/cancelable_context.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <server/hydra/entity_map.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTabletSnapshot
    : public TIntrinsicRefCounted
{
    TTabletId TabletId;
    TTabletSlotPtr Slot;
    TTableMountConfigPtr Config;

    NVersionedTableClient::TTableSchema Schema;
    NVersionedTableClient::TKeyColumns KeyColumns;

    TPartitionSnapshotPtr Eden;

    typedef std::vector<TPartitionSnapshotPtr> TPartitionList;
    typedef TPartitionList::iterator TPartitionListIterator;
    TPartitionList Partitions;

    int StoreCount = 0;

    TDynamicRowKeyComparer RowKeyComparer;

    TTabletStatisticsPtr Statistics;

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

struct TTabletStatistics
    : public TIntrinsicRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TTabletStatistics)

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NHydra::TEntityBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TTabletSlotPtr, Slot);

    DEFINE_BYVAL_RW_PROPERTY(TTabletSnapshotPtr, Snapshot);

    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TTableSchema, Schema);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);

    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, ColumnIndexToLockIndex);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, LockIndexToName);

    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, NextPivotKey);
    
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RO_PROPERTY(TCancelableContextPtr, CancelableContext);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastPartitioningTime);

public:
    explicit TTablet(const TTabletId& id);
    TTablet(
        TTableMountConfigPtr config,
        TTabletWriterOptionsPtr writerOptions,
        const TTabletId& id,
        TTabletSlotPtr slot,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        TOwningKey pivotKey,
        TOwningKey nextPivotKey);

    ~TTablet();

    ETabletState GetPersistentState() const;

    const TTableMountConfigPtr& GetConfig() const;
    void SetConfig(TTableMountConfigPtr config);

    const TTabletWriterOptionsPtr& GetWriterOptions() const;
    void SetWriterOptions(TTabletWriterOptionsPtr options);

    const TStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(TStoreManagerPtr storeManager);

    const TTabletStatisticsPtr& GetStatistics() const;

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

    int GetSchemaColumnCount() const;
    int GetKeyColumnCount() const;
    int GetColumnLockCount() const;

    void StartEpoch(TTabletSlotPtr slot);
    void StopEpoch();
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default);

    TTabletSnapshotPtr BuildSnapshot() const;

    TDynamicRowKeyComparer GetRowKeyComparer() const;

private:
    const TTableMountConfigPtr Config_;
    const TTabletWriterOptionsPtr WriterOptions_;

    TStoreManagerPtr StoreManager_;

    TTabletStatisticsPtr Statistics_;

    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;

    std::unique_ptr<TPartition> Eden_;

    TPartitionList PartitionList_;
    yhash_map<TPartitionId, TPartition*> PartitionMap_;

    yhash_map<TStoreId, IStorePtr> Stores_;
    TDynamicMemoryStorePtr ActiveStore_;

    TDynamicRowKeyComparer Comparer_;

    int ColumnLockCount_ = -1;


    void Initialize();

    TPartition* GetContainingPartition(IStorePtr store);
    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
