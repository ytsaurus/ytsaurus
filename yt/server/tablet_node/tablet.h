#pragma once

#include "public.h"
#include "partition.h"

#include <core/misc/property.h>

#include <core/actions/cancelable_context.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TNonCopyable
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TTabletSlot*, Slot);
    
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TTableSchema, Schema);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);
    
    DEFINE_BYVAL_RO_PROPERTY(NVersionedTableClient::TOwningKey, PivotKey);
    DEFINE_BYVAL_RO_PROPERTY(NVersionedTableClient::TOwningKey, NextPivotKey);
    
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RW_PROPERTY(TCancelableContextPtr, CancelableContext);
    DEFINE_BYVAL_RW_PROPERTY(IInvokerPtr, EpochAutomatonInvoker);

public:
    explicit TTablet(const TTabletId& id);
    TTablet(
        const TTabletId& id,
        TTabletSlot* slot,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        NVersionedTableClient::TOwningKey pivotKey,
        NVersionedTableClient::TOwningKey nextPivotKey);

    ~TTablet();

    const NTabletClient::TTableMountConfigPtr& GetConfig() const;
    void SetConfig(NTabletClient::TTableMountConfigPtr config);

    const TStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(TStoreManagerPtr manager);

    const std::vector<std::unique_ptr<TPartition>>& Partitions() const;
    TPartition* GetEden() const;
    TPartition* AddPartition(NVersionedTableClient::TOwningKey pivotKey);
    void MergePartitions(int firstIndex, int lastIndex);
    void SplitPartition(int index, const std::vector<NVersionedTableClient::TOwningKey>& pivotKeys);

    const yhash_map<TStoreId, IStorePtr>& Stores() const;
    void AddStore(IStorePtr store);
    void RemoveStore(const TStoreId& id);
    IStorePtr FindStore(const TStoreId& id);
    IStorePtr GetStore(const TStoreId& id);

    const TDynamicMemoryStorePtr& GetActiveStore() const;
    void SetActiveStore(TDynamicMemoryStorePtr store);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    int GetSchemaColumnCount() const;
    int GetKeyColumnCount() const;

private:
    NTabletClient::TTableMountConfigPtr Config_;
    TStoreManagerPtr StoreManager_;

    std::unique_ptr<TPartition> Eden_;
    std::vector<std::unique_ptr<TPartition>> Partitions_;

    yhash_map<TStoreId, IStorePtr> Stores_;
    TDynamicMemoryStorePtr ActiveStore_;


    TPartition* FindRelevantPartition(IStorePtr store);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
