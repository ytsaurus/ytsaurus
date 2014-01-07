#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/schema.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TNonCopyable
{
public:
    explicit TTablet(const TTabletId& id);
    TTablet(
        const TTabletId& id,
        TTabletSlot* slot,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        const NChunkClient::TChunkListId& chunkListId,
        NTabletClient::TTableMountConfigPtr config);

    ~TTablet();

    const TTabletId& GetId() const;
    TTabletSlot* GetSlot() const;
    const NVersionedTableClient::TTableSchema& Schema() const;
    const NVersionedTableClient::TKeyColumns& KeyColumns() const;
    const NChunkClient::TChunkListId& GetChunkListId() const;
    const NTabletClient::TTableMountConfigPtr& GetConfig() const;
    
    const NVersionedTableClient::TNameTablePtr& GetNameTable() const;

    const TStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(TStoreManagerPtr manager);

    ETabletState GetState() const;
    void SetState(ETabletState state);

    const TDynamicMemoryStorePtr& GetActiveStore() const;
    void SetActiveStore(TDynamicMemoryStorePtr store);

    std::vector<IStorePtr>& PassiveStores();

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    TTabletId Id_;
    TTabletSlot* Slot_;
    NVersionedTableClient::TTableSchema Schema_;
    NVersionedTableClient::TKeyColumns KeyColumns_;
    NChunkClient::TChunkListId ChunkListId_;
    NTabletClient::TTableMountConfigPtr Config_;
    
    NVersionedTableClient::TNameTablePtr NameTable_;
    
    TStoreManagerPtr StoreManager_;
    
    ETabletState State_;
    
    TDynamicMemoryStorePtr ActiveStore_;
    std::vector<IStorePtr> PassiveStores_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
