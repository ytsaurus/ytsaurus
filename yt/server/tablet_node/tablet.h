#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/schema.h>

#include <ytlib/tablet_client/public.h>

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
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        NTabletClient::TTableMountConfigPtr config);

    ~TTablet();

    const TTabletId& GetId() const;
    const NVersionedTableClient::TTableSchema& Schema() const;
    const NVersionedTableClient::TKeyColumns& KeyColumns() const;
    const NTabletClient::TTableMountConfigPtr& GetConfig() const;
    
    const NVersionedTableClient::TNameTablePtr& GetNameTable() const;

    const TStoreManagerPtr& GetStoreManager() const;
    void SetStoreManager(TStoreManagerPtr manager);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    TTabletId Id_;
    NVersionedTableClient::TTableSchema Schema_;
    NVersionedTableClient::TKeyColumns KeyColumns_;
    NTabletClient::TTableMountConfigPtr Config_;
    
    NVersionedTableClient::TNameTablePtr NameTable_;
    TStoreManagerPtr StoreManager_;
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
