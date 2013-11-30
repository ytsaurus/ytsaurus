#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/schema.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TNonCopyable
{
public:
    // Read-only parameters.
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, Id);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TTableSchema, Schema);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::TTableMountConfigPtr, Config);
    
    // In-memory stores.
    DEFINE_BYVAL_RW_PROPERTY(TDynamicMemoryStorePtr, ActiveDynamicMemoryStore);
    DEFINE_BYVAL_RW_PROPERTY(TDynamicMemoryStorePtr, PassiveDynamicMemoryStore);

public:
    explicit TTablet(const TTabletId& id);
    TTablet(
        const TTabletId& id,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns,
        NTabletClient::TTableMountConfigPtr config);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
