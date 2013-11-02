#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/schema.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TNonCopyable
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, Id);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TTableSchema, Schema);
    DEFINE_BYREF_RO_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);

public:
    explicit TTablet(const TTabletId& id);
    TTablet(
        const TTabletId& id,
        const NVersionedTableClient::TTableSchema& schema,
        const NVersionedTableClient::TKeyColumns& keyColumns);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
