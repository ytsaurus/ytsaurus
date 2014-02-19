#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TPartition
{
public:
    static const int EdenIndex = -1;

    DEFINE_BYVAL_RO_PROPERTY(TTablet*, Tablet);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(NVersionedTableClient::TOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<IStorePtr>, Stores);

public:
    TPartition(TTablet* tablet, int index);
    ~TPartition();

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
