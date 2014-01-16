#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/enum.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <server/object_server/object.h>

#include <server/table_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NObjectServer::TNonversionedObjectBase
{
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYREF_RW_PROPERTY(NVersionedTableClient::TOwningKey, PivotKey);

public:
    explicit TTablet(const TTabletId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
