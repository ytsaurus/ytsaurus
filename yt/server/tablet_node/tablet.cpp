#include "stdafx.h"
#include "tablet.h"
#include "automaton.h"
#include "memory_table.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : Id_(id)
{ }

TTablet::TTablet(
    const TTabletId& id,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    NTabletClient::TTableMountConfigPtr config)
    : Id_(id)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , Config_(config)
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id_);
    Save(context, NYT::ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(Schema_));
    Save(context, KeyColumns_);

    // TODO(babenko): save memory table
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Id_);
    Schema_ = FromProto<TTableSchema>(Load<NVersionedTableClient::NProto::TTableSchemaExt>(context));
    Load(context, KeyColumns_);

    // TODO(babenko): load memory table
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

