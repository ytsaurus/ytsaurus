#include "stdafx.h"
#include "tablet.h"
#include "automaton.h"

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
    const TKeyColumns& keyColumns)
    : Id_(id)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id_);
    Save(context, NYT::ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(Schema_));
    Save(context, KeyColumns_);
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Id_);
    Schema_ = FromProto<TTableSchema>(Load<NVersionedTableClient::NProto::TTableSchemaExt>(context));
    Load(context, KeyColumns_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

