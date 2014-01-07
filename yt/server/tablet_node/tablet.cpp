#include "stdafx.h"
#include "tablet.h"
#include "automaton.h"
#include "store_manager.h"
#include "dynamic_memory_store.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NVersionedTableClient;
using namespace NTabletClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : Id_(id)
{ }

TTablet::TTablet(
    const TTabletId& id,
    TTabletSlot* slot,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TChunkListId& chunkListId,
    NTabletClient::TTableMountConfigPtr config)
    : Id_(id)
    , Slot_(slot)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , ChunkListId_(chunkListId)
    , Config_(config)
    , NameTable_(TNameTable::FromSchema(Schema_))
{ }

TTablet::~TTablet()
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id_);
    Save(context, NYT::ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(Schema_));
    Save(context, KeyColumns_);

    // TODO(babenko)
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Id_);
    Schema_ = FromProto<TTableSchema>(Load<NVersionedTableClient::NProto::TTableSchemaExt>(context));
    Load(context, KeyColumns_);

    // TODO(babenko)
}

const TTabletId& TTablet::GetId() const
{
    return Id_;
}

TTabletSlot* TTablet::GetSlot() const
{
    return Slot_;
}

const TTableSchema& TTablet::Schema() const
{
    return Schema_;
}

const TKeyColumns& TTablet::KeyColumns() const
{
    return KeyColumns_;
}

const TChunkListId& TTablet::GetChunkListId() const
{
    return ChunkListId_;
}

const TTableMountConfigPtr& TTablet::GetConfig() const
{
    return Config_;
}

const TNameTablePtr& TTablet::GetNameTable() const
{
    return NameTable_;
}

const TStoreManagerPtr& TTablet::GetStoreManager() const
{
    return StoreManager_;
}

void TTablet::SetStoreManager(TStoreManagerPtr manager)
{
    StoreManager_ = manager;
}

const TDynamicMemoryStorePtr& TTablet::GetActiveStore() const
{
    return ActiveStore_;
}

void TTablet::SetActiveStore(TDynamicMemoryStorePtr store)
{
    ActiveStore_ = std::move(store);
}

std::vector<IStorePtr>& TTablet::PassiveStores()
{
    return PassiveStores_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

