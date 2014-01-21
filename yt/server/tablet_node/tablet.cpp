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
    , State_(ETabletState::Mounted)
    , Config_(config)
    , NameTable_(TNameTable::FromSchema(Schema_))
{ }

TTablet::~TTablet()
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id_);
    Save(context, ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(Schema_));
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

const yhash_map<TStoreId, IStorePtr>& TTablet::Stores() const
{
    return Stores_;
}

void TTablet::AddStore(IStorePtr store)
{
    YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
}

void TTablet::RemoveStore(const TStoreId& id)
{
    YCHECK(Stores_.erase(id) == 1);
}

IStorePtr TTablet::FindStore(const TStoreId& id)
{
    auto it = Stores_.find(id);
    return it == Stores_.end() ? nullptr : it->second;
}

IStorePtr TTablet::GetStore(const TStoreId& id)
{
    auto store = FindStore(id);
    YCHECK(store);
    return store;
}

const TDynamicMemoryStorePtr& TTablet::GetActiveStore() const
{
    return ActiveStore_;
}

void TTablet::SetActiveStore(TDynamicMemoryStorePtr store)
{
    ActiveStore_ = std::move(store);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

