#include "stdafx.h"
#include "tablet.h"
#include "automaton.h"
#include "store_manager.h"
#include "dynamic_memory_store.h"
#include "tablet_slot.h"
#include "tablet_manager.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/collection_helpers.h>

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
    , Slot_(nullptr)
{ }

TTablet::TTablet(
    const TTabletId& id,
    TTabletSlot* slot,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : Id_(id)
    , Slot_(slot)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , State_(ETabletState::Mounted)
    , NameTable_(TNameTable::FromSchema(Schema_))
{ }

TTablet::~TTablet()
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(Schema_));
    Save(context, ToProto<NVersionedTableClient::NProto::TNameTableExt>(NameTable_));
    Save(context, KeyColumns_);
    Save(context, State_);

    auto saveStore = [&] (IStorePtr store) {
        Save(context, store->GetId());
        store->Save(context);
    };

    Save(context, Stores_.size());
    for (const auto& pair : Stores_) {
        saveStore(pair.second);
    }

    Save(context, ActiveStore_ ? ActiveStore_->GetId() : NullStoreId);
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Slot_ = context.GetSlot();

    Schema_ = FromProto<TTableSchema>(Load<NVersionedTableClient::NProto::TTableSchemaExt>(context));
    NameTable_ = FromProto<TNameTablePtr>(Load<NVersionedTableClient::NProto::TNameTableExt>(context));
    Load(context, KeyColumns_);
    Load(context, State_);

    auto loadStore = [&] () -> IStorePtr {
        auto storeId = Load<TStoreId>(context);
        auto tabletManager = Slot_->GetTabletManager();
        auto store = tabletManager->CreateStore(this, storeId);
        store->Load(context);
        return store;
    };

    size_t storeCount = Load<size_t>(context);
    for (size_t index = 0; index < storeCount; ++index) {
        auto store = loadStore();
        YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
    }

    auto activeStoreId = Load<TStoreId>(context);
    if (activeStoreId != NullStoreId) {
        ActiveStore_ = dynamic_cast<TDynamicMemoryStore*>(GetStore(activeStoreId).Get());
        YCHECK(ActiveStore_);
    }
}

const TTableMountConfigPtr& TTablet::GetConfig() const
{
    return Config_;
}

void TTablet::SetConfig(TTableMountConfigPtr config)
{
    Config_ = config;
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

int TTablet::GetSchemaColumnCount() const
{
    return static_cast<int>(Schema_.Columns().size());
}

int TTablet::GetKeyColumnCount() const
{
    return static_cast<int>(KeyColumns_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

