#include "stdafx.h"
#include "tablet.h"
#include "partition.h"
#include "automaton.h"
#include "store_manager.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "config.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/collection_helpers.h>

#include <ytlib/new_table_client/schema.h>
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
    , Config_(New<TTableMountConfig>())
{ }

TTablet::TTablet(
    TTableMountConfigPtr config,
    const TTabletId& id,
    TTabletSlot* slot,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TOwningKey pivotKey,
    TOwningKey nextPivotKey)
    : Id_(id)
    , Slot_(slot)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , State_(ETabletState::Mounted)
    , Config_(config)
    , Eden_(std::make_unique<TPartition>(this, TPartition::EdenIndex))
{ }

TTablet::~TTablet()
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Schema_);
    Save(context, KeyColumns_);
    Save(context, PivotKey_);
    Save(context, NextPivotKey_);
    Save(context, State_);
    Save(context, *Config_);

    auto saveStore = [&] (IStorePtr store) {
        Save(context, store->GetId());
        store->Save(context);
    };

    Save(context, Stores_.size());
    for (const auto& pair : Stores_) {
        saveStore(pair.second);
    }

    Save(context, ActiveStore_ ? ActiveStore_->GetId() : NullStoreId);

    Save(context, *Eden_);

    Save(context, Partitions_.size());
    for (const auto& partition : Partitions_) {
        Save(context, *partition);
    }
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Slot_ = context.GetSlot();

    Load(context, Schema_);
    Load(context, KeyColumns_);
    Load(context, PivotKey_);
    Load(context, NextPivotKey_);
    Load(context, State_);
    Load(context, *Config_);

    auto loadStore = [&] () -> IStorePtr {
        auto storeId = Load<TStoreId>(context);
        auto tabletManager = Slot_->GetTabletManager();
        auto store = tabletManager->CreateStore(this, storeId);
        store->Load(context);
        return store;
    };

    size_t storeCount = Load<size_t>(context);
    for (int index = 0; index < static_cast<int>(storeCount); ++index) {
        auto store = loadStore();
        YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
    }

    auto activeStoreId = Load<TStoreId>(context);
    if (activeStoreId != NullStoreId) {
        ActiveStore_ = dynamic_cast<TDynamicMemoryStore*>(GetStore(activeStoreId).Get());
        YCHECK(ActiveStore_);
    }

    auto loadPartition = [&] (int index) -> std::unique_ptr<TPartition> {
        auto partition = std::make_unique<TPartition>(this, index);
        Load(context, *partition);
        for (auto store : partition->Stores()) {
            store->SetPartition(partition.get());
        }
        return partition;
    };

    Eden_ = loadPartition(TPartition::EdenIndex);

    size_t partitionCount = Load<size_t>(context);
    for (int index = 0; index < static_cast<int>(partitionCount); ++index) {
        auto partition = loadPartition(index);
        Partitions_.push_back(std::move(partition));
    }
}

const TStoreManagerPtr& TTablet::GetStoreManager() const
{
    return StoreManager_;
}

void TTablet::SetStoreManager(TStoreManagerPtr manager)
{
    StoreManager_ = manager;
}

const std::vector<std::unique_ptr<TPartition>>& TTablet::Partitions() const
{
    return Partitions_;
}

TPartition* TTablet::GetEden() const
{
    return Eden_.get();
}

TPartition* TTablet::AddPartition(TOwningKey pivotKey)
{
    auto partitionHolder = std::make_unique<TPartition>(
        this,
        static_cast<int>(Partitions_.size()));
    auto* partition = partitionHolder.get();
    partition->SetPivotKey(std::move(pivotKey));
    Partitions_.push_back(std::move(partitionHolder));
    return partition;
}

void TTablet::MergePartitions(int firstIndex, int lastIndex)
{
    for (int i = lastIndex + 1; i < static_cast<int>(Partitions_.size()); ++i) {
        Partitions_[i]->SetIndex(i - (lastIndex - firstIndex));
    }

    auto mergedPartition = std::make_unique<TPartition>(this, firstIndex);
    mergedPartition->SetPivotKey(Partitions_[firstIndex]->GetPivotKey());

    for (int i = firstIndex; i <= lastIndex; ++i) {
        const auto& existingPartition = Partitions_[i];
        for (auto store : existingPartition->Stores()) {
            YCHECK(store->GetPartition() == existingPartition.get());
            store->SetPartition(mergedPartition.get());
            YCHECK(mergedPartition->Stores().insert(store).second);
        }
    }

    Partitions_.erase(Partitions_.begin() + firstIndex, Partitions_.begin() + lastIndex + 1);
    Partitions_.insert(Partitions_.begin() + firstIndex, std::move(mergedPartition));
}

void TTablet::SplitPartition(int index, const std::vector<TOwningKey>& pivotKeys)
{
    for (int i = index + 1; i < static_cast<int>(Partitions_.size()); ++i) {
        Partitions_[i]->SetIndex(i + static_cast<int>(pivotKeys.size()) - 1);
    }

    std::vector<std::unique_ptr<TPartition>> splitPartitions;
    for (int i = 0; i < static_cast<int>(pivotKeys.size()); ++i) {
        auto partition = std::make_unique<TPartition>(
            this,
            index + i);
        partition->SetPivotKey(pivotKeys[i]);
        splitPartitions.push_back(std::move(partition));
    }

    const auto& existingPartition = Partitions_[index];
    Partitions_.erase(Partitions_.begin() + index);

    Partitions_.insert(
        Partitions_.begin() + index,
        std::make_move_iterator(splitPartitions.begin()),
        std::make_move_iterator(splitPartitions.end()));

    for (auto store : existingPartition->Stores()) {
        YCHECK(store->GetPartition() == existingPartition.get());
        auto* newPartition = FindRelevantPartition(store);
        store->SetPartition(newPartition);
        YCHECK(newPartition->Stores().insert(store).second);
    }
}

const yhash_map<TStoreId, IStorePtr>& TTablet::Stores() const
{
    return Stores_;
}

void TTablet::AddStore(IStorePtr store)
{
    auto* partition = FindRelevantPartition(store);
    store->SetPartition(partition);
    YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
    YCHECK(partition->Stores().insert(store).second);
}

void TTablet::RemoveStore(const TStoreId& id)
{
    auto store = GetStore(id);
    YCHECK(Stores_.erase(id) == 1);
    YCHECK(store->GetPartition()->Stores().erase(store) == 1);
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

TPartition* TTablet::FindRelevantPartition(IStorePtr store)
{
    // Dynamic stores must reside in Eden.
    if (store->GetState() == EStoreState::ActiveDynamic ||
        store->GetState() == EStoreState::PassiveDynamic)
    {
        return Eden_.get();
    }

    // NB: Store key range need not be completely inside the tablet's one.
    const auto& minKey = ChooseMaxKey(store->GetMinKey(), PivotKey_);
    const auto& maxKey = store->GetMaxKey();

    // Run binary search to find the relevant partition.
    auto partitionIt = std::upper_bound(
        Partitions_.begin(),
        Partitions_.end(),
        minKey,
        [] (const TOwningKey& key, const std::unique_ptr<TPartition>& partition) {
            return key < partition->GetPivotKey();
        }) - 1;

    if (partitionIt + 1 == Partitions().end()) {
        return partitionIt->get();
    }

    if (CompareRows((*(partitionIt + 1))->GetPivotKey(), maxKey) > 0) {
        return partitionIt->get();
    }

    return Eden_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

