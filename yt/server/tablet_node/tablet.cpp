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
    , WriterOptions_(New<TTabletWriterOptions>())
{ }

TTablet::TTablet(
    TTableMountConfigPtr config,
    TTabletWriterOptionsPtr writerOptions,
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
    , WriterOptions_(writerOptions)
    , Eden_(std::make_unique<TPartition>(this, TPartition::EdenIndex))
{ }

TTablet::~TTablet()
{ }

ETabletState TTablet::GetPersistentState() const
{
    switch (State_) {
        case ETabletState::FlushPending:
            return ETabletState::WaitingForLocks;
        case ETabletState::UnmountPending:
            return ETabletState::Flushing;
        default:
            return State_;
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

const TTabletWriterOptionsPtr& TTablet::GetWriterOptions() const
{
    return WriterOptions_;
}

void TTablet::SetWriterOptions(TTabletWriterOptionsPtr options)
{
    WriterOptions_ = options;
}

const TStoreManagerPtr& TTablet::GetStoreManager() const
{
    return StoreManager_;
}

void TTablet::SetStoreManager(TStoreManagerPtr manager)
{
    StoreManager_ = manager;
}

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Schema_);
    Save(context, KeyColumns_);
    Save(context, PivotKey_);
    Save(context, NextPivotKey_);
    Save(context, GetPersistentState());
    Save(context, *Config_);
    Save(context, *WriterOptions_);

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
    Load(context, *WriterOptions_);

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
        ActiveStore_ = GetStore(activeStoreId)->AsDynamicMemory();
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

const std::vector<std::unique_ptr<TPartition>>& TTablet::Partitions() const
{
    return Partitions_;
}

TPartition* TTablet::GetEden() const
{
    return Eden_.get();
}

void TTablet::CreateInitialPartition()
{
    YCHECK(Partitions_.empty());
    auto partition = std::make_unique<TPartition>(
        this,
        static_cast<int>(Partitions_.size()));
    partition->SetPivotKey(PivotKey_);
    partition->SetNextPivotKey(NextPivotKey_);
    partition->SampleKeys().push_back(partition->GetPivotKey());
    Partitions_.push_back(std::move(partition));
}

TPartition* TTablet::FindPartitionByPivotKey(const NVersionedTableClient::TOwningKey& pivotKey)
{
    auto it = std::lower_bound(
        Partitions_.begin(),
        Partitions_.end(),
        pivotKey,
        [] (const std::unique_ptr<TPartition>& partition, const TOwningKey& key) {
            return partition->GetPivotKey() < key;
        });
    return it != Partitions_.end() && (*it)->GetPivotKey() == pivotKey ? it->get() : nullptr;
}

TPartition* TTablet::GetPartitionByPivotKey(const NVersionedTableClient::TOwningKey& pivotKey)
{
    auto* partition = FindPartitionByPivotKey(pivotKey);
    YCHECK(partition);
    return partition;
}

void TTablet::MergePartitions(int firstIndex, int lastIndex)
{
    for (int i = lastIndex + 1; i < static_cast<int>(Partitions_.size()); ++i) {
        Partitions_[i]->SetIndex(i - (lastIndex - firstIndex));
    }

    auto mergedPartition = std::make_unique<TPartition>(this, firstIndex);
    mergedPartition->SetPivotKey(Partitions_[firstIndex]->GetPivotKey());
    mergedPartition->SetNextPivotKey(Partitions_[lastIndex]->GetNextPivotKey());
    mergedPartition->SetSamplingNeeded(true);

    for (int i = firstIndex; i <= lastIndex; ++i) {
        const auto& existingPartition = Partitions_[i];
        mergedPartition->SampleKeys().insert(
            mergedPartition->SampleKeys().end(),
            existingPartition->SampleKeys().begin(),
            existingPartition->SampleKeys().end());

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
    auto existingPartition = std::move(Partitions_[index]);
    YCHECK(existingPartition->GetPivotKey() == pivotKeys[0]);

    for (int i = index + 1; i < static_cast<int>(Partitions_.size()); ++i) {
        Partitions_[i]->SetIndex(i + static_cast<int>(pivotKeys.size()) - 1);
    }

    std::vector<std::unique_ptr<TPartition>> splitPartitions;
    for (int i = 0; i < static_cast<int>(pivotKeys.size()); ++i) {
        auto partition = std::make_unique<TPartition>(
            this,
            index + i);
        partition->SetPivotKey(pivotKeys[i]);
        partition->SetNextPivotKey(
            i == static_cast<int>(pivotKeys.size()) - 1
            ? existingPartition->GetNextPivotKey()
            : pivotKeys[i + 1]);
        // TODO(babenko): keep samples from existing partition
        partition->SampleKeys().push_back(partition->GetPivotKey());
        partition->SetSamplingNeeded(true);
        splitPartitions.push_back(std::move(partition));
    }

    Partitions_.erase(Partitions_.begin() + index);

    Partitions_.insert(
        Partitions_.begin() + index,
        std::make_move_iterator(splitPartitions.begin()),
        std::make_move_iterator(splitPartitions.end()));

    for (auto store : existingPartition->Stores()) {
        YCHECK(store->GetPartition() == existingPartition.get());
        auto* newPartition = GetContainingPartition(store);
        store->SetPartition(newPartition);
        YCHECK(newPartition->Stores().insert(store).second);
    }
}

TPartition* TTablet::GetContainingPartition(
    const TOwningKey& minKey,
    const TOwningKey& maxKey)
{
    auto it = std::upper_bound(
        Partitions_.begin(),
        Partitions_.end(),
        minKey,
        [] (const TOwningKey& key, const std::unique_ptr<TPartition>& partition) {
            return key < partition->GetPivotKey();
        });

    if (it != Partitions_.begin()) {
        --it;
    }

    if (it + 1 == Partitions().end()) {
        return it->get();
    }

    if ((*(it + 1))->GetPivotKey() > maxKey) {
        return it->get();
    }

    return Eden_.get();
}

TPartition* TTablet::GetContainingPartition(IStorePtr store)
{
    // Dynamic stores must reside in Eden.
    if (store->GetState() == EStoreState::ActiveDynamic ||
        store->GetState() == EStoreState::PassiveDynamic)
    {
        return Eden_.get();
    }

    return GetContainingPartition(store->GetMinKey(), store->GetMaxKey());
}

std::pair<TTablet::TPartitionListIterator, TTablet::TPartitionListIterator> TTablet::GetIntersectingPartitions(
    const TOwningKey& lowerBound,
    const TOwningKey& upperBound)
{
    auto beginIt = std::upper_bound(
        Partitions_.begin(),
        Partitions_.end(),
        lowerBound,
        [] (const TOwningKey& key, const std::unique_ptr<TPartition>& partition) {
            return key < partition->GetPivotKey();
        });

    if (beginIt != Partitions_.begin()) {
        --beginIt;
    }

    auto endIt = beginIt;
    while (endIt != Partitions_.end() && upperBound > (*endIt)->GetPivotKey()) {
        ++endIt;
    }

    return std::make_pair(beginIt, endIt);
}

const yhash_map<TStoreId, IStorePtr>& TTablet::Stores() const
{
    return Stores_;
}

void TTablet::AddStore(IStorePtr store)
{
    auto* partition = GetContainingPartition(store);
    store->SetPartition(partition);
    YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
    YCHECK(partition->Stores().insert(store).second);
    if (partition != Eden_.get()) {
        partition->SetSamplingNeeded(true);
    }
}

void TTablet::RemoveStore(IStorePtr store)
{
    YCHECK(Stores_.erase(store->GetId()) == 1);
    auto* partition = store->GetPartition();
    YCHECK(partition->Stores().erase(store) == 1);
    if (partition != Eden_.get()) {
        partition->SetSamplingNeeded(true);
    }
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

void TTablet::StartEpoch(TTabletSlotPtr slot)
{
    CancelableContext_ = New<TCancelableContext>();

    EpochAutomatonInvokers_.resize(EAutomatonThreadQueue::GetDomainSize());
    for (auto queue : EAutomatonThreadQueue::GetDomainValues()) {
        EpochAutomatonInvokers_[queue] = CancelableContext_->CreateInvoker(
            slot->GetEpochAutomatonInvoker(queue));
    }
}

void TTablet::StopEpoch()
{
    if (CancelableContext_) {
        CancelableContext_->Cancel();
        CancelableContext_.Reset();
    }

    EpochAutomatonInvokers_.clear();
}

IInvokerPtr TTablet::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return EpochAutomatonInvokers_[queue];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

