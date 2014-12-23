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

std::pair<TTabletSnapshot::TPartitionListIterator, TTabletSnapshot::TPartitionListIterator>
TTabletSnapshot::GetIntersectingPartitions(
    const TOwningKey& lowerBound,
    const TOwningKey& upperBound)
{
    auto beginIt = std::upper_bound(
        Partitions.begin(),
        Partitions.end(),
        lowerBound,
        [] (const TOwningKey& key, const TPartitionSnapshotPtr& partition) {
            return key < partition->PivotKey;
        });

    if (beginIt != Partitions.begin()) {
        --beginIt;
    }

    auto endIt = beginIt;
    while (endIt != Partitions.end() && upperBound > (*endIt)->PivotKey) {
        ++endIt;
    }

    return std::make_pair(beginIt, endIt);
}

TPartitionSnapshotPtr TTabletSnapshot::FindContainingPartition(TKey key)
{
    auto it = std::upper_bound(
        Partitions.begin(),
        Partitions.end(),
        key,
        [] (TKey key, const TPartitionSnapshotPtr& partition) {
            return key < partition->PivotKey.Get();
        });

    return it == Partitions.begin() ? nullptr : *(--it);
}

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
{
    Initialize();
}

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

void TTablet::SetStoreManager(TStoreManagerPtr storeManager)
{
    YCHECK(storeManager);
    YCHECK(!StoreManager_);
    StoreManager_ = storeManager;
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

    // NB: Call Initialize here since stores that we're about to create
    // may request some tablet properties (e.g. column lock count) during construction.
    Initialize();

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
    auto& mergedSampleKeys = mergedPartition->GetSampleKeys()->Keys;

    for (int index = firstIndex; index <= lastIndex; ++index) {
        const auto& existingPartition = Partitions_[index];
        const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
        if (index > firstIndex) {
            mergedSampleKeys.push_back(existingPartition->GetPivotKey());
        }
        mergedSampleKeys.insert(
            mergedSampleKeys.end(),
            existingSampleKeys.begin(),
            existingSampleKeys.end());

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

    for (int partitionIndex = index + 1; partitionIndex < Partitions_.size(); ++partitionIndex) {
        Partitions_[partitionIndex]->SetIndex(partitionIndex + pivotKeys.size() - 1);
    }

    std::vector<std::unique_ptr<TPartition>> splitPartitions;
    const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
    int sampleKeyIndex = 0;
    for (int pivotKeyIndex = 0; pivotKeyIndex < pivotKeys.size(); ++pivotKeyIndex) {
        auto partition = std::make_unique<TPartition>(
            this,
            index + pivotKeyIndex);
        auto thisPivotKey = pivotKeys[pivotKeyIndex];
        auto nextPivotKey = (pivotKeyIndex == pivotKeys.size() - 1)
            ? existingPartition->GetNextPivotKey()
            : pivotKeys[pivotKeyIndex + 1];
        partition->SetPivotKey(thisPivotKey);
        partition->SetNextPivotKey(nextPivotKey);

        if (sampleKeyIndex < existingSampleKeys.size() && existingSampleKeys[sampleKeyIndex] == thisPivotKey) {
            ++sampleKeyIndex;
        }

        YCHECK(sampleKeyIndex >= existingSampleKeys.size() || existingSampleKeys[sampleKeyIndex] > thisPivotKey);
        auto& sampleKeys = partition->GetSampleKeys()->Keys;
        while (sampleKeyIndex < existingSampleKeys.size() && existingSampleKeys[sampleKeyIndex] < nextPivotKey) {
            sampleKeys.push_back(existingSampleKeys[sampleKeyIndex]);
            ++sampleKeyIndex;
        }

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
}

void TTablet::RemoveStore(IStorePtr store)
{
    YCHECK(Stores_.erase(store->GetId()) == 1);
    auto* partition = store->GetPartition();
    YCHECK(partition->Stores().erase(store) == 1);
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

int TTablet::GetColumnLockCount() const
{
    return ColumnLockCount_;
}

void TTablet::StartEpoch(TTabletSlotPtr slot)
{
    CancelableContext_ = New<TCancelableContext>();

    EpochAutomatonInvokers_.resize(EAutomatonThreadQueue::GetDomainSize());
    for (auto queue : EAutomatonThreadQueue::GetDomainValues()) {
        EpochAutomatonInvokers_[queue] = CancelableContext_->CreateInvoker(
            // NB: Slot can be null in tests.
            slot
            ? slot->GetEpochAutomatonInvoker(queue)
            : GetSyncInvoker());
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
    YCHECK(!EpochAutomatonInvokers_.empty());
    return EpochAutomatonInvokers_[queue];
}

TTabletSnapshotPtr TTablet::BuildSnapshot() const
{
    auto snapshot = New<TTabletSnapshot>();
    snapshot->TabletId = Id_;
    snapshot->Slot = Slot_;
    snapshot->Config = Config_;
    snapshot->Schema = Schema_;
    snapshot->KeyColumns = KeyColumns_;
    snapshot->Eden = Eden_->BuildSnapshot();
    snapshot->Partitions.reserve(Partitions_.size());
    for (const auto& partition : Partitions_) {
        snapshot->Partitions.push_back(partition->BuildSnapshot());
    }
    return snapshot;
}

void TTablet::Initialize()
{
    Comparer_ = TDynamicRowKeyComparer(
        GetKeyColumnCount(),
        Schema_,
        Config_->EnableCodegen);

    ColumnIndexToLockIndex_.resize(Schema_.Columns().size());
    LockIndexToName_.push_back(PrimaryLockName);

    // Assign dummy lock indexes to key components.
    for (int index = 0; index < KeyColumns_.size(); ++index) {
        ColumnIndexToLockIndex_[index] = -1;
    }

    // Assign lock indexes to data components.
    yhash_map<Stroka, int> groupToIndex;
    for (int index = KeyColumns_.size(); index < Schema_.Columns().size(); ++index) {
        const auto& columnSchema = Schema_.Columns()[index];
        int lockIndex = TDynamicRow::PrimaryLockIndex;
        if (columnSchema.Lock) {
            auto it = groupToIndex.find(*columnSchema.Lock);
            if (it == groupToIndex.end()) {
                lockIndex = groupToIndex.size() + 1;
                YCHECK(groupToIndex.insert(std::make_pair(*columnSchema.Lock, lockIndex)).second);
                LockIndexToName_.push_back(*columnSchema.Lock);
            } else {
                lockIndex = it->second;
            }
        } else {
            lockIndex = TDynamicRow::PrimaryLockIndex;
        }
        ColumnIndexToLockIndex_[index] = lockIndex;
    }

    ColumnLockCount_ = groupToIndex.size() + 1;
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

TDynamicRowKeyComparer TTablet::GetDynamicRowKeyComparer() const
{
    return Comparer_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

