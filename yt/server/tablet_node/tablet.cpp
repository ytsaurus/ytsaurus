#include "tablet.h"
#include "automaton.h"
#include "chunk_store.h"
#include "config.h"
#include "dynamic_memory_store.h"
#include "partition.h"
#include "store_manager.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NQueryClient;

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
            return key < partition->PivotKey;
        });

    return it == Partitions.begin() ? nullptr : *(--it);
}

void TTabletSnapshot::ValiateMountRevision(i64 mountRevision)
{
    if (MountRevision != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            TabletId,
            MountRevision,
            mountRevision);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    const TTabletId& tabletId,
    TTabletSlotPtr slot)
    : TObjectBase(tabletId)
    , MountRevision_(0)
    , Slot_(slot)
    , Config_(New<TTableMountConfig>())
    , WriterOptions_(New<TTabletWriterOptions>())
    , ColumnEvaluatorCache_(Slot_->GetColumnEvaluatorCache())
{ }

TTablet::TTablet(
    TTableMountConfigPtr config,
    TTabletWriterOptionsPtr writerOptions,
    const TTabletId& tabletId,
    i64 mountRevision,
    const TObjectId& tableId,
    TTabletSlotPtr slot,
    TColumnEvaluatorCachePtr columnEvaluatorCache,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TOwningKey pivotKey,
    TOwningKey nextPivotKey,
    EAtomicity atomicity)
    : TObjectBase(tabletId)
    , MountRevision_(mountRevision)
    , TableId_(tableId)
    , Slot_(slot)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , State_(ETabletState::Mounted)
    , Atomicity_(atomicity)
    , EnableLookupHashTable_(config->EnableLookupHashTable)
    , Config_(config)
    , WriterOptions_(writerOptions)
    , Eden_(std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        TPartition::EdenIndex,
        PivotKey_,
        NextPivotKey_))
    , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
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

const TTabletPerformanceCountersPtr& TTablet::GetPerformanceCounters() const
{
    return PerformanceCounters_;
}

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TableId_);
    Save(context, MountRevision_);
    Save(context, GetPersistentState());
    Save(context, Schema_);
    Save(context, KeyColumns_);
    Save(context, Atomicity_);
    Save(context, EnableLookupHashTable_);

    TSizeSerializer::Save(context, Stores_.size());
    // NB: This is not stable.
    for (const auto& pair : Stores_) {
        const auto& store = pair.second;
        Save(context, store->GetId());
        store->Save(context);
    }

    Save(context, ActiveStore_ ? ActiveStore_->GetId() : NullStoreId);

    auto savePartition = [&] (const TPartition& partition) {
        Save(context, partition.GetId());
        partition.Save(context);
    };

    savePartition(*Eden_);

    TSizeSerializer::Save(context, PartitionList_.size());
    for (const auto& partition : PartitionList_) {
        savePartition(*partition);
    }
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    auto tabletManager = Slot_->GetTabletManager();

    Load(context, TableId_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 10) {
        Load(context, MountRevision_);
    }
    Load(context, State_);
    // TODO(babenko): consider moving schema and key columns to async part
    Load(context, Schema_);
    Load(context, KeyColumns_);
    Load(context, Atomicity_);
    // COMPAT(savrus)
    if (context.GetVersion() >= 11) {
        Load(context, EnableLookupHashTable_);
    }

    // NB: Call Initialize here since stores that we're about to create
    // may request some tablet properties (e.g. column lock count) during construction.
    Initialize();

    int storeCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", storeCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < storeCount; ++index) {
            auto storeId = Load<TStoreId> (context);
            auto store = tabletManager->CreateStore(this, storeId);
            YCHECK(Stores_.insert(std::make_pair(store->GetId(), store)).second);
            store->Load(context);
        }
    }

    auto activeStoreId = Load<TStoreId>(context);
    if (activeStoreId) {
        ActiveStore_ = GetStore(activeStoreId)->AsDynamicMemory();
    }

    auto loadPartition = [&] (int index) -> std::unique_ptr<TPartition> {
        auto partitionId = LoadSuspended<TPartitionId>(context);
        SERIALIZATION_DUMP_WRITE(context, "%v =>", partitionId);
        SERIALIZATION_DUMP_INDENT(context) {
            auto partition = std::make_unique<TPartition>(
                this,
                partitionId,
                index);
            Load(context, *partition);
            for (const auto& store : partition->Stores()) {
                store->SetPartition(partition.get());
            }
            return partition;
        }
    };

    SERIALIZATION_DUMP_WRITE(context, "partitions");
    SERIALIZATION_DUMP_INDENT(context) {
        Eden_ = loadPartition(TPartition::EdenIndex);

        int partitionCount = TSizeSerializer::LoadSuspended(context);
        for (int index = 0; index < partitionCount; ++index) {
            auto partition = loadPartition(index);
            YCHECK(PartitionMap_.insert(std::make_pair(partition->GetId(), partition.get())).second);
            PartitionList_.push_back(std::move(partition));
        }
    }
}

TCallback<void(TSaveContext&)> TTablet::AsyncSave()
{
    std::vector<std::pair<TStoreId, TCallback<void(TSaveContext&)>>> capturedStores;
    for (const auto& pair : Stores_) {
        const auto& store = pair.second;
        capturedStores.push_back(std::make_pair(store->GetId(), store->AsyncSave()));
    }

    auto capturedEden = Eden_->AsyncSave();

    std::vector<TCallback<void(TSaveContext&)>> capturedPartitions;
    for (const auto& partition : PartitionList_) {
        capturedPartitions.push_back(partition->AsyncSave());
    }

    return BIND(
        [
            snapshot = Snapshot_,
            capturedStores = std::move(capturedStores),
            capturedEden = std::move(capturedEden),
            capturedPartitions = std::move(capturedPartitions)
        ] (TSaveContext& context) {
            using NYT::Save;

            Save(context, *snapshot->Config);
            Save(context, *snapshot->WriterOptions);
            Save(context, snapshot->PivotKey);
            Save(context, snapshot->NextPivotKey);

            capturedEden.Run(context);
            for (const auto& callback : capturedPartitions) {
                callback.Run(context);
            }

            // NB: This is not stable.
            for (const auto& pair : capturedStores) {
                Save(context, pair.first);
                pair.second.Run(context);
            }
        });
}

void TTablet::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    Load(context, *Config_);
    Load(context, *WriterOptions_);
    Load(context, PivotKey_);
    Load(context, NextPivotKey_);

    auto loadPartition = [&] (const std::unique_ptr<TPartition>& partition) {
        SERIALIZATION_DUMP_WRITE(context, "%v =>", partition->GetId());
        SERIALIZATION_DUMP_INDENT(context) {
            partition->AsyncLoad(context);
        }
    };

    SERIALIZATION_DUMP_WRITE(context, "partitions");
    SERIALIZATION_DUMP_INDENT(context) {
        loadPartition(Eden_);
        for (const auto& partition : PartitionList_) {
            loadPartition(partition);
        }
    }

    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", Stores_.size());
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < Stores_.size(); ++index) {
            auto storeId = Load<TStoreId> (context);
            SERIALIZATION_DUMP_WRITE(context, "%v =>", storeId);
            SERIALIZATION_DUMP_INDENT(context) {
                auto store = GetStore(storeId);
                store->AsyncLoad(context);
            }
        }
    }
}

const std::vector<std::unique_ptr<TPartition>>& TTablet::Partitions() const
{
    return PartitionList_;
}

TPartition* TTablet::GetEden() const
{
    return Eden_.get();
}

void TTablet::CreateInitialPartition()
{
    YCHECK(PartitionList_.empty());
    auto partition = std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        static_cast<int>(PartitionList_.size()),
        PivotKey_,
        NextPivotKey_);
    YCHECK(PartitionMap_.insert(std::make_pair(partition->GetId(), partition.get())).second);
    PartitionList_.push_back(std::move(partition));
}

TPartition* TTablet::FindPartitionByPivotKey(const NTableClient::TOwningKey& pivotKey)
{
    auto it = std::lower_bound(
        PartitionList_.begin(),
        PartitionList_.end(),
        pivotKey,
        [] (const std::unique_ptr<TPartition>& partition, const TOwningKey& key) {
            return partition->GetPivotKey() < key;
        });
    return it != PartitionList_.end() && (*it)->GetPivotKey() == pivotKey ? it->get() : nullptr;
}

TPartition* TTablet::GetPartitionByPivotKey(const NTableClient::TOwningKey& pivotKey)
{
    auto* partition = FindPartitionByPivotKey(pivotKey);
    YCHECK(partition);
    return partition;
}

TPartition* TTablet::FindPartitionById(const TPartitionId& partitionId)
{
    const auto& it = PartitionMap_.find(partitionId);
    return it == PartitionMap_.end() ? nullptr : it->second;
}

TPartition* TTablet::GetPartitionById(const TPartitionId& partitionId)
{
    auto* partition = FindPartitionById(partitionId);
    YCHECK(partition);
    return partition;
}

void TTablet::MergePartitions(int firstIndex, int lastIndex)
{
    for (int i = lastIndex + 1; i < static_cast<int>(PartitionList_.size()); ++i) {
        PartitionList_[i]->SetIndex(i - (lastIndex - firstIndex));
    }

    auto mergedPartition = std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        firstIndex,
        PartitionList_[firstIndex]->GetPivotKey(),
        PartitionList_[lastIndex]->GetNextPivotKey());
    auto& mergedSampleKeys = mergedPartition->GetSampleKeys()->Keys;

    for (int index = firstIndex; index <= lastIndex; ++index) {
        const auto& existingPartition = PartitionList_[index];
        const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
        if (index > firstIndex) {
            mergedSampleKeys.push_back(existingPartition->GetPivotKey());
        }
        mergedSampleKeys.insert(
            mergedSampleKeys.end(),
            existingSampleKeys.begin(),
            existingSampleKeys.end());

        for (const auto& store : existingPartition->Stores()) {
            YCHECK(store->GetPartition() == existingPartition.get());
            store->SetPartition(mergedPartition.get());
            YCHECK(mergedPartition->Stores().insert(store).second);
        }
    }

    auto firstPartitionIt = PartitionList_.begin() + firstIndex;
    auto lastPartitionIt = PartitionList_.begin() + lastIndex;
    for (auto it = firstPartitionIt; it !=  lastPartitionIt; ++it) {
        PartitionMap_.erase((*it)->GetId());
    }
    YCHECK(PartitionMap_.insert(std::make_pair(mergedPartition->GetId(), mergedPartition.get())).second);
    PartitionList_.erase(firstPartitionIt, lastPartitionIt + 1);
    PartitionList_.insert(firstPartitionIt, std::move(mergedPartition));
}

void TTablet::SplitPartition(int index, const std::vector<TOwningKey>& pivotKeys)
{
    auto existingPartition = std::move(PartitionList_[index]);
    YCHECK(existingPartition->GetPivotKey() == pivotKeys[0]);

    for (int partitionIndex = index + 1; partitionIndex < PartitionList_.size(); ++partitionIndex) {
        PartitionList_[partitionIndex]->SetIndex(partitionIndex + pivotKeys.size() - 1);
    }

    std::vector<std::unique_ptr<TPartition>> splitPartitions;
    const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
    int sampleKeyIndex = 0;
    for (int pivotKeyIndex = 0; pivotKeyIndex < pivotKeys.size(); ++pivotKeyIndex) {
        auto thisPivotKey = pivotKeys[pivotKeyIndex];
        auto nextPivotKey = (pivotKeyIndex == pivotKeys.size() - 1)
            ? existingPartition->GetNextPivotKey()
            : pivotKeys[pivotKeyIndex + 1];
        auto partition = std::make_unique<TPartition>(
            this,
            GenerateId(EObjectType::TabletPartition),
            index + pivotKeyIndex,
            thisPivotKey,
            nextPivotKey);

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

    PartitionMap_.erase(existingPartition->GetId());
    for (const auto& partition : splitPartitions) {
        YCHECK(PartitionMap_.insert(std::make_pair(partition->GetId(), partition.get())).second);
    }
    PartitionList_.erase(PartitionList_.begin() + index);
    PartitionList_.insert(
        PartitionList_.begin() + index,
        std::make_move_iterator(splitPartitions.begin()),
        std::make_move_iterator(splitPartitions.end()));

    for (const auto& store : existingPartition->Stores()) {
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
        PartitionList_.begin(),
        PartitionList_.end(),
        minKey,
        [] (const TOwningKey& key, const std::unique_ptr<TPartition>& partition) {
            return key < partition->GetPivotKey();
        });

    if (it != PartitionList_.begin()) {
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

    for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
        EpochAutomatonInvokers_[queue] = CancelableContext_->CreateInvoker(
            // NB: Slot can be null in tests.
            slot
            ? slot->GetEpochAutomatonInvoker(queue)
            : GetSyncInvoker());
    }

    Eden_->StartEpoch();
    for (const auto& partition : PartitionList_) {
        partition->StartEpoch();
    }
}

void TTablet::StopEpoch()
{
    if (CancelableContext_) {
        CancelableContext_->Cancel();
        CancelableContext_.Reset();
    }

    std::fill(EpochAutomatonInvokers_.begin(), EpochAutomatonInvokers_.end(), GetNullInvoker());

    SetState(GetPersistentState());

    Eden_->StopEpoch();
    for (const auto& partition : PartitionList_) {
        partition->StopEpoch();
    }
}

IInvokerPtr TTablet::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return EpochAutomatonInvokers_[queue];
}

TTabletSnapshotPtr TTablet::RebuildSnapshot()
{
    Snapshot_ = New<TTabletSnapshot>();
    Snapshot_->Slot = Slot_;
    // NB: Slot_ could be null in tests.
    if (Slot_) {
        Snapshot_->CellId = Slot_->GetCellId();
        Snapshot_->HydraManager = Slot_->GetHydraManager();
        Snapshot_->TabletManager = Slot_->GetTabletManager();
    }
    Snapshot_->TabletId = Id_;
    Snapshot_->MountRevision = MountRevision_;
    Snapshot_->TableId = TableId_;
    Snapshot_->Config = Config_;
    Snapshot_->WriterOptions = WriterOptions_;
    Snapshot_->PivotKey = PivotKey_;
    Snapshot_->NextPivotKey = NextPivotKey_;
    Snapshot_->Schema = Schema_;
    Snapshot_->KeyColumns = KeyColumns_;
    Snapshot_->Eden = Eden_->RebuildSnapshot();
    Snapshot_->Atomicity = Atomicity_;
    Snapshot_->EnableLookupHashTable = EnableLookupHashTable_;
    Snapshot_->Partitions.reserve(PartitionList_.size());
    for (const auto& partition : PartitionList_) {
        auto partitionSnapshot = partition->RebuildSnapshot();
        Snapshot_->Partitions.push_back(partitionSnapshot);
        Snapshot_->StoreCount += partitionSnapshot->Stores.size();
        for (const auto& store : partitionSnapshot->Stores) {
            auto chunkStore = store->AsChunk();
            if (chunkStore) {
                auto preloadState = chunkStore->GetPreloadState();
                switch (preloadState) {
                    case EStorePreloadState::Scheduled:
                    case EStorePreloadState::Running:
                        ++Snapshot_->PreloadPendingStoreCount;
                        break;
                    case EStorePreloadState::Complete:
                        ++Snapshot_->PreloadCompletedStoreCount;
                        break;
                    case EStorePreloadState::Failed:
                        ++Snapshot_->PreloadFailedStoreCount;
                        break;
                    default:
                        break;
                }
            }
        }
    }
    Snapshot_->RowKeyComparer = RowKeyComparer_;
    Snapshot_->PerformanceCounters = PerformanceCounters_;
    Snapshot_->ColumnEvaluator = ColumnEvaluator_;
    return Snapshot_;
}

void TTablet::ResetSnapshot()
{
    Snapshot_.Reset();
}

void TTablet::Initialize()
{
    PerformanceCounters_ = New<TTabletPerformanceCounters>();

    RowKeyComparer_ = TDynamicRowKeyComparer::Create(
        GetKeyColumnCount(),
        Schema_);

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
        // No locking supported for non-atomic tablets, however we still need the primary
        // lock descriptor to maintain last commit timestamps.
        if (columnSchema.Lock && Atomicity_ == EAtomicity::Full) {
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

    ColumnEvaluator_ = ColumnEvaluatorCache_->Find(Schema_, KeyColumns_.size());
}

TPartition* TTablet::GetContainingPartition(IStorePtr store)
{
    // Dynamic stores must reside in Eden.
    if (store->GetStoreState() == EStoreState::ActiveDynamic ||
        store->GetStoreState() == EStoreState::PassiveDynamic)
    {
        return Eden_.get();
    }

    return GetContainingPartition(store->GetMinKey(), store->GetMaxKey());
}

const TDynamicRowKeyComparer& TTablet::GetRowKeyComparer() const
{
    return RowKeyComparer_;
}

void TTablet::ValidateMountRevision(i64 mountRevision)
{
    if (MountRevision_ != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            Id_,
            MountRevision_,
            mountRevision);
    }
}

TObjectId TTablet::GenerateId(EObjectType type)
{
    // NB: Slot can be null in tests.
    if (Slot_) {
        return Slot_->GenerateId(type);
    } else {
        return TObjectId::Create();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

