#include "store_manager.h"

#include "compaction.h"
#include "helpers.h"
#include "structured_logger.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NLsm::NTesting {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletPtr tablet,
    TLsmSimulatorConfigPtr config,
    TTableMountConfigPtr mountConfig,
    IActionQueuePtr actionQueue,
    TStructuredLoggerPtr structuredLogger)
    : Tablet_(std::move(tablet))
    , Config_(std::move(config))
    , MountConfig_(std::move(mountConfig))
    , ActionQueue_(std::move(actionQueue))
    , CompactionThrottler_(ActionQueue_, Config_->CompactionThrottler)
    , PartitioningThrottler_(ActionQueue_, Config_->PartitioningThrottler)
    , StructuredLogger_(std::move(structuredLogger))
{
    ResetLastPeriodicRotationTime();
}

void TStoreManager::StartEpoch()
{
    InitializeRotation();
}

void TStoreManager::InitializeRotation()
{
    ResetLastPeriodicRotationTime();
}

void TStoreManager::DeleteRow(TRow row)
{
    YT_VERIFY(ssize(row.DeleteTimestamps) == 1);
    YT_VERIFY(row.Values.empty());
    DynamicStore_.AddRow(row);
}

void TStoreManager::InsertRow(TRow row)
{
    YT_VERIFY(ssize(row.Values) == 1);
    YT_VERIFY(row.DeleteTimestamps.empty());
    DynamicStore_.AddRow(row);
    Statistics_.BytesWritten += row.DataSize;
}

void TStoreManager::BulkAddStores(std::vector<std::unique_ptr<TStore>> chunks)
{
    if (StructuredLogger_) {
        StructuredLogger_->OnTabletUnlocked(chunks);
    }

    for (auto&& chunk : chunks) {
        chunk->SetCreationTime(ActionQueue_->GetNow());
        Statistics_.BytesWritten += chunk->GetUncompressedDataSize();
        auto partition = FindPartition(chunk.get());
        chunk->SetPartition(partition);
        partition->AddStore(std::move(chunk));
    }
}

TTablet* TStoreManager::GetTablet() const
{
    return Tablet_.Get();
}

NLsm::TTabletPtr TStoreManager::GetLsmTablet() const
{
    auto lsmTablet = GetTablet()->ToLsmTablet();
    AddDynamicStores(lsmTablet.Get());

    auto isRotationPossible = DynamicStore_.GetRowCount() > 0;

    lsmTablet->SetIsForcedRotationPossible(isRotationPossible);
    lsmTablet->SetIsOverflowRotationNeeded(
        isRotationPossible && IsOverflowRotationNeeded());
    lsmTablet->SetLastPeriodicRotationTime(LastPeriodicRotationTime_);
    return lsmTablet;
}

i64 TStoreManager::GetCompressedDataSize() const
{
    i64 value = 0;
    for (const auto& partition : Tablet_->Partitions()) {
        value += partition->GetCompressedDataSize();
    }
    value += Tablet_->Eden()->GetCompressedDataSize();
    return value;
}

TStatistics TStoreManager::GetStatistics() const
{
    return Statistics_;
}

i64 TStoreManager::GetDynamicMemoryUsage() const
{
    return DynamicStore_.GetDataSize();
}

void TStoreManager::AddDynamicStores(NLsm::TTablet* lsmTablet) const
{
    auto lsmStore = std::make_unique<NLsm::TStore>();
    lsmStore->SetTablet(lsmTablet);
    lsmStore->SetId(TGuid{});
    lsmStore->SetType(EStoreType::SortedDynamic);
    lsmStore->SetStoreState(EStoreState::ActiveDynamic);
    lsmStore->SetCompressedDataSize(DynamicStore_.GetDataSize());
    lsmStore->SetUncompressedDataSize(DynamicStore_.GetDataSize());
    lsmStore->SetRowCount(DynamicStore_.GetRowCount());
    // TODO
    // lsmStore->SetMinTimestamp(store->GetMinTimestamp());
    // lsmStore->SetMaxTimestamp(store->GetMaxTimestamp());

    lsmStore->SetFlushState(EStoreFlushState::None);
    // TODO
    // lsmStore->SetLastFlushAttemptTimestamp(
        // dynamicStore->GetLastFlushAttemptTimestamp());
    lsmStore->SetDynamicMemoryUsage(DynamicStore_.GetDataSize());

    lsmTablet->Eden()->Stores().push_back(std::move(lsmStore));
}

auto TStoreManager::GetOrderingTuple(const TCompactionRequest& request)
{
    return std::tuple(
        !request.DiscardStores,
        request.Slack,
        -request.Effect,
        -request.Stores.size());
}

void TStoreManager::ApplyLsmActionBatch(TLsmActionBatch actions)
{
    for (const auto& action : actions.Splits) {
        StartSplitPartition(action);
    }

    for (const auto& action : actions.Merges) {
        MergePartitions(action);
    }

    if (!actions.Compactions.empty() || !actions.Partitionings.empty()) {
        Cerr << "...\n";
    }

    std::sort(
        actions.Compactions.begin(),
        actions.Compactions.end(),
        [&] (const auto& lhs, const auto& rhs) {
            return GetOrderingTuple(lhs) < GetOrderingTuple(rhs);
        });
    if (Config_->ReverseCompactionComparator) {
        std::reverse(
            actions.Compactions.begin(),
            actions.Compactions.end());
    }
    for (const auto& action : actions.Compactions) {
        StartCompaction(action);
    }

    std::sort(
        actions.Partitionings.begin(),
        actions.Partitionings.end(),
        [&] (const auto& lhs, const auto& rhs) {
            return GetOrderingTuple(lhs) < GetOrderingTuple(rhs);
        });
    if (Config_->ReverseCompactionComparator) {
        std::reverse(
            actions.Partitionings.begin(),
            actions.Partitionings.end());
    }
    for (const auto& action : actions.Partitionings) {
        StartPartitioning(action);
    }

    if (!actions.Compactions.empty() || !actions.Partitionings.empty()) {
        PrintDebug();
    }

    for (const auto& action : actions.Rotations) {
        RotateStore(action);
    }
}

void TStoreManager::PrintDebug()
{
    /*
    Cerr << "Eden\n";
    for (const auto& store : Tablet_->Eden()->Stores()) {
        Cerr << *store << "\n";
    }
    for (const auto& partition : Tablet_->Partitions()) {
        Cerr << "Partition " << partition->GetIndex() << "\n";
        Cerr << ToString(partition->PivotKey()) << " .. " <<
            ToString(partition->NextPivotKey()) << ", " <<
            partition->GetCompressedDataSize() / (1<<20) << " MB\n";
        for (const auto& store : partition->Stores()) {
            Cerr << *store << "\n";
        }
        Cerr << "\n";
    }
    Cerr << "\n";
    */

    Cerr << (Statistics_.BytesWritten >> 20) << " MB added, " <<
        (Statistics_.BytesCompacted >> 20) << " MB compacted, " <<
        "ratio = " << 1.0 * Statistics_.BytesCompacted / Statistics_.BytesWritten << "\n";
    Cerr << "Partition count: " << Tablet_->Partitions().size() << "\n";
    Cerr << "OSC: " << Tablet_->GetOverlappingStoreCount() << ", ESC: " <<
        Tablet_->Eden()->Stores().size() << "\n";
    Cerr << "Partitions: ";
    for (const auto& partition : Tablet_->Partitions()) {
        Cerr << partition->Stores().size() << " ";
    }
    Cerr << "\n";
    Cerr << "Partitions' data size: ";
    for (const auto& partition : Tablet_->Partitions()) {
        i64 totalSize = 0;
        for (const auto& store : partition->Stores()) {
            totalSize += store->GetCompressedDataSize();
        }
        Cerr << totalSize << " ";
    }
    Cerr << "\n";
    Cerr << "Partitions' row count: ";
    for (const auto& partition : Tablet_->Partitions()) {
        i64 totalSize = 0;
        for (const auto& store : partition->Stores()) {
            totalSize += store->GetRowCount();
        }
        Cerr << totalSize << " ";
    }
    Cerr << "\n";
    Cerr << "dynamic store data size: " << DynamicStore_.GetDataSize() << "\n";

    Cerr << "\n";
}

void TStoreManager::EmitOutOfBandStructuredHeartbeat() const
{
    if (StructuredLogger_) {
        StructuredLogger_->OnTabletFullHeartbeat(Tablet_.Get());
    }
}

TPartition* TStoreManager::FindPartition(TStore* store) const
{
    for (const auto& partition : Tablet_->Partitions()) {
        if (store->MinKey() >= partition->PivotKey() &&
            store->UpperBoundKey() <= partition->NextPivotKey()) {
            return partition.get();
        }
    }
    return Tablet_->TTablet::Eden().get();
}

void TStoreManager::StartCompaction(TCompactionRequest request)
{
    auto Logger = NTesting::Logger.WithTag("PartitionId: %v, Reason: %v", request.PartitionId, request.Reason);
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    if (!partition) {
        YT_LOG_INFO("Cannot compact partition since partition is missing");
        return;
    }
    if (partition->GetState() != EPartitionState::Normal) {
        YT_LOG_INFO("Cannot compact partition since state is bad (State: %v)",
            partition->GetState());
        return;
    }
    std::vector<TStore*> stores;
    for (auto storeId : request.Stores) {
        auto* store = partition->FindStore(storeId);
        if (!store) {
            YT_LOG_INFO("Cannot compact since store is missing (StoreId: %v)",
                storeId);
            return;
        }
        if (!store->GetIsCompactable()) {
            YT_LOG_INFO("Cannot compact since store is not compactable (StoreId: %v)",
                storeId);
            return;
        }
        if (store->GetCompactionState() != EStoreCompactionState::None) {
            YT_LOG_INFO("Cannot compact since store has wrong compaction state (StoreId: %v, State: %v)",
                storeId,
                store->GetCompactionState());
            return;
        }
        stores.push_back(store);
    }

    if (CompactionThrottler_.IsOverdraft()) {
        Cerr << "Compaction throttling\n";
        return;
    }

    partition->SetState(EPartitionState::Compacting);
    for (auto* store : stores) {
        store->SetCompactionState(EStoreCompactionState::Running);
        if (StructuredLogger_) {
            StructuredLogger_->OnStoreCompactionStateChanged(store);
        }
        CompactionThrottler_.Acquire(store->GetCompressedDataSize());
    }

    YT_LOG_INFO("Partition compaction started");

    // Slightly imprecise: should take timestamp from LSM action batch.
    auto retentionTimestamp = InstantToTimestamp(ActionQueue_->GetNow() - MountConfig_->MaxDataTtl).first;
    auto majorTimestamp = ComputeMajorTimestamp(partition, stores);

    auto newStores = Compact(
        stores,
        partition,
        {0},
        retentionTimestamp,
        majorTimestamp,
        MountConfig_->MinDataVersions,
        Config_->CompressionRatio);

    ActionQueue_->Schedule(
        BIND(
            &TStoreManager::FinishCompaction,
            Unretained(this),
            request,
            Passed(std::move(newStores))),
        Config_->CompactionDelay);
}

void TStoreManager::FinishCompaction(
    TCompactionRequest request,
    std::vector<std::unique_ptr<TStore>> newStores)
{
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    if (!partition) {
        YT_LOG_INFO("Cannot finish compaction since partition is missing");
        return;
    }
    YT_VERIFY(partition->GetState() == EPartitionState::Compacting);

    for (auto storeId : request.Stores) {
        auto* store = partition->FindStore(storeId);
        YT_VERIFY(store);
        YT_VERIFY(store->GetCompactionState() == EStoreCompactionState::Running);
        store->SetCompactionState(EStoreCompactionState::Complete);
        store->SetStoreState(EStoreState::Removed);
        Statistics_.BytesCompacted += store->GetUncompressedDataSize();
        if (StructuredLogger_) {
            StructuredLogger_->OnStoreCompactionStateChanged(store);
            StructuredLogger_->OnStoreStateChanged(store);
        }
        partition->RemoveStore(storeId);
    }

    std::vector<TStore*> addedStores;

    for (auto&& newStore : newStores) {
        if (!newStore) continue;
        newStore->SetCreationTime(ActionQueue_->GetNow());
        auto* newPartition = FindPartition(newStore.get());
        if (partition != Tablet_->Eden().get() && partition != newPartition) {
            // This should not normally happen.
            Cerr << "Store went to a different partition after compaction\n";
        }
        newStore->SetPartition(newPartition);
        addedStores.push_back(newStore.get());
        newPartition->AddStore(std::move(newStore));
    }

    if (StructuredLogger_) {
        StructuredLogger_->OnTabletStoresUpdateCommitted(
            addedStores,
            request.Stores,
            "compaction",
            request.Reason);
    }

    partition->SetState(EPartitionState::Normal);

    UpdateOverlappingStoreCount();

    YT_LOG_INFO("Partition compaction completed");
}

void TStoreManager::StartPartitioning(TCompactionRequest request)
{
    auto Logger = NTesting::Logger.WithTag("PartitionId: %v, Reason: %v", request.PartitionId, request.Reason);
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    YT_VERIFY(partition->IsEden());
    if (!partition) {
        YT_LOG_INFO("Cannot partition Eden since partition is missing");
        return;
    }
    if (partition->GetState() != EPartitionState::Normal) {
        YT_LOG_INFO("Cannot compact partition since state is bad (State: %v)",
            partition->GetState());
        return;
    }
    std::vector<TStore*> stores;
    for (auto storeId : request.Stores) {
        auto* store = partition->FindStore(storeId);
        if (!store) {
            YT_LOG_INFO("Cannot compact since store is missing (StoreId: %v)",
                storeId);
            return;
        }
        if (!store->GetIsCompactable()) {
            YT_LOG_INFO("Cannot compact since store is not compactable (StoreId: %v)",
                storeId);
            return;
        }
        if (store->GetCompactionState() != EStoreCompactionState::None) {
            YT_LOG_INFO("Cannot compact since store has wrong compaction state (StoreId: %v, State: %v)",
                storeId,
                store->GetCompactionState());
            return;
        }
        stores.push_back(store);
    }

    if (PartitioningThrottler_.IsOverdraft()) {
        return;
    }

    partition->SetState(EPartitionState::Partitioning);
    for (auto* store : stores) {
        store->SetCompactionState(EStoreCompactionState::Running);
        if (StructuredLogger_) {
            StructuredLogger_->OnStoreCompactionStateChanged(store);
        }
        PartitioningThrottler_.Acquire(store->GetCompressedDataSize());
    }

    YT_LOG_INFO("Eden partitioning started");

    std::vector<TKey> pivots;
    for (const auto& partition : Tablet_->Partitions()) {
        const auto& key = partition->PivotKey();
        if (key.GetCount() == 0) {
            pivots.push_back(-1);
        } else {
            pivots.push_back(key[0].Data.Int64);
        }
    }

    auto newStores = Compact(
        stores,
        partition,
        pivots,
        /*retentionTimestamp*/ NullTimestamp,
        /*majorTimestamp*/ NullTimestamp,
        /*minDataVersions*/ 1,
        Config_->CompressionRatio);

    ActionQueue_->Schedule(
        BIND(
            &TStoreManager::FinishPartitioning,
            Unretained(this),
            request,
            Passed(std::move(newStores))),
        Config_->PartitioningDelay);
}

void TStoreManager::FinishPartitioning(
    TCompactionRequest request,
    std::vector<std::unique_ptr<TStore>> newStores)
{
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    if (!partition) {
        YT_LOG_INFO("Cannot finish partitioning since partition is missing");
        return;
    }
    YT_VERIFY(partition->GetState() == EPartitionState::Partitioning);

    std::vector<TStore*> stores;
    for (auto storeId : request.Stores) {
        auto* store = partition->FindStore(storeId);
        YT_VERIFY(store);
        YT_VERIFY(store->GetCompactionState() == EStoreCompactionState::Running);
        store->SetCompactionState(EStoreCompactionState::Complete);
        store->SetStoreState(EStoreState::Removed);
        Statistics_.BytesPartitioned += store->GetUncompressedDataSize();
        if (StructuredLogger_) {
            StructuredLogger_->OnStoreCompactionStateChanged(store);
            StructuredLogger_->OnStoreStateChanged(store);
        }
        partition->RemoveStore(storeId);
    }

    std::vector<TStore*> addedStores;

    for (auto&& newStore : newStores) {
        if (newStore) {
            newStore->SetCreationTime(ActionQueue_->GetNow());
            auto* newPartition = FindPartition(newStore.get());
            newStore->SetPartition(newPartition);
            addedStores.push_back(newStore.get());
            newPartition->AddStore(std::move(newStore));
        }
    }

    if (StructuredLogger_) {
        StructuredLogger_->OnTabletStoresUpdateCommitted(
            addedStores,
            request.Stores,
            "partitioning");
    }

    partition->SetState(EPartitionState::Normal);

    UpdateOverlappingStoreCount();

    YT_LOG_INFO("Eden partitioning completed");
}

void TStoreManager::DoGetSamples(
    TPartition* partition,
    int count,
    TCallback<void(std::vector<TNativeKey>)> onSamplesReceived)
{
    std::vector<TKey> keys;
    for (const auto& store : partition->Stores()) {
        for (const auto& row : store->Rows()) {
            keys.push_back(row.Key);
        }
    }

    std::sort(keys.begin(), keys.end());
    count = std::min<int>(count, ssize(keys));

    std::vector<TNativeKey> samples;
    for (int i = 0; i < count; ++i) {
        samples.push_back(BuildNativeKey(keys[i * ssize(keys) / count]));
    }

    ActionQueue_->Schedule(
        BIND(onSamplesReceived, Passed(std::move(samples))),
        Config_->GetSamplesDelay);
}

void TStoreManager::RotateStore(TRotateStoreRequest request)
{
    YT_LOG_INFO("Rotating store (Reason: %v)", request.Reason);

    if (request.Reason == EStoreRotationReason::Periodic) {
        if (GetLastPeriodicRotationTime() != request.ExpectedLastPeriodicRotationTime) {
            return;
        }
        SetLastPeriodicRotationTime(request.NewLastPeriodicRotationTime);

        if (DynamicStore_.GetRowCount() == 0) {
            return;
        }
    }

    YT_VERIFY(DynamicStore_.GetRowCount() > 0);

    if (request.Reason != EStoreRotationReason::Periodic) {
        ResetLastPeriodicRotationTime();
    }

    OnStoreFlushed(DynamicStore_.Flush(Config_->CompressionRatio));
    DynamicStore_ = {};
}

void TStoreManager::StartSplitPartition(TSplitPartitionRequest request)
{
    auto Logger = NTesting::Logger.WithTag("PartitionId: %v", request.PartitionId);
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    if (!partition) {
        YT_LOG_INFO("Cannot split partition since partition is missing");
        return;
    }
    if (partition->GetIndex() != request.PartitionIndex) {
        YT_LOG_INFO("Cannot split partition due to index mismatch");
        return;
    }
    if (partition->GetState() != EPartitionState::Normal) {
        YT_LOG_INFO("Cannot split partition since partition is in invalid state (State: %v)",
            partition->GetState());
        return;
    }

    YT_LOG_INFO("Splitting partition");

    partition->SetState(EPartitionState::Splitting);
    DoGetSamples(
        partition,
        request.SplitFactor,
        BIND([=, this] (std::vector<TNativeKey> keys) {
            if (keys.empty()) {
                keys.push_back(partition->PivotKey());
            } else {
                keys[0] = partition->PivotKey();
            }
            FinishSplitPartition(request, std::move(keys));
        }));
}

void TStoreManager::FinishSplitPartition(
    TSplitPartitionRequest request,
    std::vector<TNativeKey> pivots)
{
    auto Logger = NTesting::Logger.WithTag("PartitionId: %v", request.PartitionId);
    auto* partition = Tablet_->FindPartition(request.PartitionId);
    if (!partition) {
        YT_LOG_INFO("Cannot split partition since partition is missing");
        return;
    }
    YT_VERIFY(partition->GetState() == EPartitionState::Splitting);

    YT_LOG_INFO("Splitting partition (Index: %v, PivotKeys: %v .. %v, NewPivots: %v)",
        partition->GetIndex(),
        partition->PivotKey(),
        partition->NextPivotKey(),
        MakeFormattableView(pivots, TDefaultFormatter{}));

    auto oldStores = std::move(partition->Stores());
    auto& partitions = Tablet_->Partitions();
    int index = partition->GetIndex();
    int firstPartitionIndex = index;
    partition->SetState(EPartitionState::Normal);
    // Avoid use-after-free.
    partition = nullptr;

    partitions.erase(partitions.begin() + index);
    for (int pivotIndex = 0; pivotIndex < ssize(pivots); ++pivotIndex) {
        auto newPartition = std::make_unique<TPartition>(Tablet_.Get());
        newPartition->PivotKey() = pivots[pivotIndex];
        newPartition->NextPivotKey() = pivotIndex + 1 < ssize(pivots)
            ? pivots[pivotIndex + 1]
            : index < ssize(partitions)
                ? partitions[index]->PivotKey()
                : NTableClient::MaxKey();
        partitions.insert(partitions.begin() + index++, std::move(newPartition));
    }

    for (auto&& store : oldStores) {
        auto* partition = FindPartition(store.get());
        store->SetPartition(partition);
        partition->AddStore(std::move(store));
    }

    for (int newIndex = 0; newIndex < ssize(partitions); ++newIndex) {
        partitions[newIndex]->SetIndex(newIndex);
    }

    UpdateOverlappingStoreCount();

    if (StructuredLogger_) {
        StructuredLogger_->OnPartitionSplit(
            Tablet_.Get(),
            request.PartitionId,
            firstPartitionIndex,
            ssize(pivots));
    }

    YT_LOG_INFO("Partition split");
}

void TStoreManager::MergePartitions(TMergePartitionsRequest request)
{
    auto Logger = NTesting::Logger.WithTag("PartitionIds: %v", request.PartitionIds);

    // Validation.
    {
        int partitionIndex = request.FirstPartitionIndex;
        for (auto partitionId : request.PartitionIds) {
            auto* partition = Tablet_->FindPartition(partitionId);
            if (!partition) {
                YT_LOG_INFO("Cannot merge partitions since one of partitions is missing");
                return;
            }

            if (partition->GetIndex() != partitionIndex) {
                YT_LOG_INFO("Cannot merge partitions due to index mismatch");
                return;
            }

            if (partition->GetState() != EPartitionState::Normal) {
                YT_LOG_INFO("Cannot merge partitions since partition is in invalid state (PartitionId: %v, State: %v)",
                    partition->GetId(), partition->GetState());
                return;
            }

            ++partitionIndex;
        }
    }

    YT_LOG_INFO("Merging partitions");

    std::vector<std::unique_ptr<TStore>> oldStores;
    for (auto partitionId : request.PartitionIds) {
        auto* partition = Tablet_->FindPartition(partitionId);
        partition->SetState(EPartitionState::Merging);
    }

    for (auto partitionId : request.PartitionIds) {
        auto* partition = Tablet_->FindPartition(partitionId);
        oldStores.insert(
            oldStores.end(),
            std::make_move_iterator(partition->Stores().begin()),
            std::make_move_iterator(partition->Stores().end()));
        partition->SetState(EPartitionState::Normal);
    }

    auto& partitions = Tablet_->Partitions();
    int firstPartitionIndex = request.FirstPartitionIndex;
    int lastPartitionIndex = firstPartitionIndex + ssize(request.PartitionIds) - 1;
    auto pivotKey = partitions[firstPartitionIndex]->PivotKey();
    auto nextPivotKey = partitions[lastPartitionIndex]->NextPivotKey();

    partitions.erase(
        partitions.begin() + firstPartitionIndex,
        partitions.begin() + lastPartitionIndex + 1);

    auto newPartitionHolder = std::make_unique<TPartition>(Tablet_.Get());
    auto* newPartition = newPartitionHolder.get();
    newPartition->PivotKey() = std::move(pivotKey);
    newPartition->NextPivotKey() = std::move(nextPivotKey);
    partitions.insert(partitions.begin() + firstPartitionIndex, std::move(newPartitionHolder));

    for (auto&& store : oldStores) {
        auto* partition = FindPartition(store.get());
        store->SetPartition(partition);
        partition->AddStore(std::move(store));
    }

    for (int newIndex = 0; newIndex < ssize(partitions); ++newIndex) {
        partitions[newIndex]->SetIndex(newIndex);
    }

    UpdateOverlappingStoreCount();

    if (StructuredLogger_) {
        StructuredLogger_->OnPartitionsMerged(
            request.PartitionIds,
            newPartition);
    }

    YT_LOG_INFO("Partitions merged");
}

void TStoreManager::OnStoreFlushed(std::unique_ptr<TStore> store)
{
    store->SetCreationTime(ActionQueue_->GetNow());
    Statistics_.BytesFlushed += store->GetUncompressedDataSize();
    ++Statistics_.ChunksFlushed;

    auto* partition = MountConfig_->AlwaysFlushToEden
        ? Tablet_->Eden().get()
        : FindPartition(store.get());

    store->SetPartition(partition);

    YT_LOG_INFO("Added store to tablet (StoreId: %v, PartitionId: %v, Eden: %v, "
        "MinKey: %v, UpperBoundKey: %v, Size: %v)",
        store->GetId(),
        partition->GetId(),
        partition->IsEden(),
        store->MinKey(),
        store->UpperBoundKey(),
        store->GetUncompressedDataSize());

    if (StructuredLogger_) {
        StructuredLogger_->OnTabletStoresUpdateCommitted(
            {store.get()},
            {},
            "flush");
    }

    partition->AddStore(std::move(store));

    UpdateOverlappingStoreCount();
}

void TStoreManager::ResetLastPeriodicRotationTime()
{
    if (MountConfig_->DynamicStoreAutoFlushPeriod) {
        SetLastPeriodicRotationTime(ActionQueue_->GetNow() - RandomDuration(*MountConfig_->DynamicStoreAutoFlushPeriod));
    } else {
        SetLastPeriodicRotationTime(std::nullopt);
    }
}

void TStoreManager::UpdateOverlappingStoreCount()
{
    int osc = 0;
    for (const auto& partition : Tablet_->Partitions()) {
        osc = std::max<int>(osc, partition->Stores().size());
    }
    Tablet_->SetOverlappingStoreCount(osc + Tablet_->Eden()->Stores().size());
}

bool TStoreManager::IsOverflowRotationNeeded() const
{
    auto threshold = MountConfig_->DynamicStoreOverflowThreshold;

    return
        DynamicStore_.GetRowCount() >=
            threshold * MountConfig_->MaxDynamicStoreRowCount ||
        DynamicStore_.GetDataSize() >=
            threshold * MountConfig_->MaxDynamicStorePoolSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
