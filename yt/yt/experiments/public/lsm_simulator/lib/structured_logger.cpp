#include "structured_logger.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

void TStructuredLogger::LogEvent(const NJson::TJsonValue& value)
{
    Events_.push_back(value);
}

void TStructuredLogger::OnTabletFullHeartbeat(TTablet* tablet)
{
    TJsonMap map;
    InsertValue(map, "entry_type", "full_heartbeat");
    InsertValue(map, "tablet_id", tablet->GetId());
    InsertValue(map, "eden", OnPartitionFullHeartbeat(tablet->Eden().get()));

    TJsonArray array;
    for (auto& partition : tablet->Partitions()) {
        array.AppendValue(OnPartitionFullHeartbeat(partition.get()));
    }
    InsertValue(map, "partitions", std::move(array));

    Events_.push_back(std::move(map));
}

TJsonValue TStructuredLogger::OnPartitionFullHeartbeat(TPartition* partition)
{
    TJsonValue map(NJson::EJsonValueType::JSON_MAP);
    InsertValue(map, "id", partition->GetId());
    InsertValue(map, "pivot_key", partition->PivotKey());
    InsertValue(map, "next_pivot_key", partition->NextPivotKey());

    TJsonMap stores;
    // todo: dynamic store
    for (auto& store : partition->Stores()) {
        stores.InsertValue(ToString(store->GetId()), OnStoreFullHeartbeat(store.get()));
    }

    InsertValue(map, "stores", std::move(stores));

    return map;
}

TJsonValue TStructuredLogger::OnStoreFullHeartbeat(TStore* store)
{
    TJsonValue map(NJson::EJsonValueType::JSON_MAP);

    InsertValue(map, "store_state", store->GetStoreState());
    InsertValue(map, "min_timestamp", store->GetMinTimestamp());
    InsertValue(map, "max_timestamp", store->GetMaxTimestamp());

    // Assuming sorted chunk.
    InsertValue(map, "min_key", store->MinKey());
    InsertValue(map, "upper_bound_key", store->UpperBoundKey());

    InsertValue(map, "preload_state", store->GetPreloadState());
    InsertValue(map, "compaction_state", store->GetCompactionState());
    InsertValue(map, "compressed_data_size", store->GetCompressedDataSize());
    InsertValue(map, "uncompressed_data_size", store->GetUncompressedDataSize());
    InsertValue(map, "row_count", store->GetRowCount());
    InsertValue(map, "partition_id", store->GetPartition()->GetId());

    return map;
}

void TStructuredLogger::OnTabletStoresUpdateCommitted(
    const std::vector<TStore*> addedStores,
    const std::vector<TStoreId> removedStoreIds,
    TStringBuf updateReason,
    std::optional<EStoreCompactionReason> compactionReason)
{
    auto map = GetEventMap("commit_update_tablet_stores");

    InsertValue(map, "update_reason", updateReason);
    InsertValue(map, "compaction_reason", compactionReason);

    TJsonMap added;
    for (auto* store : addedStores) {
        InsertValue(added, ToString(store->GetId()), OnStoreFullHeartbeat(store));
    }
    InsertValue(map, "added_stores", added);

    TJsonArray removed;
    for (auto id : removedStoreIds) {
        removed.AppendValue(ToString(id));
    }
    InsertValue(map, "removed_store_ids", removed);

    Events_.push_back(map);
}

void TStructuredLogger::OnPartitionStateChanged(TPartition* partition)
{
    auto map = GetEventMap("set_partition_state");

    InsertValue(map, "partition_id", partition->GetId());
    InsertValue(map, "state", partition->GetState());

    Events_.push_back(map);
}

void TStructuredLogger::OnStoreStateChanged(TStore* store)
{
    auto map = GetEventMap("set_store_state");

    InsertValue(map, "store_id", store->GetId());
    InsertValue(map, "state", store->GetStoreState());

    Events_.push_back(map);
}

void TStructuredLogger::OnStoreCompactionStateChanged(TStore* store)
{
    auto map = GetEventMap("set_store_compaction_state");

    InsertValue(map, "store_id", store->GetId());
    InsertValue(map, "compaction_state", store->GetCompactionState());

    Events_.push_back(map);
}

void TStructuredLogger::OnStorePreloadStateChanged(TStore* store)
{
    auto map = GetEventMap("set_store_preload_state");

    InsertValue(map, "store_id", store->GetId());
    InsertValue(map, "preload_state", store->GetPreloadState());

    Events_.push_back(map);
}

void TStructuredLogger::OnStoreFlushStateChanged(TStore* store)
{
    auto map = GetEventMap("set_store_flush_state");

    InsertValue(map, "store_id", store->GetId());
    InsertValue(map, "flush_state", store->GetFlushState());

    Events_.push_back(map);
}

void TStructuredLogger::OnPartitionSplit(
    TTablet* tablet,
    TPartitionId oldPartitionId,
    int firstPartitionIndex,
    int splitFactor)
{
    auto map = GetEventMap("split_partition");

    InsertValue(map, "old_partition_id", oldPartitionId);

    TJsonArray newPartitions;
    for (int index = 0; index < splitFactor; ++index) {
        const auto& partition = tablet->Partitions()[firstPartitionIndex + index];
        TJsonMap partitionMap;
        InsertValue(partitionMap, "partition_id", partition->GetId());
        InsertValue(partitionMap, "pivot_key", partition->PivotKey());
        newPartitions.AppendValue(partitionMap);
    }
    InsertValue(map, "new_partitions", newPartitions);

    Events_.push_back(map);
}

void TStructuredLogger::OnPartitionsMerged(
    const std::vector<TPartitionId>& oldPartitionIds,
    const TPartition* newPartition)
{
    auto map = GetEventMap("merge_partitions");

    InsertValue(map, "new_partition_id", newPartition->GetId());

    TJsonArray oldPartitions;
    for (auto id : oldPartitionIds) {
        oldPartitions.AppendValue(ToString(id));
    }
    InsertValue(map, "old_partition_ids", oldPartitions);

    Events_.push_back(map);
}

void TStructuredLogger::OnTabletUnlocked(const std::vector<std::unique_ptr<TStore>>& stores)
{
    auto map = GetEventMap("bulk_add_stores");

    InsertValue(map, "overwrite", false);

    TJsonMap added;
    for (const auto& store : stores) {
        InsertValue(added, ToString(store->GetId()), OnStoreFullHeartbeat(store.get()));
    }
    InsertValue(map, "added_stores", added);

    Events_.push_back(map);
}

std::vector<TJsonValue> TStructuredLogger::Consume()
{
    return std::move(Events_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
