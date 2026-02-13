#include "reducer.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

namespace NYT::NSequoiaReconstructor {

using namespace NCypressClient;
using namespace NSequoiaClient;

constinit const auto Logger = SequoiaReconstructorLogger;

////////////////////////////////////////////////////////////////////////////////

void ExecutePathToNodeReduce(
    const std::vector<TPathToNodeChangeRecord>& records,
    TRecordsConsumer* consumer)
{
    auto sortedRecords = records;
    SortBy(
        sortedRecords,
        [] (const auto& record) {
            return std::make_tuple(record.Path, record.TransactionAncestors.size());
        });

    // Do we have any group by function for this?
    for (size_t recordIndex = 0; recordIndex < sortedRecords.size();) {
        THashMap<TTransactionId, const TPathToNodeChangeRecord*> transactionRecords;
        const auto& path = sortedRecords[recordIndex].Path;

        // We view iterate over all records for the same path.
        // For them we need to do three actions:
        // 1. For every transaction keep only single record (multiple records may happen if node was replaced in transaction).
        // 2. Remove all tombstone records that have no node creation in ancestor transactions.
        // 3. Set ProgenitorTransactionId to the oldest transaction that has node created.
        for (; recordIndex < sortedRecords.size() && sortedRecords[recordIndex].Path == path; ++recordIndex) {
            const auto& record = sortedRecords[recordIndex];

            if (auto existingRecordIt = transactionRecords.find(record.TransactionId); existingRecordIt != transactionRecords.end()) {
                // If we have multiple records for single transaction, we should keep the non tombstone one if it exists.
                if (record.NodeId != NullObjectId) {
                    // If we have non tombstone record for transaction, all other records should be tombstone.
                    YT_LOG_FATAL_IF(
                        existingRecordIt->second->NodeId != NullObjectId,
                        "Multiple node creation records for the same path with the same transaction (TransactionId: %v, Path: %v",
                        record.TransactionId,
                        path);

                    existingRecordIt->second = &record;
                }
            } else {
                if (record.NodeId == NullObjectId) {
                    // For tombstone records we should check that at least one record exists for ancestor transactions.
                    // All ancestor records should be already processed because records are sorted by transaction depth.

                    auto ancestorTransactionRecordFound = false;
                    for (const auto& ancestorTransactionId : record.TransactionAncestors) {
                        if (transactionRecords.contains(ancestorTransactionId)) {
                            ancestorTransactionRecordFound = true;
                            break;
                        }
                    }
                    if (!ancestorTransactionRecordFound) {
                        continue;
                    }
                }
                transactionRecords[record.TransactionId] = &record;
            }
        }

        for (const auto& [transactionId, record] : transactionRecords) {
            if (transactionId == NullTransactionId) {
                // We have already processed trunk nodes and written PathToNodeId rows for them during map stage.
                // We do not need PathForks for trunk nodes.
                continue;
            }
            if (consumer->PathToNodeId) {
                consumer->PathToNodeId->Consume(NRecords::TPathToNodeId{
                    .Key = {.Path = path, .TransactionId = transactionId},
                    .NodeId = record->NodeId,
                });
            }
            if (consumer->PathForks) {
                auto progenitorTransactionId = transactionId;
                // We iterate over transaction ancestors in order from topmost to current transaction searching for any record.
                for (const auto& ancestorTransactionId : record->TransactionAncestors | std::views::reverse) {
                    if (auto it = transactionRecords.find(ancestorTransactionId); it != transactionRecords.end()) {
                        YT_VERIFY(it->second->NodeId != NullObjectId);
                        progenitorTransactionId = ancestorTransactionId;
                        break;
                    }
                }
                consumer->PathForks->Consume(NRecords::TPathFork{
                    .Key = {.TransactionId = transactionId, .Path = path},
                    .NodeId = record->NodeId,
                    .ProgenitorTransactionId = progenitorTransactionId,
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
