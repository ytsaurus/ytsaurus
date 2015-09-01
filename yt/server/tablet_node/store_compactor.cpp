#include "stdafx.h"
#include "store_compactor.h"
#include "config.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "tablet.h"
#include "chunk_store.h"
#include "partition.h"
#include "tablet_reader.h"
#include "config.h"
#include "in_memory_manager.h"
#include "private.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/scheduler.h>
#include <core/concurrency/async_semaphore.h>

#include <core/logging/log.h>

#include <core/ytree/attribute_helpers.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/timestamp_provider.h>

#include <ytlib/table_client/versioned_row.h>
#include <ytlib/table_client/versioned_reader.h>
#include <ytlib/table_client/versioned_chunk_writer.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/transaction.h>

#include <ytlib/object_client/helpers.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NHydra;
using namespace NTableClient;
using namespace NApi;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 1024;
static const size_t MaxRowsPerWrite = 1024;

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactor
    : public TRefCounted
{
public:
    TStoreCompactor(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->StoreCompactor->ThreadPoolSize, "StoreCompact"))
        , CompactionSemaphore_(Config_->StoreCompactor->MaxConcurrentCompactions)
        , PartitioningSemaphore_(Config_->StoreCompactor->MaxConcurrentPartitionings)
    { }

    void Start()
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TStoreCompactor::ScanSlot, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TThreadPoolPtr ThreadPool_;
    TAsyncSemaphore CompactionSemaphore_;
    TAsyncSemaphore PartitioningSemaphore_;


    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

        auto tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted)
            return;

        ScanPartitionForCompaction(slot, tablet->GetEden());
        ScanEdenForPartitioning(slot, tablet->GetEden());

        for (auto& partition : tablet->Partitions()) {
            ScanPartitionForCompaction(slot, partition.get());
        }
    }

    void ScanEdenForPartitioning(TTabletSlotPtr slot, TPartition* eden)
    {
        if (eden->GetState() != EPartitionState::Normal)
            return;

        auto* tablet = eden->GetTablet();
        auto storeManager = tablet->GetStoreManager();

        auto stores = PickStoresForPartitioning(eden);
        if (stores.empty())
            return;

        auto guard = TAsyncSemaphoreGuard::TryAcquire(&PartitioningSemaphore_);
        if (!guard)
            return;

        for (auto store : stores) {
            storeManager->BeginStoreCompaction(store);
        }

        eden->SetState(EPartitionState::Partitioning);

        std::vector<TOwningKey> pivotKeys;
        for (const auto& partition : tablet->Partitions()) {
            pivotKeys.push_back(partition->GetPivotKey());
        }

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreCompactor::PartitionEden,
            MakeStrong(this),
            Passed(std::move(guard)),
            eden,
            pivotKeys,
            stores));
    }

    void ScanPartitionForCompaction(TTabletSlotPtr slot, TPartition* partition)
    {
        if (partition->GetState() != EPartitionState::Normal)
            return;

        auto* tablet = partition->GetTablet();
        auto storeManager = tablet->GetStoreManager();
        auto config = tablet->GetConfig();

        // Don't compact partitions (excluding Eden) whose data size exceeds the limit.
        // Let Partition Balancer do its job.
        if (!partition->IsEden() &&
            partition->GetUncompressedDataSize() > config->MaxPartitionDataSize)
            return;

        auto stores = PickStoresForCompaction(partition);
        if (stores.empty())
            return;

        auto guard = TAsyncSemaphoreGuard::TryAcquire(&CompactionSemaphore_);
        if (!guard)
            return;

        auto majorTimestamp = ComputeMajorTimestamp(partition, stores);

        for (auto store : stores) {
            storeManager->BeginStoreCompaction(store);
        }

        partition->SetState(EPartitionState::Compacting);

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreCompactor::CompactPartition,
            MakeStrong(this),
            Passed(std::move(guard)),
            partition,
            stores,
            majorTimestamp));
    }


    std::vector<TChunkStorePtr> PickStoresForPartitioning(TPartition* eden)
    {
        auto config = eden->GetTablet()->GetConfig();

        std::vector<TChunkStorePtr> candidates;
        std::vector<TChunkStorePtr> forcedCandidates;
        for (auto store : eden->Stores()) {
            if (!TStoreManager::IsStoreCompactable(store))
                continue;

            auto candidate = store->AsChunk();
            candidates.push_back(candidate);
            if (IsCompactionForced(candidate) &&
                candidate->GetUncompressedDataSize() >= config->MinPartitionDataSize &&
                forcedCandidates.size() < config->MaxPartitioningStoreCount)
            {
                forcedCandidates.push_back(candidate);
            }
        }

        // Check for forced candidates.
        if (!forcedCandidates.empty()) {
            return forcedCandidates;
        }

        // Sort by decreasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (TChunkStorePtr lhs, TChunkStorePtr rhs) {
                return lhs->GetUncompressedDataSize() > rhs->GetUncompressedDataSize();
            });

        i64 dataSizeSum = 0;
        int bestStoreCount = -1;
        for (int i = 0; i < candidates.size(); ++i) {
            dataSizeSum += candidates[i]->GetUncompressedDataSize();
            int storeCount = i + 1;
            if (storeCount >= config->MinPartitioningStoreCount &&
                storeCount <= config->MaxPartitioningStoreCount &&
                dataSizeSum >= config->MinPartitioningDataSize &&
                // Ignore max_partitioning_data_size limit for a single store.
                (dataSizeSum <= config->MaxPartitioningDataSize || storeCount == 1))
            {
                // Prefer to partition more data.
                bestStoreCount = storeCount;
            }
        }

        return bestStoreCount >= 0
            ? std::vector<TChunkStorePtr>(candidates.begin(), candidates.begin() + bestStoreCount)
            : std::vector<TChunkStorePtr>();
    }

    std::vector<TChunkStorePtr> PickStoresForCompaction(TPartition* partition)
    {
        auto config = partition->GetTablet()->GetConfig();

        std::vector<TChunkStorePtr> candidates;
        std::vector<TChunkStorePtr> forcedCandidates;
        for (auto store : partition->Stores()) {
            if (!TStoreManager::IsStoreCompactable(store))
                continue;

            // Don't compact large Eden stores.
            if (partition->IsEden() && store->GetUncompressedDataSize() >= config->MinPartitioningDataSize)
                continue;

            auto candidate = store->AsChunk();
            candidates.push_back(candidate);
            if (IsCompactionForced(candidate) &&
                forcedCandidates.size() < config->MaxCompactionStoreCount)
            {
                forcedCandidates.push_back(candidate);
            }
        }

        // Check for forced candidates.
        if (!forcedCandidates.empty()) {
            return forcedCandidates;
        }

        // Sort by increasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (TChunkStorePtr lhs, TChunkStorePtr rhs) {
                return lhs->GetUncompressedDataSize() < rhs->GetUncompressedDataSize();
            });

        for (int i = 0; i < candidates.size(); ++i) {
            i64 dataSizeSum = 0;
            int j = i;
            while (j < candidates.size()) {
                int storeCount = j - i;
                if (storeCount > config->MaxCompactionStoreCount)
                    break;
                i64 dataSize = candidates[j]->GetUncompressedDataSize();
                if (dataSize > config->CompactionDataSizeBase &&
                    dataSizeSum > 0 && dataSize > dataSizeSum * config->CompactionDataSizeRatio)
                    break;
                dataSizeSum += dataSize;
                ++j;
            }

            int storeCount = j - i;
            if (storeCount >= config->MinCompactionStoreCount) {
                return std::vector<TChunkStorePtr>(candidates.begin() + i, candidates.begin() + j);
            }
        }

        return std::vector<TChunkStorePtr>();
    }

    TTimestamp ComputeMajorTimestamp(
        TPartition* partition,
        const std::vector<TChunkStorePtr>& stores)
    {
        auto result = MaxTimestamp;
        auto handleStore = [&] (const IStorePtr& store) {
            result = std::min(result, store->GetMinTimestamp());
        };

        auto* tablet = partition->GetTablet();
        auto* eden = tablet->GetEden();
        for (auto store : eden->Stores()) {
            handleStore(store);
        }

        for (auto store : partition->Stores()) {
            if (store->GetType() == EStoreType::Chunk) {
                if (std::find(stores.begin(), stores.end(), store->AsChunk()) == stores.end()) {
                    handleStore(store);
                }
            }
        }

        return result;
    }


    void PartitionEden(
        TAsyncSemaphoreGuard /*guard*/,
        TPartition* eden,
        const std::vector<TOwningKey>& pivotKeys,
        const std::vector<TChunkStorePtr>& stores)
    {
        // Capture everything needed below.
        // NB: Avoid accessing tablet from pool invoker.
        auto* tablet = eden->GetTablet();
        auto storeManager = tablet->GetStoreManager();
        auto slot = tablet->GetSlot();
        auto tabletId = tablet->GetTabletId();
        auto writerOptions = tablet->GetWriterOptions();
        auto tabletPivotKey = tablet->GetPivotKey();
        auto nextTabletPivotKey = tablet->GetNextPivotKey();
        auto keyColumns = tablet->KeyColumns();
        auto schema = tablet->Schema();
        auto tabletConfig = tablet->GetConfig();

        YCHECK(tabletPivotKey == pivotKeys[0]);

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v", tabletId);

        auto automatonInvoker = GetCurrentInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        try {
            i64 dataSize = 0;
            for (auto store : stores) {
                dataSize += store->GetUncompressedDataSize();
            }

            auto timestampProvider = Bootstrap_->GetMasterClient()->GetConnection()->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            LOG_INFO("Eden partitioning started (PartitionCount: %v, DataSize: %v, ChunkCount: %v, CurrentTimestamp: %v)",
                pivotKeys.size(),
                dataSize,
                stores.size(),
                currentTimestamp);

            auto reader = CreateVersionedTabletReader(
                Bootstrap_->GetQueryPoolInvoker(),
                tablet->GetSnapshot(),
                std::vector<IStorePtr>(stores.begin(), stores.end()),
                tabletPivotKey,
                nextTabletPivotKey,
                currentTimestamp,
                MinTimestamp); // NB: No major compaction during Eden partitioning.

            SwitchTo(poolInvoker);

            ITransactionPtr transaction;
            {
                LOG_INFO("Creating Eden partitioning transaction");
                NTransactionClient::TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Eden partitioning, tablet %v",
                    tabletId));
                options.Attributes = std::move(attributes);

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                LOG_INFO("Eden partitioning transaction created (TransactionId: %v)",
                    transaction->GetId());
            }

            std::vector<TVersionedRow> writeRows;
            writeRows.reserve(MaxRowsPerWrite);

            int currentPartitionIndex = 0;
            TOwningKey currentPivotKey;
            TOwningKey nextPivotKey;

            int currentPartitionRowCount = 0;
            int readRowCount = 0;
            int writeRowCount = 0;
            IVersionedMultiChunkWriterPtr currentWriter;

            TReqCommitTabletStoresUpdate hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());
            for (auto store : stores) {
                auto* descriptor = hydraRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }

            auto ensurePartitionStarted = [&] () {
                if (currentWriter)
                    return;

                LOG_INFO("Started writing partition (PartitionIndex: %v, Keys: %v .. %v)",
                    currentPartitionIndex,
                    currentPivotKey,
                    nextPivotKey);

                auto inMemoryManager = Bootstrap_->GetInMemoryManager();
                auto blockCache = inMemoryManager->CreateInterceptingBlockCache(tabletConfig->InMemoryMode);

                currentWriter = CreateVersionedMultiChunkWriter(
                    Config_->ChunkWriter,
                    writerOptions,
                    schema,
                    keyColumns,
                    Bootstrap_->GetMasterClient(),
                    transaction->GetId(),
                    NullChunkListId,
                    GetUnlimitedThrottler(),
                    blockCache);

                WaitFor(currentWriter->Open())
                    .ThrowOnError();
            };

            auto flushOutputRows = [&] () {
                if (writeRows.empty())
                    return;

                writeRowCount += writeRows.size();

                ensurePartitionStarted();
                if (!currentWriter->Write(writeRows)) {
                    WaitFor(currentWriter->GetReadyEvent())
                        .ThrowOnError();
                }

                writeRows.clear();
            };

            auto writeOutputRow = [&] (TVersionedRow row) {
                if (writeRows.size() ==  writeRows.capacity()) {
                    flushOutputRows();
                }
                writeRows.push_back(row);
                ++currentPartitionRowCount;
            };

            auto flushPartition = [&] () {
                flushOutputRows();

                if (currentWriter) {
                    WaitFor(currentWriter->Close())
                        .ThrowOnError();

                    LOG_INFO("Finished writing partition (PartitionIndex: %v, RowCount: %v)",
                        currentPartitionIndex,
                        currentPartitionRowCount);

                    for (const auto& chunkSpec : currentWriter->GetWrittenChunks()) {
                        auto* descriptor = hydraRequest.add_stores_to_add();
                        descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                        descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                    }

                    currentWriter.Reset();
                }

                currentPartitionRowCount = 0;
                ++currentPartitionIndex;
            };

            std::vector<TVersionedRow> readRows;
            readRows.reserve(MaxRowsPerRead);
            int currentRowIndex = 0;

            auto peekInputRow = [&] () -> TVersionedRow {
                if (currentRowIndex == readRows.size()) {
                    // readRows will be invalidated, must flush writeRows.
                    flushOutputRows();
                    currentRowIndex = 0;
                    while (true) {
                        if (!reader->Read(&readRows)) {
                            return TVersionedRow();
                        }
                        readRowCount += readRows.size();
                        if (!readRows.empty())
                            break;
                        WaitFor(reader->GetReadyEvent())
                            .ThrowOnError();
                    }
                }
                return readRows[currentRowIndex];
            };

            auto skipInputRow = [&] () {
                ++currentRowIndex;
            };

            WaitFor(reader->Open())
                .ThrowOnError();

            for (auto it = pivotKeys.begin(); it != pivotKeys.end(); ++it) {
                currentPivotKey = *it;
                nextPivotKey = it == pivotKeys.end() - 1 ? nextTabletPivotKey : *(it + 1);

                while (true) {
                    auto row = peekInputRow();
                    if (!row)
                        break;

                    // NB: pivot keys can be of arbitrary schema and length.
                    YCHECK(CompareRows(currentPivotKey.Begin(), currentPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0);

                    if (CompareRows(nextPivotKey.Begin(), nextPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0)
                        break;

                    skipInputRow();
                    writeOutputRow(row);
                }

                flushPartition();
            }

            SwitchTo(automatonInvoker);

            YCHECK(readRowCount == writeRowCount);
            LOG_INFO("Eden partitioning completed (RowCount: %v)",
                readRowCount);

            tablet->SetLastPartitioningTime(TInstant::Now());

            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->Commit()
                .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                    if (!error.IsOK()) {
                        LOG_ERROR(error, "Error committing tablet stores update mutation");
                    }
                }));

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error partitioning Eden, backing off");

            SwitchTo(automatonInvoker);

            for (auto store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        SwitchTo(automatonInvoker);

        YCHECK(eden->GetState() == EPartitionState::Partitioning);
        eden->SetState(EPartitionState::Normal);
    }

    void CompactPartition(
        TAsyncSemaphoreGuard /*guard*/,
        TPartition* partition,
        const std::vector<TChunkStorePtr>& stores,
        TTimestamp majorTimestamp)
    {
        // Capture everything needed below.
        // NB: Avoid accessing tablet from pool invoker.
        auto* tablet = partition->GetTablet();
        auto storeManager = tablet->GetStoreManager();
        auto slot = tablet->GetSlot();
        auto tabletId = tablet->GetTabletId();
        auto writerOptions = tablet->GetWriterOptions();
        auto tabletPivotKey = tablet->GetPivotKey();
        auto nextTabletPivotKey = tablet->GetNextPivotKey();
        auto keyColumns = tablet->KeyColumns();
        auto schema = tablet->Schema();
        auto tabletConfig = tablet->GetConfig();
        writerOptions->ChunksEden = partition->IsEden();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, Eden: %v, PartitionRange: %v .. %v",
            tabletId,
            partition->IsEden(),
            partition->GetPivotKey(),
            partition->GetNextPivotKey());

        auto automatonInvoker = GetCurrentInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        try {
            i64 dataSize = 0;
            for (auto store : stores) {
                dataSize += store->GetUncompressedDataSize();
            }

            auto timestampProvider = Bootstrap_->GetMasterClient()->GetConnection()->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            LOG_INFO("Partition compaction started (DataSize: %v, ChunkCount: %v, CurrentTimestamp: %v, MajorTimestamp: %v)",
                dataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp);

            auto reader = CreateVersionedTabletReader(
                Bootstrap_->GetQueryPoolInvoker(),
                tablet->GetSnapshot(),
                std::vector<IStorePtr>(stores.begin(), stores.end()),
                tabletPivotKey,
                nextTabletPivotKey,
                currentTimestamp,
                majorTimestamp);

            SwitchTo(poolInvoker);

            auto transactionManager = Bootstrap_->GetMasterClient()->GetTransactionManager();
        
            ITransactionPtr transaction;
            {
                LOG_INFO("Creating partition compaction transaction");
                NTransactionClient::TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Partition compaction, tablet %v",
                    tabletId));
                options.Attributes = std::move(attributes);

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                LOG_INFO("Partition compaction transaction created (TransactionId: %v)",
                    transaction->GetId());
            }

            TReqCommitTabletStoresUpdate hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());
            for (auto store : stores) {
                auto* descriptor = hydraRequest.add_stores_to_remove();
                ToProto(descriptor->mutable_store_id(), store->GetId());
            }

            auto inMemoryManager = Bootstrap_->GetInMemoryManager();
            auto blockCache = inMemoryManager->CreateInterceptingBlockCache(tabletConfig->InMemoryMode);

            auto writer = CreateVersionedMultiChunkWriter(
                Config_->ChunkWriter,
                writerOptions,
                schema,
                keyColumns,
                Bootstrap_->GetMasterClient(),
                transaction->GetId(),
                NullChunkListId,
                GetUnlimitedThrottler(),
                blockCache);

            WaitFor(reader->Open())
                .ThrowOnError();

            WaitFor(writer->Open())
                .ThrowOnError();

            std::vector<TVersionedRow> rows;

            int readRowCount = 0;
            int writeRowCount = 0;

            while (reader->Read(&rows)) {
                readRowCount += rows.size();

                if (rows.empty()) {
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                    continue;
                }

                writeRowCount += rows.size();
                if (!writer->Write(rows)) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            }

            WaitFor(writer->Close())
                .ThrowOnError();

            for (const auto& chunkSpec : writer->GetWrittenChunks()) {
                auto* descriptor = hydraRequest.add_stores_to_add();
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
            }

            SwitchTo(automatonInvoker);

            YCHECK(readRowCount == writeRowCount);
            LOG_INFO("Partition compaction completed (RowCount: %v)",
                readRowCount);

            for (auto store : stores) {
                storeManager->EndStoreCompaction(store);
                store->SetStoreState(EStoreState::Removing);
            }

            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->Commit()
                .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
                    if (!error.IsOK()) {
                        LOG_ERROR(error, "Error committing tablet stores update mutation");
                    }
                }));

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error compacting partition, backing off");

            SwitchTo(automatonInvoker);

            for (auto store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        SwitchTo(automatonInvoker);

        YCHECK(partition->GetState() == EPartitionState::Compacting);
        partition->SetState(EPartitionState::Normal);
    }


    static bool IsCompactionForced(TChunkStorePtr store)
    {
        const auto& config = store->GetTablet()->GetConfig();
        if (!config->ForcedCompactionRevision) {
            return false;
        }

        ui64 revision = CounterFromId(store->GetId());
        if (revision > *config->ForcedCompactionRevision) {
            return false;
        }

        return true;
    }

};

void StartStoreCompactor(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    New<TStoreCompactor>(config, bootstrap)->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
