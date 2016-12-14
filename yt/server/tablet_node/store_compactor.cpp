#include "store_compactor.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "config.h"
#include "in_memory_manager.h"
#include "partition.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "chunk_writer_pool.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/ytree/helpers.h>

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
using namespace NTransactionClient;

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
        , CompactionSemaphore_(New<TAsyncSemaphore>(Config_->StoreCompactor->MaxConcurrentCompactions))
        , PartitioningSemaphore_(New<TAsyncSemaphore>(Config_->StoreCompactor->MaxConcurrentPartitionings))
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
    TAsyncSemaphorePtr CompactionSemaphore_;
    TAsyncSemaphorePtr PartitioningSemaphore_;

    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (!tablet->IsPhysicallySorted()) {
            return;
        }

        ScanPartitionForCompaction(slot, tablet->GetEden());
        ScanEdenForPartitioning(slot, tablet->GetEden());

        for (auto& partition : tablet->PartitionList()) {
            ScanPartitionForCompaction(slot, partition.get());
        }
    }

    void ScanEdenForPartitioning(TTabletSlotPtr slot, TPartition* eden)
    {
        if (eden->GetState() != EPartitionState::Normal) {
            return;
        }

        auto* tablet = eden->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();

        auto stores = PickStoresForPartitioning(eden);
        if (stores.empty()) {
            return;
        }

        auto guard = TAsyncSemaphoreGuard::TryAcquire(PartitioningSemaphore_);
        if (!guard) {
            return;
        }

        std::vector<TOwningKey> pivotKeys;
        for (const auto& partition : tablet->PartitionList()) {
            pivotKeys.push_back(partition->GetPivotKey());
        }

        for (const auto& store : stores) {
            storeManager->BeginStoreCompaction(store);
        }

        eden->CheckedSetState(EPartitionState::Normal, EPartitionState::Partitioning);

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreCompactor::PartitionEden,
            MakeStrong(this),
            Passed(std::move(guard)),
            slot,
            eden,
            pivotKeys,
            stores));
    }

    void ScanPartitionForCompaction(TTabletSlotPtr slot, TPartition* partition)
    {
        if (partition->GetState() != EPartitionState::Normal) {
            return;
        }

        auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();

        auto stores = PickStoresForCompaction(partition);
        if (stores.empty()) {
            return;
        }

        auto guard = TAsyncSemaphoreGuard::TryAcquire(CompactionSemaphore_);
        if (!guard) {
            return;
        }

        auto majorTimestamp = ComputeMajorTimestamp(partition, stores);

        for (const auto& store : stores) {
            storeManager->BeginStoreCompaction(store);
        }

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStoreCompactor::CompactPartition,
            MakeStrong(this),
            Passed(std::move(guard)),
            slot,
            partition,
            stores,
            majorTimestamp));
    }


    std::vector<TSortedChunkStorePtr> PickStoresForPartitioning(TPartition* eden)
    {
        const auto* tablet = eden->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& config = tablet->GetConfig();

        std::vector<TSortedChunkStorePtr> candidates;
        std::vector<TSortedChunkStorePtr> forcedCandidates;
        for (const auto& store : eden->Stores()) {
            if (!storeManager->IsStoreCompactable(store)) {
                continue;
            }

            auto candidate = store->AsSortedChunk();
            candidates.push_back(candidate);

            if ((IsCompactionForced(candidate) || IsPeriodicCompactionNeeded(eden)) &&
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
            [] (TSortedChunkStorePtr lhs, TSortedChunkStorePtr rhs) {
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
            ? std::vector<TSortedChunkStorePtr>(candidates.begin(), candidates.begin() + bestStoreCount)
            : std::vector<TSortedChunkStorePtr>();
    }

    std::vector<TSortedChunkStorePtr> PickStoresForCompaction(TPartition* partition)
    {
        const auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& config = tablet->GetConfig();

        // XXX(savrus) Disabled. Hotfix for YT-5828
#if 0
        // Don't compact partitions (excluding Eden) whose data size exceeds the limit.
        // Let Partition Balancer do its job.
        if (!partition->IsEden() && partition->GetUncompressedDataSize() > config->MaxCompactionDataSize) {
            return std::vector<TSortedChunkStorePtr>();
        }
#endif

        std::vector<TSortedChunkStorePtr> candidates;
        std::vector<TSortedChunkStorePtr> forcedCandidates;
        for (const auto& store : partition->Stores()) {
            if (!storeManager->IsStoreCompactable(store)) {
                continue;
            }

            // Don't compact large Eden stores.
            if (partition->IsEden() && store->GetUncompressedDataSize() >= config->MinPartitioningDataSize) {
                continue;
            }

            auto candidate = store->AsSortedChunk();
            candidates.push_back(candidate);

            if ((IsCompactionForced(candidate) || IsPeriodicCompactionNeeded(partition)) &&
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
            [] (TSortedChunkStorePtr lhs, TSortedChunkStorePtr rhs) {
                return lhs->GetUncompressedDataSize() < rhs->GetUncompressedDataSize();
            });

        const auto* eden = tablet->GetEden();
        int overlappingStoreCount = partition->Stores().size() + eden->Stores().size();
        bool tooManyOverlappingStores = overlappingStoreCount >= config->MaxOverlappingStoreCount;

        for (int i = 0; i < candidates.size(); ++i) {
            i64 dataSizeSum = 0;
            int j = i;
            while (j < candidates.size()) {
                int storeCount = j - i;
                if (storeCount > config->MaxCompactionStoreCount) {
                   break;
                }
                i64 dataSize = candidates[j]->GetUncompressedDataSize();
                if (!tooManyOverlappingStores &&
                    dataSize > config->CompactionDataSizeBase &&
                    dataSizeSum > 0 && dataSize > dataSizeSum * config->CompactionDataSizeRatio) {
                    break;
                }
                dataSizeSum += dataSize;
                ++j;
            }

            int storeCount = j - i;
            if (storeCount >= config->MinCompactionStoreCount) {
                return std::vector<TSortedChunkStorePtr>(candidates.begin() + i, candidates.begin() + j);
            }
        }

        return std::vector<TSortedChunkStorePtr>();
    }

    static TTimestamp ComputeMajorTimestamp(
        TPartition* partition,
        const std::vector<TSortedChunkStorePtr>& stores)
    {
        auto result = MaxTimestamp;
        auto handleStore = [&] (const ISortedStorePtr& store) {
            result = std::min(result, store->GetMinTimestamp());
        };

        auto* tablet = partition->GetTablet();
        auto* eden = tablet->GetEden();
        for (const auto& store : eden->Stores()) {
            handleStore(store);
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                if (std::find(stores.begin(), stores.end(), store->AsSortedChunk()) == stores.end()) {
                    handleStore(store);
                }
            }
        }

        return result;
    }


    void PartitionEden(
        TAsyncSemaphoreGuard /*guard*/,
        TTabletSlotPtr slot,
        TPartition* eden,
        const std::vector<TOwningKey>& pivotKeys,
        const std::vector<TSortedChunkStorePtr>& stores)
    {
        // Capture everything needed below.
        // NB: Avoid accessing tablet from pool invoker.
        auto* tablet = eden->GetTablet();
        auto storeManager = tablet->GetStoreManager();
        auto tabletId = tablet->GetId();
        auto mountRevision = tablet->GetMountRevision();
        auto tabletPivotKey = tablet->GetPivotKey();
        auto nextTabletPivotKey = tablet->GetNextPivotKey();
        auto schema = tablet->PhysicalSchema();
        auto mountConfig = tablet->GetConfig();

        YCHECK(tabletPivotKey == pivotKeys[0]);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId);
        if (!tabletSnapshot)
            return;

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v", tabletId);

        auto automatonInvoker = GetCurrentInvoker();
        auto poolInvoker = ThreadPool_->GetInvoker();

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetUncompressedDataSize();
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            eden->SetCompactionTime(TInstant::Now());

            LOG_INFO("Eden partitioning started (PartitionCount: %v, DataSize: %v, ChunkCount: %v, CurrentTimestamp: %v)",
                pivotKeys.size(),
                dataSize,
                stores.size(),
                currentTimestamp);

            auto reader = CreateVersionedTabletReader(
                Bootstrap_->GetQueryPoolInvoker(),
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tabletPivotKey,
                nextTabletPivotKey,
                currentTimestamp,
                MinTimestamp, // NB: No major compaction during Eden partitioning.
                TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning));

            SwitchTo(poolInvoker);

            ITransactionPtr transaction;
            {
                LOG_INFO("Creating Eden partitioning transaction");

                TTransactionStartOptions options;
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

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            int writerPoolSize = std::min(
                static_cast<int>(pivotKeys.size()),
                Config_->StoreCompactor->PartitioningWriterPoolSize);
            TChunkWriterPool writerPool(
                Bootstrap_->GetInMemoryManager(),
                tabletSnapshot,
                writerPoolSize,
                tabletSnapshot->WriterConfig,
                tabletSnapshot->WriterOptions,
                Bootstrap_->GetMasterClient(),
                transaction->GetId());

            std::vector<TVersionedRow> writeRows;
            writeRows.reserve(MaxRowsPerWrite);

            int currentPartitionIndex = 0;
            TOwningKey currentPivotKey;
            TOwningKey nextPivotKey;

            int currentPartitionRowCount = 0;
            int readRowCount = 0;
            int writeRowCount = 0;
            IVersionedMultiChunkWriterPtr currentWriter;

            auto ensurePartitionStarted = [&] () {
                if (currentWriter)
                    return;

                LOG_INFO("Started writing partition (PartitionIndex: %v, Keys: %v .. %v)",
                    currentPartitionIndex,
                    currentPivotKey,
                    nextPivotKey);

                currentWriter = writerPool.AllocateWriter();
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
                if (writeRows.size() == writeRows.capacity()) {
                    flushOutputRows();
                }
                writeRows.push_back(row);
                ++currentPartitionRowCount;
            };

            auto flushPartition = [&] () {
                flushOutputRows();

                if (currentWriter) {
                    LOG_INFO("Finished writing partition (PartitionIndex: %v, RowCount: %v)",
                        currentPartitionIndex,
                        currentPartitionRowCount);

                    writerPool.ReleaseWriter(currentWriter);
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
                    if (!row) {
                        break;
                    }

                    // NB: pivot keys can be of arbitrary schema and length.
                    YCHECK(CompareRows(currentPivotKey.Begin(), currentPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0);

                    if (CompareRows(nextPivotKey.Begin(), nextPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0) {
                        break;
                    }

                    skipInputRow();
                    writeOutputRow(row);
                }

                flushPartition();
            }

            SwitchTo(automatonInvoker);

            YCHECK(readRowCount == writeRowCount);

            TReqCommitTabletStoresUpdate hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            hydraRequest.set_mount_revision(mountRevision);
            ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());

            SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = hydraRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            // TODO(sandello): Move specs?
            SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToAdd;
            for (const auto& writer : writerPool.GetAllWriters()) {
                for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                    auto* descriptor = hydraRequest.add_stores_to_add();
                    descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                    descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                    descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                    storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
                }
            }

            // NB: No exceptions must be thrown beyond this point!

            LOG_INFO("Eden partitioning completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v)",
                readRowCount,
                storeIdsToAdd,
                storeIdsToRemove);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->CommitAndLog(Logger);

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error partitioning Eden, backing off");

            SwitchTo(automatonInvoker);

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        SwitchTo(automatonInvoker);

        eden->CheckedSetState(EPartitionState::Partitioning, EPartitionState::Normal);
    }

    void CompactPartition(
        TAsyncSemaphoreGuard /*guard*/,
        TTabletSlotPtr slot,
        TPartition* partition,
        const std::vector<TSortedChunkStorePtr>& stores,
        TTimestamp majorTimestamp)
    {
        // Capture everything needed below.
        // NB: Avoid accessing tablet from pool invoker.
        auto* tablet = partition->GetTablet();
        auto storeManager = tablet->GetStoreManager();
        auto tabletId = tablet->GetId();
        auto mountRevision = tablet->GetMountRevision();
        auto tabletPivotKey = tablet->GetPivotKey();
        auto nextTabletPivotKey = tablet->GetNextPivotKey();

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId);
        if (!tabletSnapshot)
            return;

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
            for (const auto& store : stores) {
                dataSize += store->GetUncompressedDataSize();
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            auto retainedTimestamp = InstantToTimestamp(TimestampToInstant(currentTimestamp).first - tablet->GetConfig()->MinDataTtl).first;
            majorTimestamp = std::min(majorTimestamp, retainedTimestamp);

            partition->SetCompactionTime(TInstant::Now());

            LOG_INFO("Partition compaction started (DataSize: %v, ChunkCount: %v, CurrentTimestamp: %v, MajorTimestamp: %v, RetainedTimestamp: %v)",
                dataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp,
                retainedTimestamp);

            auto reader = CreateVersionedTabletReader(
                Bootstrap_->GetQueryPoolInvoker(),
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tabletPivotKey,
                nextTabletPivotKey,
                currentTimestamp,
                majorTimestamp,
                TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction));

            SwitchTo(poolInvoker);

            ITransactionPtr transaction;
            {
                LOG_INFO("Creating partition compaction transaction");

                TTransactionStartOptions options;
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

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
            writerOptions->ChunksEden = partition->IsEden();

            TChunkWriterPool writerPool(
                Bootstrap_->GetInMemoryManager(),
                tabletSnapshot,
                1,
                tabletSnapshot->WriterConfig,
                writerOptions,
                Bootstrap_->GetMasterClient(),
                transaction->GetId());
            auto writer = writerPool.AllocateWriter();

            WaitFor(reader->Open())
                .ThrowOnError();

            WaitFor(writer->Open())
                .ThrowOnError();

            std::vector<TVersionedRow> rows;
            rows.reserve(MaxRowsPerRead);

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

            SwitchTo(automatonInvoker);

            YCHECK(readRowCount == writeRowCount);

            TReqCommitTabletStoresUpdate hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            hydraRequest.set_mount_revision(mountRevision);
            hydraRequest.set_retained_timestamp(retainedTimestamp);
            ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());

            SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToAdd;
            for (const auto& store : stores) {
                auto* descriptor = hydraRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToAdd.push_back(storeId);
            }

            // TODO(sandello): Move specs?
            SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToRemove;
            for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                auto* descriptor = hydraRequest.add_stores_to_add();
                descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                storeIdsToRemove.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
            }

            // NB: No exceptions must be thrown beyond this point!

            LOG_INFO("Partition compaction completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v)",
                readRowCount,
                storeIdsToAdd,
                storeIdsToRemove);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            CreateMutation(slot->GetHydraManager(), hydraRequest)
                ->CommitAndLog(Logger);

            // Just abandon the transaction, hopefully it won't expire before the chunk is attached.
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error compacting partition, backing off");

            SwitchTo(automatonInvoker);

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        SwitchTo(automatonInvoker);

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }


    static bool IsCompactionForced(TSortedChunkStorePtr store)
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

    static bool IsPeriodicCompactionNeeded(TPartition* partition)
    {
        const auto& config = partition->GetTablet()->GetConfig();
        if (!config->AutoCompactionPeriod) {
            return false;
        }

        if (TInstant::Now() < partition->GetCompactionTime() + *config->AutoCompactionPeriod) {
            return false;
        }

        return true;
    }

};

void StartStoreCompactor(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    if (config->EnableStoreCompactor) {
        New<TStoreCompactor>(config, bootstrap)->Start();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
