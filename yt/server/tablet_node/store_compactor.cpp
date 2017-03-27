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

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_transaction.h>
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
#include <yt/ytlib/transaction_client/action.h>

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

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

static const size_t MaxRowsPerRead = 65536;
static const size_t MaxRowsPerWrite = 65536;

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
        , PartitioningSemaphore_(New<TAsyncSemaphore>(Config_->StoreCompactor->MaxConcurrentPartitionings))
        , CompactionSemaphore_(New<TAsyncSemaphore>(Config_->StoreCompactor->MaxConcurrentCompactions))
        , Profiler("/tablet_node/store_compactor")
        , FeasiblePartitionings_("/feasible_partitionings")
        , FeasibleCompactions_("/feasible_compactions")
        , ScheduledPartitionings_("/scheduled_partitionings")
        , ScheduledCompactions_("/scheduled_compactions")
    { }

    void Start()
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreCompactor::OnBeginSlotScan, MakeStrong(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreCompactor::OnScanSlot, MakeStrong(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreCompactor::OnEndSlotScan, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TThreadPoolPtr ThreadPool_;
    TAsyncSemaphorePtr PartitioningSemaphore_;
    TAsyncSemaphorePtr CompactionSemaphore_;

    const NProfiling::TProfiler Profiler;
    NProfiling::TSimpleCounter FeasiblePartitionings_;
    NProfiling::TSimpleCounter FeasibleCompactions_;
    NProfiling::TSimpleCounter ScheduledPartitionings_;
    NProfiling::TSimpleCounter ScheduledCompactions_;

    struct TTask
    {
        TTabletSlotPtr Slot;
        IInvokerPtr Invoker;
        TTabletId Tablet;
        TPartitionId Partition;
        std::vector<TStoreId> Stores;
        size_t Score;
    };

    // Variables below contain per-iteration state for slot scan.
    TSpinLock SpinLock_;
    bool ScanForPartitioning_;
    bool ScanForCompactions_;
    std::vector<TTask> PartitioningCandidates_;
    std::vector<TTask> CompactionCandidates_;

    static size_t PackTaskScore(size_t x, size_t y, size_t z)
    {
        static_assert(sizeof(size_t) == 8, "size_t must be 64-bit.");
        static constexpr size_t partSize = 21;
        static constexpr size_t partMask = static_cast<size_t>(1 << 21) - 1;
        size_t result = 0;
        result |= (x & partMask) << (partSize * 2);
        result |= ((partMask + 1) - (y & partMask)) << (partSize * 1); // negative
        result |= (z & partMask) << (partSize * 0);
        return result;
    }

    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(SpinLock_);

        // Save some scheduling resources by skipping unnecessary work.
        ScanForPartitioning_ = PartitioningSemaphore_->IsReady();
        ScanForCompactions_ = CompactionSemaphore_->IsReady();
        PartitioningCandidates_.clear(); // Though must be clear already.
        CompactionCandidates_.clear(); // Though must be clear already.
    }

    void OnScanSlot(const TTabletSlotPtr& slot)
    {
        NProfiling::TTagIdList tagIdList;
        tagIdList.push_back(NProfiling::TProfileManager::Get()->RegisterTag("slot", slot->GetIndex()));
        PROFILE_TIMING("/scan_time", tagIdList) {
            OnScanSlotImpl(slot, tagIdList);
        }
    }

    void OnScanSlotImpl(const TTabletSlotPtr& slot, const NProfiling::TTagIdList& tagIdList)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            ScanTablet(slot.Get(), pair.second);
        }
    }

    void OnEndSlotScan()
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(SpinLock_);

        if (ScanForPartitioning_) {
            Profiler.Update(FeasiblePartitionings_, PartitioningCandidates_.size());

            std::partial_sort(
                PartitioningCandidates_.begin(),
                PartitioningCandidates_.begin() + PartitioningSemaphore_->GetTotal(),
                PartitioningCandidates_.end(),
                [&] (const TTask& lhs, const TTask& rhs) -> bool {
                    return lhs.Score < rhs.Score;
                });

            size_t counter = 0;
            for (auto&& task : PartitioningCandidates_) {
                auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(PartitioningSemaphore_);
                if (!semaphoreGuard) {
                    break;
                }
                task.Invoker->Invoke(BIND(
                    &TStoreCompactor::PartitionEden,
                    MakeStrong(this),
                    Passed(std::move(semaphoreGuard)),
                    Passed(std::move(task))));
                ++counter;
            }

            Profiler.Increment(ScheduledPartitionings_, counter);

            PartitioningCandidates_.clear();
        }

        if (ScanForCompactions_) {
            Profiler.Update(FeasibleCompactions_, CompactionCandidates_.size());

            std::partial_sort(
                CompactionCandidates_.begin(),
                CompactionCandidates_.begin() + CompactionSemaphore_->GetTotal(),
                CompactionCandidates_.end(),
                [&] (const TTask& lhs, const TTask& rhs) -> bool {
                    return lhs.Score < rhs.Score;
                });

            size_t counter = 0;
            for (auto&& task : CompactionCandidates_) {
                auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(CompactionSemaphore_);
                if (!semaphoreGuard) {
                    break;
                }
                task.Invoker->Invoke(BIND(
                    &TStoreCompactor::CompactPartition,
                    MakeStrong(this),
                    Passed(std::move(semaphoreGuard)),
                    Passed(std::move(task))));
                ++counter;
            }

            Profiler.Increment(ScheduledCompactions_, counter);

            CompactionCandidates_.clear();
        }
    }

    void ScanTablet(TTabletSlot* slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (!tablet->IsPhysicallySorted()) {
            return;
        }

        if (tablet->GetConfig()->DisableCompactionAndPartitioning) {
            return;
        }

        ScanPartitionForCompaction(slot, tablet->GetEden());
        ScanEdenForPartitioning(slot, tablet->GetEden());

        for (auto& partition : tablet->PartitionList()) {
            ScanPartitionForCompaction(slot, partition.get());
        }
    }

    void ScanEdenForPartitioning(TTabletSlot* slot, TPartition* eden)
    {
        if (!ScanForPartitioning_ || eden->GetState() != EPartitionState::Normal) {
            return;
        }

        const auto* tablet = eden->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();

        auto stores = PickStoresForPartitioning(eden);
        if (stores.empty()) {
            return;
        }

        TTask candidate;
        candidate.Slot = slot;
        candidate.Invoker = tablet->GetEpochAutomatonInvoker();
        candidate.Tablet = tablet->GetId();
        candidate.Partition = eden->GetId();
        candidate.Stores = std::move(stores);
        // We aim to improve OSC; partitioning unconditionally improves OSC (given at least two stores).
        // So we consider how constrained is the tablet, and how many stores we consider for partitioning.
        const int mosc = tablet->GetConfig()->MaxOverlappingStoreCount;
        const int osc = tablet->GetOverlappingStoreCount();
        const size_t score = static_cast<size_t>(std::max(0, mosc - osc));
        // Pack score into a single 64-bit integer. This is equivalent to order on (Score, -Stores, Random) tuples.
        candidate.Score = PackTaskScore(score, candidate.Stores.size(), RandomNumber<size_t>());

        {
            auto guard = Guard(SpinLock_);
            PartitioningCandidates_.push_back(std::move(candidate));
        }
    }

    void ScanPartitionForCompaction(TTabletSlot* slot, TPartition* partition)
    {
        if (!ScanForCompactions_ || partition->GetState() != EPartitionState::Normal) {
            return;
        }

        const auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();

        auto stores = PickStoresForCompaction(partition);
        if (stores.empty()) {
            return;
        }

        TTask candidate;
        candidate.Slot = slot;
        candidate.Invoker = tablet->GetEpochAutomatonInvoker();
        candidate.Tablet = tablet->GetId();
        candidate.Partition = partition->GetId();
        candidate.Stores = std::move(stores);
        // We aim to improve OSC; compaction improves OSC _only_ if the partition contributes towards OSC.
        // So we consider how constrained is the partition, and how many stores we consider for compaction.
        const int mosc = tablet->GetConfig()->MaxOverlappingStoreCount;
        const int osc = tablet->GetOverlappingStoreCount();
        const int edenStoreCount = static_cast<int>(tablet->GetEden()->Stores().size());
        const int partitionStoreCount = static_cast<int>(partition->Stores().size());
        // For constrained partitions, this is equivalent to MOSC-OSC; for unconstrained -- includes extra slack.
        const size_t score = static_cast<size_t>(std::max(0, mosc - edenStoreCount - partitionStoreCount));
        // Pack score into a single 64-bit integer.
        candidate.Score = PackTaskScore(score, candidate.Stores.size(), RandomNumber<size_t>());

        {
            auto guard = Guard(SpinLock_);
            CompactionCandidates_.push_back(std::move(candidate));
        }
    }


    std::vector<TStoreId> PickStoresForPartitioning(TPartition* eden)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = eden->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& config = tablet->GetConfig();

        std::vector<TSortedChunkStorePtr> candidates;

        for (const auto& store : eden->Stores()) {
            if (!storeManager->IsStoreCompactable(store)) {
                continue;
            }

            auto candidate = store->AsSortedChunk();
            candidates.push_back(candidate);

            if ((IsCompactionForced(candidate) || IsPeriodicCompactionNeeded(eden)) &&
                finalists.size() < config->MaxPartitioningStoreCount)
            {
                finalists.push_back(candidate->GetId());
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
        }

        // Sort by decreasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (const TSortedChunkStorePtr& lhs, const TSortedChunkStorePtr& rhs) {
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

        if (bestStoreCount > 0) {
            finalists.reserve(bestStoreCount);
            for (int i = 0; i < bestStoreCount; ++i) {
                finalists.push_back(candidates[i]->GetId());
            }
        }

        return finalists;
    }

    std::vector<TStoreId> PickStoresForCompaction(TPartition* partition)
    {
        std::vector<TStoreId> finalists;

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
                finalists.size() < config->MaxCompactionStoreCount)
            {
                finalists.push_back(candidate->GetId());
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
        }

        // Sort by increasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (TSortedChunkStorePtr lhs, TSortedChunkStorePtr rhs) {
                return lhs->GetUncompressedDataSize() < rhs->GetUncompressedDataSize();
            });

        const auto* eden = tablet->GetEden();
        int overlappingStoreCount = static_cast<int>(partition->Stores().size() + eden->Stores().size());
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
                finalists.reserve(storeCount);
                while (i < j) {
                    finalists.push_back(candidates[i]->GetId());
                    ++i;
                }
            }
        }

        return finalists;
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


    void PartitionEden(TAsyncSemaphoreGuard /*guard*/, TTask task)
    {
        const auto& slot = task.Slot;

        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task.Tablet);
        if (!tablet) {
            return;
        }

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(task.Tablet);
        if (!tabletSnapshot) {
            return;
        }

        auto* eden = tablet->GetEden();
        if (eden->GetId() != task.Partition || eden->GetState() != EPartitionState::Normal) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task.Stores.size());
        for (const auto& storeId : task.Stores) {
            auto store = tablet->FindStore(storeId)->AsSorted();
            if (!store || !eden->Stores().has(store)) {
                return;
            }
            auto typedStore = store->AsSortedChunk();
            YCHECK(typedStore);
            stores.push_back(std::move(typedStore));
        }

        std::vector<TOwningKey> pivotKeys;
        for (const auto& partition : tablet->PartitionList()) {
            pivotKeys.push_back(partition->GetPivotKey());
        }

        YCHECK(tablet->GetPivotKey() == pivotKeys[0]);

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v", tablet->GetId());

        eden->CheckedSetState(EPartitionState::Normal, EPartitionState::Partitioning);

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetUncompressedDataSize();
                storeManager->BeginStoreCompaction(store);
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
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                MinTimestamp, // NB: No major compaction during Eden partitioning.
                TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning));

            INativeTransactionPtr transaction;
            {
                LOG_INFO("Creating Eden partitioning transaction");

                TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Eden partitioning: tablet %v",
                    tabletSnapshot->TabletId));
                options.Attributes = std::move(attributes);

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                LOG_INFO("Eden partitioning transaction created (TransactionId: %v)",
                    transaction->GetId());

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            auto asyncResult =
                BIND(
                    &TStoreCompactor::DoPartitionEden,
                    MakeStrong(this),
                    reader,
                    tabletSnapshot,
                    transaction,
                    pivotKeys,
                    tablet->GetNextPivotKey(),
                    Logger)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            std::vector<IVersionedMultiChunkWriterPtr> writers;
            int rowCount;
            std::tie(writers, rowCount) = WaitFor(asyncResult)
                .ValueOrThrow();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tablet->GetId());
            actionRequest.set_mount_revision(tablet->GetMountRevision());

            TStoreIdList storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            // TODO(sandello): Move specs?
            TStoreIdList storeIdsToAdd;
            for (const auto& writer : writers) {
                for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                    auto* descriptor = actionRequest.add_stores_to_add();
                    descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                    descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                    descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                    storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
                }
            }

            LOG_INFO("Eden partitioning completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v)",
                rowCount,
                storeIdsToAdd,
                storeIdsToRemove);

            auto actionData = MakeTransactionActionData(actionRequest);
            transaction->AddAction(Bootstrap_->GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellId(), actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error partitioning Eden, backing off");

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        eden->CheckedSetState(EPartitionState::Partitioning, EPartitionState::Normal);
    }

    std::tuple<std::vector<IVersionedMultiChunkWriterPtr>, int> DoPartitionEden(
        const IVersionedReaderPtr& reader,
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITransactionPtr& transaction,
        const std::vector<TOwningKey>& pivotKeys,
        const TOwningKey& nextTabletPivotKey,
        NLogging::TLogger Logger)
    {
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

        YCHECK(readRowCount == writeRowCount);

        return std::make_tuple(writerPool.GetAllWriters(), readRowCount);
    }

    void CompactPartition(TAsyncSemaphoreGuard /*guard*/, TTask task)
    {
        const auto& slot = task.Slot;

        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task.Tablet);
        if (!tablet) {
            return;
        }

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(task.Tablet);
        if (!tabletSnapshot) {
            return;
        }

        auto* partition = tablet->FindPartition(task.Partition);
        if (!partition || partition->GetState() != EPartitionState::Normal) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task.Stores.size());
        for (const auto& storeId : task.Stores) {
            auto store = tablet->FindStore(storeId)->AsSorted();
            if (!store || !partition->Stores().has(store)) {
                return;
            }
            auto typedStore = store->AsSortedChunk();
            YCHECK(typedStore);
            stores.push_back(std::move(typedStore));
        }

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, Eden: %v, PartitionRange: %v .. %v",
            tablet->GetId(),
            partition->IsEden(),
            partition->GetPivotKey(),
            partition->GetNextPivotKey());

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetUncompressedDataSize();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            auto majorTimestamp = ComputeMajorTimestamp(partition, stores);
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
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                majorTimestamp,
                TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction));

            INativeTransactionPtr transaction;
            {
                LOG_INFO("Creating partition compaction transaction");

                TTransactionStartOptions options;
                options.AutoAbort = false;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Partition compaction: tablet %v",
                    tabletSnapshot->TabletId));
                options.Attributes = std::move(attributes);

                auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options);
                transaction = WaitFor(asyncTransaction)
                    .ValueOrThrow();

                LOG_INFO("Partition compaction transaction created (TransactionId: %v)",
                    transaction->GetId());

                Logger.AddTag("TransactionId: %v", transaction->GetId());
            }

            auto asyncResult =
                BIND(
                    &TStoreCompactor::DoCompactPartition,
                    MakeStrong(this),
                    reader,
                    tabletSnapshot,
                    transaction,
                    partition->IsEden(),
                    Logger)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            IVersionedMultiChunkWriterPtr writer;
            int rowCount;
            std::tie(writer, rowCount) = WaitFor(asyncResult)
                .ValueOrThrow();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            ToProto(actionRequest.mutable_tablet_id(), tablet->GetId());
            actionRequest.set_mount_revision(tablet->GetMountRevision());
            actionRequest.set_retained_timestamp(retainedTimestamp);

            TStoreIdList storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            // TODO(sandello): Move specs?
            TStoreIdList storeIdsToAdd;
            for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                auto* descriptor = actionRequest.add_stores_to_add();
                descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
            }

            LOG_INFO("Partition compaction completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v)",
                rowCount,
                storeIdsToAdd,
                storeIdsToRemove);

            auto actionData = MakeTransactionActionData(actionRequest);
            transaction->AddAction(Bootstrap_->GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellId(), actionData);
            transaction->AddAction(slot->GetCellId(), actionData);

            const auto& tabletManager = slot->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error compacting partition, backing off");

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }

    std::tuple<IVersionedMultiChunkWriterPtr, int> DoCompactPartition(
        const IVersionedReaderPtr& reader,
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITransactionPtr& transaction,
        bool isEden,
        NLogging::TLogger Logger)
    {
        auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
        writerOptions->ChunksEden = isEden;

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

        YCHECK(readRowCount == writeRowCount);

        return std::make_tuple(writer, readRowCount);
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
