#include "store_compactor.h"

#include "hunk_chunk.h"
#include "in_memory_manager.h"
#include "lsm_interop.h"
#include "partition.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYTAlloc;
using namespace NYson;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 65536;
static const size_t MaxRowsPerWrite = 65536;

static const size_t FinishedQueueSize = 100;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHunkCompactionReason,
    (None)
    (ForcedCompaction)
    (GarbageRatioTooHigh)
);

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactionSessionBase
    : public TRefCounted
{
protected:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const ITransactionPtr Transaction_;
    const bool ResultsInEden_;
    const EWorkloadCategory WorkloadCategory_;
    const NLogging::TLogger Logger;

    TTabletStoreWriterConfigPtr StoreWriterConfig_;
    TTabletStoreWriterOptionsPtr StoreWriterOptions_;
    TTabletHunkWriterConfigPtr HunkWriterConfig_;
    TTabletHunkWriterOptionsPtr HunkWriterOptions_;

    IThroughputThrottlerPtr Throttler_;

    IChunkWriterPtr HunkChunkWriter_;
    IHunkChunkPayloadWriterPtr HunkChunkPayloadWriter_;

    IRemoteInMemoryBlockCachePtr BlockCache_;


    TStoreCompactionSessionBase(
        NClusterNode::TBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        ITransactionPtr transaction,
        bool resultsInEden,
        EWorkloadCategory workloadCategory,
        NLogging::TLogger logger)
        : Bootstrap_(bootstrap)
        , TabletSnapshot_(std::move(tabletSnapshot))
        , Transaction_(std::move(transaction))
        , ResultsInEden_(resultsInEden)
        , WorkloadCategory_(workloadCategory)
        , Logger(std::move(logger))
    { }

    template <class F>
    auto DoRun(const F& func) -> decltype(func())
    {
        Initialize();

        const auto& mountConfig = TabletSnapshot_->Settings.MountConfig;
        TMemoryZoneGuard memoryZoneGuard(mountConfig->InMemoryMode == EInMemoryMode::None
            ? NYTAlloc::EMemoryZone::Normal
            : NYTAlloc::EMemoryZone::Undumpable);

        auto result = func();

        Finalize();

        return result;
    }

    IVersionedMultiChunkWriterPtr CreateWriter()
    {
        auto chunkWriterFactory = [weakThis = MakeWeak(this)] (IChunkWriterPtr underlyingWriter) {
            if (auto this_ = weakThis.Lock()) {
                return this_->CreateUnderlyingWriterAdapter(std::move(underlyingWriter));
            } else {
                THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Store compactor session destroyed");
            }
        };

        auto writer = CreateVersionedMultiChunkWriter(
            std::move(chunkWriterFactory),
            StoreWriterConfig_,
            StoreWriterOptions_,
            Bootstrap_->GetMasterClient(),
            CellTagFromId(TabletSnapshot_->TabletId),
            Transaction_->GetId(),
            /*parentChunkListId*/ {},
            Throttler_,
            BlockCache_);
        Writers_.push_back(writer);
        return writer;
    }

    void CloseWriter(const IVersionedMultiChunkWriterPtr& writer)
    {
        CloseFutures_.push_back(writer->Close());
    }

private:
    std::vector<IVersionedMultiChunkWriterPtr> Writers_;
    std::vector<TFuture<void>> CloseFutures_;


    void Initialize()
    {
        StoreWriterConfig_ = CloneYsonSerializable(TabletSnapshot_->Settings.StoreWriterConfig);
        StoreWriterConfig_->MinUploadReplicationFactor = StoreWriterConfig_->UploadReplicationFactor;
        StoreWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(WorkloadCategory_);

        StoreWriterOptions_ = CloneYsonSerializable(TabletSnapshot_->Settings.StoreWriterOptions);
        StoreWriterOptions_->ChunksEden = ResultsInEden_;
        StoreWriterOptions_->ValidateResourceUsageIncrease = false;
        StoreWriterOptions_->ConsistentChunkReplicaPlacementHash = TabletSnapshot_->ConsistentChunkReplicaPlacementHash;

        HunkWriterConfig_ = CloneYsonSerializable(TabletSnapshot_->Settings.HunkWriterConfig);
        HunkWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(WorkloadCategory_);
        HunkWriterConfig_->MinUploadReplicationFactor = HunkWriterConfig_->UploadReplicationFactor;

        HunkWriterOptions_ = CloneYsonSerializable(TabletSnapshot_->Settings.HunkWriterOptions);
        HunkWriterOptions_->ValidateResourceUsageIncrease = false;
        HunkWriterOptions_->ConsistentChunkReplicaPlacementHash = TabletSnapshot_->ConsistentChunkReplicaPlacementHash;

        Throttler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            Bootstrap_->GetTabletNodeOutThrottler(WorkloadCategory_),
            TabletSnapshot_->CompactionThrottler
        });

        HunkChunkWriter_ = CreateConfirmingWriter(
            HunkWriterConfig_,
            HunkWriterOptions_,
            CellTagFromId(TabletSnapshot_->TabletId),
            Transaction_->GetId(),
            /*parentChunkListId*/ {},
            New<NNodeTrackerClient::TNodeDirectory>(),
            Bootstrap_->GetMasterClient(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            Throttler_);

        HunkChunkPayloadWriter_ = CreateHunkChunkPayloadWriter(
            HunkWriterConfig_,
            HunkChunkWriter_);

        auto blockCacheFuture = CreateRemoteInMemoryBlockCache(
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetClusterNodeMasterConnector()->GetLocalDescriptor(),
            Bootstrap_->GetRpcServer(),
            Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetCellDirectory()
                ->GetDescriptorOrThrow(TabletSnapshot_->CellId),
            TabletSnapshot_->Settings.MountConfig->InMemoryMode,
            Bootstrap_->GetInMemoryManager()->GetConfig());

        BlockCache_ = WaitFor(blockCacheFuture)
            .ValueOrThrow();
    }

    void Finalize()
    {
        CloseFutures_.push_back(HunkChunkPayloadWriter_->Close());

        WaitFor(AllSucceeded(std::move(CloseFutures_)))
            .ThrowOnError();

        std::vector<TChunkInfo> chunkInfos;
        for (const auto& writer : Writers_) {
            for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
                chunkInfos.emplace_back(
                    FromProto<TChunkId>(chunkSpec.chunk_id()),
                    New<TRefCountedChunkMeta>(chunkSpec.chunk_meta()),
                    TabletSnapshot_->TabletId,
                    TabletSnapshot_->MountRevision);
            }
        }

        WaitFor(BlockCache_->Finish(chunkInfos))
            .ThrowOnError();
    }

    IVersionedChunkWriterPtr CreateUnderlyingWriterAdapter(IChunkWriterPtr underlyingWriter) const
    {
        return CreateHunkEncodingVersionedWriter(
            CreateVersionedChunkWriter(
                StoreWriterConfig_,
                StoreWriterOptions_,
                TabletSnapshot_->PhysicalSchema,
                std::move(underlyingWriter),
                BlockCache_),
            TabletSnapshot_->PhysicalSchema,
            HunkChunkPayloadWriter_);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEdenPartitioningResult
{
    struct TPartitionWriter
    {
        IVersionedMultiChunkWriterPtr Writer;
        int PartitionIndex;
    };

    std::vector<TPartitionWriter> PartitionStoreWriters;
    IHunkChunkPayloadWriterPtr HunkWriter;
    i64 RowCount;
};

class TEdenPartitioningSession
    : public TStoreCompactionSessionBase
{
public:
    TEdenPartitioningSession(
        NClusterNode::TBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        ITransactionPtr transaction,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ false,
            EWorkloadCategory::SystemTabletPartitioning,
            std::move(logger))
    { }

    TEdenPartitioningResult Run(
        const IVersionedReaderPtr& reader,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const TLegacyOwningKey& nextTabletPivotKey)
    {
        return DoRun([&] {
            int currentPartitionIndex = 0;
            TLegacyOwningKey currentPivotKey;
            TLegacyOwningKey nextPivotKey;

            i64 currentPartitionRowCount = 0;
            i64 readRowCount = 0;
            i64 writtenRowCount = 0;

            IVersionedMultiChunkWriterPtr currentWriter;

            auto ensurePartitionStarted = [&] {
                if (currentWriter) {
                    return;
                }

                YT_LOG_INFO("Started writing partition (PartitionIndex: %v, Keys: %v .. %v)",
                    currentPartitionIndex,
                    currentPivotKey,
                    nextPivotKey);

                currentWriter = CreateWriter();
            };

            std::vector<TVersionedRow> outputRows;
            outputRows.reserve(MaxRowsPerWrite);

            auto addOutputRow = [&] (TVersionedRow row) {
                outputRows.push_back(row);
            };

            auto flushOutputRows = [&] {
                if (outputRows.empty()) {
                    return;
                }

                writtenRowCount += outputRows.size();

                ensurePartitionStarted();
                if (!currentWriter->Write(outputRows)) {
                    WaitFor(currentWriter->GetReadyEvent())
                        .ThrowOnError();
                }

                outputRows.clear();
            };

            auto writeOutputRow = [&] (TVersionedRow row) {
                if (outputRows.size() == outputRows.capacity()) {
                    flushOutputRows();
                }
                addOutputRow(row);
                ++currentPartitionRowCount;
            };

            std::vector<TEdenPartitioningResult::TPartitionWriter> partitionWriters;

            auto flushPartition = [&] (int partitionIndex) {
                flushOutputRows();

                if (currentWriter) {
                    YT_LOG_INFO("Finished writing partition (PartitionIndex: %v, RowCount: %v)",
                        currentPartitionIndex,
                        currentPartitionRowCount);

                    CloseWriter(currentWriter);
                    partitionWriters.push_back({std::move(currentWriter), partitionIndex});
                    currentWriter.Reset();
                }

                currentPartitionRowCount = 0;
                ++currentPartitionIndex;
            };

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = MaxRowsPerRead
            };

            IVersionedRowBatchPtr inputBatch;
            TSharedRange<TVersionedRow> inputRows;
            int currentRowIndex = 0;

            auto peekInputRow = [&] {
                if (currentRowIndex >= std::ssize(inputRows)) {
                    flushOutputRows();
                    inputBatch = WaitForRowBatch(reader, readOptions);
                    if (!inputBatch) {
                        return TVersionedRow();
                    }
                    readRowCount += inputBatch->GetRowCount();
                    inputRows = inputBatch->MaterializeRows();
                    currentRowIndex = 0;
                }
                return inputRows[currentRowIndex];
            };

            auto skipInputRow = [&] {
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
                    YT_VERIFY(CompareRows(currentPivotKey.Begin(), currentPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0);

                    if (CompareRows(nextPivotKey.Begin(), nextPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0) {
                        break;
                    }

                    skipInputRow();
                    writeOutputRow(row);
                }

                flushPartition(it - pivotKeys.begin());
            }

            YT_VERIFY(readRowCount == writtenRowCount);

            return TEdenPartitioningResult{
                std::move(partitionWriters),
                HunkChunkPayloadWriter_,
                readRowCount
            };
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionCompactionResult
{
    IVersionedMultiChunkWriterPtr StoreWriter;
    IHunkChunkPayloadWriterPtr HunkWriter;
    i64 RowCount;
};

class TPartitionCompactionSession
    : public TStoreCompactionSessionBase
{
public:
    TPartitionCompactionSession(
        NClusterNode::TBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        TPartition* partition,
        ITransactionPtr transaction,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ partition->IsEden(),
            EWorkloadCategory::SystemTabletCompaction,
            std::move(logger))
    { }

    TPartitionCompactionResult Run(const IVersionedReaderPtr& reader)
    {
        return DoRun([&] {
            auto writer = CreateWriter();

            WaitFor(reader->Open())
                .ThrowOnError();

            i64 rowCount = 0;

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = MaxRowsPerRead
            };

            while (auto batch = WaitForRowBatch(reader, readOptions)) {
                rowCount += batch->GetRowCount();
                auto rows = batch->MaterializeRows();
                if (!writer->Write(rows)) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            }

            CloseWriter(writer);

            return TPartitionCompactionResult{
                writer,
                HunkChunkPayloadWriter_,
                rowCount
            };
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

/*!
 * Ultimately, the goal of the compactor is to control the overlapping store count
 * by performing compactions and partitionings. A compaction operates within a partition,
 * replacing a set of stores with a newly baked one. A partitioning operates on the Eden,
 * replacing a set of Eden stores with a set of partition-bound stores.
 */
class TStoreCompactor
    : public IStoreCompactor
{
public:
    explicit TStoreCompactor(NClusterNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->TabletNode->StoreCompactor)
        , ThreadPool_(New<TThreadPool>(Config_->ThreadPoolSize, "StoreCompact"))
        , PartitioningSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentPartitionings,
            Profiler_.Gauge("/running_partitionings")))
        , CompactionSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentCompactions,
            Profiler_.Gauge("/running_compactions")))
        , OrchidService_(CreateOrchidService())
    { }

    virtual void Start() override
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TStoreCompactor::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual void OnBeginSlotScan() override
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(ScanSpinLock_);

        // Save some scheduling resources by skipping unnecessary work.
        ScanForPartitioning_ = PartitioningSemaphore_->IsReady();
        ScanForCompactions_ = CompactionSemaphore_->IsReady();
        PartitioningCandidates_.clear(); // Though must be clear already.
        CompactionCandidates_.clear(); // Though must be clear already.
    }

    virtual void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) override
    {
        TEventTimerGuard timerGuard(ScanTimer_);

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->StoreCompactor;
        if (!dynamicConfig->Enable) {
            return;
        }

        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& Logger = TabletNodeLogger;

        YT_LOG_DEBUG("Store compactor started processing action batch (CellId: %v)",
            slot->GetCellId());

        if (ScanForCompactions_) {
            const auto& Logger = TabletNodeLogger
                .WithTag("CellId: %v, TaskKind: Compaction", slot->GetCellId());

            std::vector<std::unique_ptr<TTask>> tasks;
            for (const auto& request : batch.Compactions) {
                auto* eventType = request.DiscardStores
                    ? "discard_stores_candidate"
                    : "compaction_candidate";
                if (auto task = MakeTask(slot.Get(), request, eventType, Logger)) {
                    tasks.push_back(std::move(task));
                }
            }

            auto guard = Guard(ScanSpinLock_);
            CompactionCandidates_.insert(
                CompactionCandidates_.end(),
                std::make_move_iterator(tasks.begin()),
                std::make_move_iterator(tasks.end()));
        }

        if (ScanForPartitioning_) {
            const auto& Logger = TabletNodeLogger
                .WithTag("CellId: %v, TaskKind: Partitioning", slot->GetCellId());

            std::vector<std::unique_ptr<TTask>> tasks;
            for (const auto& request : batch.Partitionings) {
                if (auto task = MakeTask(
                        slot.Get(),
                        request,
                        "partitioning_candidate",
                        Logger))
                {
                    tasks.push_back(std::move(task));
                }
            }

            auto guard = Guard(ScanSpinLock_);
            PartitioningCandidates_.insert(
                PartitioningCandidates_.end(),
                std::make_move_iterator(tasks.begin()),
                std::make_move_iterator(tasks.end()));
        }

        YT_LOG_DEBUG("Store compactor finished processing action batch (CellId: %v)",
            slot->GetCellId());
    }

    void OnEndSlotScan()
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(ScanSpinLock_);

        if (ScanForPartitioning_) {
            PickMorePartitionings(guard);
            ScheduleMorePartitionings();
        }

        if (ScanForCompactions_) {
            PickMoreCompactions(guard);
            ScheduleMoreCompactions();
        }
    }


    virtual IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TStoreCompactorConfigPtr Config_;

    const TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/store_compactor");
    const TGauge FeasiblePartitioningsCounter_ = Profiler_.Gauge("/feasible_partitionings");
    const TGauge FeasibleCompactionsCounter_ = Profiler_.Gauge("/feasible_compactions");
    const TCounter ScheduledPartitioningsCounter_ = Profiler_.Counter("/scheduled_partitionings");
    const TCounter ScheduledCompactionsCounter_ = Profiler_.Counter("/scheduled_compactions");
    const TEventTimer ScanTimer_ = Profiler_.Timer("/scan_time");

    const TThreadPoolPtr ThreadPool_;
    const TAsyncSemaphorePtr PartitioningSemaphore_;
    const TAsyncSemaphorePtr CompactionSemaphore_;

    IYPathServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .Value(true);
            }))
            ->AddChild("compaction_tasks", IYPathService::FromProducer(
                BIND(&TOrchidServiceManager::GetTasks, MakeWeak(CompactionOrchidServiceManager_))))
            ->AddChild("partitioning_tasks", IYPathService::FromProducer(
                BIND(&TOrchidServiceManager::GetTasks, MakeWeak(PartitioningOrchidServiceManager_))))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    template <typename T>
    static auto GetOrderingTuple(const T& task)
    {
        return std::make_tuple(
            !task->DiscardStores,
            task->Slack + task->FutureEffect,
            -task->Effect,
            -task->GetStoreCount(),
            task->Random);
    }

    struct TTask
    {
        ITabletSlotPtr Slot;
        IInvokerPtr Invoker;

        TTabletId TabletId;
        TRevision MountRevision;
        TString TabletLoggingTag;
        TPartitionId PartitionId;
        std::vector<TStoreId> StoreIds;

        // True if the all chunks should be discarded. The task does not
        // require reading and writing chunks.
        bool DiscardStores = false;
        // Overlapping stores slack for the task.
        // That is, the remaining number of stores in the partition till
        // the tablet hits MOSC limit.
        // Small values indicate that the tablet is in a critical state.
        int Slack = 0;
        // Guaranteed effect on the slack if this task will be done.
        // This is a conservative estimate.
        int Effect = 0;
        // Future effect on the slack due to concurrent tasks.
        // This quantity is memoized to provide a valid comparison operator.
        int FutureEffect = 0;
        // A random number to deterministically break ties.
        ui64 Random = RandomNumber<size_t>();

        // These fields are filled upon task invocation.
        TWeakPtr<TStoreCompactor> Owner;
        TAsyncSemaphoreGuard SemaphoreGuard;

        TTask() = delete;
        TTask(const TTask&) = delete;
        TTask& operator=(const TTask&) = delete;
        TTask(TTask&&) = delete;
        TTask& operator=(TTask&&) = delete;

        TTask(
            ITabletSlot* slot,
            const TTablet* tablet,
            TPartitionId partitionId,
            std::vector<TStoreId> stores)
            : Slot(slot)
            , Invoker(tablet->GetEpochAutomatonInvoker())
            , TabletId(tablet->GetId())
            , MountRevision(tablet->GetMountRevision())
            , TabletLoggingTag(tablet->GetLoggingTag())
            , PartitionId(partitionId)
            , StoreIds(std::move(stores))
        { }

        ~TTask()
        {
            if (auto owner = Owner.Lock()) {
                owner->ChangeFutureEffect(TabletId, -Effect);
            }
        }

        auto GetStoreCount()
        {
            return StoreIds.size();
        }

        void Prepare(TStoreCompactor* owner, TAsyncSemaphoreGuard&& semaphoreGuard)
        {
            Owner = MakeWeak(owner);
            SemaphoreGuard = std::move(semaphoreGuard);

            owner->ChangeFutureEffect(TabletId, Effect);
        }

        void StoreToStructuredLog(TFluentMap fluent)
        {
            fluent
                .Item("partition_id").Value(PartitionId)
                .Item("store_ids").DoListFor(StoreIds, [&] (TFluentList fluent, TStoreId storeId) {
                    fluent
                        .Item().Value(storeId);
                })
                .Item("discard_stores").Value(DiscardStores)
                .Item("slack").Value(Slack)
                .Item("effect").Value(Effect)
                .Item("future_effect").Value(FutureEffect)
                .Item("random").Value(Random);
        }

        static inline bool Comparer(
            const std::unique_ptr<TTask>& lhs,
            const std::unique_ptr<TTask>& rhs)
        {
            return TStoreCompactor::GetOrderingTuple(lhs) < TStoreCompactor::GetOrderingTuple(rhs);
        }
    };

    class TOrchidServiceManager
        : public TRefCounted
    {
    public:
        void MarkTaskAsFinished()
        {
            auto guard = Guard(QueueSpinLock_);

            YT_VERIFY(FinishedTaskQueue_.size() <= FinishedQueueSize);
            YT_VERIFY(!TaskQueue_.empty());

            if (FinishedTaskQueue_.size() == FinishedQueueSize) {
                FinishedTaskQueue_.pop_front();
            }

            FinishedTaskQueue_.push_back(TaskQueue_.front());
            TaskQueue_.pop_front();
        }

        void ResetTaskQueue(
            std::vector<std::unique_ptr<TTask>>::iterator begin,
            std::vector<std::unique_ptr<TTask>>::iterator end)
        {
            auto guard = Guard(QueueSpinLock_);

            TaskQueue_.clear();
            for (auto it = begin; it != end; ++it) {
                TaskQueue_.push_back(New<TTaskInfo>(*it->get()));
            }
        }

        void GetTasks(IYsonConsumer* consumer) const
        {
            auto guard = Guard(QueueSpinLock_);
            auto taskQueue = TaskQueue_;
            auto finishedTaskQueue = FinishedTaskQueue_;
            guard.Release();

            std::sort(taskQueue.begin(), taskQueue.end(), Comparer);

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("task_count").Value(taskQueue.size())
                    .Item("finished_task_count").Value(finishedTaskQueue.size())
                    .Item("pending_tasks")
                        .DoListFor(
                            taskQueue,
                            [&](TFluentList fluent, const auto& task) {
                                task->Serialize(fluent);
                            })
                    .Item("finished_tasks")
                        .DoListFor(
                            finishedTaskQueue,
                            [&](TFluentList fluent, const auto& task) {
                                task->Serialize(fluent);
                            })
                .EndMap();
        }

    private:
        struct TTaskInfo
            : public TRefCounted
        {
            explicit TTaskInfo(const TTask& task)
                : TabletId(task.TabletId)
                , MountRevision(task.MountRevision)
                , PartitionId(task.PartitionId)
                , StoreCount(task.StoreIds.size())
                , DiscardStores(task.DiscardStores)
                , Slack(task.Slack)
                , Effect(task.Effect)
                , FutureEffect(task.FutureEffect)
                , Random(task.Random)
            { }

            TTabletId TabletId;
            TRevision MountRevision;
            TPartitionId PartitionId;
            int StoreCount;

            bool DiscardStores;
            int Slack;
            int Effect;
            int FutureEffect;
            ui64 Random;

            auto GetStoreCount()
            {
                return StoreCount;
            }

            void Serialize(TFluentList fluent) const
            {
                fluent.Item()
                .BeginMap()
                    .Item("tablet_id").Value(TabletId)
                    .Item("mount_revision").Value(MountRevision)
                    .Item("partition_id").Value(PartitionId)
                    .Item("store_count").Value(StoreCount)
                    .Item("task_priority")
                        .BeginMap()
                            .Item("discard_stores").Value(DiscardStores)
                            .Item("slack").Value(Slack)
                            .Item("future_effect").Value(FutureEffect)
                            .Item("effect").Value(Effect)
                            .Item("random").Value(Random)
                        .EndMap()
                 .EndMap();
            }
        };

        using TTaskInfoPtr = TIntrusivePtr<TTaskInfo>;

        YT_DECLARE_SPINLOCK(TAdaptiveLock, QueueSpinLock_);
        std::deque<TTaskInfoPtr> TaskQueue_;
        std::deque<TTaskInfoPtr> FinishedTaskQueue_;

        static inline bool Comparer(const TTaskInfoPtr& lhs, const TTaskInfoPtr& rhs)
        {
            return TStoreCompactor::GetOrderingTuple(lhs) < TStoreCompactor::GetOrderingTuple(rhs);
        }
    };

    using TOrchidServiceManagerPtr = TIntrusivePtr<TOrchidServiceManager>;

    // Variables below contain per-iteration state for slot scan.
    YT_DECLARE_SPINLOCK(TAdaptiveLock, ScanSpinLock_);
    bool ScanForPartitioning_;
    bool ScanForCompactions_;
    std::vector<std::unique_ptr<TTask>> PartitioningCandidates_;
    std::vector<std::unique_ptr<TTask>> CompactionCandidates_;

    // Variables below are actually used during the scheduling.
    YT_DECLARE_SPINLOCK(TAdaptiveLock, TaskSpinLock_);
    std::vector<std::unique_ptr<TTask>> PartitioningTasks_; // Min-heap.
    size_t PartitioningTaskIndex_ = 0; // Heap end boundary.
    std::vector<std::unique_ptr<TTask>> CompactionTasks_; // Min-heap.
    size_t CompactionTaskIndex_ = 0; // Heap end boundary.

    // These are for the future accounting.
    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, FutureEffectLock_);
    THashMap<TTabletId, int> FutureEffect_;

    const TOrchidServiceManagerPtr CompactionOrchidServiceManager_ = New<TOrchidServiceManager>();
    const TOrchidServiceManagerPtr PartitioningOrchidServiceManager_ = New<TOrchidServiceManager>();
    IYPathServicePtr OrchidService_;


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreCompactor;
        ThreadPool_->Configure(config->ThreadPoolSize.value_or(Config_->ThreadPoolSize));
        PartitioningSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentPartitionings));
        CompactionSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentCompactions));
    }

    std::unique_ptr<TTask> MakeTask(
        ITabletSlot* slot,
        const NLsm::TCompactionRequest& request,
        TStringBuf eventType,
        const NLogging::TLogger& Logger)
    {
        const auto& tabletManager = slot->GetTabletManager();
        const auto* tablet = tabletManager->FindTablet(request.Tablet->GetId());
        if (!tablet) {
            YT_LOG_DEBUG("Task declined: tablet is missing (TabletId: %v)",
                request.Tablet->GetId());
            return nullptr;
        }
        if (tablet->GetMountRevision() != request.Tablet->GetMountRevision()) {
            YT_LOG_DEBUG("Task declined: mount revision mismatch (TabletId: %v, "
                "ActualMountRevision: %v, RequestMountRevision: %v)",
                request.Tablet->GetId(),
                tablet->GetMountRevision(),
                request.Tablet->GetMountRevision());
            return nullptr;
        }

        auto task = std::make_unique<TTask>(
            slot,
            tablet,
            request.PartitionId,
            request.Stores);
        task->Slack = request.Slack;
        task->Effect = request.Effect;
        task->DiscardStores = request.DiscardStores;
        task->FutureEffect = GetFutureEffect(tablet->GetId());

        tablet->GetStructuredLogger()->LogEvent(eventType)
            .Do(BIND(&TTask::StoreToStructuredLog, task.get()));

        return task;
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

    void PickMoreTasks(
        std::vector<std::unique_ptr<TTask>>* candidates,
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TOrchidServiceManagerPtr& orchidServiceManager,
        const NProfiling::TGauge& counter)
    {
        counter.Update(candidates->size());

        MakeHeap(candidates->begin(), candidates->end(), TTask::Comparer);

        {
            auto guard = Guard(TaskSpinLock_);
            orchidServiceManager->ResetTaskQueue(candidates->begin(), candidates->end());
            tasks->swap(*candidates);
            *index = tasks->size();
        }

        candidates->clear();
    }

    void PickMorePartitionings(TSpinlockGuard<TAdaptiveLock>& /*guard*/)
    {
        PickMoreTasks(
            &PartitioningCandidates_,
            &PartitioningTasks_,
            &PartitioningTaskIndex_,
            PartitioningOrchidServiceManager_,
            FeasiblePartitioningsCounter_);
    }

    void PickMoreCompactions(TSpinlockGuard<TAdaptiveLock>& /*guard*/)
    {
        PickMoreTasks(
            &CompactionCandidates_,
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchidServiceManager_,
            FeasibleCompactionsCounter_);
    }

    void ScheduleMoreTasks(
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TOrchidServiceManagerPtr& orchidServiceManager,
        const TAsyncSemaphorePtr& semaphore,
        const TCounter& counter,
        void (TStoreCompactor::*action)(TTask*))
    {
        auto taskGuard = Guard(TaskSpinLock_);

        size_t scheduled = 0;

        while (true) {
            if (*index == 0) {
                break;
            }

            auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(semaphore);
            if (!semaphoreGuard) {
                break;
            }

            // Check if we have to fix the heap. If the smallest element is okay, we just keep going.
            // Hopefully, we will rarely decide to operate on the same tablet.
            {
                auto guard = ReaderGuard(FutureEffectLock_);
                auto&& firstTask = tasks->at(0);
                if (firstTask->FutureEffect != LockedGetFutureEffect(guard, firstTask->TabletId)) {
                    for (size_t i = 0; i < *index; ++i) {
                        auto&& task = (*tasks)[i];
                        task->FutureEffect = LockedGetFutureEffect(guard, task->TabletId);
                    }
                    guard.Release();
                    MakeHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
                    orchidServiceManager->ResetTaskQueue(tasks->begin(), tasks->begin() + *index);
                }
            }

            // Extract the next task.
            ExtractHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
            --(*index);
            auto&& task = tasks->at(*index);
            task->Prepare(this, std::move(semaphoreGuard));
            ++scheduled;
            orchidServiceManager->MarkTaskAsFinished();

            // TODO(sandello): Better ownership management.
            auto invoker = task->Invoker;
            invoker->Invoke(BIND(action, MakeStrong(this), Owned(task.release())));
        }

        if (scheduled > 0) {
            counter.Increment(scheduled);
        }
    }

    void ScheduleMorePartitionings()
    {
        ScheduleMoreTasks(
            &PartitioningTasks_,
            &PartitioningTaskIndex_,
            PartitioningOrchidServiceManager_,
            PartitioningSemaphore_,
            ScheduledPartitioningsCounter_,
            &TStoreCompactor::PartitionEden);
    }

    void ScheduleMoreCompactions()
    {
        ScheduleMoreTasks(
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchidServiceManager_,
            CompactionSemaphore_,
            ScheduledCompactionsCounter_,
            &TStoreCompactor::CompactPartition);
    }

    int LockedGetFutureEffect(NConcurrency::TSpinlockReaderGuard<TReaderWriterSpinLock>&, TTabletId tabletId)
    {
        auto it = FutureEffect_.find(tabletId);
        return it != FutureEffect_.end() ? it->second : 0;
    }

    int GetFutureEffect(TTabletId tabletId)
    {
        auto guard = ReaderGuard(FutureEffectLock_);
        return LockedGetFutureEffect(guard, tabletId);
    }

    void ChangeFutureEffect(TTabletId tabletId, int delta)
    {
        if (delta == 0) {
            return;
        }
        auto guard = WriterGuard(FutureEffectLock_);
        auto pair = FutureEffect_.emplace(tabletId, delta);
        if (!pair.second) {
            pair.first->second += delta;
        }
        const auto& Logger = TabletNodeLogger;
        YT_LOG_DEBUG("Accounting for the future effect of the compaction/partitioning (TabletId: %v, FutureEffect: %v -> %v)",
            tabletId,
            pair.first->second - delta,
            pair.first->second);
        if (pair.first->second == 0) {
            FutureEffect_.erase(pair.first);
        }
    }

    NNative::ITransactionPtr StartMasterTransaction(const TTabletSnapshotPtr& tabletSnapshot, const TString& title)
    {
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", title);
        auto transactionFuture = Bootstrap_->GetMasterClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            TTransactionStartOptions{
                .AutoAbort = false,
                .Attributes = std::move(transactionAttributes),
                .CoordinatorMasterCellTag = CellTagFromId(tabletSnapshot->TabletId),
                .ReplicateToMasterCellTags = TCellTagList()
            });
        return WaitFor(transactionFuture)
            .ValueOrThrow();
    }

    NNative::ITransactionPtr StartCompactionTransaction(
        const TTabletSnapshotPtr& tabletSnapshot,
        NLogging::TLogger* logger)
    {
        const auto& Logger = *logger;

        YT_LOG_INFO("Creating partition compaction transaction");

        auto transaction = StartMasterTransaction(tabletSnapshot, Format("Partition compaction: table %v, tablet %v",
            tabletSnapshot->TablePath,
            tabletSnapshot->TabletId));

        logger->AddTag("TransactionId: %v", transaction->GetId());

        YT_LOG_INFO("Partition compaction transaction created");

        return transaction;
    }

    NNative::ITransactionPtr StartPartitioningTransaction(
        const TTabletSnapshotPtr& tabletSnapshot,
        NLogging::TLogger* logger)
    {
        const auto& Logger = *logger;

        YT_LOG_INFO("Creating Eden partitioning transaction");

        auto transaction = StartMasterTransaction(tabletSnapshot, Format("Eden partitioning: table %v, tablet %v",
            tabletSnapshot->TablePath,
            tabletSnapshot->TabletId));

        logger->AddTag("TransactionId: %v", transaction->GetId());

        YT_LOG_INFO("Eden partitioning transaction created");

        return transaction;
    }

    void FinishTabletStoresUpdateTransaction(
        TTablet* tablet,
        const ITabletSlotPtr& slot,
        NTabletServer::NProto::TReqUpdateTabletStores actionRequest,
        NNative::ITransactionPtr transaction,
        const NLogging::TLogger& Logger)
    {
        tablet->ThrottleTabletStoresUpdate(slot, Logger);

        ToProto(actionRequest.mutable_tablet_id(), tablet->GetId());
        actionRequest.set_mount_revision(tablet->GetMountRevision());

        auto actionData = MakeTransactionActionData(actionRequest);
        auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));

        transaction->AddAction(masterCellId, actionData);
        transaction->AddAction(slot->GetCellId(), actionData);

        const auto& tabletManager = slot->GetTabletManager();
        WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
            .ThrowOnError();
    }

    void PartitionEden(TTask* task)
    {
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning),
            .ReadSessionId = TReadSessionId::Create()
        };

        auto Logger = TabletNodeLogger
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMorePartitionings();
        });

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting partitioning");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(task->TabletId, task->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting partitioning");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_partitioning")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->PartitionId);
        };


        auto* eden = tablet->GetEden();
        if (eden->GetId() != task->PartitionId) {
            YT_LOG_DEBUG("Eden is missing, aborting partitioning");
            logFailure("eden_missing");
            return;
        }

        if (eden->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG("Eden is in improper state, aborting partitioning (EdenState: %v)", eden->GetState());
            logFailure("improper_state")
                .Item("state").Value(eden->GetState());
            return;
        }

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task->StoreIds.size());
        for (const auto& storeId : task->StoreIds) {
            auto store = tablet->FindStore(storeId);
            if (!store || !eden->Stores().contains(store->AsSorted())) {
                YT_LOG_DEBUG("Eden store is missing, aborting partitioning (StoreId: %v)", storeId);
                logFailure("store_missing")
                    .Item("store_id").Value(storeId);
                return;
            }
            auto typedStore = store->AsSortedChunk();
            YT_VERIFY(typedStore);
            if (typedStore->GetCompactionState() != EStoreCompactionState::None) {
                YT_LOG_DEBUG("Eden store is in improper state, aborting partitioning (StoreId: %v, CompactionState: %v)",
                    storeId,
                    typedStore->GetCompactionState());
                logFailure("improper_store_state")
                    .Item("store_id").Value(storeId)
                    .Item("store_compaction_state").Value(typedStore->GetCompactionState());
                return;
            }
            stores.push_back(std::move(typedStore));
        }

        std::vector<TLegacyOwningKey> pivotKeys;
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        for (const auto& partition : tablet->PartitionList()) {
            if (!mountConfig->EnablePartitionSplitWhileEdenPartitioning &&
                partition->GetState() == EPartitionState::Splitting)
            {
                YT_LOG_DEBUG("Other partition is splitting, aborting eden partitioning (PartitionId: %v)",
                     partition->GetId());
                logFailure("other_partition_splitting")
                    .Item("other_partition_id").Value(partition->GetId());
                return;
            }
            pivotKeys.push_back(partition->GetPivotKey());
        }

        YT_VERIFY(tablet->GetPivotKey() == pivotKeys[0]);

        eden->CheckedSetState(EPartitionState::Normal, EPartitionState::Partitioning);

        IVersionedReaderPtr reader;
        TEdenPartitioningResult partitioningResult;

        auto readerProfiler = New<TReaderProfiler>();
        auto writerProfiler = New<TWriterProfiler>();

        bool failed = false;

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetCompressedDataSize();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            NProfiling::TWallTimer timer;
            eden->SetCompactionTime(timer.GetStartTime());

            structuredLogger->LogEvent("start_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp);

            YT_LOG_INFO("Eden partitioning started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "PartitionCount: %v, CompressedDataSize: %v, "
                "ChunkCount: %v, CurrentTimestamp: %llx, RetentionConfig: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                pivotKeys.size(),
                dataSize,
                stores.size(),
                currentTimestamp,
                ConvertTo<TRetentionConfigPtr>(mountConfig));

            reader = CreateReader(
                tablet,
                tabletSnapshot,
                stores,
                chunkReadOptions,
                currentTimestamp,
                // NB: No major compaction during Eden partitioning.
                /*majorTimestamp*/ MinTimestamp,
                Logger);

            auto transaction = StartPartitioningTransaction(tabletSnapshot, &Logger);

            auto partitioningSession = New<TEdenPartitioningSession>(
                Bootstrap_,
                tabletSnapshot,
                transaction,
                Logger);

            auto parititioningResultFuture =
                BIND(
                    &TEdenPartitioningSession::Run,
                    partitioningSession,
                    reader,
                    pivotKeys,
                    tablet->GetNextPivotKey())
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            partitioningResult = WaitFor(parititioningResultFuture)
                .ValueOrThrow();
            const auto& partitionWriters = partitioningResult.PartitionStoreWriters;

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Partitioning));
            for (const auto& [writer, partitionIndex] : partitionWriters) {
                AddStoresToAdd(&actionRequest, writer);
            }
            AddStoresToAdd(&actionRequest, partitioningResult.HunkWriter);
            AddStoresToRemove(&actionRequest, stores);

            std::vector<TStoreId> storeIdsToAdd;
            for (const auto& [writer, partitionIndex] : partitionWriters) {
                for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
                    storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
                }
                tabletSnapshot->PerformanceCounters->PartitioningDataWeightCount += writer->GetDataStatistics().data_weight();
            }

            YT_LOG_INFO("Eden partitioning completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v%v, WallTime: %v)",
                partitioningResult.RowCount,
                storeIdsToAdd,
                MakeFormattableView(stores, TStoreIdFormatter()),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (partitioningResult.HunkWriter->HasHunks()) {
                        builder->AppendFormat(", HunkChunkIdToAdd: %v",
                            partitioningResult.HunkWriter->GetChunkId());
                    }
                }),
                timer.GetElapsedTime());

            structuredLogger->LogEvent("end_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("stores_to_add").DoList([&] (auto fluent) {
                    for (const auto& [writer, partitionIndex] : partitionWriters) {
                        for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
                            auto chunkId = FromProto<TStoreId>(chunkSpec.chunk_id());
                            fluent
                                .Item().BeginMap()
                                    .Item("chunk_id").Value(chunkId)
                                    .Item("partition_index").Value(partitionIndex)
                                .EndMap();
                        }
                    }
                })
                .DoIf(partitioningResult.HunkWriter->HasHunks(), [&] (auto fluent) {
                    fluent
                        .Item("hunk_chunk_id_to_add").Value(partitioningResult.HunkWriter->GetChunkId());
                })
                .Item("output_row_count").Value(partitioningResult.RowCount);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Partitioning].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Partitioning);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Partitioning].Store(error);
            YT_LOG_ERROR(error, "Error partitioning Eden, backing off");

            structuredLogger->LogEvent("backoff_partitioning")
                .Item("partition_id").Value(eden->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            failed = true;
        }

        for (const auto& [writer, _] : partitioningResult.PartitionStoreWriters) {
            writerProfiler->Update(writer);
        }
        // TODO(babenko): update with hunk writer
        readerProfiler->Update(reader, chunkReadOptions.ChunkReaderStatistics);

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Partitioning, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Partitioning, failed);

        eden->CheckedSetState(EPartitionState::Partitioning, EPartitionState::Normal);
    }

    void DiscardPartitionStores(
        TPartition* partition,
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITabletSlotPtr& slot,
        const std::vector<TSortedChunkStorePtr>& stores,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Discarding expired partition stores (PartitionId: %v)",
            partition->GetId());

        auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        try {
            for (const auto& store : stores) {
                storeManager->BeginStoreCompaction(store);
            }

            auto transaction = StartCompactionTransaction(tabletSnapshot, &Logger);

            auto retainedTimestamp = NullTimestamp;
            for (const auto& store : stores) {
                retainedTimestamp = std::max(retainedTimestamp, store->GetMaxTimestamp());
            }
            ++retainedTimestamp;

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));
            AddStoresToRemove(&actionRequest, stores);

            YT_LOG_INFO("Partition stores discarded by TTL (UnmergedRowCount: %v, CompressedDataSize: %v, StoreIdsToRemove: %v)",
                partition->GetUnmergedRowCount(),
                partition->GetCompressedDataSize(),
                MakeFormattableView(stores, TStoreIdFormatter()));

            structuredLogger->LogEvent("discard_stores")
                .Item("partition_id").Value(partition->GetId());

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(error);
            YT_LOG_ERROR(error, "Error discarding expired partition stores, backing off");

            structuredLogger->LogEvent("backoff_discard_stores")
                .Item("partition_id").Value(partition->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }

    void CompactPartition(TTask* task)
    {
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction),
            .ReadSessionId = TReadSessionId::Create()
        };

        auto Logger = TabletNodeLogger
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMoreCompactions();
        });

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting compaction");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(task->TabletId, task->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting compaction");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_compaction")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->PartitionId);
        };

        auto* partition = tablet->GetEden()->GetId() == task->PartitionId
            ? tablet->GetEden()
            : tablet->FindPartition(task->PartitionId);
        if (!partition) {
            YT_LOG_DEBUG("Partition is missing, aborting compaction");
            logFailure("partition_missing");
            return;
        }

        if (partition->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG("Partition is in improper state, aborting compaction (PartitionState: %v)", partition->GetState());
            logFailure("improper_state")
                .Item("state").Value(partition->GetState());
            return;
        }

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task->StoreIds.size());
        for (const auto& storeId : task->StoreIds) {
            auto store = tablet->FindStore(storeId);
            if (!store || !partition->Stores().contains(store->AsSorted())) {
                YT_LOG_DEBUG("Partition store is missing, aborting compaction (StoreId: %v)", storeId);
                logFailure("store_missing")
                    .Item("store_id").Value(storeId);

                return;
            }
            auto typedStore = store->AsSortedChunk();
            YT_VERIFY(typedStore);
            if (typedStore->GetCompactionState() != EStoreCompactionState::None) {
                YT_LOG_DEBUG("Partition store is in improper state, aborting compaction (StoreId: %v, CompactionState: %v)",
                    storeId,
                    typedStore->GetCompactionState());
                logFailure("improper_store_state")
                    .Item("store_id").Value(storeId)
                    .Item("store_compaction_state").Value(typedStore->GetCompactionState());
                return;
            }
            stores.push_back(std::move(typedStore));
        }

        if (task->DiscardStores) {
            DiscardPartitionStores(partition, tabletSnapshot, slot, stores, Logger);
            return;
        }

        Logger.AddTag("Eden: %v, PartitionRange: %v .. %v, PartitionId: %v",
            partition->IsEden(),
            partition->GetPivotKey(),
            partition->GetNextPivotKey(),
            partition->GetId());

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        IVersionedReaderPtr reader;
        TPartitionCompactionResult compactionResult;

        auto writerProfiler = New<TWriterProfiler>();
        auto readerProfiler = New<TReaderProfiler>();

        bool failed = false;

        try {
            i64 inputDataSize = 0;
            i64 inputRowCount = 0;
            for (const auto& store : stores) {
                inputDataSize += store->GetCompressedDataSize();
                inputRowCount += store->GetRowCount();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            NProfiling::TWallTimer timer;
            partition->SetCompactionTime(timer.GetStartTime());

            const auto& mountConfig = tablet->GetSettings().MountConfig;
            auto retainedTimestamp = std::min(
                InstantToTimestamp(TimestampToInstant(currentTimestamp).second - mountConfig->MinDataTtl).second,
                currentTimestamp);

            auto majorTimestamp = std::min(ComputeMajorTimestamp(partition, stores), retainedTimestamp);

            structuredLogger->LogEvent("start_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp)
                // NB: deducible.
                .Item("major_timestamp").Value(majorTimestamp)
                .Item("retained_timestamp").Value(retainedTimestamp);

            YT_LOG_INFO("Partition compaction started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "RowCount: %v, CompressedDataSize: %v, ChunkCount: %v, "
                "CurrentTimestamp: %llx, MajorTimestamp: %llx, RetainedTimestamp: %llx, RetentionConfig: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                inputRowCount,
                inputDataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp,
                retainedTimestamp,
                ConvertTo<TRetentionConfigPtr>(mountConfig));

            reader = CreateReader(
                tablet,
                tabletSnapshot,
                stores,
                chunkReadOptions,
                currentTimestamp,
                majorTimestamp,
                Logger);

            auto transaction = StartCompactionTransaction(tabletSnapshot, &Logger);

            auto compactionSession = New<TPartitionCompactionSession>(
                Bootstrap_,
                tabletSnapshot,
                partition,
                transaction,
                Logger);

            auto compactionResultFuture =
                BIND(
                    &TPartitionCompactionSession::Run,
                    compactionSession,
                    reader)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            compactionResult = WaitFor(compactionResultFuture)
                .ValueOrThrow();

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));
            AddStoresToAdd(&actionRequest, compactionResult.StoreWriter);
            AddStoresToAdd(&actionRequest, compactionResult.HunkWriter);
            AddStoresToRemove(&actionRequest, stores);

            std::vector<TStoreId> storeIdsToAdd;
            for (const auto& chunkSpec : compactionResult.StoreWriter->GetWrittenChunkSpecs()) {
                storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
            }
            tabletSnapshot->PerformanceCounters->CompactionDataWeightCount += compactionResult.StoreWriter->GetDataStatistics().data_weight();

            YT_LOG_INFO("Partition compaction completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v%v, WallTime: %v)",
                compactionResult.RowCount,
                storeIdsToAdd,
                MakeFormattableView(stores, TStoreIdFormatter()),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (compactionResult.HunkWriter->HasHunks()) {
                        builder->AppendFormat(", HunkChunkIdToAdd: %v",
                            compactionResult.HunkWriter->GetChunkId());
                    }
                }),
                timer.GetElapsedTime());

            structuredLogger->LogEvent("end_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("store_ids_to_add").List(storeIdsToAdd)
                .DoIf(compactionResult.HunkWriter->HasHunks(), [&] (auto fluent) {
                    fluent
                        .Item("hunk_chunk_id_to_add").Value(compactionResult.HunkWriter->GetChunkId());
                })
                .Item("output_row_count").Value(compactionResult.RowCount);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(error);
            YT_LOG_ERROR(error, "Error compacting partition, backing off");

            structuredLogger->LogEvent("backoff_compaction")
                .Item("partition_id").Value(partition->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            failed = true;
        }

        writerProfiler->Update(compactionResult.StoreWriter);
        // TODO(babenko): update with hunk writer
        readerProfiler->Update(reader, chunkReadOptions.ChunkReaderStatistics);

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Compaction, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Compaction, failed);

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }


    static bool IsHunkCompactionForced(
        const TTablet* tablet,
        const THunkChunkPtr& chunk)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto forcedCompactionRevision = std::max(
            mountConfig->ForcedCompactionRevision,
            mountConfig->ForcedHunkCompactionRevision);

        auto revision = CounterFromId(chunk->GetId());
        return revision <= forcedCompactionRevision.value_or(NHydra::NullRevision);
    }

    static bool IsHunkCompactionGarbageRatioTooHigh(
        const TTablet* tablet,
        const THunkChunkPtr& hunkChunk)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        return
            hunkChunk->GetTotalHunkLength() >= mountConfig->MinHunkCompactionTotalHunkLength &&
            static_cast<double>(hunkChunk->GetReferencedTotalHunkLength()) / hunkChunk->GetTotalHunkLength() < 1.0 - mountConfig->MaxHunkCompactionGarbageRatio;
    }

    static EHunkCompactionReason GetHunkCompactionReason(
        const TTablet* tablet,
        const THunkChunkPtr& hunkChunk)
    {
        if (IsHunkCompactionForced(tablet, hunkChunk)) {
            return EHunkCompactionReason::ForcedCompaction;
        }

        if (IsHunkCompactionGarbageRatioTooHigh(tablet, hunkChunk)) {
            return EHunkCompactionReason::GarbageRatioTooHigh;
        }

        return EHunkCompactionReason::None;
    }

    THashSet<TChunkId> PickCompactableHunkChunkIds(
        const TTablet* tablet,
        const TTabletSnapshotPtr& tabletSnapshot,
        const std::vector<TSortedChunkStorePtr>& stores,
        const NLogging::TLogger& logger)
    {
        const auto& Logger = logger;
        THashSet<TChunkId> result;
        for (const auto& store : stores) {
            for (const auto& hunkRef : store->HunkChunkRefs()) {
                const auto& hunkChunk = hunkRef.HunkChunk;
                auto compactionReason = GetHunkCompactionReason(tablet, hunkChunk);
                if (compactionReason != EHunkCompactionReason::None) {
                    if (result.insert(hunkChunk->GetId()).second) {
                        YT_LOG_DEBUG_IF(tabletSnapshot->Settings.MountConfig->EnableLsmVerboseLogging,
                            "Hunk chunk is picked for compaction (HunkChunkId: %v, Reason: %v)",
                            hunkChunk->GetId(),
                            compactionReason);
                    }
                }
            }
        }
        return result;
    }


    IVersionedReaderPtr CreateReader(
        TTablet* tablet,
        const TTabletSnapshotPtr& tabletSnapshot,
        const std::vector<TSortedChunkStorePtr>& stores,
        const TClientChunkReadOptions& chunkReadOptions,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp,
        const NLogging::TLogger& logger)
    {
        return CreateHunkInliningVersionedReader(
            tablet->GetSettings().HunkReaderConfig,
            CreateVersionedTabletReader(
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                majorTimestamp,
                chunkReadOptions,
                stores.size(),
                ETabletDistributedThrottlerKind::CompactionRead,
                chunkReadOptions.WorkloadDescriptor.Category),
            tablet->GetChunkFragmentReader(),
            tablet->GetPhysicalSchema(),
            PickCompactableHunkChunkIds(
                tablet,
                tabletSnapshot,
                stores,
                logger),
            chunkReadOptions);
    }


    static int GetOverlappingStoreLimit(const TTableMountConfigPtr& config)
    {
        return std::min(
            config->MaxOverlappingStoreCount,
            config->CriticalOverlappingStoreCount.value_or(config->MaxOverlappingStoreCount));
    }


    static void AddStoresToRemove(
        NTabletServer::NProto::TReqUpdateTabletStores* actionRequest,
        const std::vector<TSortedChunkStorePtr>& stores)
    {
        for (const auto& store : stores) {
            auto storeId = store->GetId();
            auto* descriptor = actionRequest->add_stores_to_remove();
            ToProto(descriptor->mutable_store_id(), storeId);
        }
    }

    static void AddStoresToAdd(
        NTabletServer::NProto::TReqUpdateTabletStores* actionRequest,
        const IVersionedMultiChunkWriterPtr& writer)
    {
        for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
            auto* descriptor = actionRequest->add_stores_to_add();
            descriptor->set_store_type(ToProto<int>(EStoreType::SortedChunk));
            *descriptor->mutable_store_id() = chunkSpec.chunk_id();
            *descriptor->mutable_chunk_meta() = chunkSpec.chunk_meta();
            FilterProtoExtensions(descriptor->mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
        }
    }

    static void AddStoresToAdd(
        NTabletServer::NProto::TReqUpdateTabletStores* actionRequest,
        const IHunkChunkPayloadWriterPtr& writer)
    {
        if (!writer->HasHunks()) {
            return;
        }

        auto* descriptor = actionRequest->add_hunk_chunks_to_add();
        ToProto(descriptor->mutable_chunk_id(), writer->GetChunkId());
        *descriptor->mutable_chunk_meta() = *writer->GetMeta();
        FilterProtoExtensions(descriptor->mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactor)

IStoreCompactorPtr CreateStoreCompactor(NClusterNode::TBootstrap* bootstrap)
{
    return New<TStoreCompactor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
