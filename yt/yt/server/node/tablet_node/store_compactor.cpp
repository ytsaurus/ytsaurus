#include "store_compactor.h"

#include "background_activity_orchid.h"
#include "bootstrap.h"
#include "hunk_chunk.h"
#include "in_memory_manager.h"
#include "partition.h"
#include "public.h"
#include "sorted_chunk_store.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

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
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

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

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NThreading;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;
using namespace NTracing;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 65536;
static const size_t MaxRowsPerWrite = 65536;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
auto GetOrderingTuple(const T& task)
{
    return std::tuple(
        !task.DiscardStores,
        task.Slack + task.FutureEffect,
        -task.Effect,
        -task.GetStoreCount(),
        task.Random);
}

class TTask;

////////////////////////////////////////////////////////////////////////////////

void SyncThrottleMediumWrite(
    const IBootstrap* bootstrap,
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::NProto::TDataStatistics& dataStatistics,
    const NChunkClient::NProto::TDataStatistics& hunkDataStatistics,
    const NLogging::TLogger& Logger)
{
    auto mediumThrottler = GetBlobMediumWriteThrottler(
        bootstrap->GetDynamicConfigManager(),
        tabletSnapshot);

    auto totalDiskSpace = CalculateDiskSpaceUsage(
        tabletSnapshot->Settings.StoreWriterOptions->ReplicationFactor,
        dataStatistics.regular_disk_space(),
        dataStatistics.erasure_disk_space());

    totalDiskSpace += CalculateDiskSpaceUsage(
        tabletSnapshot->Settings.HunkWriterOptions->ReplicationFactor,
        hunkDataStatistics.regular_disk_space(),
        hunkDataStatistics.erasure_disk_space());

    YT_LOG_DEBUG("Throttling blobs media write (DiskSpace: %v)",
        totalDiskSpace);

    WaitFor(mediumThrottler->Throttle(totalDiskSpace))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

struct TCompactionTaskInfo
    : public TTaskInfoBase
{
    TPartitionId PartitionId;
    ui64 Random;
    int StoreCount;
    bool DiscardStores;
    int Slack;
    int FutureEffect;
    int Effect;
    NLsm::EStoreCompactionReason Reason;

    int GetStoreCount() const
    {
        return StoreCount;
    }

    bool ComparePendingTasks(const TCompactionTaskInfo& other) const
    {
        return GetOrderingTuple(*this) < GetOrderingTuple(other);
    }
};

void Serialize(const TCompactionTaskInfo& task, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Do([&] (auto fluent) {
            Serialize(static_cast<const TTaskInfoBase&>(task), fluent.GetConsumer());
        })
        .Item("partition_id").Value(task.PartitionId)
        .Item("store_count").Value(task.StoreCount)
        .Item("task_priority")
            .BeginMap()
                .Item("discard_stores").Value(task.DiscardStores)
                .Item("slack").Value(task.Slack)
                .Item("future_effect").Value(task.FutureEffect)
                .Item("effect").Value(task.Effect)
                .Item("random").Value(task.Random)
            .EndMap()
        .Item("reason").Value(task.Reason)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

using TCompactionOrchid = TBackgroundActivityOrchid<TCompactionTaskInfo>;
using TCompactionOrchidPtr = TIntrusivePtr<TCompactionOrchid>;

DEFINE_REFCOUNTED_TYPE(TCompactionOrchid);

////////////////////////////////////////////////////////////////////////////////

struct TCompactionSessionFinalizeResult
{
    std::vector<NChunkClient::NProto::TDataStatistics> WriterStatistics;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactionSessionBase
    : public TRefCounted
{
protected:
    IBootstrap* const Bootstrap_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const ITransactionPtr Transaction_;
    const bool ResultsInEden_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const NLogging::TLogger Logger;

    TTabletStoreWriterConfigPtr StoreWriterConfig_;
    TTabletStoreWriterOptionsPtr StoreWriterOptions_;
    TTabletHunkWriterConfigPtr HunkWriterConfig_;
    TTabletHunkWriterOptionsPtr HunkWriterOptions_;

    IChunkWriterPtr HunkChunkWriter_;
    IHunkChunkPayloadWriterPtr HunkChunkPayloadWriter_;

    IHunkChunkWriterStatisticsPtr HunkChunkWriterStatistics_;

    IRemoteInMemoryBlockCachePtr BlockCache_;


    TStoreCompactionSessionBase(
        IBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        ITransactionPtr transaction,
        bool resultsInEden,
        TClientChunkReadOptions chunkReadOptions,
        NLogging::TLogger logger)
        : Bootstrap_(bootstrap)
        , TabletSnapshot_(std::move(tabletSnapshot))
        , Transaction_(std::move(transaction))
        , ResultsInEden_(resultsInEden)
        , ChunkReadOptions_(std::move(chunkReadOptions))
        , Logger(std::move(logger))
    { }

    template <class F>
    auto DoRun(const F& func) -> std::pair<decltype(func()), TCompactionSessionFinalizeResult>
    {
        Initialize();

        auto compactionResult = func();

        auto finalizeResult = Finalize();

        return std::pair(compactionResult, finalizeResult);
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
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            CellTagFromId(TabletSnapshot_->TabletId),
            Transaction_->GetId(),
            TabletSnapshot_->SchemaId,
            /*parentChunkListId*/ {},
            Bootstrap_->GetOutThrottler(ChunkReadOptions_.WorkloadDescriptor.Category),
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
        StoreWriterConfig_ = CloneYsonStruct(TabletSnapshot_->Settings.StoreWriterConfig);
        StoreWriterConfig_->MinUploadReplicationFactor = StoreWriterConfig_->UploadReplicationFactor;
        StoreWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(ChunkReadOptions_.WorkloadDescriptor.Category);

        StoreWriterOptions_ = CloneYsonStruct(TabletSnapshot_->Settings.StoreWriterOptions);
        StoreWriterOptions_->ChunksEden = ResultsInEden_;
        StoreWriterOptions_->ValidateResourceUsageIncrease = false;
        StoreWriterOptions_->ConsistentChunkReplicaPlacementHash = TabletSnapshot_->ConsistentChunkReplicaPlacementHash;
        StoreWriterOptions_->MemoryTracker = Bootstrap_->GetMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletBackground);
        StoreWriterOptions_->MemoryReferenceTracker = Bootstrap_->GetNodeMemoryReferenceTracker()->WithCategory(EMemoryCategory::TabletBackground);

        HunkWriterConfig_ = CloneYsonStruct(TabletSnapshot_->Settings.HunkWriterConfig);
        HunkWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(ChunkReadOptions_.WorkloadDescriptor.Category);
        HunkWriterConfig_->MinUploadReplicationFactor = HunkWriterConfig_->UploadReplicationFactor;

        HunkWriterOptions_ = CloneYsonStruct(TabletSnapshot_->Settings.HunkWriterOptions);
        HunkWriterOptions_->ValidateResourceUsageIncrease = false;
        HunkWriterOptions_->ConsistentChunkReplicaPlacementHash = TabletSnapshot_->ConsistentChunkReplicaPlacementHash;

        HunkChunkWriterStatistics_ = CreateHunkChunkWriterStatistics(
            TabletSnapshot_->Settings.MountConfig->EnableHunkColumnarProfiling,
            TabletSnapshot_->PhysicalSchema);

        HunkChunkWriter_ = CreateConfirmingWriter(
            HunkWriterConfig_,
            HunkWriterOptions_,
            CellTagFromId(TabletSnapshot_->TabletId),
            Transaction_->GetId(),
            TabletSnapshot_->SchemaId,
            /*parentChunkListId*/ {},
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetOutThrottler(ChunkReadOptions_.WorkloadDescriptor.Category));

        HunkChunkPayloadWriter_ = CreateHunkChunkPayloadWriter(
            TWorkloadDescriptor(ChunkReadOptions_.WorkloadDescriptor.Category),
            HunkWriterConfig_,
            HunkChunkWriter_);
        if (TabletSnapshot_->PhysicalSchema->HasHunkColumns()) {
            WaitFor(HunkChunkPayloadWriter_->Open())
                .ThrowOnError();
        }

        auto blockCacheFuture = CreateRemoteInMemoryBlockCache(
            Bootstrap_->GetClient(),
            Bootstrap_->GetControlInvoker(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetRpcServer(),
            Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetCellDirectory()
                ->GetDescriptorByCellIdOrThrow(TabletSnapshot_->CellId),
            TabletSnapshot_->Settings.MountConfig->InMemoryMode,
            Bootstrap_->GetInMemoryManager()->GetConfig());

        BlockCache_ = WaitFor(blockCacheFuture)
            .ValueOrThrow();
    }

    TCompactionSessionFinalizeResult Finalize()
    {
        WaitFor(AllSucceeded(std::move(CloseFutures_)))
            .ThrowOnError();

        TCompactionSessionFinalizeResult result;
        result.WriterStatistics.reserve(Writers_.size());

        if (TabletSnapshot_->PhysicalSchema->HasHunkColumns()) {
            WaitFor(HunkChunkPayloadWriter_->Close())
                .ThrowOnError();
        }

        std::vector<TChunkInfo> chunkInfos;
        for (const auto& writer : Writers_) {
            result.WriterStatistics.push_back(writer->GetDataStatistics());
            for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
                chunkInfos.push_back({
                    .ChunkId = FromProto<TChunkId>(chunkSpec.chunk_id()),
                    .ChunkMeta = New<TRefCountedChunkMeta>(chunkSpec.chunk_meta()),
                    .TabletId = TabletSnapshot_->TabletId,
                    .MountRevision = TabletSnapshot_->MountRevision
                });
            }
        }

        WaitFor(BlockCache_->Finish(chunkInfos))
            .ThrowOnError();

        if (TabletSnapshot_->Settings.MountConfig->RegisterChunkReplicasOnStoresUpdate) {
            const auto& chunkReplicaCache = Bootstrap_->GetConnection()->GetChunkReplicaCache();
            auto registerReplicas = [&] (TChunkId chunkId, const auto& writtenReplicas) {
                // TODO(kvk1920): Consider using chunk + location instead of chunk + node + medium.
                TChunkReplicaWithMediumList replicas;
                replicas.reserve(writtenReplicas.size());
                for (auto replica : writtenReplicas) {
                    replicas.push_back(replica);
                }

                chunkReplicaCache->RegisterReplicas(chunkId, replicas);
            };

            for (const auto& writer : Writers_) {
                for (const auto& [chunkId, replicas] : writer->GetWrittenChunkWithReplicasList()) {
                    registerReplicas(chunkId, replicas);
                }
            }

            if (HunkChunkPayloadWriter_->HasHunks()) {
                registerReplicas(HunkChunkWriter_->GetChunkId(), HunkChunkWriter_->GetWrittenChunkReplicas());
            }
        }

        return result;
    }

    IVersionedChunkWriterPtr CreateUnderlyingWriterAdapter(IChunkWriterPtr underlyingWriter) const
    {
        return CreateHunkEncodingVersionedWriter(
            CreateVersionedChunkWriter(
                StoreWriterConfig_,
                StoreWriterOptions_,
                TabletSnapshot_->PhysicalSchema,
                std::move(underlyingWriter),
                /*dataSink*/ std::nullopt,
                BlockCache_),
            TabletSnapshot_->PhysicalSchema,
            HunkChunkPayloadWriter_,
            HunkChunkWriterStatistics_,
            TabletSnapshot_->DictionaryCompressionFactory,
            ChunkReadOptions_);
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
    IHunkChunkWriterStatisticsPtr HunkWriterStatistics;
    i64 RowCount;
};

class TEdenPartitioningSession
    : public TStoreCompactionSessionBase
{
public:
    TEdenPartitioningSession(
        IBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        ITransactionPtr transaction,
        TClientChunkReadOptions chunkReadOptions,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ false,
            std::move(chunkReadOptions),
            std::move(logger))
    { }

    std::pair<TEdenPartitioningResult, TCompactionSessionFinalizeResult> Run(
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
                    inputBatch = ReadRowBatch(reader, readOptions);
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
                    YT_VERIFY(CompareValueRanges(currentPivotKey.Elements(), row.Keys()) <= 0);

                    if (CompareValueRanges(nextPivotKey.Elements(), row.Keys()) <= 0) {
                        break;
                    }

                    skipInputRow();
                    writeOutputRow(row);
                }

                flushPartition(it - pivotKeys.begin());
            }

            YT_VERIFY(readRowCount == writtenRowCount);

            return TEdenPartitioningResult{
                .PartitionStoreWriters = std::move(partitionWriters),
                .HunkWriter = HunkChunkPayloadWriter_,
                .HunkWriterStatistics = HunkChunkWriterStatistics_,
                .RowCount = readRowCount
            };
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionCompactionResult
{
    IVersionedMultiChunkWriterPtr StoreWriter;
    IHunkChunkPayloadWriterPtr HunkWriter;
    IHunkChunkWriterStatisticsPtr HunkWriterStatistics;
    i64 RowCount;
};

class TPartitionCompactionSession
    : public TStoreCompactionSessionBase
{
public:
    TPartitionCompactionSession(
        IBootstrap* bootstrap,
        TTabletSnapshotPtr tabletSnapshot,
        TPartition* partition,
        ITransactionPtr transaction,
        TClientChunkReadOptions chunkReadOptions,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ partition->IsEden(),
            std::move(chunkReadOptions),
            std::move(logger))
    { }

    std::pair<TPartitionCompactionResult, TCompactionSessionFinalizeResult>
    Run(const IVersionedReaderPtr& reader)
    {
        return DoRun([&] {
            auto writer = CreateWriter();

            WaitFor(reader->Open())
                .ThrowOnError();

            i64 rowCount = 0;

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = MaxRowsPerRead
            };

            while (auto batch = ReadRowBatch(reader, readOptions)) {
                rowCount += batch->GetRowCount();
                auto rows = batch->MaterializeRows();
                if (!writer->Write(rows)) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            }

            CloseWriter(writer);

            return TPartitionCompactionResult{
                .StoreWriter = writer,
                .HunkWriter = HunkChunkPayloadWriter_,
                .HunkWriterStatistics = HunkChunkWriterStatistics_,
                .RowCount = rowCount
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
    explicit TStoreCompactor(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->TabletNode->StoreCompactor)
        , ThreadPool_(CreateThreadPool(Config_->ThreadPoolSize, "StoreCompact"))
        , PartitioningSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentPartitionings,
            Profiler_.Gauge("/running_partitionings")))
        , CompactionSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentCompactions,
            Profiler_.Gauge("/running_compactions")))
        , CompactionOrchid_(New<TCompactionOrchid>(
            Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode->StoreCompactor->Orchid))
        , PartitioningOrchid_(New<TCompactionOrchid>(
            Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode->StoreCompactor->Orchid))
        , OrchidService_(CreateOrchidService())
    { }

    void Start() override
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TStoreCompactor::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void OnBeginSlotScan() override
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(ScanSpinLock_);

        // Save some scheduling resources by skipping unnecessary work.
        ScanForPartitioning_ = PartitioningSemaphore_->IsReady();
        ScanForCompactions_ = CompactionSemaphore_->IsReady();
        PartitioningCandidates_.clear(); // Though must be clear already.
        CompactionCandidates_.clear(); // Though must be clear already.
    }

    void ProcessLsmActionBatch(
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
                if (auto task = MakeTask(
                    slot.Get(),
                    request,
                    eventType,
                    ssize(tasks) < dynamicConfig->MaxCompactionStructuredLogEvents,
                    Logger))
                {
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
                    ssize(tasks) < dynamicConfig->MaxPartitioningStructuredLogEvents,
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

    void OnEndSlotScan() override
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


    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

private:
    IBootstrap* const Bootstrap_;
    const TStoreCompactorConfigPtr Config_;

    const TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/store_compactor");
    const TGauge FeasiblePartitioningsCounter_ = Profiler_.Gauge("/feasible_partitionings");
    const TGauge FeasibleCompactionsCounter_ = Profiler_.Gauge("/feasible_compactions");
    const TCounter ScheduledPartitioningsCounter_ = Profiler_.Counter("/scheduled_partitionings");
    const TCounter ScheduledCompactionsCounter_ = Profiler_.Counter("/scheduled_compactions");
    const TCounter FutureEffectMismatchesCounter_ = Profiler_.Counter("/future_effect_mismatches");
    const TEventTimer ScanTimer_ = Profiler_.Timer("/scan_time");

    const IThreadPoolPtr ThreadPool_;
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
                BIND(&TCompactionOrchid::Serialize, MakeWeak(CompactionOrchid_))))
            ->AddChild("partitioning_tasks", IYPathService::FromProducer(
                BIND(&TCompactionOrchid::Serialize, MakeWeak(PartitioningOrchid_))))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    struct TTask
    {
        ITabletSlotPtr Slot;
        IInvokerPtr Invoker;
        TCancelableContextPtr CancelableContext;

        TGuid TaskId;
        TTabletId TabletId;
        TRevision MountRevision;
        TString TabletLoggingTag;
        TString TablePath;
        TString TabletCellBundle;
        TPartitionId PartitionId;
        std::vector<TStoreId> StoreIds;
        NLsm::EStoreCompactionReason Reason;

        TEnumIndexedArray<EHunkCompactionReason, i64> HunkChunkCountByReason;

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
        TWeakPtr<TCompactionOrchid> Orchid;

        bool Failed = false;

        TTask() = delete;
        TTask(const TTask&) = delete;
        TTask& operator=(const TTask&) = delete;
        TTask(TTask&&) = delete;
        TTask& operator=(TTask&&) = delete;

        TTask(
            ITabletSlot* slot,
            const TTablet* tablet,
            TPartitionId partitionId,
            std::vector<TStoreId> stores,
            NLsm::EStoreCompactionReason reason)
            : Slot(slot)
            , Invoker(tablet->GetEpochAutomatonInvoker())
            , CancelableContext(tablet->GetCancelableContext())
            , TaskId(TGuid::Create())
            , TabletId(tablet->GetId())
            , MountRevision(tablet->GetMountRevision())
            , TabletLoggingTag(tablet->GetLoggingTag())
            , TablePath(tablet->GetTablePath())
            , TabletCellBundle(slot->GetTabletCellBundleName())
            , PartitionId(partitionId)
            , StoreIds(std::move(stores))
            , Reason(reason)
        { }

        ~TTask();

        int GetStoreCount() const
        {
            return ssize(StoreIds);
        }

        void Prepare(
            TStoreCompactor* owner,
            TCompactionOrchid* orchid,
            TAsyncSemaphoreGuard&& semaphoreGuard)
        {
            Owner = MakeWeak(owner);
            Orchid = MakeWeak(orchid);
            SemaphoreGuard = std::move(semaphoreGuard);

            owner->ChangeFutureEffect(TabletId, Effect);
            orchid->OnTaskStarted(TaskId);
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
                .Item("random").Value(Random)
                .Item("reason").Value(Reason);
        }

        TCompactionTaskInfo BuildTaskInfo() const
        {
            return TCompactionTaskInfo{
                TTaskInfoBase{
                    .TaskId = TaskId,
                    .TabletId = TabletId,
                    .MountRevision = MountRevision,
                    .TablePath = TablePath,
                    .TabletCellBundle = TabletCellBundle,
                },
                PartitionId,
                Random,
                static_cast<int>(ssize(StoreIds)),
                DiscardStores,
                Slack,
                FutureEffect,
                Effect,
                Reason,
            };
        }

        static bool Comparer(const std::unique_ptr<TTask>& lhs, const std::unique_ptr<TTask>& rhs)
        {
            return GetOrderingTuple(*lhs) < GetOrderingTuple(*rhs);
        }
    };

    // Variables below contain per-iteration state for slot scan.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ScanSpinLock_);
    bool ScanForPartitioning_;
    bool ScanForCompactions_;
    std::vector<std::unique_ptr<TTask>> PartitioningCandidates_;
    std::vector<std::unique_ptr<TTask>> CompactionCandidates_;

    // Variables below are actually used during the scheduling.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TaskSpinLock_);
    std::vector<std::unique_ptr<TTask>> PartitioningTasks_; // Min-heap.
    size_t PartitioningTaskIndex_ = 0; // Heap end boundary.
    std::vector<std::unique_ptr<TTask>> CompactionTasks_; // Min-heap.
    size_t CompactionTaskIndex_ = 0; // Heap end boundary.

    // These are for the future accounting.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, FutureEffectLock_);
    THashMap<TTabletId, int> FutureEffect_;

    const TCompactionOrchidPtr CompactionOrchid_;
    const TCompactionOrchidPtr PartitioningOrchid_;
    const IYPathServicePtr OrchidService_;


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreCompactor;
        ThreadPool_->Configure(config->ThreadPoolSize.value_or(Config_->ThreadPoolSize));
        PartitioningSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentPartitionings));
        CompactionSemaphore_->SetTotal(config->MaxConcurrentCompactions.value_or(Config_->MaxConcurrentCompactions));
        PartitioningOrchid_->Reconfigure(config->Orchid);
        CompactionOrchid_->Reconfigure(config->Orchid);
    }

    std::unique_ptr<TTask> MakeTask(
        ITabletSlot* slot,
        const NLsm::TCompactionRequest& request,
        TStringBuf eventType,
        bool logStructured,
        const NLogging::TLogger& Logger)
    {
        const auto& tabletManager = slot->GetTabletManager();

        const auto* tablet = tabletManager->FindTablet(request.Tablet->GetId());
        if (!tablet) {
            YT_LOG_DEBUG("Compaction task declined: tablet is missing (TabletId: %v)",
                request.Tablet->GetId());
            return nullptr;
        }

        if (tablet->GetMountRevision() != request.Tablet->GetMountRevision()) {
            YT_LOG_DEBUG("Compaction task declined: mount revision mismatch (%v, "
                "ActualMountRevision: %v, RequestMountRevision: %v)",
                tablet->GetLoggingTag(),
                tablet->GetMountRevision(),
                request.Tablet->GetMountRevision());
            return nullptr;
        }

        if (!tablet->SmoothMovementData().IsTabletStoresUpdateAllowed(/*isCommonFlush*/ false)) {
            YT_LOG_DEBUG("Compaction task declined: tablet participates in smooth movement (%v)",
                tablet->GetLoggingTag());
            return nullptr;
        }

        auto task = std::make_unique<TTask>(
            slot,
            tablet,
            request.PartitionId,
            request.Stores,
            request.Reason);
        task->Slack = request.Slack;
        task->Effect = request.Effect;
        task->DiscardStores = request.DiscardStores;
        task->FutureEffect = GetFutureEffect(tablet->GetId());

        if (logStructured) {
            tablet->GetStructuredLogger()->LogEvent(eventType)
                .Do(BIND(&TTask::StoreToStructuredLog, task.get()));
        }

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

    template <class TIterator>
    static void ResetOrchidPendingTasks(
        const TCompactionOrchidPtr& orchid,
        TIterator begin,
        TIterator end)
    {
        TCompactionOrchid::TTaskMap pendingTasks;
        for (auto it = begin; it != end; ++it) {
            pendingTasks.emplace((*it)->TaskId, (*it)->BuildTaskInfo());
        }
        orchid->ResetPendingTasks(std::move(pendingTasks));
    }

    void PickMoreTasks(
        std::vector<std::unique_ptr<TTask>>* candidates,
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TCompactionOrchidPtr& orchid,
        const NProfiling::TGauge& counter)
    {
        counter.Update(candidates->size());

        MakeHeap(candidates->begin(), candidates->end(), TTask::Comparer);

        {
            auto guard = Guard(TaskSpinLock_);
            ResetOrchidPendingTasks(orchid, candidates->begin(), candidates->end());
            tasks->swap(*candidates);
            *index = tasks->size();
        }

        candidates->clear();
    }

    void PickMorePartitionings(TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        PickMoreTasks(
            &PartitioningCandidates_,
            &PartitioningTasks_,
            &PartitioningTaskIndex_,
            PartitioningOrchid_,
            FeasiblePartitioningsCounter_);
    }

    void PickMoreCompactions(TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        PickMoreTasks(
            &CompactionCandidates_,
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchid_,
            FeasibleCompactionsCounter_);
    }

    void ScheduleMoreTasks(
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TCompactionOrchidPtr& orchid,
        const TAsyncSemaphorePtr& semaphore,
        const TCounter& counter,
        void (TStoreCompactor::*action)(TTask*))
    {
        const auto& Logger = TabletNodeLogger;

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
                    FutureEffectMismatchesCounter_.Increment();
                    YT_LOG_DEBUG("Remaking compaction task heap due to future effect mismatch "
                        "(TabletId: %v, TaskFutureEffect: %v, TabletFutureEffect: %v)",
                        firstTask->TabletId,
                        firstTask->FutureEffect,
                        LockedGetFutureEffect(guard, firstTask->TabletId));

                    for (size_t i = 0; i < *index; ++i) {
                        auto&& task = (*tasks)[i];
                        task->FutureEffect = LockedGetFutureEffect(guard, task->TabletId);
                    }
                    guard.Release();
                    MakeHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
                    ResetOrchidPendingTasks(orchid, tasks->begin(), tasks->begin() + *index);
                }
            }

            // Extract the next task.
            ExtractHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
            --(*index);
            auto&& task = tasks->at(*index);
            task->Prepare(this, orchid.Get(), std::move(semaphoreGuard));
            ++scheduled;

            // TODO(sandello): Better ownership management.
            auto invoker = task->Invoker;
            auto cancelableContext = task->CancelableContext;
            auto taskFuture = BIND(action, MakeStrong(this), Owned(task.release()))
                .AsyncVia(invoker)
                .Run();
            if (cancelableContext) {
                cancelableContext->PropagateTo(taskFuture);
            }
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
            PartitioningOrchid_,
            PartitioningSemaphore_,
            ScheduledPartitioningsCounter_,
            &TStoreCompactor::PartitionEden);
    }

    void ScheduleMoreCompactions()
    {
        ScheduleMoreTasks(
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchid_,
            CompactionSemaphore_,
            ScheduledCompactionsCounter_,
            &TStoreCompactor::CompactPartition);
    }

    int LockedGetFutureEffect(NThreading::TReaderGuard<TReaderWriterSpinLock>&, TTabletId tabletId)
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
        YT_LOG_DEBUG("Accounting for the future effect of the compaction/partitioning "
            "(TabletId: %v, FutureEffect: %v -> %v)",
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
        TTransactionStartOptions transactionOptions;
        transactionOptions.AutoAbort = false;
        transactionOptions.Attributes = std::move(transactionAttributes);
        transactionOptions.CoordinatorMasterCellTag = CellTagFromId(tabletSnapshot->TabletId);
        transactionOptions.ReplicateToMasterCellTags = TCellTagList();
        transactionOptions.StartCypressTransaction = false;
        auto transactionFuture = Bootstrap_->GetClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionOptions);
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
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryReferenceTracker = Bootstrap_->GetNodeMemoryReferenceTracker()->WithCategory(EMemoryCategory::TabletBackground)
        };

        auto Logger = TabletNodeLogger
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMorePartitionings();
        });

        auto traceId = task->TaskId;
        TTraceContextGuard traceContextGuard(
            TTraceContext::NewRoot("StoreCompactor", traceId));

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
                .Item("partition_id").Value(task->PartitionId)
                .Item("trace_id").Value(traceId);
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
                YT_LOG_DEBUG("Eden store is in improper state, aborting partitioning "
                    "(StoreId: %v, CompactionState: %v)",
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
        TCompactionSessionFinalizeResult finalizeResult;
        NChunkClient::NProto::TDataStatistics writerDataStatistics;

        auto readerProfiler = New<TReaderProfiler>();
        auto writerProfiler = New<TWriterProfiler>();

        chunkReadOptions.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
            tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
            tabletSnapshot->PhysicalSchema);

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetCompressedDataSize();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            NProfiling::TWallTimer timer;
            eden->SetCompactionTime(timer.GetStartTime());

            structuredLogger->LogEvent("start_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp)
                .Item("trace_id").Value(traceId);

            YT_LOG_INFO("Eden partitioning started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "PartitionCount: %v, CompressedDataSize: %v, "
                "ChunkCount: %v, CurrentTimestamp: %v, RetentionConfig: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                pivotKeys.size(),
                dataSize,
                stores.size(),
                currentTimestamp,
                ConvertTo<TRetentionConfigPtr>(mountConfig));

            reader = CreateReader(
                task,
                tablet,
                tabletSnapshot,
                stores,
                chunkReadOptions,
                currentTimestamp,
                // NB: No major compaction during Eden partitioning.
                /*majorTimestamp*/ MinTimestamp,
                tabletSnapshot->PartitioningThrottler,
                Logger);

            auto transaction = StartPartitioningTransaction(tabletSnapshot, &Logger);

            auto partitioningSession = New<TEdenPartitioningSession>(
                Bootstrap_,
                tabletSnapshot,
                transaction,
                chunkReadOptions,
                Logger);

            auto partitioningResultFuture =
                BIND(
                    &TEdenPartitioningSession::Run,
                    partitioningSession,
                    reader,
                    pivotKeys,
                    tablet->GetNextPivotKey())
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            std::tie(partitioningResult, finalizeResult) = WaitFor(partitioningResultFuture)
                .ValueOrThrow();
            const auto& partitionWriters = partitioningResult.PartitionStoreWriters;

            for (const auto& [writer, _] : partitioningResult.PartitionStoreWriters) {
                writerDataStatistics += writer->GetDataStatistics();
                writerProfiler->Update(writer);
            }

            SyncThrottleMediumWrite(
                Bootstrap_,
                tabletSnapshot,
                writerDataStatistics,
                partitioningResult.HunkWriter->GetDataStatistics(),
                Logger);

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
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
                tabletSnapshot->PerformanceCounters->PartitioningDataWeight.Counter.fetch_add(
                    writer->GetDataStatistics().data_weight(),
                    std::memory_order::relaxed);
            }

            YT_LOG_INFO("Eden partitioning completed "
                "(RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v%v, WallTime: %v)",
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
                .Item("output_row_count").Value(partitioningResult.RowCount)
                .Item("trace_id").Value(traceId);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Partitioning].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Partitioning);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Partitioning].Store(error);
            YT_LOG_ERROR(error, "Error partitioning Eden, backing off");

            structuredLogger->LogEvent("backoff_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("trace_id").Value(traceId);

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            task->Failed = true;
        }

        writerProfiler->Update(
            partitioningResult.HunkWriter,
            partitioningResult.HunkWriterStatistics);

        readerProfiler->Update(
            reader,
            chunkReadOptions.ChunkReaderStatistics,
            chunkReadOptions.HunkChunkReaderStatistics);

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Partitioning, task->Failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Partitioning, task->Failed);

        if (!task->Failed) {
            tabletSnapshot->TableProfiler->GetLsmCounters()->ProfilePartitioning(
                task->Reason,
                task->HunkChunkCountByReason,
                reader->GetDataStatistics(),
                writerDataStatistics,
                chunkReadOptions.HunkChunkReaderStatistics,
                partitioningResult.HunkWriter->GetDataStatistics());
        }

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
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));
            AddStoresToRemove(&actionRequest, stores);

            YT_LOG_INFO("Partition stores discarded by TTL "
                "(UnmergedRowCount: %v, CompressedDataSize: %v, StoreIdsToRemove: %v)",
                partition->GetUnmergedRowCount(),
                partition->GetCompressedDataSize(),
                MakeFormattableView(stores, TStoreIdFormatter()));

            structuredLogger->LogEvent("discard_stores")
                .Item("partition_id").Value(partition->GetId());

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Compaction].Store(error);
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
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryReferenceTracker = Bootstrap_->GetNodeMemoryReferenceTracker()->WithCategory(EMemoryCategory::TabletBackground)
        };

        auto Logger = TabletNodeLogger
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMoreCompactions();
        });

        auto traceId = task->TaskId;
        TTraceContextGuard traceContextGuard(
            TTraceContext::NewRoot("StoreCompactor", traceId));

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
                .Item("partition_id").Value(task->PartitionId)
                .Item("trace_id").Value(traceId);
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
        TCompactionSessionFinalizeResult finalizeResult;

        auto writerProfiler = New<TWriterProfiler>();
        auto readerProfiler = New<TReaderProfiler>();

        chunkReadOptions.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
            tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
            tabletSnapshot->PhysicalSchema);

        try {
            i64 inputDataSize = 0;
            i64 inputRowCount = 0;
            for (const auto& store : stores) {
                inputDataSize += store->GetCompressedDataSize();
                inputRowCount += store->GetRowCount();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            NProfiling::TWallTimer timer;
            partition->SetCompactionTime(timer.GetStartTime());

            const auto& mountConfig = tablet->GetSettings().MountConfig;
            auto retainedTimestamp = CalculateRetainedTimestamp(currentTimestamp, mountConfig->MinDataTtl);

            TTimestamp majorTimestamp;
            if (tablet->GetBackupCheckpointTimestamp()) {
                majorTimestamp = MinTimestamp;
                YT_LOG_DEBUG("Compaction will not delete old versions due to backup in progress "
                    "(BackupCheckpointTimestamp: %v)",
                    tablet->GetBackupCheckpointTimestamp());
            } else {
                majorTimestamp = std::min(ComputeMajorTimestamp(partition, stores), retainedTimestamp);
            }

            structuredLogger->LogEvent("start_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp)
                .Item("reason").Value(task->Reason)
                .Item("trace_id").Value(traceId)
                // NB: deducible.
                .Item("major_timestamp").Value(majorTimestamp)
                .Item("retained_timestamp").Value(retainedTimestamp);

            YT_LOG_INFO("Partition compaction started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "RowCount: %v, CompressedDataSize: %v, ChunkCount: %v, "
                "CurrentTimestamp: %v, MajorTimestamp: %v, RetainedTimestamp: %v, RetentionConfig: %v, "
                "Reason: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                inputRowCount,
                inputDataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp,
                retainedTimestamp,
                ConvertTo<TRetentionConfigPtr>(mountConfig),
                task->Reason);

            reader = CreateReader(
                task,
                tablet,
                tabletSnapshot,
                stores,
                chunkReadOptions,
                currentTimestamp,
                majorTimestamp,
                tabletSnapshot->CompactionThrottler,
                Logger);

            auto transaction = StartCompactionTransaction(tabletSnapshot, &Logger);

            auto compactionSession = New<TPartitionCompactionSession>(
                Bootstrap_,
                tabletSnapshot,
                partition,
                transaction,
                chunkReadOptions,
                Logger);

            auto compactionResultFuture =
                BIND(
                    &TPartitionCompactionSession::Run,
                    compactionSession,
                    reader)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            std::tie(compactionResult, finalizeResult) = WaitFor(compactionResultFuture)
                .ValueOrThrow();

            SyncThrottleMediumWrite(
                Bootstrap_,
                tabletSnapshot,
                compactionResult.StoreWriter->GetDataStatistics(),
                compactionResult.HunkWriter->GetDataStatistics(),
                Logger);

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));
            AddStoresToAdd(&actionRequest, compactionResult.StoreWriter);
            AddStoresToAdd(&actionRequest, compactionResult.HunkWriter);
            AddStoresToRemove(&actionRequest, stores);

            std::vector<TStoreId> storeIdsToAdd;
            for (const auto& chunkSpec : compactionResult.StoreWriter->GetWrittenChunkSpecs()) {
                storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
            }
            tabletSnapshot->PerformanceCounters->CompactionDataWeight.Counter.fetch_add(
                compactionResult.StoreWriter->GetDataStatistics().data_weight(),
                std::memory_order::relaxed);

            i64 outputTotalDataWeight = 0;
            for (const auto& statistics : finalizeResult.WriterStatistics) {
                outputTotalDataWeight += statistics.data_weight();
            }

            YT_LOG_INFO("Partition compaction completed "
                "(RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v%v, WallTime: %v)",
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
                .Item("output_row_count").Value(compactionResult.RowCount)
                .Item("output_data_weight").Value(outputTotalDataWeight)
                .Item("trace_id").Value(traceId);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Compaction].Store(error);
            YT_LOG_ERROR(error, "Error compacting partition, backing off");

            structuredLogger->LogEvent("backoff_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("trace_id").Value(traceId);

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            task->Failed = true;
        }

        writerProfiler->Update(compactionResult.StoreWriter);
        writerProfiler->Update(
            compactionResult.HunkWriter,
            compactionResult.HunkWriterStatistics);

        readerProfiler->Update(
            reader,
            chunkReadOptions.ChunkReaderStatistics,
            chunkReadOptions.HunkChunkReaderStatistics);

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Compaction, task->Failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Compaction, task->Failed);

        if (!task->Failed) {
            tabletSnapshot->TableProfiler->GetLsmCounters()->ProfileCompaction(
                task->Reason,
                task->HunkChunkCountByReason,
                partition->IsEden(),
                reader->GetDataStatistics(),
                compactionResult.StoreWriter->GetDataStatistics(),
                chunkReadOptions.HunkChunkReaderStatistics,
                compactionResult.HunkWriter->GetDataStatistics());
        }

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
        auto referencedHunkLengthRatio = static_cast<double>(hunkChunk->GetReferencedTotalHunkLength()) /
            hunkChunk->GetTotalHunkLength();
        return referencedHunkLengthRatio < 1.0 - mountConfig->MaxHunkCompactionGarbageRatio;
    }

    static bool IsSmallHunkCompactionNeeded(
        const TTablet* tablet,
        const THunkChunkPtr& hunkChunk)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        return hunkChunk->GetTotalHunkLength() <= mountConfig->MaxHunkCompactionSize;
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

        if (IsSmallHunkCompactionNeeded(tablet, hunkChunk)) {
            return EHunkCompactionReason::HunkChunkTooSmall;
        }

        return EHunkCompactionReason::None;
    }

    THashSet<TChunkId> PickCompactableHunkChunkIds(
        TTask* task,
        const TTablet* tablet,
        const std::vector<TSortedChunkStorePtr>& stores,
        const NLogging::TLogger& logger)
    {
        const auto& Logger = logger;
        const auto& mountConfig = tablet->GetSettings().MountConfig;

        // Forced or garbage ratio too high. Will be compacted unconditionally.
        THashSet<TChunkId> finalistIds;

        // Too small hunk chunks. Will be compacted only if there are enough of them.
        THashSet<THunkChunkPtr> candidates;

        for (const auto& store : stores) {
            for (const auto& hunkRef : store->HunkChunkRefs()) {
                const auto& hunkChunk = hunkRef.HunkChunk;
                auto compactionReason = GetHunkCompactionReason(tablet, hunkChunk);

                if (compactionReason == EHunkCompactionReason::None) {
                    continue;
                } else if (compactionReason == EHunkCompactionReason::HunkChunkTooSmall) {
                    candidates.insert(hunkChunk);
                } else {
                    if (finalistIds.insert(hunkChunk->GetId()).second) {
                        // NB: GetHunkCompactionReason will produce same result for each hunk chunk occurrence.
                        ++task->HunkChunkCountByReason[compactionReason];
                        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                            "Hunk chunk is picked for compaction (HunkChunkId: %v, Reason: %v)",
                            hunkChunk->GetId(),
                            compactionReason);
                    }
                }
            }
        }

        // Fast path.
        if (ssize(candidates) < mountConfig->MinHunkCompactionChunkCount)
        {
            return finalistIds;
        }

        std::vector<THunkChunkPtr> sortedCandidates(
            candidates.begin(),
            candidates.end());
        std::sort(
            sortedCandidates.begin(),
            sortedCandidates.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs->GetTotalHunkLength() < rhs->GetTotalHunkLength();
            });

        for (int i = 0; i < ssize(sortedCandidates); ++i) {
            i64 totalSize = 0;
            int j = i;
            while (j < ssize(sortedCandidates)) {
                if (j - i == mountConfig->MaxHunkCompactionChunkCount) {
                    break;
                }

                i64 size = sortedCandidates[j]->GetTotalHunkLength();
                if (size > mountConfig->HunkCompactionSizeBase &&
                    totalSize > 0 &&
                    size > totalSize * mountConfig->HunkCompactionSizeRatio)
                {
                    break;
                }

                totalSize += size;
                ++j;
            }

            if (j - i >= mountConfig->MinHunkCompactionChunkCount) {
                while (i < j) {
                    const auto& candidate = sortedCandidates[i];
                    YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                        "Hunk chunk is picked for compaction (HunkChunkId: %v, Reason: %v)",
                        candidate->GetId(),
                        EHunkCompactionReason::HunkChunkTooSmall);
                    ++task->HunkChunkCountByReason[EHunkCompactionReason::HunkChunkTooSmall];
                    InsertOrCrash(finalistIds, candidate->GetId());
                    ++i;
                }
                break;
            }
        }

        return finalistIds;
    }

    IVersionedReaderPtr CreateReader(
        TTask* task,
        TTablet* tablet,
        const TTabletSnapshotPtr& tabletSnapshot,
        const std::vector<TSortedChunkStorePtr>& stores,
        const TClientChunkReadOptions& chunkReadOptions,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp,
        IThroughputThrottlerPtr inboundThrottler,
        const NLogging::TLogger& logger)
    {
        return CreateHunkInliningVersionedReader(
            tablet->GetSettings().HunkReaderConfig,
            CreateCompactionTabletReader(
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                majorTimestamp,
                chunkReadOptions,
                stores.size(),
                ETabletDistributedThrottlerKind::CompactionRead,
                std::move(inboundThrottler),
                chunkReadOptions.WorkloadDescriptor.Category),
            tablet->GetChunkFragmentReader(),
            tabletSnapshot->DictionaryCompressionFactory,
            tablet->GetPhysicalSchema(),
            PickCompactableHunkChunkIds(
                task,
                tablet,
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
            FilterProtoExtensions(
                descriptor->mutable_chunk_meta()->mutable_extensions(),
                GetMasterChunkMetaExtensionTagsFilter());
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
        FilterProtoExtensions(
            descriptor->mutable_chunk_meta()->mutable_extensions(),
            GetMasterChunkMetaExtensionTagsFilter());
    }
};

TStoreCompactor::TTask::~TTask()
{
    if (auto owner = Owner.Lock()) {
        owner->ChangeFutureEffect(TabletId, -Effect);
    }

    if (auto orchid = Orchid.Lock()) {
        if (Failed) {
            orchid->OnTaskFailed(TaskId);
        } else {
            orchid->OnTaskCompleted(TaskId);
        }
    }
}

DEFINE_REFCOUNTED_TYPE(TStoreCompactor)

IStoreCompactorPtr CreateStoreCompactor(IBootstrap* bootstrap)
{
    return New<TStoreCompactor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
