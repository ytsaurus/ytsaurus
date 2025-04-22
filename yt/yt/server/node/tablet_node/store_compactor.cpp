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
#include "versioned_chunk_meta_manager.h"

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

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

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
using namespace NLsm;
using namespace NHydra;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 65536;
static const size_t MaxRowsPerWrite = 65536;

////////////////////////////////////////////////////////////////////////////////

namespace {

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
    : public TBackgroundActivityTaskInfoBase
{
    struct TCompactionRuntimeData
        : public TRuntimeData
    {
        TReaderStatistics ProcessedReaderStatistics;
        TReaderStatistics TotalReaderStatistics;

        TEnumIndexedArray<EHunkCompactionReason, i64> HunkChunkCountByReason;
    };

    const TPartitionId PartitionId;
    const EStoreCompactionReason Reason;
    // True if the all chunks should be discarded. The task does not
    // require reading and writing chunks.
    const bool DiscardStores;
    // Overlapping stores slack for the task.
    // That is, the remaining number of stores in the partition till
    // the tablet hits MOSC limit.
    // Small values indicate that the tablet is in a critical state.
    const int Slack;
    // Guaranteed effect on the slack if this task will be done.
    // This is a conservative estimate.
    const int Effect;
    // Future effect on the slack due to concurrent tasks.
    // This quantity is memoized to provide a valid comparison operator.
    // Can be modified only before adding task to orchid.
    int FutureEffect;
    // A random number to deterministically break ties.
    const ui64 Random = RandomNumber<size_t>();

    const std::vector<TStoreId> StoreIds;

    TCompactionRuntimeData RuntimeData;

    TCompactionTaskInfo(
        TGuid taskId,
        TTabletId tabletId,
        NHydra::TRevision mountRevision,
        TString tablePath,
        TString tabletCellBundle,
        TPartitionId partitionId,
        EStoreCompactionReason reason,
        bool discardStores,
        int slack,
        int effect,
        int futureEffect,
        std::vector<TStoreId> storeIds)
        : TBackgroundActivityTaskInfoBase(
            taskId,
            tabletId,
            mountRevision,
            std::move(tablePath),
            std::move(tabletCellBundle))
        , PartitionId(partitionId)
        , Reason(reason)
        , DiscardStores(discardStores)
        , Slack(slack)
        , Effect(effect)
        , FutureEffect(futureEffect)
        , StoreIds(std::move(storeIds))
    { }

    auto GetOrderingTuple() const
    {
        return std::tuple(
            !DiscardStores,
            Slack + FutureEffect,
            -Effect,
            -ssize(StoreIds),
            Random);
    }

    bool ComparePendingTasks(const TCompactionTaskInfo& other) const
    {
        return GetOrderingTuple() < other.GetOrderingTuple();
    }
};

DEFINE_REFCOUNTED_TYPE(TCompactionTaskInfo);
using TCompactionTaskInfoPtr = TIntrusivePtr<TCompactionTaskInfo>;

void SerializeFragment(const TCompactionTaskInfo::TCompactionRuntimeData& runtimeData, IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Do([&] (auto fluent) {
            SerializeFragment(
                static_cast<const TBackgroundActivityTaskInfoBase::TRuntimeData&>(runtimeData),
                fluent.GetConsumer());
        })
        .DoIf(runtimeData.ShowStatistics, [&] (auto fluent) {
            fluent
                .Item("processed_input_rows_ratio").Value(
                    static_cast<double>(runtimeData.ProcessedReaderStatistics.UnmergedRowCount) /
                    runtimeData.TotalReaderStatistics.UnmergedRowCount)
                .Item("processed_reader_statistics").Value(runtimeData.ProcessedReaderStatistics)
                .Item("total_reader_statistics").Value(runtimeData.TotalReaderStatistics);
        })
        .Item("hunk_chunk_count_by_reason").Value(runtimeData.HunkChunkCountByReason);
}

void Serialize(const TCompactionTaskInfo& task, IYsonConsumer* consumer)
{
    auto guard = Guard(task.RuntimeData.SpinLock);

    BuildYsonFluently(consumer).BeginMap()
        .Do([&] (auto fluent) {
            Serialize(static_cast<const TBackgroundActivityTaskInfoBase&>(task), fluent.GetConsumer());
        })
        .Item("partition_id").Value(task.PartitionId)
        .Item("reason").Value(task.Reason)
        .Item("task_priority")
            .BeginMap()
                .Item("discard_stores").Value(task.DiscardStores)
                .Item("slack").Value(task.Slack)
                .Item("effect").Value(task.Effect)
                .Item("future_effect").Value(task.FutureEffect)
                .Item("random").Value(task.Random)
            .EndMap()
        .Item("store_ids").List(task.StoreIds)
        .Do([&] (auto fluent) {
            SerializeFragment(task.RuntimeData, fluent.GetConsumer());
        })
    .EndMap();
}

void Serialize(const TCompactionTaskInfoPtr& task, IYsonConsumer* consumer)
{
    Serialize(*task, consumer);
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

class TStoreCompactor;

struct TCompactionTask
    : public TGuardedTaskInfo<TCompactionTaskInfo>
{
    const ITabletSlotPtr Slot;
    const IInvokerPtr Invoker;
    const TCancelableContextPtr CancelableContext;

    const std::string TabletLoggingTag;

    // These fields are filled upon task invocation.
    TWeakPtr<TStoreCompactor> Owner;
    TAsyncSemaphoreGuard SemaphoreGuard;

    TCompactionTask() = delete;
    TCompactionTask(const TCompactionTask&) = delete;
    TCompactionTask& operator=(const TCompactionTask&) = delete;
    TCompactionTask(TCompactionTask&&) = delete;
    TCompactionTask& operator=(TCompactionTask&&) = delete;

    TCompactionTask(
        ITabletSlot* slot,
        const TTablet* tablet,
        TPartitionId partitionId,
        std::vector<TStoreId> storeIds,
        NLsm::EStoreCompactionReason reason,
        bool discardStores,
        int slack,
        int effect,
        int futureEffect,
        TCompactionOrchidPtr orchid);

    ~TCompactionTask();

    void Prepare(
        TStoreCompactor* owner,
        TAsyncSemaphoreGuard&& semaphoreGuard);

    void StoreToStructuredLog(TFluentMap fluent);
};

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
    const IChunkWriter::TWriteBlocksOptions WriteBlocksOptions_;
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
        IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
        NLogging::TLogger logger)
        : Bootstrap_(bootstrap)
        , TabletSnapshot_(std::move(tabletSnapshot))
        , Transaction_(std::move(transaction))
        , ResultsInEden_(resultsInEden)
        , ChunkReadOptions_(std::move(chunkReadOptions))
        , WriteBlocksOptions_(std::move(writeBlocksOptions))
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
        auto chunkWriterFactory = [this, weakThis = MakeWeak(this)] (IChunkWriterPtr underlyingWriter) {
            if (auto this_ = weakThis.Lock()) {
                return CreateUnderlyingWriterAdapter(std::move(underlyingWriter));
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

    virtual ETabletBackgroundActivity GetActivityKind() const = 0;

private:
    std::vector<IVersionedMultiChunkWriterPtr> Writers_;
    std::vector<TFuture<void>> CloseFutures_;


    void Initialize()
    {
        auto enableCollocatedDatNodeThrottling = Bootstrap_->GetDynamicConfigManager()
            ->GetConfig()->TabletNode->EnableCollocatedDatNodeThrottling;

        StoreWriterConfig_ = CloneYsonStruct(TabletSnapshot_->Settings.StoreWriterConfig);
        StoreWriterConfig_->MinUploadReplicationFactor = StoreWriterConfig_->UploadReplicationFactor;
        StoreWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(ChunkReadOptions_.WorkloadDescriptor.Category);
        StoreWriterConfig_->EnableLocalThrottling = enableCollocatedDatNodeThrottling;

        StoreWriterOptions_ = CloneYsonStruct(TabletSnapshot_->Settings.StoreWriterOptions);
        StoreWriterOptions_->ChunksEden = ResultsInEden_;
        StoreWriterOptions_->ValidateResourceUsageIncrease = false;
        StoreWriterOptions_->ConsistentChunkReplicaPlacementHash = TabletSnapshot_->ConsistentChunkReplicaPlacementHash;
        StoreWriterOptions_->MemoryUsageTracker = Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletBackground);

        HunkWriterConfig_ = CloneYsonStruct(TabletSnapshot_->Settings.HunkWriterConfig);
        HunkWriterConfig_->WorkloadDescriptor = TWorkloadDescriptor(ChunkReadOptions_.WorkloadDescriptor.Category);
        HunkWriterConfig_->MinUploadReplicationFactor = HunkWriterConfig_->UploadReplicationFactor;
        HunkWriterConfig_->EnableLocalThrottling = enableCollocatedDatNodeThrottling;

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
            HunkChunkWriter_,
            WriteBlocksOptions_);
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

            for (const auto& writer : Writers_) {
                for (const auto& [chunkId, replicasInfo] : writer->GetWrittenChunkReplicasInfos()) {
                    chunkReplicaCache->UpdateReplicas(chunkId, TAllyReplicasInfo::FromWrittenChunkReplicasInfo(replicasInfo));
                }
            }

            if (HunkChunkPayloadWriter_->HasHunks()) {
                chunkReplicaCache->UpdateReplicas(
                    HunkChunkWriter_->GetChunkId(),
                    TAllyReplicasInfo::FromChunkWriter(HunkChunkWriter_));
            }
        }

        return result;
    }

    IVersionedChunkWriterPtr CreateUnderlyingWriterAdapter(IChunkWriterPtr underlyingWriter) const
    {
        auto writer = CreateHunkEncodingVersionedWriter(
            CreateVersionedChunkWriter(
                StoreWriterConfig_,
                StoreWriterOptions_,
                TabletSnapshot_->PhysicalSchema,
                std::move(underlyingWriter),
                WriteBlocksOptions_,
                /*dataSink*/ std::nullopt,
                BlockCache_),
            TabletSnapshot_->PhysicalSchema,
            HunkChunkPayloadWriter_,
            HunkChunkWriterStatistics_,
            TabletSnapshot_->DictionaryCompressionFactory,
            ChunkReadOptions_);
        if (TabletSnapshot_->Settings.MountConfig->InsertMetaUponStoreUpdate) {
            writer->GetMeta()->SubscribeMetaFinalized(BIND(
                &TStoreCompactionSessionBase::OnChunkMetaFinalized,
                MakeWeak(this),
                MakeWeak(writer)));
        }
        return writer;
    }

    void OnChunkMetaFinalized(const TWeakPtr<IVersionedChunkWriter>& weakWriter, const TRefCountedChunkMeta* finalizedMeta) const
    {
        auto writer = weakWriter.Lock();
        if (!writer) {
            return;
        }

        auto result = false;
        if (auto chunkMetaManager = Bootstrap_->GetVersionedChunkMetaManager()) {
            result = chunkMetaManager->InsertMeta(
                TVersionedChunkMetaCacheKey{
                    .ChunkId = writer->GetChunkId(),
                    .TableSchemaKeyColumnCount = TabletSnapshot_->PhysicalSchema->GetKeyColumnCount(),
                    .PreparedColumnarMeta = true,
                },
                New<TRefCountedChunkMeta>(*finalizedMeta));
        }

        YT_LOG_DEBUG("Propagating versioned chunk meta cache upon chunk finalization "
            "(Success: %v, ChunkId: %v, Activity: %v)",
            result,
            writer->GetChunkId(),
            GetActivityKind());
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
        IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ false,
            std::move(chunkReadOptions),
            std::move(writeBlocksOptions),
            std::move(logger))
    { }

    ETabletBackgroundActivity GetActivityKind() const override
    {
        return ETabletBackgroundActivity::Partitioning;
    }

    std::pair<TEdenPartitioningResult, TCompactionSessionFinalizeResult> Run(
        const IVersionedReaderPtr& reader,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const TLegacyOwningKey& nextTabletPivotKey,
        TCompactionTask* task)
    {
        return DoRun([&] {
            int currentPartitionIndex = 0;
            TLegacyOwningKey currentPivotKey;
            TLegacyOwningKey nextPivotKey;

            i64 currentPartitionRowCount = 0;
            i64 readRowCount = 0;
            i64 writtenRowCount = 0;

            IVersionedMultiChunkWriterPtr currentWriter;

            TBackgroundActivityTaskInfoBase::TWriterStatistics currentWriterStatistics;
            auto updateWriterStatistics = [&] {
                TBackgroundActivityTaskInfoBase::TWriterStatistics iterationStatistics(currentWriter->GetDataStatistics());
                {
                    auto guard = Guard(task->Info->RuntimeData.SpinLock);
                    task->Info->RuntimeData.ProcessedWriterStatistics += iterationStatistics - currentWriterStatistics;
                }
                currentWriterStatistics = iterationStatistics;
            };

            auto ensurePartitionStarted = [&] {
                if (currentWriter) {
                    return;
                }

                YT_LOG_INFO("Started writing partition (PartitionIndex: %v, Keys: %v .. %v)",
                    currentPartitionIndex,
                    currentPivotKey,
                    nextPivotKey);

                currentWriter = CreateWriter();
                currentWriterStatistics = {};
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

                updateWriterStatistics();
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
                    if (inputBatch) {
                        readRowCount += inputBatch->GetRowCount();
                        inputRows = inputBatch->MaterializeRows();
                        currentRowIndex = 0;

                        auto guard = Guard(task->Info->RuntimeData.SpinLock);
                        task->Info->RuntimeData.ProcessedReaderStatistics = TBackgroundActivityTaskInfoBase::TReaderStatistics(reader->GetDataStatistics());
                    } else {
                        return TVersionedRow();
                    }
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
                .RowCount = readRowCount,
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
        IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
        NLogging::TLogger logger)
        : TStoreCompactionSessionBase(
            bootstrap,
            std::move(tabletSnapshot),
            std::move(transaction),
            /*resultsInEden*/ partition->IsEden(),
            std::move(chunkReadOptions),
            std::move(writeBlocksOptions),
            std::move(logger))
    { }

    ETabletBackgroundActivity GetActivityKind() const override
    {
        return ETabletBackgroundActivity::Compaction;
    }

    std::pair<TPartitionCompactionResult, TCompactionSessionFinalizeResult>
    Run(const IVersionedReaderPtr& reader, TCompactionTask* task)
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

                auto guard = Guard(task->Info->RuntimeData.SpinLock);
                task->Info->RuntimeData.ProcessedReaderStatistics = TBackgroundActivityTaskInfoBase::TReaderStatistics(reader->GetDataStatistics());
                task->Info->RuntimeData.ProcessedWriterStatistics = TBackgroundActivityTaskInfoBase::TWriterStatistics(writer->GetDataStatistics());
            }

            CloseWriter(writer);

            return TPartitionCompactionResult{
                .StoreWriter = std::move(writer),
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
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TStoreCompactor::OnDynamicConfigChanged, MakeWeak(this)));
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


        auto scanForCandidates = [&] (
            const std::vector<TCompactionRequest>& requests,
            TStringBuf taskKind,
            int maxStructuredLogEvents,
            auto getEventType,
            std::vector<std::unique_ptr<TCompactionTask>>* candidates,
            const TCompactionOrchidPtr& orchid)
        {
            auto Logger = TabletNodeLogger().WithTag("CellId: %v, TaskKind: %v",
                slot->GetCellId(),
                taskKind);

            std::vector<std::unique_ptr<TCompactionTask>> tasks;
            std::vector<TCompactionTaskInfoPtr> pendingTaskInfos;
            for (const auto& request : requests) {
                if (auto task = MakeTask(
                    slot.Get(),
                    request,
                    getEventType(request.DiscardStores),
                    ssize(tasks) < maxStructuredLogEvents,
                    orchid,
                    Logger))
                {
                    pendingTaskInfos.push_back(task->Info);
                    tasks.push_back(std::move(task));
                }
            }

            auto guard = Guard(ScanSpinLock_);
            candidates->insert(
                candidates->end(),
                std::make_move_iterator(tasks.begin()),
                std::make_move_iterator(tasks.end()));

            orchid->AddPendingTasks(std::move(pendingTaskInfos));
        };

        if (ScanForCompactions_) {
            scanForCandidates(
                batch.Compactions,
                "Compaction",
                dynamicConfig->MaxCompactionStructuredLogEvents,
                [] (bool discardStores) { return discardStores ? "discard_stores_candidate" : "compaction_candidate";},
                &CompactionCandidates_,
                CompactionOrchid_);
        }

        if (ScanForPartitioning_) {
            scanForCandidates(
                batch.Partitionings,
                "Partitioning",
                dynamicConfig->MaxPartitioningStructuredLogEvents,
                [] (bool) { return "partitioning_candidate";},
                &PartitioningCandidates_,
                PartitioningOrchid_);
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
    friend TCompactionTask;

    IBootstrap* const Bootstrap_;
    const TStoreCompactorConfigPtr Config_;

    const TProfiler Profiler_ = TabletNodeProfiler().WithPrefix("/store_compactor");
    const TGauge FeasiblePartitioningsCounter_ = Profiler_.Gauge("/feasible_partitionings");
    const TGauge FeasibleCompactionsCounter_ = Profiler_.Gauge("/feasible_compactions");
    const TCounter ScheduledPartitioningsCounter_ = Profiler_.Counter("/scheduled_partitionings");
    const TCounter ScheduledCompactionsCounter_ = Profiler_.Counter("/scheduled_compactions");
    const TCounter FutureEffectMismatchesCounter_ = Profiler_.Counter("/future_effect_mismatches");
    const TEventTimer ScanTimer_ = Profiler_.Timer("/scan_time");

    const IThreadPoolPtr ThreadPool_;
    const TAsyncSemaphorePtr PartitioningSemaphore_;
    const TAsyncSemaphorePtr CompactionSemaphore_;

    // Variables below contain per-iteration state for slot scan.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ScanSpinLock_);
    bool ScanForPartitioning_;
    bool ScanForCompactions_;
    std::vector<std::unique_ptr<TCompactionTask>> PartitioningCandidates_;
    std::vector<std::unique_ptr<TCompactionTask>> CompactionCandidates_;

    // Variables below are actually used during the scheduling.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TaskSpinLock_);
    std::vector<std::unique_ptr<TCompactionTask>> PartitioningTasks_; // Min-heap.
    size_t PartitioningTaskIndex_ = 0; // Heap end boundary.
    std::vector<std::unique_ptr<TCompactionTask>> CompactionTasks_; // Min-heap.
    size_t CompactionTaskIndex_ = 0; // Heap end boundary.

    // These are for the future accounting.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, FutureEffectLock_);
    THashMap<TTabletId, int> FutureEffect_;

    const TCompactionOrchidPtr CompactionOrchid_;
    const TCompactionOrchidPtr PartitioningOrchid_;
    const IYPathServicePtr OrchidService_;


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

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreCompactor;
        ThreadPool_->SetThreadCount(config->ThreadPoolSize.value_or(Config_->ThreadPoolSize));
        PartitioningSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentPartitionings));
        CompactionSemaphore_->SetTotal(config->MaxConcurrentCompactions.value_or(Config_->MaxConcurrentCompactions));
        PartitioningOrchid_->Reconfigure(config->Orchid);
        CompactionOrchid_->Reconfigure(config->Orchid);
    }

    std::unique_ptr<TCompactionTask> MakeTask(
        ITabletSlot* slot,
        const NLsm::TCompactionRequest& request,
        TStringBuf eventType,
        bool logStructured,
        TCompactionOrchidPtr orchid,
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

        if (request.DiscardStores && tablet->GetTableSchema()->HasTtlColumn()) {
            YT_LOG_DEBUG("Compaction task declined: tablet has TTL column and has been compacted "
                "by discard stores (%v)",
                tablet->GetLoggingTag());
            return nullptr;
        }

        auto task = std::make_unique<TCompactionTask>(
            slot,
            tablet,
            request.PartitionId,
            request.Stores,
            request.Reason,
            request.DiscardStores,
            request.Slack,
            request.Effect,
            GetFutureEffect(tablet->GetId()),
            std::move(orchid));

        if (logStructured) {
            tablet->GetStructuredLogger()->LogEvent(eventType)
                .Do(BIND(&TCompactionTask::StoreToStructuredLog, task.get()));
        }

        return task;
    }

    static TTimestamp ComputeMajorTimestamp(
        TPartition* partition,
        const std::vector<TSortedChunkStorePtr>& stores)
    {
        if (stores.empty()) {
            return MaxTimestamp;
        }

        auto minKey = stores.front()->GetMinKey();
        auto upperBoundKey = stores.front()->GetUpperBoundKey();
        for (int index = 1; index < ssize(stores); ++index) {
            minKey = std::min(minKey, stores[index]->GetMinKey());
            upperBoundKey = std::max(upperBoundKey, stores[index]->GetUpperBoundKey());
        }

        auto result = MaxTimestamp;
        auto handleStore = [&] (const ISortedStorePtr& store) {
            if (minKey < store->GetUpperBoundKey() && store->GetMinKey() < upperBoundKey) {
                result = std::min(result, store->GetMinTimestamp());
            }
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
        std::vector<std::unique_ptr<TCompactionTask>>* candidates,
        std::vector<std::unique_ptr<TCompactionTask>>* tasks,
        size_t* index,
        const NProfiling::TGauge& counter)
    {
        counter.Update(candidates->size());

        MakeHeap(candidates->begin(), candidates->end(), CompactionTaskComparer);

        {
            auto guard = Guard(TaskSpinLock_);
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
            FeasiblePartitioningsCounter_);
    }

    void PickMoreCompactions(TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        PickMoreTasks(
            &CompactionCandidates_,
            &CompactionTasks_,
            &CompactionTaskIndex_,
            FeasibleCompactionsCounter_);
    }

    void ScheduleMoreTasks(
        std::vector<std::unique_ptr<TCompactionTask>>* tasks,
        size_t* index,
        const TAsyncSemaphorePtr& semaphore,
        const TCounter& counter,
        void (TStoreCompactor::*action)(TCompactionTask*))
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
                if (firstTask->Info->FutureEffect != LockedGetFutureEffect(guard, firstTask->Info->TabletId)) {
                    FutureEffectMismatchesCounter_.Increment();
                    YT_LOG_DEBUG("Remaking compaction task heap due to future effect mismatch "
                        "(TabletId: %v, TaskFutureEffect: %v, TabletFutureEffect: %v)",
                        firstTask->Info->TabletId,
                        firstTask->Info->FutureEffect,
                        LockedGetFutureEffect(guard, firstTask->Info->TabletId));

                    for (size_t i = 0; i < *index; ++i) {
                        auto&& task = (*tasks)[i];
                        task->Info->FutureEffect = LockedGetFutureEffect(guard, task->Info->TabletId);
                    }
                    guard.Release();
                    MakeHeap(tasks->begin(), tasks->begin() + *index, CompactionTaskComparer);
                }
            }

            // Extract the next task.
            ExtractHeap(tasks->begin(), tasks->begin() + *index, CompactionTaskComparer);
            --(*index);
            auto&& task = tasks->at(*index);
            task->Prepare(this, std::move(semaphoreGuard));
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
            PartitioningSemaphore_,
            ScheduledPartitioningsCounter_,
            &TStoreCompactor::PartitionEden);
    }

    void ScheduleMoreCompactions()
    {
        ScheduleMoreTasks(
            &CompactionTasks_,
            &CompactionTaskIndex_,
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
        actionRequest.set_mount_revision(ToProto(tablet->GetMountRevision()));

        auto actionData = MakeTransactionActionData(actionRequest);
        auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));

        transaction->AddAction(masterCellId, actionData);
        transaction->AddAction(slot->GetCellId(), actionData);

        const auto& tabletManager = slot->GetTabletManager();
        WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
            .ThrowOnError();
    }

    void PartitionEden(TCompactionTask* task)
    {
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning),
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryUsageTracker = Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletBackground),
        };

        auto Logger = TabletNodeLogger()
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMorePartitionings();
        });

        auto traceId = task->Info->TaskId;
        TTraceContextGuard traceContextGuard(
            TTraceContext::NewRoot("StoreCompactor", traceId));

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->Info->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting partitioning");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(
            task->Info->TabletId,
            task->Info->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting partitioning");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_partitioning")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->Info->PartitionId)
                .Item("trace_id").Value(traceId);
        };


        auto* eden = tablet->GetEden();
        if (eden->GetId() != task->Info->PartitionId) {
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

        const auto& storeIds = task->Info->StoreIds;

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(storeIds.size());
        for (const auto& storeId : storeIds) {
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

        {
            auto guard = Guard(task->Info->RuntimeData.SpinLock);
            task->Info->RuntimeData.TotalReaderStatistics = TBackgroundActivityTaskInfoBase::TReaderStatistics(stores);
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

        task->OnStarted();
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
                .Item("store_ids").Value(task->Info->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp)
                .Item("trace_id").Value(traceId);

            YT_LOG_INFO("Eden partitioning started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "PartitionCount: %v, CompressedDataSize: %v, "
                "ChunkCount: %v, CurrentTimestamp: %v, RetentionConfig: %v)",
                task->Info->Slack,
                task->Info->FutureEffect,
                task->Info->Effect,
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
                IChunkWriter::TWriteBlocksOptions(),
                Logger);

            auto partitioningResultFuture =
                BIND(
                    &TEdenPartitioningSession::Run,
                    partitioningSession,
                    reader,
                    pivotKeys,
                    tablet->GetNextPivotKey(),
                    task)
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

            if (RandomNumber<double>() < mountConfig->Testing.PartitioningFailureProbability) {
                THROW_ERROR_EXCEPTION("Partitioning failed for testing purposes");
            }

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            actionRequest.set_update_reason(ToProto(ETabletStoresUpdateReason::Partitioning));
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

            task->OnFailed(std::move(error));
        }

        writerProfiler->Update(
            partitioningResult.HunkWriter,
            partitioningResult.HunkWriterStatistics);

        readerProfiler->Update(
            reader,
            chunkReadOptions.ChunkReaderStatistics,
            chunkReadOptions.HunkChunkReaderStatistics);

        bool failed = task->IsFailed();

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Partitioning, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Partitioning, failed);

        if (!failed) {
            tabletSnapshot->TableProfiler->GetLsmCounters()->ProfilePartitioning(
                task->Info->Reason,
                task->Info->RuntimeData.HunkChunkCountByReason,
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
            actionRequest.set_update_reason(ToProto(ETabletStoresUpdateReason::Compaction));
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

    void CompactPartition(TCompactionTask* task)
    {
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction),
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryUsageTracker = Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletBackground),
        };

        auto Logger = TabletNodeLogger()
            .WithTag("%v, ReadSessionId: %v",
                task->TabletLoggingTag,
                chunkReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMoreCompactions();
        });

        auto traceId = task->Info->TaskId;
        TTraceContextGuard traceContextGuard(
            TTraceContext::NewRoot("StoreCompactor", traceId));

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->Info->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting compaction");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(task->Info->TabletId, task->Info->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting compaction");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_compaction")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->Info->PartitionId)
                .Item("trace_id").Value(traceId);
        };

        auto* partition = tablet->GetEden()->GetId() == task->Info->PartitionId
            ? tablet->GetEden()
            : tablet->FindPartition(task->Info->PartitionId);
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

        const auto& storeIds = task->Info->StoreIds;

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(storeIds.size());
        for (const auto& storeId : storeIds) {
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
        {
            auto guard = Guard(task->Info->RuntimeData.SpinLock);
            task->Info->RuntimeData.TotalReaderStatistics = TBackgroundActivityTaskInfoBase::TReaderStatistics(stores);
        }

        if (task->Info->DiscardStores) {
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

        task->OnStarted();
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
                .Item("store_ids").Value(storeIds)
                .Item("current_timestamp").Value(currentTimestamp)
                .Item("reason").Value(task->Info->Reason)
                .Item("trace_id").Value(traceId)
                // NB: Deducible.
                .Item("major_timestamp").Value(majorTimestamp)
                .Item("retained_timestamp").Value(retainedTimestamp);

            YT_LOG_INFO("Partition compaction started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "RowCount: %v, CompressedDataSize: %v, ChunkCount: %v, "
                "CurrentTimestamp: %v, MajorTimestamp: %v, RetainedTimestamp: %v, RetentionConfig: %v, "
                "Reason: %v)",
                task->Info->Slack,
                task->Info->FutureEffect,
                task->Info->Effect,
                inputRowCount,
                inputDataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp,
                retainedTimestamp,
                ConvertTo<TRetentionConfigPtr>(mountConfig),
                task->Info->Reason);

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
                IChunkWriter::TWriteBlocksOptions(),
                Logger);

            auto compactionResultFuture =
                BIND(
                    &TPartitionCompactionSession::Run,
                    compactionSession,
                    reader,
                    task)
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

            if (RandomNumber<double>() < mountConfig->Testing.CompactionFailureProbability) {
                THROW_ERROR_EXCEPTION("Compaction failed for testing purposes");
            }

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_create_hunk_chunks_during_prepare(true);
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto(ETabletStoresUpdateReason::Compaction));
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

            task->OnFailed(std::move(error));
        }

        writerProfiler->Update(compactionResult.StoreWriter);
        writerProfiler->Update(
            compactionResult.HunkWriter,
            compactionResult.HunkWriterStatistics);

        readerProfiler->Update(
            reader,
            chunkReadOptions.ChunkReaderStatistics,
            chunkReadOptions.HunkChunkReaderStatistics);

        bool failed = task->IsFailed();

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Compaction, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Compaction, failed);

        if (!failed) {
            tabletSnapshot->TableProfiler->GetLsmCounters()->ProfileCompaction(
                task->Info->Reason,
                task->Info->RuntimeData.HunkChunkCountByReason,
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

        auto revision = RevisionFromId(chunk->GetId());
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
        TCompactionTask* task,
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
                        {
                            auto guard = Guard(task->Info->RuntimeData.SpinLock);
                            // NB: GetHunkCompactionReason will produce same result for each hunk chunk occurrence.
                            ++task->Info->RuntimeData.HunkChunkCountByReason[compactionReason];
                        }

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

                    {
                        auto guard = Guard(task->Info->RuntimeData.SpinLock);
                        ++task->Info->RuntimeData.HunkChunkCountByReason[EHunkCompactionReason::HunkChunkTooSmall];
                    }

                    InsertOrCrash(finalistIds, candidate->GetId());
                    ++i;
                }
                break;
            }
        }

        return finalistIds;
    }

    IVersionedReaderPtr CreateReader(
        TCompactionTask* task,
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
                chunkReadOptions.WorkloadDescriptor.Category,
                Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::TabletBackground)),
            tablet->GetChunkFragmentReader(),
            tabletSnapshot->DictionaryCompressionFactory,
            tablet->GetPhysicalSchema(),
            PickCompactableHunkChunkIds(
                task,
                tablet,
                stores,
                logger),
            chunkReadOptions,
            tabletSnapshot->PerformanceCounters);
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
            descriptor->set_store_type(ToProto(EStoreType::SortedChunk));
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

    static bool CompactionTaskComparer(
        const std::unique_ptr<TCompactionTask>& lhs,
        const std::unique_ptr<TCompactionTask>& rhs)
    {
        return lhs->Info->GetOrderingTuple() < rhs->Info->GetOrderingTuple();
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactor)

////////////////////////////////////////////////////////////////////////////////

TCompactionTask::TCompactionTask(
    ITabletSlot* slot,
    const TTablet* tablet,
    TPartitionId partitionId,
    std::vector<TStoreId> storeIds,
    NLsm::EStoreCompactionReason reason,
    bool discardStores,
    int slack,
    int effect,
    int futureEffect,
    TCompactionOrchidPtr orchid)
    : TGuardedTaskInfo<TCompactionTaskInfo>(
        New<TCompactionTaskInfo>(
            TGuid::Create(),
            tablet->GetId(),
            tablet->GetMountRevision(),
            tablet->GetTablePath(),
            slot->GetTabletCellBundleName(),
            partitionId,
            reason,
            discardStores,
            slack,
            effect,
            futureEffect,
            std::move(storeIds)),
        std::move(orchid))
    , Slot(slot)
    , Invoker(tablet->GetEpochAutomatonInvoker())
    , CancelableContext(tablet->GetCancelableContext())
    , TabletLoggingTag(tablet->GetLoggingTag())
{ }

TCompactionTask::~TCompactionTask() {
    if (auto owner = Owner.Lock()) {
        owner->ChangeFutureEffect(Info->TabletId, -Info->Effect);
    }
}

void TCompactionTask::Prepare(
    TStoreCompactor* owner,
    TAsyncSemaphoreGuard&& semaphoreGuard)
{
    Owner = MakeWeak(owner);
    SemaphoreGuard = std::move(semaphoreGuard);

    owner->ChangeFutureEffect(Info->TabletId, Info->Effect);
}

void TCompactionTask::StoreToStructuredLog(TFluentMap fluent)
{
    fluent
        .Item("partition_id").Value(Info->PartitionId)
        .Item("store_ids").List(Info->StoreIds)
        .Item("discard_stores").Value(Info->DiscardStores)
        .Item("slack").Value(Info->Slack)
        .Item("effect").Value(Info->Effect)
        .Item("future_effect").Value(Info->FutureEffect)
        .Item("random").Value(Info->Random)
        .Item("reason").Value(Info->Reason);
}

////////////////////////////////////////////////////////////////////////////////

IStoreCompactorPtr CreateStoreCompactor(IBootstrap* bootstrap)
{
    return New<TStoreCompactor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
