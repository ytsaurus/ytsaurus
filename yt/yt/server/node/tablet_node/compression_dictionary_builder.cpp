#include "compression_dictionary_builder.h"

#include "bootstrap.h"
#include "config.h"
#include "hunk_chunk.h"
#include "private.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <util/random/shuffle.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTableChunkFormat;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TDictionaryCompressionSampleTag
{ };

static constexpr int MinSampledBlockCountPerChunk = 3;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBuildCompressionDictionarySession)

// TODO(akozhikhov): Proper memory managment within session.
class TBuildCompressionDictionarySession
    : public TRefCounted
{
public:
    TBuildCompressionDictionarySession(
        IBootstrap* bootstrap,
        IInvokerPtr dictionaryBuilderInvoker,
        ITabletSlotPtr slot,
        const TTablet* tablet,
        EDictionaryCompressionPolicy policy,
        std::vector<TStoreId> storeIds,
        std::vector<i64> storeWeights,
        TAsyncSemaphoreGuard semaphoreGuard)
        : Bootstrap_(bootstrap)
        , Config_(tablet->GetSettings().MountConfig->ValueDictionaryCompression)
        , DictionaryBuilderInvoker_(std::move(dictionaryBuilderInvoker))
        , Slot_(std::move(slot))
        , TabletId_(tablet->GetId())
        , MountRevision_(tablet->GetMountRevision())
        , Policy_(policy)
        , StoreIds_(std::move(storeIds))
        , StoreWeights_(std::move(storeWeights))
        , SemaphoreGuard_(std::move(semaphoreGuard))
        , Logger(TabletNodeLogger
            .WithTag("Policy: %v", Policy_)
            .WithTag("CellId: %v", Slot_->GetCellId())
            .WithTag("%v", tablet->GetLoggingTag()))
    {
        YT_VERIFY(!StoreIds_.empty());
        YT_VERIFY(StoreIds_.size() == StoreWeights_.size());
    }

    void Run()
    {
        auto readerProfiler = New<TReaderProfiler>();
        auto writerProfiler = New<TWriterProfiler>();

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(WorkloadCategory_),
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryReferenceTracker = Bootstrap_->GetNodeMemoryReferenceTracker()->WithCategory(
                EMemoryCategory::TabletBackground)
        };

        Logger.AddTag("ReadSessionId: %v",
            chunkReadOptions.ReadSessionId);

        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot(
            "CompressionDictionaryBuilder"));

        const auto& tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(TabletId_);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting dictionary building");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(TabletId_, MountRevision_);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting dictionary building");
            OnSessionFailed(tablet, /*backoff*/ false);
            return;
        }

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(StoreIds_.size());
        for (const auto& storeId : StoreIds_) {
            auto store = tablet->FindStore(storeId);
            if (!store) {
                YT_LOG_DEBUG("Store is missing, aborting dictionary building (StoreId: %v)",
                    storeId);
                OnSessionFailed(tablet, /*backoff*/ false);
                return;
            }

            auto typedStore = store->AsSortedChunk();
            YT_VERIFY(typedStore);
            stores.push_back(std::move(typedStore));
        }

        chunkReadOptions.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
            tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
            tabletSnapshot->PhysicalSchema);

        bool failed = false;
        try {
            NProfiling::TWallTimer timer;

            YT_LOG_DEBUG("Compression dictionary building started");

            auto transaction = StartTransaction(tabletSnapshot->TablePath);

            tablet->ThrottleTabletStoresUpdate(Slot_, Logger);

            WaitFor(BIND(
                &TBuildCompressionDictionarySession::DoRun,
                MakeStrong(this),
                tabletSnapshot,
                stores,
                chunkReadOptions,
                transaction->GetId(),
                readerProfiler)
                .AsyncVia(DictionaryBuilderInvoker_)
                .Run())
                .ThrowOnError();

            SemaphoreGuard_.Release();

            YT_VERIFY(HunkWriter_);
            if (HunkWriter_->HasHunks()) {
                NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
                ToProto(actionRequest.mutable_tablet_id(), TabletId_);
                actionRequest.set_mount_revision(tablet->GetMountRevision());
                actionRequest.set_create_hunk_chunks_during_prepare(true);
                actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::DictionaryBuilding));

                auto* descriptor = actionRequest.add_hunk_chunks_to_add();
                ToProto(descriptor->mutable_chunk_id(), HunkWriter_->GetChunkId());
                *descriptor->mutable_chunk_meta() = *HunkWriter_->GetMeta();
                FilterProtoExtensions(
                    descriptor->mutable_chunk_meta()->mutable_extensions(),
                    GetMasterChunkMetaExtensionTagsFilter());

                auto actionData = MakeTransactionActionData(actionRequest);
                auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(TabletId_));

                transaction->AddAction(masterCellId, actionData);
                transaction->AddAction(Slot_->GetCellId(), actionData);
            } else {
                THROW_ERROR_EXCEPTION("Could not train any actual compression dictionary");
            }

            const auto& tabletManager = Slot_->GetTabletManager();
            WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
                .ThrowOnError();

            OnSessionSucceeded(tablet);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::DictionaryBuilding].Store(TError());

            YT_LOG_DEBUG("Compression dictionary building completed "
                "(WallTime: %v, DictionaryTrainingTime: %v, AddedHunkChunkId: %v)",
                timer.GetElapsedTime(),
                DictionaryTrainingTimer_.GetElapsedTime(),
                (HunkWriter_->HasHunks() ? HunkWriter_->GetChunkId() : NullChunkId));
        } catch (const std::exception& ex) {
            failed = true;

            auto error = TError(ex)
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::DictionaryBuilding);
            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::DictionaryBuilding].Store(error);

            OnSessionFailed(tablet, /*backoff*/ true);

            YT_LOG_ERROR(error, "Compression dictionary building failed");
        }

        readerProfiler->Update(
            /*reader*/ nullptr,
            chunkReadOptions.ChunkReaderStatistics,
            chunkReadOptions.HunkChunkReaderStatistics);
        writerProfiler->Update(
            HunkWriter_,
            /*hunkChunkWriterStatistics*/ nullptr);

        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::DictionaryBuilding, failed);
        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::DictionaryBuilding, failed);
    }

private:
    struct TSamplerInfo
    {
        struct TColumnInfo
        {
            const NTableClient::TColumnStableName StableName;

            std::vector<TSharedRef> Samples;
            int ProcessedNonNullSampleCount = 0;
            int ProcessedSampleCount = 0;

            i64 SampledDataWeight = 0;
            i64 ProcessedDataWeight = 0;

            TSharedRef Dictionary;
        };

        THashMap<int, TColumnInfo> ColumnIdToInfo;

        int ProcessedChunkCount = 0;
        int FetchedBlocksSize = 0;
    };

    IBootstrap* const Bootstrap_;
    const TDictionaryCompressionConfigPtr Config_;
    const IInvokerPtr DictionaryBuilderInvoker_;
    // TODO(akozhikhov): New workload category?
    const EWorkloadCategory WorkloadCategory_ = EWorkloadCategory::SystemTabletCompaction;
    const ITabletSlotPtr Slot_;
    const TTabletId TabletId_;
    const TRevision MountRevision_;

    const EDictionaryCompressionPolicy Policy_;
    const std::vector<TStoreId> StoreIds_;
    std::vector<i64> StoreWeights_;

    TAsyncSemaphoreGuard SemaphoreGuard_;

    IVersionedReaderPtr Reader_;
    IHunkChunkPayloadWriterPtr HunkWriter_;

    TSamplerInfo SamplerInfo_;
    TWallTimer DictionaryTrainingTimer_;

    TLogger Logger;


    NNative::ITransactionPtr StartTransaction(const TYPath& tablePath)
    {
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", Format("Compression dictionary building: table %v, tablet %v",
            tablePath,
            TabletId_));

        TTransactionStartOptions transactionOptions;
        transactionOptions.AutoAbort = false;
        transactionOptions.Attributes = std::move(transactionAttributes);
        transactionOptions.CoordinatorMasterCellTag = CellTagFromId(TabletId_);
        transactionOptions.ReplicateToMasterCellTags = TCellTagList();
        transactionOptions.StartCypressTransaction = false;

        auto transaction = WaitFor(Bootstrap_->GetClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            transactionOptions))
            .ValueOrThrow();

        Logger.AddTag("TransactionId: %v",
            transaction->GetId());

        YT_LOG_DEBUG("Compression dictionary building transaction created (TransactionId: %v)",
            transaction->GetId());

        return transaction;
    }

    void DoRun(
        const TTabletSnapshotPtr& tabletSnapshot,
        const std::vector<TSortedChunkStorePtr>& stores,
        const TClientChunkReadOptions& chunkReadOptions,
        TTransactionId transactionId,
        TReaderProfilerPtr readerProfiler)
    {
        YT_VERIFY(stores.size() == StoreWeights_.size());

        const auto& schema = tabletSnapshot->PhysicalSchema;
        for (int index = schema->GetKeyColumnCount(); index < schema->GetColumnCount(); ++index) {
            if (schema->Columns()[index].MaxInlineHunkSize()) {
                EmplaceOrCrash(
                    SamplerInfo_.ColumnIdToInfo,
                    index,
                    TSamplerInfo::TColumnInfo{
                        .StableName = schema->Columns()[index].StableName(),
                    });
            }
        }

        YT_VERIFY(StoreWeights_[0] > 0);
        for (int index = 1; index < std::ssize(StoreWeights_); ++index) {
            YT_VERIFY(StoreWeights_[index] > 0);
            StoreWeights_[index] += StoreWeights_[index - 1];
        }

        while (ShouldSampleMore()) {
            FetchSamplesFromChunk(tabletSnapshot, stores, chunkReadOptions, readerProfiler);
            ++SamplerInfo_.ProcessedChunkCount;
        }

        ProduceCompressionDictionariesForColumns();
        WriteCompressionDictionary(tabletSnapshot, transactionId);
    }

    void FetchSamplesFromChunk(
        const TTabletSnapshotPtr& tabletSnapshot,
        const std::vector<TSortedChunkStorePtr>& stores,
        const TClientChunkReadOptions& chunkReadOptions,
        const TReaderProfilerPtr& readerProfiler)
    {
        std::vector<int> unsaturatedColumnIds;
        for (const auto& [columnId, columnInfo] : SamplerInfo_.ColumnIdToInfo) {
            if (!IsColumnSaturated(columnInfo)) {
                unsaturatedColumnIds.push_back(columnId);
            }
        }

        TColumnFilter columnFilter(unsaturatedColumnIds);

        int randomNumber = RandomNumber<ui32>(StoreWeights_.back());
        auto storeIndex = std::distance(
            StoreWeights_.begin(),
            std::upper_bound(StoreWeights_.begin(), StoreWeights_.end(), randomNumber));

        const auto& store = stores[storeIndex];
        YT_VERIFY(store->GetId() == StoreIds_[storeIndex]);

        YT_LOG_DEBUG("Compression dictionary builder will fetch samples "
            "(IterationCount: %v, StoreIndex: %v/%v, StoreId: %v)",
            SamplerInfo_.ProcessedChunkCount,
            storeIndex,
            StoreIds_.size(),
            StoreIds_[storeIndex]);

        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        bool prepareColumnarMeta = mountConfig->EnableNewScanReaderForLookup || mountConfig->EnableNewScanReaderForSelect;
        auto backendReader = store->GetBackendReaders(WorkloadCategory_);
        auto chunkMeta = WaitFor(store->GetCachedVersionedChunkMeta(
            backendReader.ChunkReader,
            chunkReadOptions,
            prepareColumnarMeta))
            .ValueOrThrow();

        // This is just some very rough approximation.
        auto desiredPerChunkSampleCount = std::max(Config_->DesiredSampleCount / Config_->DesiredProcessedChunkCount, 1);

        std::vector<int> blockIndexes(chunkMeta->DataBlockMeta()->data_blocks_size());
        std::iota(blockIndexes.begin(), blockIndexes.end(), 0);
        Shuffle(blockIndexes.begin(), blockIndexes.end());

        std::vector<int> pickedBlockIndexes;

        int currentSampleCount = 0;
        while (!blockIndexes.empty()) {
            pickedBlockIndexes.push_back(blockIndexes.back());
            blockIndexes.pop_back();

            const auto& blockMeta = chunkMeta->DataBlockMeta()->data_blocks(pickedBlockIndexes.back());
            currentSampleCount += blockMeta.row_count();
            SamplerInfo_.FetchedBlocksSize += blockMeta.uncompressed_size();

            if (currentSampleCount >= desiredPerChunkSampleCount &&
                pickedBlockIndexes.size() >= MinSampledBlockCountPerChunk)
            {
                break;
            }
        }

        Sort(pickedBlockIndexes);

        std::vector<TUnversionedOwningRow> owningBounds;
        owningBounds.reserve(pickedBlockIndexes.size() * 2);
        std::vector<TRowRange> readRanges;
        readRanges.reserve(pickedBlockIndexes.size());

        auto transformBound = [&] (int blockIndex) {
            const auto& protoKeyBound = chunkMeta->DataBlockMeta()->data_blocks(blockIndex).last_key();
            auto owningRow = FromProto<TUnversionedOwningRow>(protoKeyBound);
            return WidenKeySuccessor(
                owningRow,
                tabletSnapshot->PhysicalSchema->GetKeyColumnCount());
        };

        for (auto blockIndex : pickedBlockIndexes) {
            auto blockUpperBound = transformBound(blockIndex);
            owningBounds.push_back(blockUpperBound);
            auto blockLowerBound = blockIndex == 0
                ? MinKey()
                : transformBound(blockIndex - 1);
            owningBounds.push_back(blockLowerBound);
            readRanges.emplace_back(blockLowerBound, blockUpperBound);
        }

        auto sharedReadRanges = MakeSharedRange(std::move(readRanges), std::move(owningBounds));

        THashSet<TChunkId> referencedHunkChunkIds;
        for (const auto& hunkRef : store->HunkChunkRefs()) {
            EmplaceOrCrash(referencedHunkChunkIds, hunkRef.HunkChunk->GetId());
        }

        // NB: As of now we cannot read from unversioned chunks with specified column fitler.
        bool produceAllVersions;
        switch (chunkMeta->GetChunkFormat()) {
            case EChunkFormat::TableVersionedSimple:
            case EChunkFormat::TableVersionedSlim:
            case EChunkFormat::TableVersionedIndexed:
            case EChunkFormat::TableVersionedColumnar:
                produceAllVersions = true;
                break;

            case EChunkFormat::TableUnversionedSchemalessHorizontal:
            case EChunkFormat::TableUnversionedColumnar:
                produceAllVersions = false;
                break;

            default:
                THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv",
                    chunkMeta->GetChunkFormat());
        }

        // TODO(akozhikhov): Prevent block cache pollution when reading here.
        auto reader = CreateHunkInliningVersionedReader(
            tabletSnapshot->Settings.HunkReaderConfig,
            store->CreateReader(
                tabletSnapshot,
                sharedReadRanges,
                AsyncLastCommittedTimestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                WorkloadCategory_),
            tabletSnapshot->ChunkFragmentReader,
            tabletSnapshot->DictionaryCompressionFactory,
            tabletSnapshot->PhysicalSchema,
            referencedHunkChunkIds,
            chunkReadOptions);

        WaitForFast(reader->Open())
            .ThrowOnError();

        auto processedRowCount = ProcessRows(reader);
        YT_LOG_ALERT_IF(currentSampleCount != processedRowCount,
            "Compression dictionary builder processed unexpected amount of rows "
            "(Actual: %v, Expected: %v, ChunkId: %v, Ranges: [%v])",
            processedRowCount,
            currentSampleCount,
            store->GetId(),
            MakeFormattableView(
                sharedReadRanges,
                [&] (auto* builder, const auto& readRange) {
                    builder->AppendFormat("(%v : %v);",
                        readRange.first,
                        readRange.second);
                }));
        // YT_VERIFY(currentSampleCount == processedRowCount);

        readerProfiler->Update(
            reader,
            /*chunkReaderStatistics*/ nullptr,
            /*hunkChunkReaderStatistics*/ nullptr);
    }

    int ProcessRows(const IVersionedReaderPtr& reader)
    {
        int processedRowCount = 0;
        TRowBatchReadOptions readOptions;
        while (auto batch = reader->Read(readOptions)) {
            if (batch->IsEmpty()) {
                WaitForFast(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto row : batch->MaterializeRows()) {
                ++processedRowCount;

                for (const auto& value : row.Values()) {
                    // FIXME(akozhikhov): Now we ignore column filter when reading all versions from backing store.
                    // auto& columnInfo = GetOrCrash(SamplerInfo_.ColumnIdToInfo, value.Id);
                    auto it = SamplerInfo_.ColumnIdToInfo.find(value.Id);
                    if (it == SamplerInfo_.ColumnIdToInfo.end()) {
                        continue;
                    }
                    auto& columnInfo = it->second;
                    ++columnInfo.ProcessedSampleCount;

                    if (value.Type == EValueType::Null) {
                        continue;
                    }

                    YT_VERIFY(IsStringLikeType(value.Type));
                    YT_VERIFY(None(value.Flags & EValueFlags::Hunk));

                    MaybeAddSample(
                        &columnInfo,
                        TRef(value.Data.String, value.Length));
                };
            }
        }

        return processedRowCount;
    }

    void MaybeAddSample(TSamplerInfo::TColumnInfo* columnInfo, TRef sample)
    {
        ++columnInfo->ProcessedNonNullSampleCount;
        columnInfo->ProcessedDataWeight += sample.Size();

        bool canAddSample = false;
        if (IsColumnSaturated(*columnInfo)) {
            // NB: This is some variation of reservoir sampling where we have additional constraint
            // over data weight which makes it a little chaotic.
            int indexInReservoir = RandomNumber<ui32>(columnInfo->ProcessedNonNullSampleCount);
            if (indexInReservoir < std::ssize(columnInfo->Samples)) {
                std::swap(columnInfo->Samples.back(), columnInfo->Samples[indexInReservoir]);
                auto newDataWeight = columnInfo->SampledDataWeight - std::ssize(columnInfo->Samples.back()) + std::ssize(sample);
                if (newDataWeight >= Config_->DesiredSampledDataWeight) {
                    columnInfo->SampledDataWeight -= columnInfo->Samples.back().Size();
                    columnInfo->Samples.pop_back();
                }
                canAddSample = true;
            }
        } else {
            canAddSample = true;
        }

        if (canAddSample) {
            columnInfo->Samples.push_back(TSharedRef::MakeCopy<TDictionaryCompressionSampleTag>(sample));
            columnInfo->SampledDataWeight += sample.Size();
        }
    }

    bool ShouldSampleMore() const
    {
        auto logUponFinishedSampling = [&] (TStringBuf reason) {
            YT_LOG_DEBUG("Compression dictionary builder finished sampling rows "
                "(Reason: %v, ChunkCount: %v, FetchedBlocksSize: %v, ColumnStatistics: {%v})",
                reason,
                SamplerInfo_.ProcessedChunkCount,
                SamplerInfo_.FetchedBlocksSize,
                MakeFormattableView(
                    SamplerInfo_.ColumnIdToInfo,
                    [&] (auto* builder, const auto& columnInfo) {
                        builder->AppendFormat("%Qv: {", columnInfo.second.StableName);
                        builder->AppendFormat("Samples: %v/%v/%v, ",
                            columnInfo.second.Samples.size(),
                            columnInfo.second.ProcessedNonNullSampleCount,
                            columnInfo.second.ProcessedSampleCount);
                        builder->AppendFormat("DataWeight: %v/%v}",
                            columnInfo.second.SampledDataWeight,
                            columnInfo.second.ProcessedDataWeight);
                    }));
        };

        if (SamplerInfo_.FetchedBlocksSize > Config_->MaxFetchedBlocksSize) {
            logUponFinishedSampling("\"max_fetched_blocks_size\" exceeded");
            return false;
        }

        if (SamplerInfo_.ProcessedChunkCount > Config_->MaxProcessedChunkCount) {
            logUponFinishedSampling("\"max_processed_chunk_count\" exceeded");
            return false;
        }

        bool hasUnsaturatedColumn = false;
        for (const auto& [_, columnInfo] : SamplerInfo_.ColumnIdToInfo) {
            if (columnInfo.ProcessedSampleCount > Config_->MaxProcessedSampleCount) {
                logUponFinishedSampling("\"max_processed_sample_count\" exceeded");
                return false;
            }

            if (columnInfo.ProcessedDataWeight > Config_->MaxProcessedDataWeight) {
                logUponFinishedSampling("\"max_processed_data_weight\" exceeded");
                return false;
            }

            hasUnsaturatedColumn |= !IsColumnSaturated(columnInfo);
        }

        if (SamplerInfo_.ProcessedChunkCount < Config_->DesiredProcessedChunkCount) {
            return true;
        }

        if (!hasUnsaturatedColumn) {
            logUponFinishedSampling("all columns are saturated");
        }

        return hasUnsaturatedColumn;
    }

    bool IsColumnSaturated(const TSamplerInfo::TColumnInfo& columnInfo) const
    {
        return
            std::ssize(columnInfo.Samples) >= Config_->DesiredSampleCount &&
            columnInfo.SampledDataWeight >= Config_->DesiredSampledDataWeight;
    }

    void ProduceCompressionDictionariesForColumns()
    {
        DictionaryTrainingTimer_.Restart();
        for (auto& [columnId, columnInfo] : SamplerInfo_.ColumnIdToInfo) {
            i64 totalDataWeight = 0;
            for (const auto& sample : columnInfo.Samples) {
                totalDataWeight += sample.Size();
            }
            YT_VERIFY(columnInfo.SampledDataWeight == totalDataWeight);
            YT_VERIFY(columnInfo.SampledDataWeight <= columnInfo.ProcessedDataWeight);
            YT_VERIFY(std::ssize(columnInfo.Samples) <= columnInfo.ProcessedNonNullSampleCount);
            YT_VERIFY(columnInfo.ProcessedNonNullSampleCount <= columnInfo.ProcessedSampleCount);

            auto dictionaryOrError = NCompression::GetDictionaryCompressionCodec()->TrainCompressionDictionary(
                Config_->ColumnDictionarySize,
                columnInfo.Samples);
            if (dictionaryOrError.IsOK()) {
                columnInfo.Dictionary = dictionaryOrError.Value();
            } else {
                YT_LOG_DEBUG(dictionaryOrError,
                    "Compression dictionary training error occurred; builder will skip corresponding column ",
                    "(ColumnId: %v)",
                    columnId);
            }
            columnInfo.Samples.clear();
        }
        DictionaryTrainingTimer_.Stop();
    }

    void WriteCompressionDictionary(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTransactionId transactionId)
    {
        auto hunkWriterConfig = CloneYsonStruct(tabletSnapshot->Settings.HunkWriterConfig);
        hunkWriterConfig->WorkloadDescriptor = TWorkloadDescriptor(WorkloadCategory_);
        hunkWriterConfig->MinUploadReplicationFactor = hunkWriterConfig->UploadReplicationFactor;

        auto hunkWriterOptions = CloneYsonStruct(tabletSnapshot->Settings.HunkWriterOptions);
        hunkWriterOptions->ValidateResourceUsageIncrease = false;
        hunkWriterOptions->ConsistentChunkReplicaPlacementHash = tabletSnapshot->ConsistentChunkReplicaPlacementHash;
        // NB: Disable erasure encoding for dictionary hunk chunks.
        hunkWriterOptions->ErasureCodec = NErasure::ECodec::None;

        auto chunkWriter = CreateConfirmingWriter(
            hunkWriterConfig,
            hunkWriterOptions,
            CellTagFromId(tabletSnapshot->TabletId),
            transactionId,
            tabletSnapshot->SchemaId,
            /*parentChunkListId*/ {},
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalHostName(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            Bootstrap_->GetOutThrottler(WorkloadCategory_));

        HunkWriter_ = CreateHunkChunkPayloadWriter(
            TWorkloadDescriptor(WorkloadCategory_),
            hunkWriterConfig,
            chunkWriter);

        WaitFor(HunkWriter_->Open())
            .ThrowOnError();

        struct TColumnDictionaryAddress
        {
            int BlockIndex;
            i64 BlockOffset;
            i64 Length;
        };

        THashMap<int, TColumnDictionaryAddress> addresses;
        for (const auto& [columnId, columnInfo] : SamplerInfo_.ColumnIdToInfo) {
            if (!columnInfo.Dictionary) {
                continue;
            }

            auto [blockIndex, blockOffset, ready] = HunkWriter_->WriteHunk(
                columnInfo.Dictionary,
                columnInfo.Dictionary.size());
            EmplaceOrCrash(
                addresses,
                columnId,
                TColumnDictionaryAddress{
                    .BlockIndex = blockIndex,
                    .BlockOffset = blockOffset,
                    .Length = std::ssize(columnInfo.Dictionary),
                });
            if (!ready) {
                WaitFor(HunkWriter_->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        // This is actually redundant.
        HunkWriter_->OnParentReaderFinished(/*compressionDictionaryId*/ NullChunkId);
        YT_VERIFY(HunkWriter_->GetErasureCodecId() == NErasure::ECodec::None);

        HunkWriter_->GetMeta()->RegisterFinalizer([&] (TDeferredChunkMeta* meta) mutable {
            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
            miscExt.set_dictionary_compression_policy(ToProto<int>(Policy_));
            SetProtoExtension(meta->mutable_extensions(), miscExt);

            NTableClient::NProto::TCompressionDictionaryExt compressionDictionaryExt;
            for (const auto& [columnId, columnInfo] : SamplerInfo_.ColumnIdToInfo) {
                auto addressIt = addresses.find(columnId);
                if (addressIt == addresses.end()) {
                    continue;
                }

                auto* protoColumnInfo = compressionDictionaryExt.add_column_infos();
                ToProto(protoColumnInfo->mutable_stable_name(), columnInfo.StableName);
                protoColumnInfo->set_length(addressIt->second.Length);
                protoColumnInfo->set_block_index(addressIt->second.BlockIndex);
                protoColumnInfo->set_block_offset(addressIt->second.BlockOffset);
            }
            SetProtoExtension(meta->mutable_extensions(), compressionDictionaryExt);
        });

        WaitFor(HunkWriter_->Close())
            .ThrowOnError();

        if (!HunkWriter_->HasHunks()) {
            return;
        }

        if (tabletSnapshot->Settings.MountConfig->RegisterChunkReplicasOnStoresUpdate) {
            // TODO(kvk1920): Consider using chunk + location instead of chunk + node + medium.
            auto writtenReplicas = chunkWriter->GetWrittenChunkReplicas();
            TChunkReplicaWithMediumList replicas;
            replicas.reserve(writtenReplicas.size());
            for (auto replica : writtenReplicas) {
                replicas.push_back(replica);
            }

            const auto& chunkReplicaCache = Bootstrap_->GetConnection()->GetChunkReplicaCache();
            chunkReplicaCache->RegisterReplicas(chunkWriter->GetChunkId(), replicas);
        }

        YT_LOG_DEBUG("Compression dictionary builder finished writing dictionary hunk chunk "
            "(ChunkId: %v, DictionarySizes: %v)",
            chunkWriter->GetChunkId(),
            MakeFormattableView(
                SamplerInfo_.ColumnIdToInfo,
                [&] (auto* builder, const auto& columnInfoIt) {
                    if (!columnInfoIt.second.Dictionary) {
                        builder->AppendString("<null>");
                    } else {
                        builder->AppendFormat("%v",
                            columnInfoIt.second.Dictionary.size());
                    }
                }));
    }

    void OnSessionSucceeded(TTablet* tablet) const
    {
        tablet->SetDictionaryBuildingInProgress(Policy_, false);
        tablet->SetCompressionDictionaryRebuildBackoffTime(Policy_, TInstant::Now() + Config_->RebuildPeriod);
    }

    void OnSessionFailed(TTablet* tablet, bool backoff) const
    {
        tablet->SetDictionaryBuildingInProgress(Policy_, false);
        if (backoff) {
            tablet->SetCompressionDictionaryRebuildBackoffTime(Policy_, TInstant::Now() + Config_->BackoffPeriod);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TBuildCompressionDictionarySession)

////////////////////////////////////////////////////////////////////////////////

class TCompressionDictionaryBuilder
    : public ICompressionDictionaryBuilder
{
public:
    TCompressionDictionaryBuilder(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode->CompressionDictionaryBuilder)
        , ThreadPool_(CreateThreadPool(Config_->ThreadPoolSize, "ComprDctBldr"))
        , BuildTaskSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentBuildTasks,
            Profiler_.Gauge("/running_tasks")))
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(
            &TCompressionDictionaryBuilder::OnScanSlot,
            MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(
            &TCompressionDictionaryBuilder::OnDynamicConfigChanged,
            MakeWeak(this)));
    }

private:
    struct TBuildCompressionDictionaryRequest
    {
        TTabletId TabletId;
        TInstant DictionaryRebuildBackoffTime;
        NTableClient::EDictionaryCompressionPolicy Policy;

        std::vector<TStoreId> StoreIds;
        std::vector<i64> StoreWeights;
    };

    IBootstrap* const Bootstrap_;
    const TCompressionDictionaryBuilderConfigPtr Config_;

    const TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/compression_dict_builder");
    const TCounter StartedTasksCounter_ = Profiler_.Counter("/started_tasks");

    // TODO(akozhikhov): Use Compression thread pool when it becomes fair share.
    const IThreadPoolPtr ThreadPool_;
    const TAsyncSemaphorePtr BuildTaskSemaphore_;


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig)
    {
        const auto& dynamicConfig = newConfig->TabletNode->CompressionDictionaryBuilder;
        ThreadPool_->Configure(dynamicConfig->ThreadPoolSize.value_or(Config_->ThreadPoolSize));
        BuildTaskSemaphore_->SetTotal(dynamicConfig->MaxConcurrentBuildTasks.value_or(Config_->MaxConcurrentBuildTasks));
    }

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        YT_LOG_DEBUG("Compression dictionary builder started scanning slot (CellId: %v)",
            slot->GetCellId());

        const auto& tabletManager = slot->GetTabletManager();
        std::vector<TBuildCompressionDictionaryRequest> requests;
        for (const auto& [_, tablet] : tabletManager->Tablets()) {
            auto tabletRequests = ScanTablet(tablet);
            requests.insert(
                requests.end(),
                std::make_move_iterator(tabletRequests.begin()),
                std::make_move_iterator(tabletRequests.end()));
        }

        YT_LOG_DEBUG("Compression dictionary builder finished scanning slot (CellId: %v)",
            slot->GetCellId());

        SortBy(requests, [] (const auto& request) {
            return request.DictionaryRebuildBackoffTime;
        });

        int requestIndex = 0;
        for (; requestIndex < std::ssize(requests); ++requestIndex) {
            if (!StartTask(slot, requests[requestIndex])) {
                break;
            }
        }

        YT_LOG_DEBUG("Compression dictionary builder finished scheduling tasks "
            "(CellId: %v, FeasibleTaskCount: %v, StartedTaskCount: %v)",
            slot->GetCellId(),
            requests.size(),
            requestIndex);
    }

    std::vector<TBuildCompressionDictionaryRequest> ScanTablet(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return {};
        }

        if (!tablet->IsPhysicallySorted() || tablet->IsReplicated()) {
            return {};
        }

        const auto& mountConfig = tablet->GetSettings().MountConfig;
        const auto& config = mountConfig->ValueDictionaryCompression;
        if (!config->Enable) {
            return {};
        }

        const auto& schema = tablet->GetPhysicalSchema();
        bool hasHunkValueColumns = false;
        for (int index = schema->GetKeyColumnCount(); index < schema->GetColumnCount(); ++index) {
            hasHunkValueColumns |= schema->Columns()[index].MaxInlineHunkSize().has_value();
        }
        if (!hasHunkValueColumns) {
            return {};
        }

        if (tablet->GetSettings().StoreWriterOptions->OptimizeFor != EOptimizeFor::Lookup) {
            return {};
        }

        std::vector<TBuildCompressionDictionaryRequest> requests;

        if (!tablet->IsDictionaryBuildingInProgress(EDictionaryCompressionPolicy::LargeChunkFirst) &&
            TInstant::Now() >= tablet->GetCompressionDictionaryRebuildBackoffTime(
                EDictionaryCompressionPolicy::LargeChunkFirst))
        {
            MaybeAddWeightBasedRequest(tablet, &requests);
        }

        if (!tablet->IsDictionaryBuildingInProgress(EDictionaryCompressionPolicy::FreshChunkFirst) &&
            TInstant::Now() >= tablet->GetCompressionDictionaryRebuildBackoffTime(
                EDictionaryCompressionPolicy::FreshChunkFirst))
        {
            MaybeAddFreshnessBasedRequest(tablet, &requests);
        }

        return requests;
    }

    void MaybeAddWeightBasedRequest(TTablet* tablet, std::vector<TBuildCompressionDictionaryRequest>* requests)
    {
        MaybeAddRequest(
            EDictionaryCompressionPolicy::LargeChunkFirst,
            tablet,
            requests,
            [] (const IChunkStorePtr& store) {
                return store->GetUncompressedDataSize();
            });
    }

    void MaybeAddFreshnessBasedRequest(TTablet* tablet, std::vector<TBuildCompressionDictionaryRequest>* requests)
    {
        std::optional<TInstant> oldestChunkCreationTime;
        auto findOldestChunk = [&] (const TPartition* partition) {
            for (const auto& store : partition->Stores()) {
                if (store->GetType() == EStoreType::SortedChunk) {
                    auto chunkStore = store->AsChunk();
                    oldestChunkCreationTime = oldestChunkCreationTime
                        ? std::min(*oldestChunkCreationTime, chunkStore->GetCreationTime())
                        : chunkStore->GetCreationTime();
                }
            }
        };

        findOldestChunk(tablet->GetEden());
        for (const auto& partition : tablet->PartitionList()) {
            findOldestChunk(partition.get());
        }

        MaybeAddRequest(
            EDictionaryCompressionPolicy::FreshChunkFirst,
            tablet,
            requests,
            [&] (const IChunkStorePtr& store) {
                auto minutes = (store->GetCreationTime() - *oldestChunkCreationTime).Minutes();
                return static_cast<i64>(std::log(minutes + 3));
            });
    }

    template <typename TComputeWeight>
    void MaybeAddRequest(
        EDictionaryCompressionPolicy policy,
        TTablet* tablet,
        std::vector<TBuildCompressionDictionaryRequest>* requests,
        const TComputeWeight& computeWeight)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        const auto& config = mountConfig->ValueDictionaryCompression;
        if (!config->AppliedPolicies.contains(policy)) {
            return;
        }

        auto& request = requests->emplace_back();
        request.TabletId = tablet->GetId();
        request.DictionaryRebuildBackoffTime = tablet->GetCompressionDictionaryRebuildBackoffTime(policy);
        request.Policy = policy;

        auto addStoresToRequest = [&] (const TPartition* partition) {
            for (const auto& store : partition->Stores()) {
                if (store->GetType() == EStoreType::SortedChunk && TypeFromId(store->GetId()) != EObjectType::ChunkView) {
                    auto chunkStore = store->AsChunk();
                    auto chunkFormat = CheckedEnumCast<EChunkFormat>(chunkStore->GetChunkMeta().format());
                    if (OptimizeForFromFormat(chunkFormat) == EOptimizeFor::Lookup) {
                        request.StoreIds.push_back(chunkStore->GetId());
                        request.StoreWeights.push_back(computeWeight(chunkStore));
                    }
                }
            }
        };

        addStoresToRequest(tablet->GetEden());
        for (const auto& partition : tablet->PartitionList()) {
            addStoresToRequest(partition.get());
        }

        if (request.StoreIds.empty()) {
            requests->pop_back();
            return;
        }

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Added compression dictionary build request "
            "(%v, Policy: %v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            policy,
            request.StoreIds.size());
    }

    bool StartTask(
        const ITabletSlotPtr& slot,
        const TBuildCompressionDictionaryRequest& request)
    {
        auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(BuildTaskSemaphore_);
        if (!semaphoreGuard) {
            return false;
        }

        auto* tablet = slot->GetTabletManager()->GetTablet(request.TabletId);

        YT_VERIFY(!tablet->IsDictionaryBuildingInProgress(request.Policy));
        tablet->SetDictionaryBuildingInProgress(request.Policy, true);

        auto session = New<TBuildCompressionDictionarySession>(
            Bootstrap_,
            ThreadPool_->GetInvoker(),
            slot,
            tablet,
            request.Policy,
            request.StoreIds,
            request.StoreWeights,
            std::move(semaphoreGuard));

        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TBuildCompressionDictionarySession::Run,
            session));

        YT_LOG_DEBUG("Compression dictionary builder started new task "
            "(%v, Policy: %v, DictionaryRebuildBackoffTime: %v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            request.Policy,
            request.DictionaryRebuildBackoffTime,
            request.StoreIds.size());

        StartedTasksCounter_.Increment();

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICompressionDictionaryBuilderPtr CreateCompressionDictionaryBuilder(IBootstrap* bootstrap)
{
    return New<TCompressionDictionaryBuilder>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
