#include "in_memory_manager.h"

#include "bootstrap.h"
#include "in_memory_service_proxy.h"
#include "private.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/algorithm_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

#include <yt/yt/core/rpc/local_channel.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NRpc;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTAlloc;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TBlocksExt;
using NTableClient::NProto::TBlockMetaExt;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

using TInMemorySessionId = TGuid;

////////////////////////////////////////////////////////////////////////////////

namespace {

void FinalizeChunkData(
    const TInMemoryChunkDataPtr& data,
    TChunkId id,
    const TRefCountedChunkMetaPtr& meta,
    const TTabletSnapshotPtr& tabletSnapshot)
{
    if (!data->ChunkMeta) {
        data->ChunkMeta = TCachedVersionedChunkMeta::Create(id, *meta, tabletSnapshot->PhysicalSchema);
    }

    if (data->MemoryTrackerGuard) {
        data->MemoryTrackerGuard.IncrementSize(data->ChunkMeta->GetMemoryUsage());
    }

    if (tabletSnapshot->HashTableSize > 0) {
        data->LookupHashTable = CreateChunkLookupHashTable(
            data->StartBlockIndex,
            data->Blocks,
            data->ChunkMeta,
            tabletSnapshot->PhysicalSchema,
            tabletSnapshot->RowKeyComparer);
        if (data->LookupHashTable && data->MemoryTrackerGuard) {
            data->MemoryTrackerGuard.IncrementSize(data->LookupHashTable->GetByteSize());
        }
    }
}

} // namespace

EBlockType MapInMemoryModeToBlockType(EInMemoryMode mode)
{
    switch (mode) {
        case EInMemoryMode::Compressed:
            return EBlockType::CompressedData;

        case EInMemoryMode::Uncompressed:
            return EBlockType::UncompressedData;

        case EInMemoryMode::None:
            return EBlockType::None;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInMemoryManager)

class TInMemoryManager
    : public IInMemoryManager
{
public:
    TInMemoryManager(
        TInMemoryManagerConfigPtr config,
        IBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , CompressionInvoker_(CreateFixedPriorityInvoker(
            NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            Config_->WorkloadDescriptor.GetPriority()))
        , PreloadSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentPreloads))
    {
        auto slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TInMemoryManager::ScanSlot, MakeWeak(this)));
    }

    virtual TInMemoryChunkDataPtr EvictInterceptedChunkData(TChunkId chunkId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(InterceptedDataSpinLock_);

        auto it = ChunkIdToData_.find(chunkId);
        if (it == ChunkIdToData_.end()) {
            return nullptr;
        }

        auto chunkData = std::move(it->second);
        ChunkIdToData_.erase(it);

        YT_LOG_INFO("Intercepted chunk data evicted (ChunkId: %v, Mode: %v)",
            chunkId,
            chunkData->InMemoryMode);

        return chunkData;
    }

    virtual void FinalizeChunk(
        TChunkId chunkId,
        TInMemoryChunkDataPtr data,
        const TRefCountedChunkMetaPtr& chunkMeta,
        const TTabletSnapshotPtr& tabletSnapshot) override
    {
        FinalizeChunkData(std::move(data), chunkId, chunkMeta, tabletSnapshot);

        {
            auto guard = WriterGuard(InterceptedDataSpinLock_);

            // Replace the old data, if any, by a new one.
            ChunkIdToData_[chunkId] = data;
        }

        // Schedule eviction.
        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TInMemoryManager::EvictInterceptedChunkData), MakeStrong(this), chunkId),
            Config_->InterceptedDataRetentionTime);
    }

    const TInMemoryManagerConfigPtr& GetConfig() const override
    {
        return Config_;
    }

private:
    const TInMemoryManagerConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    const IInvokerPtr CompressionInvoker_;

    TAsyncSemaphorePtr PreloadSemaphore_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, InterceptedDataSpinLock_);
    THashMap<TChunkId, TInMemoryChunkDataPtr> ChunkIdToData_;


    void ScanSlot(const ITabletSlotPtr& slot)
    {
        const auto& tabletManager = slot->GetTabletManager();
        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(const ITabletSlotPtr& slot, TTablet* tablet)
    {
        auto state = tablet->GetState();
        if (IsInUnmountWorkflow(state)) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        while (true) {
            auto store = storeManager->PeekStoreForPreload();

            if (!store) {
                break;
            }
            auto guard = TAsyncSemaphoreGuard::TryAcquire(PreloadSemaphore_);
            if (!guard) {
                break;
            }

            auto preloadStoreCallback =
                BIND(
                    &TInMemoryManager::PreloadStore,
                    MakeStrong(this),
                    Passed(std::move(guard)),
                    slot,
                    tablet,
                    store,
                    storeManager)
                .AsyncVia(tablet->GetEpochAutomatonInvoker());
            storeManager->BeginStorePreload(store, preloadStoreCallback);
        }
    }

    void PreloadStore(
        TAsyncSemaphoreGuard /*guard*/,
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        const IChunkStorePtr& store,
        const IStoreManagerPtr& storeManager)
    {
        VERIFY_INVOKERS_AFFINITY(std::vector{
            tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
            tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)
        });

        auto readSessionId = TReadSessionId::Create();
        auto mode = store->GetInMemoryMode();

        auto Logger = TabletNodeLogger
            .WithTag("%v, StoreId: %v, Mode: %v, ReadSessionId: %v",
                tablet->GetLoggingTag(),
                store->GetId(),
                mode,
                readSessionId);

        YT_LOG_INFO("Preloading in-memory store");

        YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Running);

        if (mode == EInMemoryMode::None) {
            // Mode has been changed while current action was waiting in action queue
            YT_LOG_INFO("In-memory mode has been changed");

            store->SetPreloadState(EStorePreloadState::None);
            tablet->GetStructuredLogger()->OnStorePreloadStateChanged(store);
            store->SetPreloadFuture(TFuture<void>());
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(tablet->GetId(), tablet->GetMountRevision());
        if (!tabletSnapshot) {
            YT_LOG_INFO("Tablet snapshot is missing");

            store->UpdatePreloadAttempt(false);
            storeManager->BackoffStorePreload(store);
            return;
        }

        bool failed = false;
        auto readerProfiler = New<TReaderProfiler>();
        auto profileGuard = Finally([&] () {
            readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Preload, failed);
        });

        try {
            // This call may suspend the current fiber.
            auto chunkData = PreloadInMemoryStore(
                tabletSnapshot,
                store,
                readSessionId,
                Bootstrap_->GetMemoryUsageTracker(),
                CompressionInvoker_,
                readerProfiler);

            VERIFY_INVOKERS_AFFINITY(std::vector{
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)
            });

            YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Running);

            store->Preload(std::move(chunkData));
            storeManager->EndStorePreload(store);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Preload].Store(TError());
        } catch (const std::exception& ex) {
            // Do not back off if fiber cancellation exception was thrown.
            // SetInMemoryMode with other mode was called during current action execution.

            YT_LOG_ERROR(ex, "Error preloading tablet store, backing off");
            store->UpdatePreloadAttempt(true);
            storeManager->BackoffStorePreload(store);

            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Preload);

            failed = true;
            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Preload].Store(error);
        } catch (const TFiberCanceledException&) {
            YT_LOG_DEBUG("Preload cancelled");
            throw;
        } catch (...) {
            YT_LOG_DEBUG("Unknown exception in preload");
            throw;
        }

        snapshotStore->RegisterTabletSnapshot(slot, tablet);
    }

};

DEFINE_REFCOUNTED_TYPE(TInMemoryManager)

IInMemoryManagerPtr CreateInMemoryManager(
    TInMemoryManagerConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TInMemoryManager>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    TReadSessionId readSessionId,
    const NClusterNode::TNodeMemoryTrackerPtr& memoryTracker,
    const IInvokerPtr& compressionInvoker,
    const TReaderProfilerPtr& readerProfiler)
{
    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    auto mode = mountConfig->InMemoryMode;

    auto Logger = TabletNodeLogger
        .WithTag("%v, StoreId: %v, Mode: %v, ReadSessionId: %v",
            tabletSnapshot->LoggingTag,
            store->GetId(),
            mode,
            readSessionId);

    YT_LOG_INFO("Store preload started");

    TClientChunkReadOptions chunkReadOptions{
        .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPreload),
        .ReadSessionId = readSessionId
    };

    readerProfiler->SetChunkReaderStatistics(chunkReadOptions.ChunkReaderStatistics);

    auto reader = store->GetReaders(EWorkloadCategory::SystemTabletPreload).ChunkReader;
    auto meta = WaitFor(reader->GetMeta(chunkReadOptions))
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<TMiscExt>(meta->extensions());
    auto blocksExt = GetProtoExtension<TBlocksExt>(meta->extensions());
    auto format = CheckedEnumCast<EChunkFormat>(meta->format());

    if (format == EChunkFormat::TableSchemalessHorizontal ||
        format == EChunkFormat::TableUnversionedColumnar)
    {
        // For unversioned chunks verify that block size is correct
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
            if (miscExt.max_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                    << TErrorAttribute("chunk_id", store->GetId())
                    << TErrorAttribute("block_size", miscExt.max_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }

    auto erasureCodec = FromProto<NErasure::ECodec>(miscExt.erasure_codec());
    if (erasureCodec != NErasure::ECodec::None) {
        THROW_ERROR_EXCEPTION("Preloading erasure-coded store %v is not supported; consider using replicated chunks or disabling in-memory mode",
            store->GetId());
    }

    auto codecId = CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec());
    auto* codec = NCompression::GetCodec(codecId);

    auto chunkData = New<TInMemoryChunkData>();

    int startBlockIndex;
    int endBlockIndex;

    // TODO(ifsmirnov): support columnar chunks (YT-11707).
    bool canDeduceBlockRange =
        format == EChunkFormat::TableSchemalessHorizontal ||
        format == EChunkFormat::TableVersionedSimple;

    if (store->IsSorted() && canDeduceBlockRange) {
        chunkData->ChunkMeta = TCachedVersionedChunkMeta::Create(
            store->GetChunkId(),
            *meta,
            tabletSnapshot->PhysicalSchema);

        auto sortedStore = store->AsSortedChunk();
        auto lowerBound = std::max(tabletSnapshot->PivotKey, sortedStore->GetMinKey());
        auto upperBound = std::min(tabletSnapshot->NextPivotKey, sortedStore->GetUpperBoundKey());

        auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(meta->extensions());

        startBlockIndex = BinarySearch(0, blocksExt.blocks_size(), [&] (int index) {
            return chunkData->ChunkMeta->LegacyBlockLastKeys()[index] < lowerBound;
        });

        endBlockIndex = BinarySearch(0, blocksExt.blocks_size(), [&] (int index) {
            return chunkData->ChunkMeta->LegacyBlockLastKeys()[index] < upperBound;
        });
        if (endBlockIndex < blocksExt.blocks_size()) {
            ++endBlockIndex;
        }
    } else {
        startBlockIndex = 0;
        endBlockIndex = blocksExt.blocks_size();
    }

    i64 preallocatedMemory = 0;
    i64 allocatedMemory = 0;
    i64 compressedDataSize = 0;

    TDuration decompressionTime;

    for (int i = startBlockIndex; i < endBlockIndex; ++i) {
        preallocatedMemory += blocksExt.blocks(i).size();
    }

    if (memoryTracker && memoryTracker->GetFree(EMemoryCategory::TabletStatic) < preallocatedMemory) {
        THROW_ERROR_EXCEPTION("Preload is cancelled due to memory pressure");
    }

    chunkData->InMemoryMode = mode;
    chunkData->StartBlockIndex = startBlockIndex;
    if (memoryTracker) {
        chunkData->MemoryTrackerGuard = TMemoryUsageTrackerGuard::Acquire(
            memoryTracker->WithCategory(EMemoryCategory::TabletStatic),
            preallocatedMemory,
            MemoryUsageGranularity);
    }
    chunkData->Blocks.reserve(endBlockIndex - startBlockIndex);

    while (startBlockIndex < endBlockIndex) {
        YT_LOG_DEBUG("Started reading chunk blocks (FirstBlock: %v)",
            startBlockIndex);

        auto compressedBlocks = WaitFor(reader->ReadBlocks(
            chunkReadOptions,
            startBlockIndex,
            endBlockIndex - startBlockIndex))
            .ValueOrThrow();

        int readBlockCount = compressedBlocks.size();
        YT_LOG_DEBUG("Finished reading chunk blocks (Blocks: %v-%v)",
            startBlockIndex,
            startBlockIndex + readBlockCount - 1);

        for (const auto& compressedBlock : compressedBlocks) {
            compressedDataSize += compressedBlock.Size();
            readerProfiler->SetCompressedDataSize(compressedDataSize);
        }

        std::vector<TBlock> cachedBlocks;
        switch (mode) {
            case EInMemoryMode::Compressed: {
                for (const auto& compressedBlock : compressedBlocks) {
                    TMemoryZoneGuard memoryZoneGuard(EMemoryZone::Undumpable);
                    auto undumpableData = TSharedRef::MakeCopy<TPreloadedBlockTag>(compressedBlock.Data);
                    auto block = TBlock(undumpableData, compressedBlock.Checksum, compressedBlock.BlockOrigin);
                    cachedBlocks.push_back(std::move(block));
                }

                break;
            }

            case EInMemoryMode::Uncompressed: {
                YT_LOG_DEBUG("Decompressing chunk blocks (Blocks: %v-%v, Codec: %v)",
                    startBlockIndex,
                    startBlockIndex + readBlockCount - 1,
                    codec->GetId());

                std::vector<TFuture<std::pair<TSharedRef, TDuration>>> asyncUncompressedBlocks;
                for (auto& compressedBlock : compressedBlocks) {
                    asyncUncompressedBlocks.push_back(
                        BIND([&] {
                                TMemoryZoneGuard memoryZoneGuard(EMemoryZone::Undumpable);
                                NProfiling::TFiberWallTimer timer;
                                auto block = codec->Decompress(compressedBlock.Data);
                                return std::make_pair(std::move(block), timer.GetElapsedTime());
                            })
                            .AsyncVia(compressionInvoker)
                            .Run());
                }

                auto results = WaitFor(AllSucceeded(asyncUncompressedBlocks))
                    .ValueOrThrow();

                for (const auto& [block, duration] : results) {
                    cachedBlocks.emplace_back(block);
                    decompressionTime += duration;
                }

                break;
            }

            default:
                YT_ABORT();
        }

        for (const auto& cachedBlock : cachedBlocks) {
            allocatedMemory += cachedBlock.Size();
        }

        chunkData->Blocks.insert(
            chunkData->Blocks.end(),
            std::make_move_iterator(cachedBlocks.begin()),
            std::make_move_iterator(cachedBlocks.end()));

        startBlockIndex += readBlockCount;
    }

    TCodecStatistics decompressionStatistics;
    decompressionStatistics.Append(TCodecDuration{codecId, decompressionTime});
    readerProfiler->SetCodecStatistics(decompressionStatistics);

    if (chunkData->MemoryTrackerGuard) {
        chunkData->MemoryTrackerGuard.IncrementSize(allocatedMemory - preallocatedMemory);
    }

    FinalizeChunkData(chunkData, store->GetChunkId(), meta, tabletSnapshot);

    YT_LOG_INFO(
        "Store preload completed (MemoryUsage: %v, LookupHashTable: %v)",
        allocatedMemory,
        static_cast<bool>(chunkData->LookupHashTable));

    return chunkData;
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TNode)

struct TNode
    : public TRefCounted
{
    const TNodeDescriptor Descriptor;
    const TInMemorySessionId SessionId;
    const TDuration ControlRpcTimeout;

    TInMemoryServiceProxy Proxy;
    TPeriodicExecutorPtr PingExecutor;

    TNode(
        const TNodeDescriptor& descriptor,
        TInMemorySessionId sessionId,
        IChannelPtr channel,
        const TDuration controlRpcTimeout)
        : Descriptor(descriptor)
        , SessionId(sessionId)
        , ControlRpcTimeout(controlRpcTimeout)
        , Proxy(std::move(channel))
    { }

    void SendPing()
    {
        YT_LOG_DEBUG("Sending ping (Address: %v, SessionId: %v)",
            Descriptor.GetDefaultAddress(),
            SessionId);

        auto req = Proxy.PingSession();
        req->SetTimeout(ControlRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId);
        req->Invoke().Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TInMemoryServiceProxy::TErrorOrRspPingSessionPtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_WARNING(rspOrError, "Ping failed (Address: %v, SessionId: %v)",
                        Descriptor.GetDefaultAddress(),
                        SessionId);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TNode)

////////////////////////////////////////////////////////////////////////////////

class TRemoteInMemoryBlockCache
    : public IRemoteInMemoryBlockCache
{
public:
    TRemoteInMemoryBlockCache(
        std::vector<TNodePtr> nodes,
        EInMemoryMode inMemoryMode,
        size_t batchSize,
        const TDuration controlRpcTimeout,
        const TDuration heavyRpcTimeout)
        : Nodes_(std::move(nodes))
        , InMemoryMode_(inMemoryMode)
        , BatchSize_(batchSize)
        , ControlRpcTimeout_(controlRpcTimeout)
        , HeavyRpcTimeout_(heavyRpcTimeout)
    { }

    virtual void PutBlock(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data) override
    {
        if (type != MapInMemoryModeToBlockType(InMemoryMode_)) {
            return;
        }

        if (Dropped_.load()) {
            return;
        }

        bool batchIsReady;
        {
            auto guard = Guard(SpinLock_);
            Blocks_.emplace_back(id, data.Data);
            CurrentSize_ += data.Data.Size();
            batchIsReady = CurrentSize_ > BatchSize_;
        }

        if (batchIsReady) {
            bool expected = false;
            if (Sending_.compare_exchange_strong(expected, true)) {
                ReadyEvent_ = SendNextBatch();
                ReadyEvent_.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError&) {
                    Sending_ = false;
                }));
            }
        }
    }

    virtual TCachedBlock FindBlock(
        const TBlockId& /* id */,
        EBlockType /* type */) override
    {
        return TCachedBlock();
    }

    virtual std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /* id */,
        EBlockType /* type */) override
    {
        return CreateActiveCachedBlockCookie();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return MapInMemoryModeToBlockType(InMemoryMode_);
    }

    virtual TFuture<void> Finish(const std::vector<TChunkInfo>& chunkInfos) override
    {
        bool expected = false;
        if (Sending_.compare_exchange_strong(expected, true)) {
            ReadyEvent_ = SendNextBatch();
        }

        return ReadyEvent_
            .Apply(BIND(
                &TRemoteInMemoryBlockCache::DoFinish,
                MakeStrong(this),
                chunkInfos));
    }

private:
    TFuture<void> SendNextBatch()
    {
        auto guard = Guard(SpinLock_);
        if (Blocks_.empty()) {
            return VoidFuture;
        }

        if (Nodes_.empty()) {
            Dropped_ = true;
            Blocks_.clear();
            CurrentSize_ = 0;
            return MakeFuture(TError("All sessions are dropped"));
        }

        std::vector<std::pair<TBlockId, TSharedRef>> blocks;
        size_t groupSize = 0;
        while (!Blocks_.empty() && groupSize < BatchSize_) {
            auto block = std::move(Blocks_.front());
            groupSize += block.second.Size();
            blocks.push_back(std::move(block));
            Blocks_.pop_front();
        }

        CurrentSize_ -= groupSize;

        guard.Release();

        std::vector<TFuture<TInMemoryServiceProxy::TRspPutBlocksPtr>> asyncResults;
        for (const auto& node : Nodes_) {
            auto req = node->Proxy.PutBlocks();
            req->SetTimeout(HeavyRpcTimeout_);
            req->SetMemoryZone(EMemoryZone::Undumpable);
            ToProto(req->mutable_session_id(), node->SessionId);

            for (const auto& block : blocks) {
                YT_LOG_DEBUG("Sending in-memory block (ChunkId: %v, BlockIndex: %v, SessionId: %v, Address: %v)",
                    block.first.ChunkId,
                    block.first.BlockIndex,
                    node->SessionId,
                    node->Descriptor.GetDefaultAddress());

                ToProto(req->add_block_ids(), block.first);
                req->Attachments().push_back(block.second);
            }

            asyncResults.push_back(req->Invoke());
        }

        return AllSet(asyncResults)
            .Apply(BIND(&TRemoteInMemoryBlockCache::OnSendResponse, MakeStrong(this)));
    }

    TFuture<void> OnSendResponse(
        const std::vector<TErrorOr<TInMemoryServiceProxy::TRspPutBlocksPtr>>& results)
    {
        std::vector<TNodePtr> activeNodes;
        for (size_t index = 0; index < results.size(); ++index) {
            if (!results[index].IsOK()) {
                YT_LOG_WARNING("Error sending batch (SessionId: %v, Address: %v)",
                    Nodes_[index]->SessionId,
                    Nodes_[index]->Descriptor.GetDefaultAddress());
                continue;
            }

            auto result = results[index].Value();

            if (result->dropped()) {
                YT_LOG_WARNING("Dropped in-memory session (SessionId: %v, Address: %v)",
                    Nodes_[index]->SessionId,
                    Nodes_[index]->Descriptor.GetDefaultAddress());
                continue;
            }

            activeNodes.push_back(Nodes_[index]);
        }

        Nodes_.swap(activeNodes);

        return SendNextBatch();
    }

    TFuture<void> DoFinish(const std::vector<TChunkInfo>& chunkInfos)
    {
        YT_LOG_DEBUG("Finishing in-memory sessions (SessionIds: %v)",
            MakeFormattableView(Nodes_, [] (TStringBuilderBase* builder, const TNodePtr& node) {
                FormatValue(builder, node->SessionId, TStringBuf());
            }));

        std::vector<TFuture<void>> asyncResults;
        for (const auto& node : Nodes_) {
            if (node->PingExecutor) {
                node->PingExecutor->Stop();
                node->PingExecutor.Reset();
            }

            auto req = node->Proxy.FinishSession();
            req->SetTimeout(HeavyRpcTimeout_);
            ToProto(req->mutable_session_id(), node->SessionId);

            for (const auto& chunkInfo : chunkInfos) {
                ToProto(req->add_chunk_id(), chunkInfo.ChunkId);
                *req->add_chunk_meta() = *chunkInfo.ChunkMeta;
                ToProto(req->add_tablet_id(), chunkInfo.TabletId);
                req->add_mount_revision(chunkInfo.MountRevision);
            }

            asyncResults.push_back(req->Invoke().As<void>());
        }

        return AllSucceeded(asyncResults);
    }

private:
    std::vector<TNodePtr> Nodes_;
    const EInMemoryMode InMemoryMode_;
    const size_t BatchSize_;
    const TDuration ControlRpcTimeout_;
    const TDuration HeavyRpcTimeout_;

    TFuture<void> ReadyEvent_ = VoidFuture;
    std::atomic<bool> Sending_ = {false};
    std::atomic<bool> Dropped_ = {false};

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    std::deque<std::pair<TBlockId, TSharedRef>> Blocks_;
    size_t CurrentSize_ = 0;

};

class TDummyInMemoryBlockCache
    : public IRemoteInMemoryBlockCache
{
public:
    virtual void PutBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/) override
    { }

    virtual TCachedBlock FindBlock(
        const TBlockId& /* id */,
        EBlockType /* type */) override
    {
        return TCachedBlock();
    }

    virtual std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return CreateActiveCachedBlockCookie();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::None;
    }

    virtual TFuture<void> Finish(const std::vector<TChunkInfo>& /*chunkInfos*/) override
    {
        return VoidFuture;
    }
};

IRemoteInMemoryBlockCachePtr DoCreateRemoteInMemoryBlockCache(
    NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    const NHiveClient::TCellDescriptor& cellDescriptor,
    EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config)
{
    std::vector<TNodePtr> nodes;
    for (const auto& target : cellDescriptor.Peers) {
        auto channel = target.GetDefaultAddress() == localDescriptor.GetDefaultAddress()
            ? CreateLocalChannel(localRpcServer)
            : client->GetChannelFactory()->CreateChannel(target);

        TInMemoryServiceProxy proxy(channel);

        auto req = proxy.StartSession();
        req->SetTimeout(config->ControlRpcTimeout);
        req->set_in_memory_mode(static_cast<int>(inMemoryMode));

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error starting in-memory session at node %v",
                target.GetDefaultAddress())
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        auto sessionId = FromProto<TInMemorySessionId>(rsp->session_id());

        auto node = New<TNode>(
            target,
            sessionId,
            std::move(channel),
            config->ControlRpcTimeout);

        node->PingExecutor = New<TPeriodicExecutor>(
            NChunkClient::TDispatcher::Get()->GetWriterInvoker(),
            BIND(&TNode::SendPing, MakeWeak(node)),
            config->PingPeriod);
        node->PingExecutor->Start();

        nodes.push_back(node);
    }

    YT_LOG_DEBUG("In-memory sessions started (SessionIds: %v)",
        MakeFormattableView(nodes, [] (TStringBuilderBase* builder, const TNodePtr& node) {
            FormatValue(builder, node->SessionId, TStringBuf());
        }));

    return New<TRemoteInMemoryBlockCache>(
        std::move(nodes),
        inMemoryMode,
        config->BatchSize,
        config->ControlRpcTimeout,
        config->HeavyRpcTimeout);
}

TFuture<IRemoteInMemoryBlockCachePtr> CreateRemoteInMemoryBlockCache(
    NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    const NHiveClient::TCellDescriptor& cellDescriptor,
    EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config)
{
    if (inMemoryMode == EInMemoryMode::None) {
        return MakeFuture<IRemoteInMemoryBlockCachePtr>(New<TDummyInMemoryBlockCache>());
    }

    return BIND(&DoCreateRemoteInMemoryBlockCache)
        .AsyncVia(GetCurrentInvoker())
        .Run(client, localDescriptor, localRpcServer, cellDescriptor, inMemoryMode, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
