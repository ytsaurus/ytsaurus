#include "config.h"
#include "in_memory_manager.h"
#include "private.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_lookup_hash_table.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/misc/finally.h>

namespace NYT {
namespace NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTabletClient;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TBlocksExt;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void FinalizeChunkData(
    const TInMemoryChunkDataPtr& data,
    const TChunkId& id,
    const TChunkMeta& meta,
    const TTabletSnapshotPtr& tabletSnapshot)
{
    data->ChunkMeta = TCachedVersionedChunkMeta::Create(id, meta, tabletSnapshot->PhysicalSchema);

    if (data->MemoryTrackerGuard) {
        data->MemoryTrackerGuard.UpdateSize(data->ChunkMeta->GetMemoryUsage());
    }

    if (tabletSnapshot->HashTableSize > 0) {
        data->LookupHashTable = CreateChunkLookupHashTable(
            data->Blocks,
            data->ChunkMeta,
            tabletSnapshot->RowKeyComparer);
        if (data->LookupHashTable && data->MemoryTrackerGuard) {
            data->MemoryTrackerGuard.UpdateSize(data->LookupHashTable->GetByteSize());
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
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TInMemoryManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , CompressionInvoker_(CreateFixedPriorityInvoker(
            NChunkClient::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            Config_->WorkloadDescriptor.GetPriority()))
        , PreloadSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentPreloads))
        , Throttler_(CreateReconfigurableThroughputThrottler(config->PreloadThrottler))
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TImpl::ScanSlot, MakeStrong(this)));
    }

    IBlockCachePtr CreateInterceptingBlockCache(EInMemoryMode mode)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return New<TInterceptingBlockCache>(this, mode);
    }

    TInMemoryChunkDataPtr EvictInterceptedChunkData(const TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWriterGuard guard(InterceptedDataSpinLock_);

        auto it = ChunkIdToData_.find(chunkId);
        if (it == ChunkIdToData_.end()) {
            return nullptr;
        }

        auto chunkData = std::move(it->second);
        ChunkIdToData_.erase(it);

        LOG_INFO("Intercepted chunk data evicted (ChunkId: %v, Mode: %v)",
            chunkId,
            chunkData->InMemoryMode);

        return chunkData;
    }

    void FinalizeChunk(
        const TChunkId& chunkId,
        const TChunkMeta& chunkMeta,
        const TTabletSnapshotPtr& tabletSnapshot)
    {
        TInMemoryChunkDataPtr data;

        {
            TWriterGuard guard(InterceptedDataSpinLock_);
            auto it = ChunkIdToData_.find(chunkId);
            if (it != ChunkIdToData_.end()) {
                data = it->second;
            }
        }

        if (!data) {
            LOG_INFO("Cannot find intercepted chunk data for finalization (ChunkId: %v)", chunkId);
            return;
        }

        FinalizeChunkData(data, chunkId, chunkMeta, tabletSnapshot);
    }

private:
    const TInMemoryManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    const IInvokerPtr CompressionInvoker_;

    TAsyncSemaphorePtr PreloadSemaphore_;

    const NProfiling::TTagId PreloadTag_ = NProfiling::TProfileManager::Get()->RegisterTag("method", "preload");

    TReaderWriterSpinLock InterceptedDataSpinLock_;
    THashMap<TChunkId, TInMemoryChunkDataPtr> ChunkIdToData_;

    IThroughputThrottlerPtr Throttler_;

    void ScanSlot(const TTabletSlotPtr& slot)
    {
        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(const TTabletSlotPtr& slot, TTablet* tablet)
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
                    &TImpl::PreloadStore,
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
        const TTabletSlotPtr& slot,
        TTablet* tablet,
        const IChunkStorePtr& store,
        const IStoreManagerPtr& storeManager)
    {
        auto readSessionId = TReadSessionId::Create();
        auto mode = store->GetInMemoryMode();

        std::vector<IInvokerPtr> feasibleInvokers{
            tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
            tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)};
        VERIFY_INVOKERS_AFFINITY(feasibleInvokers);

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, StoreId: %v, Mode: %v, ReadSessionId: %v",
            tablet->GetId(),
            store->GetId(),
            mode,
            readSessionId);

        LOG_INFO("Preloading in-memory store");

        YCHECK(store->GetPreloadState() == EStorePreloadState::Running);

        if (mode == EInMemoryMode::None) {
            // Mode has been changed while current action was waiting in action queue
            LOG_INFO("In-memory mode has been changed");

            store->SetPreloadState(EStorePreloadState::None);
            store->SetPreloadFuture(TFuture<void>());
            return;
        }

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->FindTabletSnapshot(tablet->GetId());
        if (!tabletSnapshot) {
            THROW_ERROR_EXCEPTION("Tablet snapshot is missing");
        }

        try {
            // This call may suspend the current fiber.
            auto chunkData = PreloadInMemoryStore(
                tabletSnapshot,
                store,
                readSessionId,
                Bootstrap_->GetMemoryUsageTracker(),
                CompressionInvoker_,
                Throttler_,
                PreloadTag_);

            std::vector<IInvokerPtr> feasibleInvokers{
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)};
            VERIFY_INVOKERS_AFFINITY(feasibleInvokers);

            YCHECK(store->GetPreloadState() == EStorePreloadState::Running);

            store->Preload(std::move(chunkData));
            storeManager->EndStorePreload(store);

            tabletSnapshot->RuntimeData->Errors[ETabletBackgroundActivity::Preload].Store(TError());
        } catch (const std::exception& ex) {
            // Do not back off if fiber cancellation exception was thrown.
            // SetInMemoryMode with other mode was called during current action execution.

            LOG_ERROR(ex, "Error preloading tablet store, backing off");
            storeManager->BackoffStorePreload(store);

            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Preload);

            tabletSnapshot->RuntimeData->Errors[ETabletBackgroundActivity::Preload].Store(error);
        }

        slotManager->RegisterTabletSnapshot(slot, tablet);
    }

    class TInterceptingBlockCache
        : public IBlockCache
    {
    public:
        TInterceptingBlockCache(TImplPtr owner, EInMemoryMode mode)
            : Owner_(owner)
            , Mode_(mode)
            , BlockType_(MapInMemoryModeToBlockType(Mode_))
        { }

        ~TInterceptingBlockCache()
        {
            for (const auto& chunkId : ChunkIds_) {
                TDelayedExecutor::Submit(
                    BIND(IgnoreResult(&TImpl::EvictInterceptedChunkData), Owner_, chunkId),
                    Owner_->Config_->InterceptedDataRetentionTime);
            }
        }

        virtual void Put(
            const TBlockId& id,
            EBlockType type,
            const TBlock& block,
            const TNullable<NNodeTrackerClient::TNodeDescriptor>& /*source*/) override
        {
            if (type != BlockType_) {
                return;
            }

            TGuard<TSpinLock> guard(SpinLock_);

            if (Owner_->IsMemoryLimitExceeded()) {
                Dropped_ = true;
            }

            if (Dropped_) {
                Owner_->DropChunkData(id.ChunkId);
                return;
            }

            auto it = ChunkIds_.find(id.ChunkId);
            TInMemoryChunkDataPtr data;
            if (it == ChunkIds_.end()) {
                data = Owner_->CreateChunkData(id.ChunkId, Mode_);
                YCHECK(ChunkIds_.insert(id.ChunkId).second);
            } else {
                data = Owner_->GetChunkData(id.ChunkId, Mode_);
            }

            if (data->Blocks.size() <= id.BlockIndex) {
                size_t blockCapacity = std::max(data->Blocks.capacity(), static_cast<size_t>(1));
                while (blockCapacity <= id.BlockIndex) {
                    blockCapacity *= 2;
                }
                data->Blocks.reserve(blockCapacity);
                data->Blocks.resize(id.BlockIndex + 1);
            }

            YCHECK(!data->Blocks[id.BlockIndex].Data);
            data->Blocks[id.BlockIndex] = block;
            if (data->MemoryTrackerGuard) {
                data->MemoryTrackerGuard.UpdateSize(block.Size());
            }
            YCHECK(!data->ChunkMeta);
        }

        virtual TBlock Find(const TBlockId& /*id*/, EBlockType /*type*/) override
        {
            return TBlock();
        }

        virtual EBlockType GetSupportedBlockTypes() const override
        {
            return BlockType_;
        }

    private:
        const TImplPtr Owner_;
        const EInMemoryMode Mode_;
        const EBlockType BlockType_;

        TSpinLock SpinLock_;
        THashSet<TChunkId> ChunkIds_;
        bool Dropped_ = false;
    };

    TInMemoryChunkDataPtr GetChunkData(const TChunkId& chunkId, EInMemoryMode mode)
    {
        TReaderGuard guard(InterceptedDataSpinLock_);

        auto it = ChunkIdToData_.find(chunkId);
        YCHECK(it != ChunkIdToData_.end());

        auto chunkData = it->second;
        YCHECK(chunkData->InMemoryMode == mode);

        return chunkData;
    }

    TInMemoryChunkDataPtr CreateChunkData(const TChunkId& chunkId, EInMemoryMode mode)
    {
        TWriterGuard guard(InterceptedDataSpinLock_);

        auto chunkData = New<TInMemoryChunkData>();
        chunkData->InMemoryMode = mode;
        chunkData->MemoryTrackerGuard = NCellNode::TNodeMemoryTrackerGuard::Acquire(
            Bootstrap_->GetMemoryUsageTracker(),
            EMemoryCategory::TabletStatic,
            0,
            MemoryUsageGranularity);

        // Replace the old data, if any, by a new one.
        ChunkIdToData_[chunkId] = chunkData;

        LOG_INFO("Intercepted chunk data created (ChunkId: %v, Mode: %v)",
            chunkId,
            mode);

        return chunkData;
    }

    void DropChunkData(const TChunkId& chunkId)
    {
        TWriterGuard guard(InterceptedDataSpinLock_);

        if (ChunkIdToData_.erase(chunkId) == 1) {
            LOG_WARNING("Intercepted chunk data dropped due to memory pressure (ChunkId: %v)",
                chunkId);
        }
    }

    bool IsMemoryLimitExceeded() const
    {
        const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        return tracker->IsExceeded(EMemoryCategory::TabletStatic);
    }
};

////////////////////////////////////////////////////////////////////////////////

TInMemoryManager::TInMemoryManager(
    TInMemoryManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TInMemoryManager::~TInMemoryManager() = default;

IBlockCachePtr TInMemoryManager::CreateInterceptingBlockCache(EInMemoryMode mode)
{
    return Impl_->CreateInterceptingBlockCache(mode);
}

TInMemoryChunkDataPtr TInMemoryManager::EvictInterceptedChunkData(const TChunkId& chunkId)
{
    return Impl_->EvictInterceptedChunkData(chunkId);
}

void TInMemoryManager::FinalizeChunk(
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TTabletSnapshotPtr& tabletSnapshot)
{
    Impl_->FinalizeChunk(chunkId, chunkMeta, tabletSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    const TReadSessionId& readSessionId,
    TNodeMemoryTracker* memoryUsageTracker,
    const IInvokerPtr& compressionInvoker,
    const NConcurrency::IThroughputThrottlerPtr& throttler,
    const NProfiling::TTagId& preloadTag)
{
    auto mode = tabletSnapshot->Config->InMemoryMode;

    NLogging::TLogger Logger(TabletNodeLogger);
    Logger.AddTag(
        "TabletId: %v, StoreId: %v, Mode: %v, ReadSessionId: %v",
        tabletSnapshot->TabletId, store->GetId(), mode, readSessionId);

    LOG_INFO("Store preload started");

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPreload);
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    blockReadOptions.ReadSessionId = readSessionId;

    auto reader = store->GetChunkReader(throttler);
    auto meta = WaitFor(reader->GetMeta(blockReadOptions))
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
    auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());
    auto format = ETableChunkFormat(meta.version());

    if (format == ETableChunkFormat::SchemalessHorizontal ||
        format == ETableChunkFormat::UnversionedColumnar)
    {
        // For unversioned chunks verify that block size is correct
        if (auto blockSizeLimit = tabletSnapshot->Config->MaxUnversionedBlockSize) {
            if (miscExt.max_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                    << TErrorAttribute("chunk_id", store->GetId())
                    << TErrorAttribute("block_size", miscExt.max_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }

    auto erasureCodec = NErasure::ECodec(miscExt.erasure_codec());
    if (erasureCodec != NErasure::ECodec::None) {
        THROW_ERROR_EXCEPTION("Preloading erasure-coded store %v is not supported; consider using replicated chunks or disabling in-memory mode",
            store->GetId());
    }

    auto codecId = NCompression::ECodec(miscExt.compression_codec());
    auto* codec = NCompression::GetCodec(codecId);

    int startBlockIndex = 0;
    int totalBlockCount = blocksExt.blocks_size();

    i64 preallocatedMemory = 0;
    i64 allocatedMemory = 0;
    i64 compressedDataSize = 0;

    TDuration decompressionTime;

    for (int i = 0; i < totalBlockCount; ++i) {
        preallocatedMemory += blocksExt.blocks(i).size();
    }

    if (memoryUsageTracker && memoryUsageTracker->GetFree(EMemoryCategory::TabletStatic) < preallocatedMemory) {
        THROW_ERROR_EXCEPTION("Preload is cancelled due to memory pressure");
    }

    auto chunkData = New<TInMemoryChunkData>();
    chunkData->InMemoryMode = mode;
    if (memoryUsageTracker) {
        chunkData->MemoryTrackerGuard = NCellNode::TNodeMemoryTrackerGuard::Acquire(
            memoryUsageTracker,
            EMemoryCategory::TabletStatic,
            preallocatedMemory,
            MemoryUsageGranularity);
    }
    chunkData->Blocks.reserve(totalBlockCount);

    while (startBlockIndex < totalBlockCount) {
        LOG_DEBUG("Started reading chunk blocks (FirstBlock: %v)",
            startBlockIndex);

        auto compressedBlocks = WaitFor(reader->ReadBlocks(
            blockReadOptions,
            startBlockIndex,
            totalBlockCount - startBlockIndex))
            .ValueOrThrow();

        int readBlockCount = compressedBlocks.size();
        LOG_DEBUG("Finished reading chunk blocks (Blocks: %v-%v)",
            startBlockIndex,
            startBlockIndex + readBlockCount - 1);

        for (const auto& compressedBlock : compressedBlocks) {
            compressedDataSize += compressedBlock.Size();
        }

        std::vector<TBlock> cachedBlocks;
        switch (mode) {
            case EInMemoryMode::Compressed: {
                cachedBlocks = std::move(compressedBlocks);
                break;
            }

            case EInMemoryMode::Uncompressed: {
                LOG_DEBUG("Decompressing chunk blocks (Blocks: %v-%v, Codec: %v)",
                    startBlockIndex,
                    startBlockIndex + readBlockCount - 1,
                    codec->GetId());

                std::vector<TFuture<std::pair<TSharedRef, TDuration>>> asyncUncompressedBlocks;
                for (auto& compressedBlock : compressedBlocks) {
                    asyncUncompressedBlocks.push_back(
                        BIND([&] {
                                NProfiling::TCpuTimer timer;
                                auto block = codec->Decompress(compressedBlock.Data);
                                return std::make_pair(std::move(block), timer.GetElapsedTime());
                            })
                            .AsyncVia(compressionInvoker)
                            .Run());
                }

                auto results = WaitFor(Combine(asyncUncompressedBlocks))
                    .ValueOrThrow();

                for (const auto& pair : results) {
                    cachedBlocks.emplace_back(std::move(pair.first));
                    decompressionTime += pair.second;
                }

                break;
            }

            default:
                Y_UNREACHABLE();
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
    NChunkClient::NProto::TDataStatistics dataStatistics;
    dataStatistics.set_compressed_data_size(compressedDataSize);

    ProfileChunkReader(
        tabletSnapshot,
        dataStatistics,
        decompressionStatistics,
        blockReadOptions.ChunkReaderStatistics,
        preloadTag);

    if (chunkData->MemoryTrackerGuard) {
        chunkData->MemoryTrackerGuard.UpdateSize(allocatedMemory - preallocatedMemory);
    }

    FinalizeChunkData(chunkData, store->GetId(), meta, tabletSnapshot);

    LOG_INFO(
        "Store preload completed (MemoryUsage: %v, LookupHashTable: %v)",
        allocatedMemory,
        static_cast<bool>(chunkData->LookupHashTable));

    return chunkData;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
