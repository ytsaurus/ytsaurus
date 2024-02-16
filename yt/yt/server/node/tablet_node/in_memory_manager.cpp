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
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

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
#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <yt/yt/core/rpc/local_channel.h>

#include <yt/yt/library/undumpable/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NRpc;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTabletClient;

using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TBlocksExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

using TInMemorySessionId = TGuid;

////////////////////////////////////////////////////////////////////////////////

void CollocateImMemoryBlocks(std::vector<NChunkClient::TBlock>& blocks, const INodeMemoryReferenceTrackerPtr& memoryReferenceTracker)
{
    i64 totalSize = 0;
    for (const auto& block: blocks) {
        totalSize += block.Data.Size();
    }

    YT_LOG_DEBUG("Collocating memory blocks (BlockCount: %v, TotalByteCount: %v)",
        blocks.size(),
        totalSize);

    auto buffer = TSharedMutableRef::Allocate<TPreloadedBlockTag>(totalSize, {.InitializeStorage = false});
    i64 offset = 0;

    for (auto& block : blocks) {
        auto slice = buffer.Slice(offset, offset + block.Data.Size());
        ::memcpy(slice.Begin(), block.Data.Begin(), block.Data.Size());
        offset += block.Data.Size();
        block.Data = TrackMemory(memoryReferenceTracker, EMemoryCategory::TabletStatic, std::move(slice));
    }
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryChunkDataPtr CreateInMemoryChunkData(
    NChunkClient::TChunkId chunkId,
    NTabletClient::EInMemoryMode mode,
    int startBlockIndex,
    std::vector<NChunkClient::TBlock> blocksWithCategory,
    const NTableClient::TCachedVersionedChunkMetaPtr& versionedChunkMeta,
    const TTabletSnapshotPtr& tabletSnapshot,
    const INodeMemoryReferenceTrackerPtr& memoryReferenceTracker,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    CollocateImMemoryBlocks(blocksWithCategory, memoryReferenceTracker);

    std::vector<NChunkClient::TBlock> blocks;
    blocks.reserve(blocksWithCategory.size());

    for (const auto& block : blocksWithCategory) {
        blocks.push_back(block);
        blocks.back().Data = TrackMemory(memoryReferenceTracker, EMemoryCategory::Unknown, block.Data);
    }

    for (auto& block: blocks) {
        block.Data = MarkUndumpable(block.Data);
    }

    if (versionedChunkMeta->GetChunkFormat() == NChunkClient::EChunkFormat::TableVersionedColumnar &&
        mode == EInMemoryMode::Uncompressed)
    {
        YT_VERIFY(startBlockIndex == 0);

        class TBlockProvider
            : public NColumnarChunkFormat::IBlockDataProvider
        {
        public:
            TBlockProvider(const std::vector<TBlock>& blocks)
                : Blocks_(blocks)
            { }

            const char* GetBlock(ui32 blockIndex) override
            {
                YT_VERIFY(blockIndex < Blocks_.size());
                return Blocks_[blockIndex].Data.Begin();
            }

        private:
            const std::vector<TBlock>& Blocks_;
        } blockProvider{blocks};

        // Prepare new meta.
        versionedChunkMeta->GetPreparedChunkMeta(&blockProvider);
    }

    NTableClient::TChunkLookupHashTablePtr lookupHashTable;

    auto metaMemoryTrackerGuard = TMemoryUsageTrackerGuard::Acquire(
        memoryTracker,
        versionedChunkMeta->GetMemoryUsage(),
        MemoryUsageGranularity);

    if (tabletSnapshot->HashTableSize > 0) {
        lookupHashTable = CreateChunkLookupHashTable(
            chunkId,
            startBlockIndex,
            blocks,
            versionedChunkMeta,
            tabletSnapshot->PhysicalSchema,
            tabletSnapshot->RowKeyComparer.UUComparer);
        if (lookupHashTable) {
            metaMemoryTrackerGuard.IncrementSize(lookupHashTable->GetByteSize());
        }
    }

    return New<TInMemoryChunkData>(
        mode,
        startBlockIndex,
        versionedChunkMeta,
        lookupHashTable,
        std::move(metaMemoryTrackerGuard),
        std::move(blocks),
        std::move(blocksWithCategory));
}

EBlockType GetBlockTypeFromInMemoryMode(EInMemoryMode mode)
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
    explicit TInMemoryManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , CompressionInvoker_(CreateFixedPriorityInvoker(
            NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            GetConfig()->WorkloadDescriptor.GetPriority()))
        , PreloadSemaphore_(New<TAsyncSemaphore>(GetConfig()->MaxConcurrentPreloads))
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TInMemoryManager::ScanSlot, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TInMemoryManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    TInMemoryChunkDataPtr EvictInterceptedChunkData(TChunkId chunkId) override
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

    void FinalizeChunk(TChunkId chunkId, TInMemoryChunkDataPtr chunkData) override
    {
        {
            auto guard = WriterGuard(InterceptedDataSpinLock_);

            // Replace the old data, if any, by a new one.
            ChunkIdToData_[chunkId] = chunkData;
        }

        // Schedule eviction.
        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TInMemoryManager::EvictInterceptedChunkData), MakeStrong(this), chunkId),
            GetConfig()->InterceptedDataRetentionTime);
    }

    TInMemoryManagerConfigPtr GetConfig() const override
    {
        return Config_.Acquire();
    }

private:
    TAtomicIntrusivePtr<TInMemoryManagerConfig> Config_{New<TInMemoryManagerConfig>()};

    IBootstrap* const Bootstrap_;
    const IInvokerPtr CompressionInvoker_;

    const TAsyncSemaphorePtr PreloadSemaphore_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, InterceptedDataSpinLock_);
    THashMap<TChunkId, TInMemoryChunkDataPtr> ChunkIdToData_;


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& staticConfig = Bootstrap_->GetConfig()->TabletNode->InMemoryManager;
        auto config = staticConfig->ApplyDynamic(newNodeConfig->TabletNode->InMemoryManager);
        Config_.Store(config);

        PreloadSemaphore_->SetTotal(config->MaxConcurrentPreloads);
    }

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
                readerProfiler,
                Bootstrap_->GetNodeMemoryReferenceTracker(),
                GetConfig()->EnablePreliminaryNetworkThrottling,
                Bootstrap_->GetInThrottler(EWorkloadCategory::SystemTabletPreload));

            VERIFY_INVOKERS_AFFINITY(std::vector{
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
                tablet->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)
            });

            YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Running);

            store->Preload(std::move(chunkData));
            storeManager->EndStorePreload(store);

            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Preload].Store(TError());
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
            tabletSnapshot->TabletRuntimeData->Errors
                .BackgroundErrors[ETabletBackgroundActivity::Preload].Store(error);
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

IInMemoryManagerPtr CreateInMemoryManager(IBootstrap* bootstrap)
{
    return New<TInMemoryManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> GetEstimatedBlockRangeSize(
    bool enablePreliminaryNetworkThrottling,
    const TCachedVersionedChunkMetaPtr& meta,
    int startBlockIndex,
    int blocksCount)
{
    if (!enablePreliminaryNetworkThrottling) {
        return {};
    }

    const auto& metaMisc = meta->Misc();
    if (metaMisc.compressed_data_size() == 0 || metaMisc.uncompressed_data_size() == 0) {
        return {};
    }

    i64 uncompressedSize = 0;
    const auto& dataBlockMeta = meta->DataBlockMeta();
    for (int index = startBlockIndex; index < startBlockIndex + blocksCount; ++index) {
        uncompressedSize += dataBlockMeta->data_blocks(index).uncompressed_size();
    }

    auto compressionRatio = static_cast<double>(metaMisc.compressed_data_size()) / metaMisc.uncompressed_data_size();
    return uncompressedSize * compressionRatio;
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    TReadSessionId readSessionId,
    const INodeMemoryTrackerPtr& memoryTracker,
    const IInvokerPtr& compressionInvoker,
    const TReaderProfilerPtr& readerProfiler,
    const INodeMemoryReferenceTrackerPtr& memoryReferenceTracker,
    bool enablePreliminaryNetworkThrottling,
    const IThroughputThrottlerPtr& networkThrottler)
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

    IChunkReader::TReadBlocksOptions readBlocksOptions{
        .ClientOptions = TClientChunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPreload),
            .ReadSessionId = readSessionId,
        },
        .DisableBandwidthThrottler = enablePreliminaryNetworkThrottling,
    };

    readerProfiler->SetChunkReaderStatistics(readBlocksOptions.ClientOptions.ChunkReaderStatistics);

    auto reader = store->GetBackendReaders(EWorkloadCategory::SystemTabletPreload).ChunkReader;
    auto meta = WaitFor(reader->GetMeta(readBlocksOptions.ClientOptions))
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<TMiscExt>(meta->extensions());
    auto format = CheckedEnumCast<EChunkFormat>(meta->format());

    if (format == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        format == EChunkFormat::TableUnversionedColumnar)
    {
        // For unversioned chunks verify that block size is correct
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
            if (miscExt.max_data_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                    << TErrorAttribute("chunk_id", store->GetId())
                    << TErrorAttribute("block_size", miscExt.max_data_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }

    auto compressionCodecId = CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec());
    auto* compressionCodec = NCompression::GetCodec(compressionCodecId);
    auto erasureCodecId = FromProto<NErasure::ECodec>(miscExt.erasure_codec());

    auto versionedChunkMeta = TCachedVersionedChunkMeta::Create(
        /*prepareColumnarMeta*/ false,
        /*memoryTracker*/ nullptr,
        meta);

    auto dataBlockCount = versionedChunkMeta->DataBlockMeta()->data_blocks_size();

    int commonKeyPrefix = GetCommonKeyPrefix(
        versionedChunkMeta->ChunkSchema()->GetKeyColumns(),
        tabletSnapshot->PhysicalSchema->GetKeyColumns());

    // Expected to be equal for dynamic tables.
    YT_VERIFY(commonKeyPrefix == versionedChunkMeta->ChunkSchema()->GetKeyColumnCount());

    auto sortOrders = GetSortOrders(tabletSnapshot->PhysicalSchema->GetSortColumns());

    int startBlockIndex;
    int endBlockIndex;

    // TODO(ifsmirnov): support columnar chunks (YT-11707).
    bool canDeduceBlockRange =
        format == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        format == EChunkFormat::TableVersionedSimple;

    if (store->IsSorted() && canDeduceBlockRange) {
        auto sortedStore = store->AsSortedChunk();
        auto lowerBound = std::max(tabletSnapshot->PivotKey, sortedStore->GetMinKey());
        auto upperBound = std::min(tabletSnapshot->NextPivotKey, sortedStore->GetUpperBoundKey());

        const auto& blockLastKeys = versionedChunkMeta->BlockLastKeys();

        YT_VERIFY(dataBlockCount == std::ssize(blockLastKeys));

        startBlockIndex = BinarySearch(0, dataBlockCount, [&] (int index) {
            return !TestKeyWithWidening(
                ToKeyRef(blockLastKeys[index], commonKeyPrefix),
                ToKeyBoundRef(lowerBound, /*upper*/ false, sortOrders.size()),
                sortOrders);
        });

        endBlockIndex = BinarySearch(0, dataBlockCount, [&] (int index) {
            return TestKeyWithWidening(
                ToKeyRef(blockLastKeys[index], commonKeyPrefix),
                ToKeyBoundRef(upperBound, /*upper*/ true, sortOrders.size()),
                sortOrders);
        });
        if (endBlockIndex < dataBlockCount) {
            ++endBlockIndex;
        }
    } else {
        startBlockIndex = 0;
        endBlockIndex = dataBlockCount;
    }

    i64 preallocatedMemory = 0;
    i64 compressedDataSize = 0;

    TDuration decompressionTime;

    if (erasureCodecId == NErasure::ECodec::None) {
        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());
        YT_VERIFY(dataBlockCount <= blocksExt.blocks_size());
        for (int i = startBlockIndex; i < endBlockIndex; ++i) {
            preallocatedMemory += blocksExt.blocks(i).size();
        }
    } else {
        // NB: For erasure case we overestimate preallocated memory size
        // because we cannot predict repair. Memory usage will be adjusted at the end of preload.
        preallocatedMemory = miscExt.compressed_data_size();
    }

    if (mode == EInMemoryMode::Uncompressed &&
        compressionCodecId != NCompression::ECodec::None)
    {
        for (int i = startBlockIndex; i < endBlockIndex; ++i) {
            preallocatedMemory += versionedChunkMeta->DataBlockMeta()->data_blocks(i).uncompressed_size();
        }
    }

    if (memoryTracker) {
        auto freeMemory = memoryTracker->GetFree(EMemoryCategory::TabletStatic);
        if (freeMemory < preallocatedMemory) {
            THROW_ERROR_EXCEPTION("Preload is cancelled due to memory pressure")
                << TErrorAttribute("free_memory", freeMemory)
                << TErrorAttribute("requested_memory", preallocatedMemory);
        }
    }

    auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
        memoryTracker->WithCategory(EMemoryCategory::TabletStatic),
        preallocatedMemory,
        MemoryUsageGranularity);

    std::vector<NChunkClient::TBlock> blocks;
    blocks.reserve(endBlockIndex - startBlockIndex);

    auto preThrottledBytes = GetEstimatedBlockRangeSize(
        enablePreliminaryNetworkThrottling,
        versionedChunkMeta,
        startBlockIndex,
        endBlockIndex - startBlockIndex);

    if (preThrottledBytes) {
        YT_LOG_DEBUG("Preliminary throttling of network bandwidth for preload  (Blocks: %v-%v, Bytes: %v)",
            startBlockIndex,
            endBlockIndex,
            preThrottledBytes);

        WaitFor(networkThrottler->Throttle(*preThrottledBytes))
            .ThrowOnError();
    }

    for (int blockIndex = startBlockIndex; blockIndex < endBlockIndex;) {
        YT_LOG_DEBUG("Started reading chunk blocks (FirstBlock: %v)",
            blockIndex);

        YT_VERIFY(blockIndex < dataBlockCount);
        auto compressedBlocks = WaitFor(reader->ReadBlocks(
            readBlocksOptions,
            blockIndex,
            endBlockIndex - blockIndex))
            .ValueOrThrow();

        int readBlockCount = compressedBlocks.size();
        YT_LOG_DEBUG("Finished reading chunk blocks (Blocks: %v-%v)",
            blockIndex,
            blockIndex + readBlockCount - 1);

        for (const auto& compressedBlock : compressedBlocks) {
            compressedDataSize += compressedBlock.Size();
        }
        readerProfiler->SetCompressedDataSize(compressedDataSize);

        switch (mode) {
            case EInMemoryMode::Compressed: {
                for (const auto& compressedBlock : compressedBlocks) {
                    auto block = TBlock(compressedBlock.Data, compressedBlock.Checksum);
                    blocks.push_back(std::move(block));
                }

                break;
            }

            case EInMemoryMode::Uncompressed: {
                YT_LOG_DEBUG("Decompressing chunk blocks (Blocks: %v-%v, Codec: %v)",
                    blockIndex,
                    blockIndex + readBlockCount - 1,
                    compressionCodec->GetId());

                std::vector<TFuture<std::pair<TSharedRef, TDuration>>> asyncUncompressedBlocks;
                asyncUncompressedBlocks.reserve(compressedBlocks.size());
                for (auto& compressedBlock : compressedBlocks) {
                    asyncUncompressedBlocks.push_back(
                        BIND([&] {
                                NProfiling::TFiberWallTimer timer;
                                auto block = compressionCodec->Decompress(compressedBlock.Data);
                                return std::pair(std::move(block), timer.GetElapsedTime());
                            })
                            .AsyncVia(compressionInvoker)
                            .Run());
                }

                auto results = WaitFor(AllSucceeded(std::move(asyncUncompressedBlocks)))
                    .ValueOrThrow();

                for (auto& [block, duration] : results) {
                    blocks.emplace_back(std::move(block));
                    decompressionTime += duration;
                }

                break;
            }

            default:
                YT_ABORT();
        }

        blockIndex += readBlockCount;
    }

    if (enablePreliminaryNetworkThrottling) {
        auto difference = compressedDataSize - preThrottledBytes.value_or(0);
        YT_LOG_DEBUG("Throttling the difference between received and estimated data size (Estimated: %v, Received %v)",
            preThrottledBytes,
            compressedDataSize);

        WaitFor(networkThrottler->Throttle(std::max(0l, difference)))
            .ThrowOnError();
    }

    TCodecStatistics decompressionStatistics;
    decompressionStatistics.Append(TCodecDuration{compressionCodecId, decompressionTime});
    readerProfiler->SetCodecStatistics(decompressionStatistics);

    i64 allocatedMemory = 0;
    for (auto& block: blocks) {
        allocatedMemory += block.Size();
    }

    if (memoryUsageGuard) {
        memoryUsageGuard.SetSize(0);
    }

    auto chunkData = CreateInMemoryChunkData(
        store->GetChunkId(),
        mode,
        startBlockIndex,
        std::move(blocks),
        versionedChunkMeta,
        tabletSnapshot,
        memoryReferenceTracker,
        memoryTracker->WithCategory(EMemoryCategory::TabletStatic));

    YT_LOG_INFO(
        "Store preload completed (MemoryUsage: %v, PreallocatedMemory: %v, LookupHashTable: %v)",
        allocatedMemory,
        preallocatedMemory,
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
            BIND([=, this, this_ = MakeStrong(this)] (const TInMemoryServiceProxy::TErrorOrRspPingSessionPtr& rspOrError) {
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
        i64 remoteSendBatchSize,
        const TDuration controlRpcTimeout,
        const TDuration heavyRpcTimeout)
        : InMemoryMode_(inMemoryMode)
        , RemoteSendBatchSize_(remoteSendBatchSize)
        , ControlRpcTimeout_(controlRpcTimeout)
        , HeavyRpcTimeout_(heavyRpcTimeout)
        , Nodes_(std::move(nodes))
    { }

    void PutBlock(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data) override
    {
        if (type != GetBlockTypeFromInMemoryMode(InMemoryMode_)) {
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
            batchIsReady = CurrentSize_ > RemoteSendBatchSize_;
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

    TCachedBlock FindBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return TCachedBlock();
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return CreateActiveCachedBlockCookie();
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return GetBlockTypeFromInMemoryMode(InMemoryMode_);
    }

    bool IsBlockTypeActive(EBlockType blockType) const override
    {
        return Any(GetSupportedBlockTypes() & blockType);
    }

    TFuture<void> Finish(const std::vector<TChunkInfo>& chunkInfos) override
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
    const EInMemoryMode InMemoryMode_;
    const i64 RemoteSendBatchSize_;
    const TDuration ControlRpcTimeout_;
    const TDuration HeavyRpcTimeout_;

    std::vector<TNodePtr> Nodes_;

    TFuture<void> ReadyEvent_ = VoidFuture;
    std::atomic<bool> Sending_ = false;
    std::atomic<bool> Dropped_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::deque<std::pair<TBlockId, TSharedRef>> Blocks_;
    i64 CurrentSize_ = 0;


    TFuture<void> SendNextBatch()
    {
        return BIND([this_ = MakeStrong(this), this] () {
            while (DoSendNextBatch())
            { }
        })
        .AsyncVia(GetCurrentInvoker())
        .Run();
    }

    bool DoSendNextBatch()
    {
        auto guard = Guard(SpinLock_);
        if (Blocks_.empty()) {
            return false;
        }

        if (Nodes_.empty()) {
            Dropped_ = true;
            Blocks_.clear();
            CurrentSize_ = 0;
            THROW_ERROR_EXCEPTION("All sessions are dropped");
        }

        std::vector<std::pair<TBlockId, TSharedRef>> blocks;
        i64 groupSize = 0;
        while (!Blocks_.empty() && groupSize < RemoteSendBatchSize_) {
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
            req->SetResponseHeavy(true);
            req->SetTimeout(HeavyRpcTimeout_);
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

        auto future = AllSet(asyncResults)
            .Apply(BIND(&TRemoteInMemoryBlockCache::OnSendResponse, MakeStrong(this)));

        WaitFor(future).
            ThrowOnError();

        return true;
    }

    void OnSendResponse(
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
                YT_UNUSED_FUTURE(node->PingExecutor->Stop());
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
};

class TDummyInMemoryBlockCache
    : public IRemoteInMemoryBlockCache
{
public:
    void PutBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/) override
    { }

    TCachedBlock FindBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return TCachedBlock();
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return CreateActiveCachedBlockCookie();
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::None;
    }

    bool IsBlockTypeActive(EBlockType /*blockType*/) const override
    {
        return false;
    }

    TFuture<void> Finish(const std::vector<TChunkInfo>& /*chunkInfos*/) override
    {
        return VoidFuture;
    }
};

IRemoteInMemoryBlockCachePtr DoCreateRemoteInMemoryBlockCache(
    const NNative::IClientPtr& client,
    const IInvokerPtr& controlInvoker,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    const NHiveClient::TCellDescriptorPtr& cellDescriptor,
    EInMemoryMode inMemoryMode,
    const TInMemoryManagerConfigPtr& config)
{
    std::vector<TNodePtr> nodes;
    for (const auto& target : cellDescriptor->Peers) {
        const auto& address = target.GetDefaultAddress();
        auto channel = address == localDescriptor.GetDefaultAddress()
            ? CreateLocalChannel(localRpcServer)
            : client->GetChannelFactory()->CreateChannel(target);

        YT_LOG_DEBUG("Starting in-memory session (Address: %v)",
            address);

        TInMemoryServiceProxy proxy(channel);

        auto req = proxy.StartSession();
        req->SetTimeout(config->ControlRpcTimeout);
        req->set_in_memory_mode(ToProto<int>(inMemoryMode));

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error starting in-memory session at node %v",
                address)
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        auto sessionId = FromProto<TInMemorySessionId>(rsp->session_id());

        YT_LOG_DEBUG("In-memory session started (Address: %v, SessionId: %v)",
            address,
            sessionId);

        auto node = New<TNode>(
            target,
            sessionId,
            std::move(channel),
            config->ControlRpcTimeout);

        node->PingExecutor = New<TPeriodicExecutor>(
            controlInvoker,
            BIND(&TNode::SendPing, MakeWeak(node)),
            config->PingPeriod);
        node->PingExecutor->Start();

        nodes.push_back(node);
    }

    return New<TRemoteInMemoryBlockCache>(
        std::move(nodes),
        inMemoryMode,
        config->RemoteSendBatchSize,
        config->ControlRpcTimeout,
        config->HeavyRpcTimeout);
}

TFuture<IRemoteInMemoryBlockCachePtr> CreateRemoteInMemoryBlockCache(
    NNative::IClientPtr client,
    IInvokerPtr controlInvoker,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    NHiveClient::TCellDescriptorPtr cellDescriptor,
    EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config)
{
    if (inMemoryMode == EInMemoryMode::None) {
        return MakeFuture<IRemoteInMemoryBlockCachePtr>(New<TDummyInMemoryBlockCache>());
    }

    return BIND(&DoCreateRemoteInMemoryBlockCache)
        .AsyncVia(controlInvoker)
        .Run(
            std::move(client),
            controlInvoker,
            localDescriptor,
            std::move(localRpcServer),
            std::move(cellDescriptor),
            inMemoryMode,
            std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
