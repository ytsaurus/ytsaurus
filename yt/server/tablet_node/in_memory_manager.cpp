#include "stdafx.h"
#include "in_memory_manager.h"
#include "config.h"
#include "chunk_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "private.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/async_semaphore.h>
#include <core/concurrency/rw_spinlock.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/compression/codec.h>

#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <server/misc/memory_usage_tracker.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

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
        , PreloadSemaphore_(Config_->MaxConcurrentPreloads)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TImpl::ScanSlot, MakeStrong(this)));
    }

    IBlockCachePtr CreateInterceptingBlockCache(EInMemoryMode mode)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return New<TInterceptingBlockCache>(this, mode);
    }

    TInterceptedChunkDataPtr EvictInterceptedChunkData(const TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWriterGuard guard(InterceptedDataSpinLock_);

        auto it = ChunkIdToData_.find(chunkId);
        if (it == ChunkIdToData_.end()) {
            return nullptr;
        }

        auto data = it->second;
        ChunkIdToData_.erase(it);

        LOG_INFO("Intercepted chunk data evicted (ChunkId: %v, Mode: %v)",
            chunkId,
            data->InMemoryMode);

        return data;
    }

private:
    const TInMemoryManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TAsyncSemaphore PreloadSemaphore_;

    TReaderWriterSpinLock InterceptedDataSpinLock_;
    yhash_map<TChunkId, TInterceptedChunkDataPtr> ChunkIdToData_;


    void ScanSlot(TTabletSlotPtr slot)
    {
        if (IsMemoryLimitExceeded())
            return;

        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

        auto tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(tablet);
        }
    }

    void ScanTablet(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted)
            return;

        auto storeManager = tablet->GetStoreManager();
        while (true) {
            auto store = storeManager->PeekStoreForPreload();
            if (!store)
                break;
            if (!ScanStore(tablet, store))
                break;
        }
    }

    bool ScanStore(TTablet* tablet, TChunkStorePtr store)
    {
        auto guard = TAsyncSemaphoreGuard::TryAcquire(&PreloadSemaphore_);
        if (!guard) {
            return false;
        }

        auto future =
            BIND(
                &TImpl::PreloadStore,
                MakeStrong(this),
                Passed(std::move(guard)),
                tablet,
                store)
            .AsyncVia(tablet->GetEpochAutomatonInvoker())
            .Run();

        auto storeManager = tablet->GetStoreManager();
        storeManager->BeginStorePreload(store, future);
        return true;
    }

    void PreloadStore(
        TAsyncSemaphoreGuard /*guard*/,
        TTablet* tablet,
        TChunkStorePtr store)
    {
        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("TabletId: %v, StoreId: %v",
            tablet->GetTabletId(),
            store->GetId());

        auto storeManager = tablet->GetStoreManager();

        try {
            GuardedPreloadStore(tablet, store, Logger);
            storeManager->EndStorePreload(store);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error preloading tablet store, backing off");
            storeManager->BackoffStorePreload(store);
        }
    }

    void GuardedPreloadStore(
        TTablet* tablet,
        TChunkStorePtr store,
        const NLogging::TLogger& Logger)
    {
        if (IsMemoryLimitExceeded())
            return;

        auto mode = tablet->GetConfig()->InMemoryMode;
        if (mode == EInMemoryMode::None)
            return;

        EBlockType blockType;
        switch (mode) {
            case EInMemoryMode::Compressed:
                blockType = EBlockType::CompressedData;
                break;
            case EInMemoryMode::Uncompressed:
                blockType = EBlockType::UncompressedData;
                break;
            default:
                YUNREACHABLE();
        }

        auto reader = store->GetChunkReader();

        auto blockCache = store->GetPreloadedBlockCache();
        if (!blockCache)
            return;

        LOG_INFO("Store preload started");

        std::vector<int> extensionTags = {
            TProtoExtensionTag<TMiscExt>::Value,
            TProtoExtensionTag<TBlocksExt>::Value
        };

        auto meta = WaitFor(reader->GetMeta(Null, extensionTags))
            .ValueOrThrow();

        auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
        auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());

        auto codecId = NCompression::ECodec(miscExt.compression_codec());
        auto* codec = NCompression::GetCodec(codecId);

        int startBlockIndex = 0;
        int totalBlockCount = blocksExt.blocks_size();
        while (startBlockIndex < totalBlockCount) {
            LOG_DEBUG("Started reading chunk blocks (FirstBlock: %v)",
                startBlockIndex);

            auto asyncResult = reader->ReadBlocks(startBlockIndex, totalBlockCount - startBlockIndex);
            auto compressedBlocks = WaitFor(asyncResult)
                .ValueOrThrow();

            int readBlockCount = compressedBlocks.size();
            LOG_DEBUG("Finished reading chunk blocks (Blocks: %v-%v)",
                startBlockIndex,
                startBlockIndex + readBlockCount - 1);

            std::vector<TSharedRef> cachedBlocks;
            switch (mode) {
                case EInMemoryMode::Compressed:
                    cachedBlocks = std::move(compressedBlocks);
                    break;

                case EInMemoryMode::Uncompressed: {
                    LOG_DEBUG("Decompressing chunk blocks (Blocks: %v-%v)",
                        startBlockIndex,
                        startBlockIndex + readBlockCount - 1);

                    std::vector<TFuture<TSharedRef>> asyncUncompressedBlocks;
                    for (const auto& compressedBlock : compressedBlocks) {
                        asyncUncompressedBlocks.push_back(
                            BIND([=] () { return codec->Decompress(compressedBlock); })
                                .AsyncVia(NChunkClient::TDispatcher::Get()->GetCompressionPoolInvoker())
                                .Run());
                    }

                    cachedBlocks = WaitFor(Combine(asyncUncompressedBlocks))
                        .ValueOrThrow();
                    break;
                }

                default:
                    YUNREACHABLE();
            }

            for (int index = 0; index < readBlockCount; ++index) {
                auto blockId = TBlockId(reader->GetChunkId(), startBlockIndex + index);
                blockCache->Put(blockId, blockType, cachedBlocks[index], Null);
            }

            startBlockIndex += readBlockCount;
        }

        LOG_INFO("Store preload completed");
    }


    class TInterceptingBlockCache
        : public IBlockCache
    {
    public:
        TInterceptingBlockCache(
            TImplPtr owner,
            EInMemoryMode mode)
            : Owner_(owner)
            , Mode_(mode)
            , BlockType_(InMemoryModeToBlockType(Mode_))
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
            const TSharedRef& block,
            const TNullable<NNodeTrackerClient::TNodeDescriptor>& /*source*/) override
        {
            if (type != BlockType_)
                return;

            if (Owner_->IsMemoryLimitExceeded())
                return;

            TGuard<TSpinLock> guard(SpinLock_);

            auto it = ChunkIds_.find(id.ChunkId);
            TInterceptedChunkDataPtr data;
            if (it == ChunkIds_.end()) {
                data = Owner_->CreateChunkData(id.ChunkId, Mode_);
                YCHECK(ChunkIds_.insert(id.ChunkId).second);
            } else {
                data = Owner_->GetChunkData(id.ChunkId, Mode_);
            }

            if (data->Blocks.size() <= id.BlockIndex) {
                data->Blocks.resize(id.BlockIndex + 1);
            }
            data->Blocks[id.BlockIndex] = block;
        }

        virtual TSharedRef Find(
            const TBlockId& /*id*/,
            EBlockType /*type*/) override
        {
            return TSharedRef();
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
        yhash_set<TChunkId> ChunkIds_;


        static EBlockType InMemoryModeToBlockType(EInMemoryMode mode)
        {
            switch (mode) {
                case EInMemoryMode::Compressed:
                    return EBlockType::CompressedData;

                case EInMemoryMode::Uncompressed:
                    return EBlockType::UncompressedData;

                case EInMemoryMode::None:
                    return EBlockType::None;

                default:
                    YUNREACHABLE();
            }
        }

    };

    TInterceptedChunkDataPtr GetChunkData(const TChunkId& chunkId, EInMemoryMode mode)
    {
        TReaderGuard guard(InterceptedDataSpinLock_);
        auto it = ChunkIdToData_.find(chunkId);
        YCHECK(it != ChunkIdToData_.end());
        auto data = it->second;
        YCHECK(data->InMemoryMode == mode);
        return data;
    }

    TInterceptedChunkDataPtr CreateChunkData(const TChunkId& chunkId, EInMemoryMode mode)
    {
        TWriterGuard guard(InterceptedDataSpinLock_);

        auto data = New<TInterceptedChunkData>();
        data->InMemoryMode = mode;
        YCHECK(ChunkIdToData_.insert(std::make_pair(chunkId, data)).second);

        LOG_INFO("Intercepted chunk data created (ChunkId: %v, Mode: %v)",
            chunkId,
            mode);

        return data;
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

TInMemoryManager::~TInMemoryManager()
{ }

IBlockCachePtr TInMemoryManager::CreateInterceptingBlockCache(EInMemoryMode mode)
{
    return Impl_->CreateInterceptingBlockCache(mode);
}

TInterceptedChunkDataPtr TInMemoryManager::EvictInterceptedChunkData(const TChunkId& chunkId)
{
    return Impl_->EvictInterceptedChunkData(chunkId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
