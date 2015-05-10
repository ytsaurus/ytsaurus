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

#include <core/compression/codec.h>

#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , PreloadSemaphore_(Config_->StorePreloader->MaxConcurrentPreloads)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TImpl::ScanSlot, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TAsyncSemaphore PreloadSemaphore_;


    void ScanSlot(TTabletSlotPtr slot)
    {
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
        auto mode = tablet->GetConfig()->InMemoryMode;
        if (mode == EInMemoryMode::None || mode == EInMemoryMode::Disabled)
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

        int firstBlockIndex = 0;
        while (firstBlockIndex < blocksExt.blocks_size()) {
            i64 size = 0;
            int lastBlockIndex = firstBlockIndex;
            while (lastBlockIndex < blocksExt.blocks_size() && size <= Config_->StorePreloader->WindowSize) {
                size += blocksExt.blocks(lastBlockIndex).size();
                ++lastBlockIndex;
            }

            LOG_DEBUG("Reading chunk blocks (BlockIndexes: %v-%v)",
                firstBlockIndex,
                lastBlockIndex - 1);

            auto asyncResult = reader->ReadBlocks(firstBlockIndex, lastBlockIndex - firstBlockIndex);
            auto compressedBlocks = WaitFor(asyncResult)
                .ValueOrThrow();

            std::vector<TSharedRef> cachedBlocks;

            switch (mode) {
                case EInMemoryMode::Compressed:
                    cachedBlocks = std::move(compressedBlocks);
                    break;

                case EInMemoryMode::Uncompressed: {
                    LOG_DEBUG("Decompressing chunk blocks (BlockIndexes: %v-%v)",
                        firstBlockIndex,
                        lastBlockIndex - 1);

                    std::vector<TFuture<TSharedRef>> asyncUncompressedBlocks;
                    for (const auto& compressedBlock : compressedBlocks) {
                        asyncUncompressedBlocks.push_back(
                            BIND(&NCompression::ICodec::Decompress, codec, compressedBlock)
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

            for (int blockIndex = firstBlockIndex; blockIndex < lastBlockIndex; ++blockIndex) {
                auto blockId = TBlockId(reader->GetChunkId(), blockIndex);
                blockCache->Put(blockId, blockType, cachedBlocks[blockIndex - firstBlockIndex], Null);
            }

            firstBlockIndex = lastBlockIndex;
        }

        LOG_INFO("Store preload completed");
    }
};

////////////////////////////////////////////////////////////////////////////////

TInMemoryManager::TInMemoryManager(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TInMemoryManager::~TInMemoryManager()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
