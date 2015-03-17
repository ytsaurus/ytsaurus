#include "stdafx.h"
#include "store_preloader.h"
#include "config.h"
#include "chunk_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "tablet_slot_manager.h"
#include "private.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/async_semaphore.h>
#include <core/concurrency/delayed_executor.h>

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

class TStorePreloader
    : public TRefCounted
{
public:
    TStorePreloader(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Semaphore_(Config_->StorePreloader->MaxConcurrentPreloads)
    {
        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        tabletSlotManager->SubscribeScanSlot(BIND(&TStorePreloader::ScanSlot, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TAsyncSemaphore Semaphore_;


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

        while (true) {
            auto store = tablet->PeekStoreForPreload();
            if (!store)
                break;
            if (!ScanStore(tablet, store))
                break;
        }
    }

    bool ScanStore(TTablet* tablet, TChunkStorePtr store)
    {
        auto guard = TAsyncSemaphoreGuard::TryAcquire(&Semaphore_);
        if (!guard) {
            return false;
        }

        tablet->PopStoreForPreload(store);
        tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
            &TStorePreloader::PreloadStore,
            MakeStrong(this),
            Passed(std::move(guard)),
            tablet,
            store));
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

        try {
            LOG_INFO("Store preload started");

            const auto& meta = store->GetChunkMeta();
            auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
            auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());

            auto reader = store->GetChunkReader();
            auto blockCache = store->GetUncompressedPreloadedBlockCache();

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

                LOG_DEBUG("Reading chunk blocks (BlockIndexes: %d-%d)",
                    firstBlockIndex,
                    lastBlockIndex - 1);

                auto asyncResult = reader->ReadBlocks(firstBlockIndex, lastBlockIndex - firstBlockIndex);
                auto compressedBlocks = WaitFor(asyncResult)
                    .ValueOrThrow();

                LOG_DEBUG("Decompressing chunk blocks (BlockIndexes: %d-%d)",
                    firstBlockIndex,
                    lastBlockIndex - 1);

                std::vector<TFuture<TSharedRef>> asyncUncompressedBlocks;
                for (const auto& compressedBlock : compressedBlocks) {
                    asyncUncompressedBlocks.push_back(
                        BIND(&NCompression::ICodec::Decompress, codec, compressedBlock)
                            .AsyncVia(NChunkClient::TDispatcher::Get()->GetCompressionPoolInvoker())
                            .Run());
                }

                auto uncompressedBlocks = WaitFor(Combine(asyncUncompressedBlocks))
                    .ValueOrThrow();

                for (int blockIndex = firstBlockIndex; blockIndex < lastBlockIndex; ++blockIndex) {
                    auto blockId = TBlockId(reader->GetChunkId(), blockIndex);
                    blockCache->Put(blockId, uncompressedBlocks[blockIndex - firstBlockIndex], Null);
                }

                firstBlockIndex = lastBlockIndex;
            }

            LOG_INFO("Store preload completed");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error preloading tablet store, backing off");

            if (store->GetPreloadState() == EStorePreloadState::Running) {
                tablet->BackoffStorePreload(store, Config_->TabletManager->ErrorBackoffTime);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void StartStorePreloader(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    New<TStorePreloader>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
