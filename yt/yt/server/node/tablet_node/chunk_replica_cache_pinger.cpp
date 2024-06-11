#include "chunk_replica_cache_pinger.h"
#include "bootstrap.h"
#include "hunk_chunk.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TSlotChunksPinger
{
public:
    void PingSlotChunks(const ITabletSlotPtr& slot, const IChunkReplicaCachePtr& chunkReplicaCache)
    {
        Logger = TabletNodeLogger().WithTag("CellId: %v", slot->GetCellId());

        YT_LOG_DEBUG("Chunk replica cache pinger scans slot");
        chunkReplicaCache->PingChunks(CollectChunks(slot));
        YT_LOG_DEBUG("Chunk replica cache pinger slot scanning finished");
    }

private:
    TLogger Logger;

    std::vector<TChunkId> CollectChunks(const ITabletSlotPtr& slot)
    {
        const auto& tabletManager = slot->GetTabletManager();
        std::vector<TChunkId> chunkIds;

        {
            TForbidContextSwitchGuard guard;

            for (auto [tabletId, tablet] : tabletManager->Tablets()) {
                ScanTablet(tablet, &chunkIds);
            }
        }

        return chunkIds;
    }

    void ScanTablet(TTablet* tablet, std::vector<TChunkId>* chunkIds)
    {
        for (const auto& [id, store] : tablet->StoreIdMap()) {
            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                chunkIds->push_back(chunkStore->GetChunkId());
            }
        }

        for (const auto& [id, hunkChunk] : tablet->HunkChunkMap()) {
            chunkIds->push_back(hunkChunk->GetId());
        }
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaCachePinger
    : public IChunkReplicaCachePinger
{
public:
    explicit TChunkReplicaCachePinger(
        IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , ChunkReplicaCache_(Bootstrap_->GetClient()->GetNativeConnection()->GetChunkReplicaCache())
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TChunkReplicaCachePinger::OnScanSlot, MakeWeak(this)));
    }

private:
    IBootstrap* const Bootstrap_;
    const IChunkReplicaCachePtr ChunkReplicaCache_;

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != NHydra::EPeerState::Leading) {
            return;
        }

        NDetail::TSlotChunksPinger().PingSlotChunks(slot, ChunkReplicaCache_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaCachePingerPtr CreateChunkReplicaCachePinger(IBootstrap* bootstrap)
{
    return New<TChunkReplicaCachePinger>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
